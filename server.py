from fastmcp import FastMCP
from pathlib import Path
from hdfs import InsecureClient
import os
import subprocess
import json
import logging
import time
import functools
from datetime import datetime
from typing import Optional, Dict, Any, Callable

mcp = FastMCP("File System Server")

HDFS_NAMENODE = os.getenv("HDFS_NAMENODE", "http://localhost:9870")
HDFS_USER = os.getenv("HDFS_USER", "root")
AUDIT_LOG_FILE = os.getenv("AUDIT_LOG_FILE", "hdfs_audit.log")
SERVER_LOG_FILE = os.getenv("SERVER_LOG_FILE", "hdfs_server.log")

RETRY_MAX_ATTEMPTS = int(os.getenv("RETRY_MAX_ATTEMPTS", "3"))
RETRY_BASE_DELAY = float(os.getenv("RETRY_BASE_DELAY", "1.0"))   
RETRY_MAX_DELAY = float(os.getenv("RETRY_MAX_DELAY", "30.0"))    
RETRY_BACKOFF_FACTOR = float(os.getenv("RETRY_BACKOFF_FACTOR", "2.0"))
HDFS_CALL_TIMEOUT = int(os.getenv("HDFS_CALL_TIMEOUT", "30"))    
HDFS_CMD_TIMEOUT = int(os.getenv("HDFS_CMD_TIMEOUT", "300"))     

audit_logger = logging.getLogger("hdfs_audit")
audit_logger.setLevel(logging.INFO)
audit_logger.propagate = False

_audit_handler = logging.FileHandler(AUDIT_LOG_FILE, encoding="utf-8")
_audit_handler.setFormatter(logging.Formatter("%(message)s"))
audit_logger.addHandler(_audit_handler)

logger = logging.getLogger("hdfs_server")
logger.setLevel(logging.INFO)
logger.propagate = False

_server_handler = logging.FileHandler(SERVER_LOG_FILE, encoding="utf-8")
_server_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
logger.addHandler(_server_handler)


class RetryExhaustedError(Exception):
    """Все попытки retry исчерпаны."""
    pass


class DeadlineExceededError(Exception):
    """Превышен общий дедлайн операции."""
    pass


def retry_with_backoff(
    max_attempts: int = RETRY_MAX_ATTEMPTS,
    base_delay: float = RETRY_BASE_DELAY,
    max_delay: float = RETRY_MAX_DELAY,
    backoff_factor: float = RETRY_BACKOFF_FACTOR,
    deadline: Optional[float] = None,           
    retriable_exceptions: tuple = (Exception,),
    operation_name: Optional[str] = None,        
):
    """
    Декоратор для синхронных функций.
    Повторяет вызов при исключениях с экспоненциальной задержкой.

    Args:
        max_attempts:          максимум попыток (включая первую)
        base_delay:            начальная задержка в секундах
        max_delay:             максимальная задержка между попытками
        backoff_factor:        множитель задержки (delay *= backoff_factor^attempt)
        deadline:              абсолютный monotonic timestamp крайнего срока (None = без дедлайна)
        retriable_exceptions:  кортеж типов исключений, при которых делать retry
        operation_name:        имя операции для логов (если None — берётся func.__name__)
    """
    def decorator(func: Callable):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            name = operation_name or func.__name__
            last_exc = None
            for attempt in range(1, max_attempts + 1):
                if deadline is not None and time.monotonic() >= deadline:
                    raise DeadlineExceededError(
                        f"Дедлайн превышен до выполнения попытки {attempt} для '{name}'"
                    )

                try:
                    return func(*args, **kwargs)
                except retriable_exceptions as exc:
                    last_exc = exc
                    if attempt == max_attempts:
                        break

                    delay = min(base_delay * (backoff_factor ** (attempt - 1)), max_delay)

                    if deadline is not None:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            raise DeadlineExceededError(
                                f"Дедлайн превышен после попытки {attempt} для '{name}'"
                            ) from exc
                        delay = min(delay, remaining)

                    logger.warning(
                        f"[retry] '{name}' попытка {attempt}/{max_attempts} "
                        f"завершилась ошибкой: {exc}. "
                        f"Следующая попытка через {delay:.2f}s"
                    )
                    time.sleep(delay)

            raise RetryExhaustedError(
                f"'{name}' не выполнена за {max_attempts} попыток. "
                f"Последняя ошибка: {last_exc}"
            ) from last_exc

        return wrapper
    return decorator


def with_timeout(timeout: float):
    """
    Декоратор-обёртка для синхронных функций: выполняет их в отдельном потоке
    через concurrent.futures и бросает TimeoutError если превышен timeout.
    """
    import concurrent.futures

    def decorator(func: Callable):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(func, *args, **kwargs)
                try:
                    return future.result(timeout=timeout)
                except concurrent.futures.TimeoutError:
                    raise TimeoutError(
                        f"Функция '{func.__name__}' превысила таймаут {timeout}s"
                    )
        return wrapper
    return decorator


def get_hdfs_client():
    return InsecureClient(HDFS_NAMENODE, user=HDFS_USER)


def log_audit(
    operation: str,
    user: str,
    context: Dict[str, Any],
    permission_diff: Optional[Dict[str, Any]] = None,
    status: str = "SUCCESS"
):
    """Записывает операцию в аудит-лог."""
    log_entry = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "operation": operation,
        "user": user,
        "context": context,
        "permission_diff": permission_diff,
        "status": status
    }
    audit_logger.info(json.dumps(log_entry, ensure_ascii=False))


def get_hdfs_status(path: str) -> Optional[Dict[str, Any]]:
    """Получает текущий статус файла/директории для аудита (с retry)."""
    @retry_with_backoff(max_attempts=RETRY_MAX_ATTEMPTS)
    @with_timeout(HDFS_CALL_TIMEOUT)
    def _get():
        client = get_hdfs_client()
        return client.status(path, strict=False)

    try:
        return _get()
    except Exception:
        return None


def run_hdfs_command(command):
    """Выполняет shell-команду внутри Docker-контейнера namenode."""
    try:
        result = subprocess.run(
            ["docker", "exec", "-it", "hdfs-namenode"] + command,
            capture_output=True,
            text=True,
            timeout=HDFS_CMD_TIMEOUT
        )
        return result.stdout.strip() if result.returncode == 0 else f"Ошибка: {result.stderr.strip()}"
    except subprocess.TimeoutExpired:
        return f"Ошибка: Превышено время выполнения команды ({HDFS_CMD_TIMEOUT}s)"
    except Exception as e:
        return f"Ошибка выполнения: {str(e)}"


def hdfs_operation(
    operation_name: str,
    func: Callable,
    user: str,
    context: Dict[str, Any],
    permission_diff_fn: Optional[Callable] = None,
    deadline_seconds: Optional[float] = None,
):
    """
    Выполняет HDFS-операцию с retry/backoff/timeout/deadline.

    Args:
        operation_name:    имя операции для аудита
        func:              функция, выполняющая саму операцию (без аргументов)
        user:              текущий пользователь
        context:           контекст для аудита
        permission_diff_fn: опционально — callable, возвращающий permission_diff
        deadline_seconds:  дедлайн в секундах с момента вызова
    """
    deadline_ts = (time.monotonic() + deadline_seconds) if deadline_seconds else None

    @retry_with_backoff(
        max_attempts=RETRY_MAX_ATTEMPTS,
        deadline=deadline_ts,
        operation_name=operation_name,
    )
    @with_timeout(HDFS_CALL_TIMEOUT)
    def _execute():
        return func()

    try:
        result = _execute()
        perm_diff = permission_diff_fn() if permission_diff_fn else None
        log_audit(operation_name, user, context, permission_diff=perm_diff)
        return result
    except DeadlineExceededError as e:
        log_audit(operation_name, user, context, status=f"DEADLINE_EXCEEDED: {e}")
        return f"Ошибка: дедлайн операции превышен. {e}"
    except RetryExhaustedError as e:
        log_audit(operation_name, user, context, status=f"RETRY_EXHAUSTED: {e}")
        return f"Ошибка: все попытки исчерпаны. {e}"
    except TimeoutError as e:
        log_audit(operation_name, user, context, status=f"TIMEOUT: {e}")
        return f"Ошибка: таймаут операции. {e}"
    except Exception as e:
        log_audit(operation_name, user, context, status=f"ERROR: {str(e)}")
        return f"Ошибка HDFS: {str(e)}"


@mcp.tool()
def hdfs_list(path: str) -> str:
    user = HDFS_USER
    context = {"path": path}

    def _op():
        client = get_hdfs_client()
        status = client.status(path, strict=False)
        if status is None:
            return f"Путь '{path}' не существует в HDFS."
        if status['type'] == 'FILE':
            size = status.get('length', 0)
            return f"[FILE] {status['pathSuffix']} ({size} bytes)"
        items = client.list(path, status=True)
        result = []
        for name, info in items:
            if info['type'] == 'DIRECTORY':
                result.append(f"[DIR] {name}")
            else:
                size = info.get('length', 0)
                result.append(f"[FILE] {name} ({size} bytes)")
        return "\n".join(result) if result else "Папка пуста."

    return hdfs_operation("hdfs_list", _op, user, context, deadline_seconds=60)


@mcp.tool()
def hdfs_stat(path: str) -> str:
    user = HDFS_USER
    context = {"path": path}

    def _op():
        client = get_hdfs_client()
        status = client.status(path, strict=False)
        if status is None:
            return f"Путь '{path}' не существует в HDFS."
        result = [
            f"Path: {status.get('pathSuffix', path)}",
            f"Type: {status.get('type', 'UNKNOWN')}",
            f"Size: {status.get('length', 0)} bytes",
            f"Replication: {status.get('block_replication', 'N/A')}",
            f"Block Size: {status.get('blocksize', 'N/A')} bytes",
            f"Owner: {status.get('owner', 'N/A')}",
            f"Group: {status.get('group', 'N/A')}",
            f"Permission: {status.get('permission', 'N/A')}",
            f"Access Time: {status.get('accessTime', 'N/A')}",
            f"Modification Time: {status.get('modificationTime', 'N/A')}",
        ]
        return "\n".join(result)

    return hdfs_operation("hdfs_stat", _op, user, context, deadline_seconds=30)


@mcp.tool()
def hdfs_mkdir(path: str) -> str:
    user = HDFS_USER
    context = {"path": path}

    def _op():
        client = get_hdfs_client()
        client.makedirs(path)
        return f"Директория '{path}' успешно создана в HDFS."

    return hdfs_operation("hdfs_mkdir", _op, user, context, deadline_seconds=60)


@mcp.tool()
def hdfs_chmod(path: str, permission: str) -> str:
    user = HDFS_USER
    context = {"path": path, "permission": permission}

    before_status = get_hdfs_status(path)
    before_permission = before_status.get('permission', 'UNKNOWN') if before_status else 'UNKNOWN'

    def _op():
        client = get_hdfs_client()
        client.set_permission(path, permission)
        return f"Права доступа изменены: {path} -> {permission}"

    def _diff():
        after_status = get_hdfs_status(path)
        after_permission = after_status.get('permission', 'UNKNOWN') if after_status else 'UNKNOWN'
        return {"path": path, "before": before_permission, "after": after_permission}

    return hdfs_operation("hdfs_chmod", _op, user, context, permission_diff_fn=_diff, deadline_seconds=60)


@mcp.tool()
def hdfs_chown(path: str, owner: str, group: str = None) -> str:
    user = HDFS_USER
    context = {"path": path, "owner": owner, "group": group}

    before_status = get_hdfs_status(path)
    before_owner = before_status.get('owner', 'UNKNOWN') if before_status else 'UNKNOWN'
    before_group = before_status.get('group', 'UNKNOWN') if before_status else 'UNKNOWN'

    def _op():
        client = get_hdfs_client()
        new_owner = f"{owner}:{group}" if group else owner
        client.set_owner(path, new_owner)
        if group:
            return f"Владелец и группа изменены: {path} -> {owner}:{group}"
        return f"Владелец изменён: {path} -> {owner}"

    def _diff():
        after_status = get_hdfs_status(path)
        after_owner = after_status.get('owner', 'UNKNOWN') if after_status else 'UNKNOWN'
        after_group = after_status.get('group', 'UNKNOWN') if after_status else 'UNKNOWN'
        return {
            "path": path,
            "owner": {"before": before_owner, "after": after_owner},
            "group": {"before": before_group, "after": after_group},
        }

    return hdfs_operation("hdfs_chown", _op, user, context, permission_diff_fn=_diff, deadline_seconds=60)


@mcp.tool()
def hdfs_setquota(
    path: str,
    namespace_quota: int | None = None,
    space_quota: str | None = None
) -> str:
    """Устанавливает квоту на директорию в HDFS."""
    user = HDFS_USER
    context = {"path": path, "namespace_quota": namespace_quota, "space_quota": space_quota}

    if namespace_quota is None and space_quota is None:
        log_audit("hdfs_setquota", user, context, status="INVALID_ARGS")
        return "Укажите namespace_quota и/или space_quota."

    def _op():
        before_output = run_hdfs_command(["hdfs", "dfs", "-count", "-q", path])
        before_values = before_output.split()[:4] if not before_output.startswith("Ошибка") else ["unknown"] * 4

        if namespace_quota is not None:
            result = run_hdfs_command(["hdfs", "dfsadmin", "-setQuota", str(namespace_quota), path])
            if result.startswith("Ошибка"):
                raise RuntimeError(result)

        if space_quota is not None:
            result = run_hdfs_command(["hdfs", "dfsadmin", "-setSpaceQuota", str(space_quota), path])
            if result.startswith("Ошибка"):
                raise RuntimeError(result)

        after_output = run_hdfs_command(["hdfs", "dfs", "-count", "-q", path])
        after_values = after_output.split()[:4] if not after_output.startswith("Ошибка") else ["unknown"] * 4

        return {
            "message": f"Квота установлена для {path}: namespace={namespace_quota}, space={space_quota}",
            "permission_diff": {
                "path": path,
                "namespace_quota": {"before": before_values[0], "after": namespace_quota},
                "space_quota": {"before": before_values[2], "after": space_quota},
            }
        }

    try:
        deadline_ts = time.monotonic() + 120

        @retry_with_backoff(
            max_attempts=RETRY_MAX_ATTEMPTS,
            deadline=deadline_ts,
            operation_name="hdfs_setquota",
        )
        def _execute():
            return _op()

        outcome = _execute()
        log_audit("hdfs_setquota", user, context, permission_diff=outcome["permission_diff"])
        return outcome["message"]
    except DeadlineExceededError as e:
        log_audit("hdfs_setquota", user, context, status=f"DEADLINE_EXCEEDED")
        return f"Ошибка: дедлайн превышен. {e}"
    except RetryExhaustedError as e:
        log_audit("hdfs_setquota", user, context, status=f"RETRY_EXHAUSTED")
        return f"Ошибка: все попытки исчерпаны. {e}"
    except Exception as e:
        log_audit("hdfs_setquota", user, context, status=f"ERROR: {str(e)}")
        return f"Ошибка выполнения: {str(e)}"


@mcp.tool()
def hdfs_getquota(path: str) -> str:
    """Получает информацию о квотах директории в HDFS."""
    user = HDFS_USER
    context = {"path": path}

    def _op():
        output = run_hdfs_command(["hdfs", "dfs", "-count", "-q", path])
        if output.startswith("Ошибка"):
            raise RuntimeError(output)
        parts = output.split()
        if len(parts) < 8:
            raise RuntimeError(f"Неожиданный формат вывода:\n{output}")

        def normalize(value):
            return "Not set" if value == "none" else value

        return (
            f"Path: {path}\n"
            f"Namespace Quota: {normalize(parts[0])}\n"
            f"Remaining Namespace: {normalize(parts[1])}\n"
            f"Space Quota: {normalize(parts[2])}\n"
            f"Remaining Space: {normalize(parts[3])}\n"
            f"Directories: {parts[4]}\n"
            f"Files: {parts[5]}\n"
            f"Content Size: {parts[6]} bytes"
        )

    return hdfs_operation("hdfs_getquota", _op, user, context, deadline_seconds=60)


@mcp.tool()
def hdfs_upload(local_path: str, hdfs_path: str) -> str:
    user = HDFS_USER
    context = {"local_path": local_path, "hdfs_path": hdfs_path}

    if not Path(local_path).exists():
        log_audit("hdfs_upload", user, context, status="NOT_FOUND")
        return f"Ошибка: Локальный файл '{local_path}' не найден."

    def _op():
        client = get_hdfs_client()
        client.upload(hdfs_path, local_path, overwrite=True)
        return f"Файл загружен: {local_path} -> {hdfs_path}"

    return hdfs_operation("hdfs_upload", _op, user, context, deadline_seconds=300)


@mcp.tool()
def hdfs_download(hdfs_path: str, local_path: str) -> str:
    user = HDFS_USER
    context = {"hdfs_path": hdfs_path, "local_path": local_path}

    def _op():
        client = get_hdfs_client()
        client.download(hdfs_path, local_path, overwrite=True)
        return f"Файл скачан: {hdfs_path} -> {local_path}"

    return hdfs_operation("hdfs_download", _op, user, context, deadline_seconds=300)


@mcp.tool()
def hdfs_snapshot_create(path: str, snapshot_name: str) -> str:
    user = HDFS_USER
    context = {"path": path, "snapshot_name": snapshot_name}

    def _op():
        client = get_hdfs_client()
        status = client.status(path, strict=False)
        if status is None:
            raise ValueError(f"Путь '{path}' не существует.")
        if status['type'] != 'DIRECTORY':
            raise ValueError("Snapshot можно создать только для директории.")
        client.create_snapshot(path, snapshot_name)
        return f"Snapshot '{snapshot_name}' создан для '{path}'"

    return hdfs_operation("hdfs_snapshot_create", _op, user, context, deadline_seconds=60)


@mcp.tool()
def hdfs_snapshot_delete(path: str, snapshot_name: str) -> str:
    user = HDFS_USER
    context = {"path": path, "snapshot_name": snapshot_name}

    def _op():
        client = get_hdfs_client()
        status = client.status(path, strict=False)
        if status is None:
            raise ValueError(f"Путь '{path}' не существует.")
        client.delete_snapshot(path, snapshot_name)
        return f"Snapshot '{snapshot_name}' удалён из '{path}'"

    return hdfs_operation("hdfs_snapshot_delete", _op, user, context, deadline_seconds=60)


@mcp.tool()
def hdfs_balancer_trigger(threshold: int = 10) -> str:
    user = HDFS_USER
    context = {"threshold": threshold}

    def _op():
        output = run_hdfs_command(["hdfs", "balancer", "-threshold", str(threshold)])
        if output.startswith("Ошибка"):
            raise RuntimeError(output)
        return f"Балансировщик запущен с порогом {threshold}%. {output}"

    return hdfs_operation("hdfs_balancer_trigger", _op, user, context, deadline_seconds=600)


@mcp.tool()
def hdfs_balancer_status() -> str:
    user = HDFS_USER
    context = {}

    def _op():
        output = run_hdfs_command(["jps", "-m"])
        if "Balancer" in output:
            return "Статус: Балансировщик запущен\n\n" + output
        return (
            "Статус: Балансировщик не активен\n\nДля проверки использования дисков:\n"
            + run_hdfs_command(["hdfs", "dfsadmin", "-report"])
        )

    return hdfs_operation("hdfs_balancer_status", _op, user, context, deadline_seconds=60)


if __name__ == "__main__":
    mcp.run(transport="sse", host="127.0.0.1", port=8000)