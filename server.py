from fastmcp import FastMCP
from pathlib import Path
from hdfs import InsecureClient
import os
import subprocess
import json
import logging
from datetime import datetime
from typing import Optional, Dict, Any

mcp = FastMCP("File System Server")

HDFS_NAMENODE = os.getenv("HDFS_NAMENODE", "http://localhost:9870")
HDFS_USER = os.getenv("HDFS_USER", "root")
AUDIT_LOG_FILE = os.getenv("AUDIT_LOG_FILE", "hdfs_audit.log")

# Настройка логгера аудита
audit_logger = logging.getLogger("hdfs_audit")
audit_logger.setLevel(logging.INFO)

file_handler = logging.FileHandler(AUDIT_LOG_FILE, encoding="utf-8")
file_handler.setFormatter(logging.Formatter("%(message)s"))
audit_logger.addHandler(file_handler)


def get_hdfs_client():
    return InsecureClient(HDFS_NAMENODE, user=HDFS_USER)


def log_audit(
    operation: str,
    user: str,
    context: Dict[str, Any],
    permission_diff: Optional[Dict[str, Any]] = None,
    status: str = "SUCCESS"
):
    """Записывает операцию в аудит-лог"""
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
    """Получает текущий статус файла/директории для аудита"""
    try:
        client = get_hdfs_client()
        return client.status(path, strict=False)
    except:
        return None


@mcp.tool()
def hdfs_list(path: str) -> str:
    user = HDFS_USER
    context = {"path": path}
    
    try:
        client = get_hdfs_client()
        status = client.status(path, strict=False)

        if status is None:
            log_audit("hdfs_list", user, context, status="NOT_FOUND")
            return f"Путь '{path}' не существует в HDFS."

        if status['type'] == 'FILE':
            size = status.get('length', 0)
            log_audit("hdfs_list", user, context)
            return f"[FILE] {status['pathSuffix']} ({size} bytes)"

        items = client.list(path, status=True)
        result = []
        for name, info in items:
            if info['type'] == 'DIRECTORY':
                result.append(f"[DIR] {name}")
            else:
                size = info.get('length', 0)
                result.append(f"[FILE] {name} ({size} bytes)")

        log_audit("hdfs_list", user, context)
        return "\n".join(result) if result else "Папка пуста."
    except Exception as e:
        log_audit("hdfs_list", user, context, status=f"ERROR: {str(e)}")
        return f"Ошибка HDFS: {str(e)}"


@mcp.tool()
def hdfs_stat(path: str) -> str:
    user = HDFS_USER
    context = {"path": path}
    
    try:
        client = get_hdfs_client()
        status = client.status(path, strict=False)

        if status is None:
            log_audit("hdfs_stat", user, context, status="NOT_FOUND")
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

        log_audit("hdfs_stat", user, context)
        return "\n".join(result)
    except Exception as e:
        log_audit("hdfs_stat", user, context, status=f"ERROR: {str(e)}")
        return f"Ошибка HDFS: {str(e)}"


@mcp.tool()
def hdfs_mkdir(path: str) -> str:
    user = HDFS_USER
    context = {"path": path}
    
    try:
        client = get_hdfs_client()
        client.makedirs(path)
        log_audit("hdfs_mkdir", user, context)
        return f"Директория '{path}' успешно создана в HDFS."
    except Exception as e:
        log_audit("hdfs_mkdir", user, context, status=f"ERROR: {str(e)}")
        return f"Ошибка создания: {str(e)}"


@mcp.tool()
def hdfs_chmod(path: str, permission: str) -> str:
    user = HDFS_USER
    context = {"path": path, "permission": permission}
    
    try:
        client = get_hdfs_client()
        
        before_status = get_hdfs_status(path)
        before_permission = before_status.get('permission', 'UNKNOWN') if before_status else 'UNKNOWN'
        
        client.set_permission(path, permission)
        
        after_status = get_hdfs_status(path)
        after_permission = after_status.get('permission', 'UNKNOWN') if after_status else 'UNKNOWN'
        
        permission_diff = {
            "path": path,
            "before": before_permission,
            "after": after_permission
        }
        
        log_audit("hdfs_chmod", user, context, permission_diff=permission_diff)
        return f"Права доступа изменены: {path} -> {permission}"
    except Exception as e:
        log_audit("hdfs_chmod", user, context, status=f"ERROR: {str(e)}")
        return f"Ошибка изменения прав: {str(e)}"


@mcp.tool()
def hdfs_chown(path: str, owner: str, group: str = None) -> str:
    user = HDFS_USER
    context = {"path": path, "owner": owner, "group": group}
    
    try:
        client = get_hdfs_client()
        
        before_status = get_hdfs_status(path)
        before_owner = before_status.get('owner', 'UNKNOWN') if before_status else 'UNKNOWN'
        before_group = before_status.get('group', 'UNKNOWN') if before_status else 'UNKNOWN'
        
        if group:
            new_owner = f"{owner}:{group}"
        else:
            new_owner = owner
        
        client.set_owner(path, new_owner)
        
        after_status = get_hdfs_status(path)
        after_owner = after_status.get('owner', 'UNKNOWN') if after_status else 'UNKNOWN'
        after_group = after_status.get('group', 'UNKNOWN') if after_status else 'UNKNOWN'
        
        permission_diff = {
            "path": path,
            "owner": {"before": before_owner, "after": after_owner},
            "group": {"before": before_group, "after": after_group}
        }
        
        log_audit("hdfs_chown", user, context, permission_diff=permission_diff)
        
        if group:
            return f"Владелец и группа изменены: {path} -> {owner}:{group}"
        else:
            return f"Владелец изменён: {path} -> {owner}"
    except Exception as e:
        log_audit("hdfs_chown", user, context, status=f"ERROR: {str(e)}")
        return f"Ошибка изменения владельца: {str(e)}"

@mcp.tool()
def hdfs_setquota(
    path: str,
    namespace_quota: int | None = None,
    space_quota: str | None = None
) -> str:
    """Устанавливает квоту на директорию в HDFS."""
    user = HDFS_USER
    context = {
        "path": path,
        "namespace_quota": namespace_quota,
        "space_quota": space_quota
    }
    
    try:
        if namespace_quota is None and space_quota is None:
            log_audit("hdfs_setquota", user, context, status="INVALID_ARGS")
            return "Укажите namespace_quota и/или space_quota."

        # Получаем текущие квоты для аудита (до изменений)
        before_output = run_hdfs_command(["hdfs", "dfs", "-count", "-q", path])
        before_values = before_output.split()[:4] if not before_output.startswith("Ошибка") else ["unknown"] * 4

        # Namespace quota
        if namespace_quota is not None:
            result = run_hdfs_command([
                "hdfs", "dfsadmin",
                "-setQuota", str(namespace_quota),
                path
            ])
            if result.startswith("Ошибка"):
                log_audit("hdfs_setquota", user, context, status=f"ERROR: {result}")
                return result

        # Space quota
        if space_quota is not None:
            result = run_hdfs_command([
                "hdfs", "dfsadmin",
                "-setSpaceQuota", str(space_quota),
                path
            ])
            if result.startswith("Ошибка"):
                log_audit("hdfs_setquota", user, context, status=f"ERROR: {result}")
                return result

        # Получаем новые квоты для аудита (после изменений)
        after_output = run_hdfs_command(["hdfs", "dfs", "-count", "-q", path])
        after_values = after_output.split()[:4] if not after_output.startswith("Ошибка") else ["unknown"] * 4

        permission_diff = {
            "path": path,
            "namespace_quota": {"before": before_values[0], "after": namespace_quota},
            "space_quota": {"before": before_values[2], "after": space_quota}
        }

        log_audit("hdfs_setquota", user, context, permission_diff=permission_diff)

        return (
            f"Квота установлена для {path}: "
            f"namespace={namespace_quota}, space={space_quota}"
        )

    except Exception as e:
        log_audit("hdfs_setquota", user, context, status=f"ERROR: {str(e)}")
        return f"Ошибка выполнения: {str(e)}"


@mcp.tool()
def hdfs_getquota(path: str) -> str:
    """Получает информацию о квотах директории в HDFS."""
    user = HDFS_USER
    context = {"path": path}
    
    try:
        output = run_hdfs_command([
            "hdfs", "dfs",
            "-count", "-q",
            path
        ])

        if output.startswith("Ошибка"):
            log_audit("hdfs_getquota", user, context, status=f"ERROR: {output}")
            return output

        parts = output.split()

        if len(parts) < 8:
            log_audit("hdfs_getquota", user, context, status="INVALID_OUTPUT")
            return f"Неожиданный формат вывода:\n{output}"

        quota = parts[0]
        remaining_quota = parts[1]
        space_quota = parts[2]
        remaining_space = parts[3]
        dir_count = parts[4]
        file_count = parts[5]
        content_size = parts[6]

        def normalize(value):
            return "Not set" if value == "none" else value

        log_audit("hdfs_getquota", user, context)

        return (
            f"Path: {path}\n"
            f"Namespace Quota: {normalize(quota)}\n"
            f"Remaining Namespace: {normalize(remaining_quota)}\n"
            f"Space Quota: {normalize(space_quota)}\n"
            f"Remaining Space: {normalize(remaining_space)}\n"
            f"Directories: {dir_count}\n"
            f"Files: {file_count}\n"
            f"Content Size: {content_size} bytes"
        )

    except Exception as e:
        log_audit("hdfs_getquota", user, context, status=f"ERROR: {str(e)}")
        return f"Ошибка выполнения: {str(e)}"
    
@mcp.tool()
def hdfs_upload(local_path: str, hdfs_path: str) -> str:
    user = HDFS_USER
    context = {"local_path": local_path, "hdfs_path": hdfs_path}
    
    try:
        client = get_hdfs_client()
        if not Path(local_path).exists():
            log_audit("hdfs_upload", user, context, status="NOT_FOUND")
            return f"Ошибка: Локальный файл '{local_path}' не найден."

        client.upload(hdfs_path, local_path, overwrite=True)
        log_audit("hdfs_upload", user, context)
        return f"Файл загружен: {local_path} -> {hdfs_path}"
    except Exception as e:
        log_audit("hdfs_upload", user, context, status=f"ERROR: {str(e)}")
        return f"Ошибка загрузки: {str(e)}"


@mcp.tool()
def hdfs_download(hdfs_path: str, local_path: str) -> str:
    user = HDFS_USER
    context = {"hdfs_path": hdfs_path, "local_path": local_path}
    
    try:
        client = get_hdfs_client()
        client.download(hdfs_path, local_path, overwrite=True)
        log_audit("hdfs_download", user, context)
        return f"Файл скачан: {hdfs_path} -> {local_path}"
    except Exception as e:
        log_audit("hdfs_download", user, context, status=f"ERROR: {str(e)}")
        return f"Ошибка скачивания: {str(e)}"


@mcp.tool()
def hdfs_snapshot_create(path: str, snapshot_name: str) -> str:
    user = HDFS_USER
    context = {"path": path, "snapshot_name": snapshot_name}
    
    try:
        client = get_hdfs_client()
        
        status = client.status(path, strict=False)
        if status is None:
            log_audit("hdfs_snapshot_create", user, context, status="NOT_FOUND")
            return f"Ошибка: Путь '{path}' не существует."
        
        if status['type'] != 'DIRECTORY':
            log_audit("hdfs_snapshot_create", user, context, status="INVALID_PATH")
            return f"Ошибка: Snapshot можно создать только для директории."
        
        client.create_snapshot(path, snapshot_name)
        log_audit("hdfs_snapshot_create", user, context)
        return f"Snapshot '{snapshot_name}' создан для '{path}'"
    except Exception as e:
        log_audit("hdfs_snapshot_create", user, context, status=f"ERROR: {str(e)}")
        return f"Ошибка создания snapshot: {str(e)}"


@mcp.tool()
def hdfs_snapshot_delete(path: str, snapshot_name: str) -> str:
    user = HDFS_USER
    context = {"path": path, "snapshot_name": snapshot_name}
    
    try:
        client = get_hdfs_client()
        
        status = client.status(path, strict=False)
        if status is None:
            log_audit("hdfs_snapshot_delete", user, context, status="NOT_FOUND")
            return f"Ошибка: Путь '{path}' не существует."
        
        client.delete_snapshot(path, snapshot_name)
        log_audit("hdfs_snapshot_delete", user, context)
        return f"Snapshot '{snapshot_name}' удалён из '{path}'"
    except Exception as e:
        log_audit("hdfs_snapshot_delete", user, context, status=f"ERROR: {str(e)}")
        return f"Ошибка удаления snapshot: {str(e)}"


def run_hdfs_command(command):
    try:
        result = subprocess.run(
            ["docker", "exec", "-it", "hdfs-namenode"] + command,
            capture_output=True,
            text=True,
            timeout=300
        )
        return result.stdout.strip() if result.returncode == 0 else f"Ошибка: {result.stderr.strip()}"
    except subprocess.TimeoutExpired:
        return "Ошибка: Превышено время выполнения команды"
    except Exception as e:
        return f"Ошибка выполнения: {str(e)}"


@mcp.tool()
def hdfs_balancer_trigger(threshold: int = 10) -> str:
    user = HDFS_USER
    context = {"threshold": threshold}
    
    try:
        command = ["hdfs", "balancer", "-threshold", str(threshold)]
        output = run_hdfs_command(command)
        
        if "Ошибка" in output:
            log_audit("hdfs_balancer_trigger", user, context, status="ERROR")
            return output
        
        log_audit("hdfs_balancer_trigger", user, context)
        return f"Балансировщик запущен с порогом {threshold}%. {output}"
    except Exception as e:
        log_audit("hdfs_balancer_trigger", user, context, status=f"ERROR: {str(e)}")
        return f"Ошибка запуска балансировщика: {str(e)}"


@mcp.tool()
def hdfs_balancer_status() -> str:
    user = HDFS_USER
    context = {}
    
    try:
        command = ["jps", "-m"]
        output = run_hdfs_command(command)
        
        if "Balancer" in output:
            log_audit("hdfs_balancer_status", user, context, status="BALANCER_RUNNING")
            return "Статус: Балансировщик запущен\n\n" + output
        else:
            log_audit("hdfs_balancer_status", user, context, status="BALANCER_IDLE")
            return "Статус: Балансировщик не активен\n\nДля проверки использования дисков:\n" + run_hdfs_command(["hdfs", "dfsadmin", "-report"])
    except Exception as e:
        log_audit("hdfs_balancer_status", user, context, status=f"ERROR: {str(e)}")
        return f"Ошибка проверки статуса: {str(e)}"


if __name__ == "__main__":
    mcp.run(transport="sse", host="127.0.0.1", port=8000)