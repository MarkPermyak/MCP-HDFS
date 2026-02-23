from fastmcp import FastMCP
from pathlib import Path
from hdfs import InsecureClient
import os
import subprocess

mcp = FastMCP("File System Server")

HDFS_NAMENODE = os.getenv("HDFS_NAMENODE", "http://localhost:9870")
HDFS_USER = os.getenv("HDFS_USER", "root")


def get_hdfs_client():
    return InsecureClient(HDFS_NAMENODE, user=HDFS_USER)

@mcp.tool()
def hdfs_list(path: str) -> str:
    """Выводит список файлов и директорий в HDFS по указанному пути."""
    try:
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
    except Exception as e:
        return f"Ошибка HDFS: {str(e)}"


@mcp.tool()
def hdfs_stat(path: str) -> str:
    """Отображает детальную информацию о файле или директории в HDFS."""
    try:
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
    except Exception as e:
        return f"Ошибка HDFS: {str(e)}"
    
@mcp.tool()
def hdfs_mkdir(path: str) -> str:
    """Создаёт директорию в HDFS."""
    try:
        client = get_hdfs_client()
        client.makedirs(path)
        return f"Директория '{path}' успешно создана в HDFS."
    except Exception as e:
        return f"Ошибка создания: {str(e)}"

@mcp.tool()
def hdfs_chmod(path: str, permission: str) -> str:
    """Изменяет права доступа к файлу или директории в HDFS.
    
    Args:
        path: Путь к файлу или директории в HDFS
        permission: Права в формате octal (например, '755', '644') или symbolic (например, 'u+x', 'go-w')
    """
    try:
        client = get_hdfs_client()
        client.set_permission(path, permission)
        return f"Права доступа изменены: {path} -> {permission}"
    except Exception as e:
        return f"Ошибка изменения прав: {str(e)}"


@mcp.tool()
def hdfs_chown(path: str, owner: str, group: str = None) -> str:
    """Изменяет владельца и/или группу файла или директории в HDFS.
    
    Args:
        path: Путь к файлу или директории в HDFS
        owner: Новый владелец (username)
        group: Новая группа (опционально)
    """
    try:
        client = get_hdfs_client()
        if group:
            new_owner = f"{owner}:{group}"
        else:
            new_owner = owner
        
        client.set_owner(path, new_owner)
        
        if group:
            return f"Владелец и группа изменены: {path} -> {owner}:{group}"
        else:
            return f"Владелец изменён: {path} -> {owner}"
    except Exception as e:
        return f"Ошибка изменения владельца: {str(e)}"

# @mcp.tool()
# def hdfs_setquota(
#     path: str,
#     namespace_quota: int | None = None,
#     space_quota: int | None = None
# ) -> str:
#     """
#     Устанавливает квоту на директорию в HDFS.

#     Args:
#         path: Путь к директории в HDFS
#         namespace_quota: Квота на количество объектов (файлы + директории)
#         space_quota: Квота на занимаемое пространство в байтах
#     """
#     try:
#         client = get_hdfs_client()

#         if namespace_quota is None and space_quota is None:
#             return "Необходимо указать хотя бы одну квоту: namespace_quota или space_quota."
        
#         if not path.startswith("/"):
#             path = "/" + path

#         url = f"{HDFS_NAMENODE}/webhdfs/v1{path}"

#         if namespace_quota is not None:
#             client._request(
#                 method="PUT",
#                 url=url,
#                 params={
#                     "op": "SETQUOTA",
#                     "quota": namespace_quota
#                 }
#             )

#         if space_quota is not None:
#             client._request(
#                 method="PUT",
#                 url=url,
#                 params={
#                     "op": "SETSPACEQUOTA",
#                     "spaceQuota": space_quota
#                 }
#             )

#         return (
#             f"Квота установлена для '{path}': "
#             f"namespace={namespace_quota}, space={space_quota}"
#         )
    
#     except Exception as e:
#         return f"Ошибка установки квоты: {str(e)}"


# @mcp.tool()
# def hdfs_getquota(path: str) -> str:
#     """
#     Получает информацию о квотах директории в HDFS.

#     Args:
#         path: Путь к директории в HDFS
#     """
#     try:
#         client = get_hdfs_client()

#         if not path.startswith("/"):
#             path = "/" + path

#         url = f"{HDFS_NAMENODE}/webhdfs/v1{path}"

#         response = client._request(
#             method="GET",
#             url=url,
#             params={"op": "GETCONTENTSUMMARY"}
#         )

#         data = response.json()["ContentSummary"]

#         quota = data.get("quota", -1)
#         space_quota = data.get("spaceQuota", -1)

#         result = [
#             f"Path: {path}",
#             f"Namespace Quota: {quota if quota != -1 else 'Not set'}",
#             f"Space Quota: {space_quota if space_quota != -1 else 'Not set'}",
#             f"Space Consumed: {data.get('spaceConsumed', 0)} bytes",
#             f"File Count: {data.get('fileCount', 0)}",
#             f"Directory Count: {data.get('directoryCount', 0)}",
#         ]

#         return "\n".join(result)

#     except Exception as e:
#         return f"Ошибка получения квоты: {str(e)}"

@mcp.tool()
def hdfs_upload(local_path: str, hdfs_path: str) -> str:
    """Загружает локальный файл в HDFS.

    Args:
        local_path: Путь файлу в локальной директории
        hdfs_path: Путь файла на hdfs
    """
    try:
        client = get_hdfs_client()
        if not Path(local_path).exists():
            return f"Ошибка: Локальный файл '{local_path}' не найден."

        client.upload(hdfs_path, local_path, overwrite=True)
        return f"Файл загружен: {local_path} -> {hdfs_path}"
    except Exception as e:
        return f"Ошибка загрузки: {str(e)}"

@mcp.tool()
def hdfs_download(hdfs_path: str, local_path: str) -> str:
    """Скачивает файл из HDFS на локальную машину.

    Args:
        hdfs_path: Путь файла на hdfs
        local_path: Путь файлу в локальной директории
    """
    try:
        client = get_hdfs_client()
        client.download(hdfs_path, local_path, overwrite=True)
        return f"Файл скачан: {hdfs_path} -> {local_path}"
    except Exception as e:
        return f"Ошибка скачивания: {str(e)}"

@mcp.tool()
def hdfs_snapshot_create(path: str, snapshot_name: str) -> str:
    """Создаёт snapshot директории в HDFS.
    
    Args:
        path: Путь к директории в HDFS (должна быть snapshot-enabled)
        snapshot_name: Имя snapshot
    """
    try:
        client = get_hdfs_client()
        
        status = client.status(path, strict=False)
        if status is None:
            return f"Ошибка: Путь '{path}' не существует."
        
        if status['type'] != 'DIRECTORY':
            return f"Ошибка: Snapshot можно создать только для директории."
        
        client.create_snapshot(path, snapshot_name)
        return f"Snapshot '{snapshot_name}' создан для '{path}'"
    except Exception as e:
        return f"Ошибка создания snapshot: {str(e)}"


@mcp.tool()
def hdfs_snapshot_delete(path: str, snapshot_name: str) -> str:
    """Удаляет snapshot директории в HDFS.
    
    Args:
        path: Путь к директории в HDFS
        snapshot_name: Имя snapshot для удаления
    """
    try:
        client = get_hdfs_client()
        
        status = client.status(path, strict=False)
        if status is None:
            return f"Ошибка: Путь '{path}' не существует."
        
        client.delete_snapshot(path, snapshot_name)
        return f"Snapshot '{snapshot_name}' удалён из '{path}'"
    except Exception as e:
        return f"Ошибка удаления snapshot: {str(e)}"

if __name__ == "__main__":
    mcp.run(transport="sse", host="127.0.0.1", port=8000)