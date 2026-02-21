from fastmcp import FastMCP
from pathlib import Path
from hdfs import InsecureClient
import os

mcp = FastMCP("File System Server")

HDFS_NAMENODE = os.getenv("HDFS_NAMENODE", "http://localhost:9870")
HDFS_USER = os.getenv("HDFS_USER", "root")


def get_hdfs_client():
    return InsecureClient(HDFS_NAMENODE, user=HDFS_USER)


@mcp.tool()
def list_directory(path: str) -> str:
    """Возвращает список файлов и папок в указанном пути (локально)."""
    try:
        p = Path(path)
        if not p.exists():
            return f"Ошибка: Путь '{path}' не существует."

        items = []
        for item in p.iterdir():
            item_type = "[DIR]" if item.is_dir() else "[FILE]"
            items.append(f"{item_type} {item.name}")

        return "\n".join(items) if items else "Папка пуста."
    except Exception as e:
        return f"Ошибка доступа: {str(e)}"


@mcp.tool()
def read_file(path: str) -> str:
    """Читает содержимое текстового файла (локально)."""
    try:
        p = Path(path)
        if not p.exists():
            return f"Ошибка: Файл '{path}' не найден."
        if not p.is_file():
            return f"Ошибка: '{path}' не является файлом."

        return p.read_text(encoding="utf-8")
    except Exception as e:
        return f"Ошибка чтения: {str(e)}"


@mcp.tool()
def hdfs_list(path: str) -> str:
    """Список файлов и папок в HDFS по указанному пути."""
    try:
        client = get_hdfs_client()
        status = client.status(path, strict=False)

        if status is None:
            return f"Путь '{path}' не существует в HDFS."

        if status['type'] == 'FILE':
            return f"[FILE] {status['pathSuffix']}"

        items = client.list(path, status=True)
        result = []
        for name, info in items:
            item_type = "[DIR]" if info['type'] == 'DIRECTORY' else "[FILE]"
            result.append(f"{item_type} {name}")

        return "\n".join(result) if result else "Папка пуста."
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
def hdfs_upload(local_path: str, hdfs_path: str) -> str:
    """Загружает локальный файл в HDFS."""
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
    """Скачивает файл из HDFS на локальную машину."""
    try:
        client = get_hdfs_client()
        client.download(hdfs_path, local_path, overwrite=True)
        return f"Файл скачан: {hdfs_path} -> {local_path}"
    except Exception as e:
        return f"Ошибка скачивания: {str(e)}"


if __name__ == "__main__":
    mcp.run(transport="sse", host="127.0.0.1", port=8000)