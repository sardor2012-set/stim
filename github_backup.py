"""
GitHub Database Backup Module
Автоматическое сохранение базы данных SQLite в GitHub репозиторий через API
"""

import os
import time
import json
import logging
import threading
from datetime import datetime
from pathlib import Path

# Загрузка переменных окружения из .env файла
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # Если dotenv не установлен, используем системные переменные

import requests

# Настройка логирования
logger = logging.getLogger(__name__)

# Конфигурация GitHub (можно изменить через переменные окружения)
GITHUB_TOKEN = os.environ.get('GITHUB_TOKEN', '')
GITHUB_REPO = os.environ.get('GITHUB_REPO', '')  # Формат: "owner/repo"
GITHUB_BRANCH = os.environ.get('GITHUB_BRANCH', 'main')
DB_PATH = os.environ.get('DB_PATH', 'bot.db')
BACKUP_PATH = os.environ.get('BACKUP_PATH', 'backups')  # Папка для бэкапов в репозитории
AUTO_SAVE_INTERVAL = int(os.environ.get('AUTO_SAVE_INTERVAL', 300))  # Интервал автосохранения в секундах (по умолчанию 5 минут)

# Глобальные переменные для отслеживания состояния
_last_save_time = None
_auto_save_thread = None
_stop_auto_save = threading.Event()


def configure(github_token: str, github_repo: str, github_branch: str = 'main', 
              db_path: str = 'bot.db', backup_path: str = 'backups',
              auto_save_interval: int = 300):
    """
    Настройка конфигурации GitHub бэкапа
    
    Args:
        github_token: GitHub Personal Access Token
        github_repo: Репозиторий в формате "owner/repo"
        github_branch: Ветка репозитория (по умолчанию main)
        db_path: Путь к файлу базы данных
        backup_path: Папка для бэкапов в репозитории
        auto_save_interval: Интервал автосохранения в секундах
    """
    global GITHUB_TOKEN, GITHUB_REPO, GITHUB_BRANCH, DB_PATH, BACKUP_PATH, AUTO_SAVE_INTERVAL
    
    GITHUB_TOKEN = github_token
    GITHUB_REPO = github_repo
    GITHUB_BRANCH = github_branch
    DB_PATH = db_path
    BACKUP_PATH = backup_path
    AUTO_SAVE_INTERVAL = auto_save_interval
    
    logger.info(f"GitHub backup configured: repo={github_repo}, branch={github_branch}, db={db_path}")


def _get_headers():
    """Получение заголовков для GitHub API"""
    return {
        'Authorization': f'token {GITHUB_TOKEN}',
        'Accept': 'application/vnd.github.v3+json',
        'Content-Type': 'application/json'
    }


def _get_file_sha(repo: str, file_path: str) -> str | None:
    """Получение SHA существующего файла для обновления"""
    url = f"https://api.github.com/repos/{repo}/contents/{file_path}"
    params = {'ref': GITHUB_BRANCH}
    
    try:
        response = requests.get(url, headers=_get_headers(), params=params)
        if response.status_code == 200:
            data = response.json()
            return data.get('sha')
        elif response.status_code == 404:
            return None
        else:
            logger.warning(f"Failed to get file SHA: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        logger.error(f"Error getting file SHA: {e}")
        return None


def push_db(message: str = "Auto backup", use_thread: bool = False) -> bool:
    """
    Загрузка базы данных в GitHub репозиторий
    
    Args:
        message: Commit message
        use_thread: Если True, выполняет загрузку в отдельном потоке
    
    Returns:
        True если успешно, False в противном случае
    """
    if use_thread:
        thread = threading.Thread(target=push_db, args=(message, False))
        thread.daemon = True
        thread.start()
        return True
    
    global _last_save_time
    
    # Проверка конфигурации
    if not GITHUB_TOKEN or not GITHUB_REPO:
        logger.warning("GitHub token or repo not configured. Skipping backup.")
        return False
    
    # Проверка существования файла базы данных
    db_file = Path(DB_PATH)
    if not db_file.exists():
        logger.warning(f"Database file {DB_PATH} not found. Skipping backup.")
        return False
    
    try:
        # Чтение файла базы данных
        with open(db_file, 'rb') as f:
            db_content = f.read()
        
        # Также архивируем WAL и SHM файлы если они есть
        wal_file = Path(f"{DB_PATH}-wal")
        shm_file = Path(f"{DB_PATH}-shm")
        
        # Определяем имя файла с timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_filename = f"bot_{timestamp}.db"
        
        # Путь к файлу в репозитории
        file_path = f"{BACKUP_PATH}/{backup_filename}"
        
        # Получаем SHA если файл существует
        file_sha = _get_file_sha(GITHUB_REPO, file_path)
        
        # Подготовка данных для GitHub API
        import base64
        content_base64 = base64.b64encode(db_content).decode('utf-8')
        
        url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{file_path}"
        data = {
            'message': message,
            'content': content_base64,
            'branch': GITHUB_BRANCH
        }
        
        # Если файл существует, добавляем SHA для обновления
        if file_sha:
            data['sha'] = file_sha
        
        # Отправка файла
        response = requests.put(url, headers=_get_headers(), json=data)
        
        if response.status_code in [200, 201]:
            _last_save_time = datetime.now()
            logger.info(f"✅ Database successfully backed up to GitHub: {backup_filename}")
            
            # Также обновляем latest.db как символическую ссылку на последний бэкап
            _update_latest_backup(db_content)
            
            return True
        else:
            logger.error(f"❌ Failed to backup database: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error during database backup: {e}")
        return False


def _update_latest_backup(db_content: bytes) -> bool:
    """Обновление файла latest.db с последней версией базы данных"""
    try:
        import base64
        
        latest_path = f"{BACKUP_PATH}/latest.db"
        file_sha = _get_file_sha(GITHUB_REPO, latest_path)
        
        url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{latest_path}"
        data = {
            'message': f"Update latest backup",
            'content': base64.b64encode(db_content).decode('utf-8'),
            'branch': GITHUB_BRANCH
        }
        
        if file_sha:
            data['sha'] = file_sha
        
        response = requests.put(url, headers=_get_headers(), json=data)
        return response.status_code in [200, 201]
    except Exception as e:
        logger.error(f"Error updating latest backup: {e}")
        return False


def start_auto_save():
    """Запуск автоматического сохранения базы данных в отдельном потоке"""
    global _auto_save_thread, _stop_auto_save
    
    if _auto_save_thread and _auto_save_thread.is_alive():
        logger.warning("Auto save is already running")
        return False
    
    if not GITHUB_TOKEN or not GITHUB_REPO:
        logger.warning("GitHub not configured. Auto save disabled.")
        return False
    
    _stop_auto_save.clear()
    _auto_save_thread = threading.Thread(target=_auto_save_loop, daemon=True)
    _auto_save_thread.start()
    logger.info(f"Auto save started with interval {AUTO_SAVE_INTERVAL} seconds")
    return True


def stop_auto_save():
    """Остановка автоматического сохранения"""
    global _stop_auto_save
    
    if _auto_save_thread and _auto_save_thread.is_alive():
        _stop_auto_save.set()
        _auto_save_thread.join(timeout=5)
        logger.info("Auto save stopped")
        return True
    return False


def _auto_save_loop():
    """Основной цикл автоматического сохранения"""
    while not _stop_auto_save.is_set():
        time.sleep(AUTO_SAVE_INTERVAL)
        
        if not _stop_auto_save.is_set():
            push_db(message=f"Auto backup - {datetime.now().strftime('%Y-%m-%d %H:%M')}")


def get_last_save_time() -> datetime | None:
    """Получение времени последнего сохранения"""
    return _last_save_time


def restore_from_github(backup_filename: str = None, local_path: str = None) -> bool:
    """
    Восстановление базы данных из GitHub
    
    Args:
        backup_filename: Имя файла бэкапа (если None - скачивается latest.db)
        local_path: Локальный путь для сохранения (по умолчанию - перезаписывает bot.db)
    
    Returns:
        True если успешно
    """
    if not GITHUB_TOKEN or not GITHUB_REPO:
        logger.warning("GitHub token or repo not configured")
        return False
    
    try:
        import base64
        
        file_path = f"{BACKUP_PATH}/{backup_filename}" if backup_filename else f"{BACKUP_PATH}/latest.db"
        url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{file_path}"
        params = {'ref': GITHUB_BRANCH}
        
        response = requests.get(url, headers=_get_headers(), params=params)
        
        if response.status_code == 200:
            data = response.json()
            content = base64.b64decode(data['content'])
            
            save_path = local_path or DB_PATH
            with open(save_path, 'wb') as f:
                f.write(content)
            
            logger.info(f"✅ Database restored from GitHub: {file_path} -> {save_path}")
            return True
        else:
            logger.error(f"Failed to restore: {response.status_code}")
            return False
            
    except Exception as e:
        logger.error(f"Error restoring database: {e}")
        return False


# Функция для быстрого вызова после commit
def auto_push():
    """Быстрая функция для автоматического пуша без ожидания результата"""
    return push_db(message="Auto push after commit", use_thread=True)
