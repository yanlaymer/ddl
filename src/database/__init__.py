# config/__init__.py

from .config import DB_CONFIG

# Функция для получения конфигурации базы данных
def get_db_config():
    return DB_CONFIG
