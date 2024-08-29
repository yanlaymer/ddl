from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from .config import DB_CONFIG
from loguru import logger

def get_engine():
    try:
        # Формируем строку подключения к базе данных
        DATABASE_URL = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@" \
                       f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
        
        # Логируем начало процесса создания движка
        logger.info(f"Connecting to database at {DB_CONFIG['host']}:{DB_CONFIG['port']}")
        
        # Создаем движок
        engine = create_engine(DATABASE_URL)
        
        # Логируем успешное создание движка
        logger.success("Database engine created successfully")
        
        return engine
    
    except Exception as e:
        # Логируем возможные ошибки при создании движка
        logger.error(f"Failed to create database engine: {e}")
        raise

def get_session(engine):
    try:
        # Создаем сессию
        Session = sessionmaker(bind=engine)
        
        # Логируем успешное создание сессии
        logger.success("Database session created successfully")
        
        return Session()
    
    except Exception as e:
        # Логируем возможные ошибки при создании сессии
        logger.error(f"Failed to create database session: {e}")
        raise

def healthcheck(engine):
    try:
        # Проверяем состояние подключения к базе данных с помощью простого запроса
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        
        # Логируем успешное выполнение healthcheck
        logger.success("Database healthcheck passed successfully")
        return True

    except Exception as e:
        # Логируем ошибку при healthcheck
        logger.error(f"Database healthcheck failed: {e}")
        return False
