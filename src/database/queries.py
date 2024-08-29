# db/queries.py

import pandas as pd
from loguru import logger

def load_mt4_trades(engine):
    
    logger.info("Loading MT4 trades...")
    query = """
        SELECT * FROM hr_vacancies.mt4_trades
    """
    return pd.read_sql(query, con=engine)

def load_mt5_deals(engine):
    query = """
        SELECT * FROM hr_vacancies.mt5_deals
    """
    return pd.read_sql(query, con=engine)
