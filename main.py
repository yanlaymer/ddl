import os
import pandas as pd
from loguru import logger

from src.database.create_engine import get_engine, get_session, healthcheck
from src.database.queries import load_mt4_trades, load_mt5_deals
from src.tasks.task_1_2 import calculate_metrics
from src.tasks.task_3 import find_user_pairs


def log_and_validate_data(df, data_name):
    """Log and validate if the data is not empty."""
    if df.empty:
        logger.error(f"{data_name} data is empty. Exiting...")
        return False
    logger.success(f"{data_name} data loaded successfully")
    return True


def preprocess_dates(df, date_columns):
    """Convert specified columns to datetime."""
    for col in date_columns:
        df[col] = pd.to_datetime(df[col], errors='coerce')


def save_to_csv(data, filename):
    """Save DataFrame to CSV."""
    output_dir = 'data/'
    os.makedirs(output_dir, exist_ok=True)
    data.to_csv(os.path.join(output_dir, filename), index=False)
    logger.info(f"Data saved to {filename}")


def main():
    engine = get_engine()
    session = get_session(engine)

    if not healthcheck(engine):
        logger.error("Database connection failed. Exiting...")
        return

    logger.info("Starting the problem-solving process")

    mt4_trades = load_mt4_trades(engine)
    if not log_and_validate_data(mt4_trades, "MT4 trades"):
        return

    mt5_deals = load_mt5_deals(engine)
    if not log_and_validate_data(mt5_deals, "MT5 deals"):
        return

    preprocess_dates(mt4_trades, ['open_time', 'close_time'])
    preprocess_dates(mt5_deals, ['time'])

    mt4_short_trade_counts, mt4_trade_pair_counts = calculate_metrics(mt4_trades, schema_type='mt4')
    mt4_user_pair_counts = find_user_pairs(mt4_trades, schema_type='mt4')

    mt5_short_trade_counts, mt5_trade_pair_counts = calculate_metrics(mt5_deals, schema_type='mt5')
    mt5_user_pair_counts = find_user_pairs(mt5_deals, schema_type='mt5')

    logger.info("Saving data to CSV...")

    task_1_2_answer = mt4_short_trade_counts.merge(mt4_trade_pair_counts, on='login', how='outer')
    save_to_csv(task_1_2_answer, 'short_trades_and_trade_pairs_mt4.csv')

    save_to_csv(mt5_user_pair_counts, 'user_pairs_mt5.csv')


if __name__ == "__main__":
    main()
