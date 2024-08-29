from sqlalchemy import func, and_, cast, DateTime, Integer, text, Float, case, alias
from sqlalchemy.orm import aliased
from loguru import logger
import pandas as pd
import numpy as np

def calculate_metrics(filtered_trades, schema_type='mt4'):
    logger.info(f"Starting metric calculations for schema: {schema_type}")

    if schema_type == 'mt4':
        logger.info("Filtering short trades...")
        filtered_trades['duration'] = (filtered_trades['close_time'] - filtered_trades['open_time']).dt.total_seconds()
        short_trades = filtered_trades[filtered_trades['duration'] < 60]
        short_trade_counts = short_trades.groupby('login').size().reset_index(name='short_trade_count')
        logger.info(f"Found {len(short_trade_counts)} users with short trades")

        logger.info("Calculating trade pairs...")
        mt4_open_trades = filtered_trades[['login', 'open_time', 'cmd', 'ticket']].copy()
        mt4_open_trades = mt4_open_trades.dropna(subset=['open_time'])
        mt4_open_trades = mt4_open_trades.sort_values(by=['login', 'open_time'])
        
        mt4_open_trades['next_open_time'] = mt4_open_trades.groupby('login')['open_time'].shift(-1)
        mt4_open_trades['next_cmd'] = mt4_open_trades.groupby('login')['cmd'].shift(-1)
        mt4_open_trades['time_diff'] = (mt4_open_trades['next_open_time'] - mt4_open_trades['open_time']).dt.total_seconds()
        mt4_open_trades['opposite_direction'] = mt4_open_trades['cmd'] != mt4_open_trades['next_cmd']
        mt4_open_trades['within_30_seconds'] = mt4_open_trades['time_diff'] <= 30
        
        mt4_trade_pairs = mt4_open_trades[(mt4_open_trades['within_30_seconds'] == True) & (mt4_open_trades['opposite_direction'] == True)]

        trade_pair_counts = mt4_trade_pairs.groupby('login').size().reset_index(name='trade_pairs_count')

    elif schema_type == 'mt5':
        logger.info("Filtering short trades for MT5...")
        filtered_trades['time'] = pd.to_datetime(filtered_trades['time'], errors='coerce')

        mt5_open_trades = filtered_trades[filtered_trades['entry'] == 0].copy()
        mt5_close_trades = filtered_trades[filtered_trades['entry'] != 0].copy()

        mt5_merged_trades = pd.merge(mt5_open_trades[['positionid', 'login', 'time']], 
                                    mt5_close_trades[['positionid', 'time']], 
                                    on='positionid', suffixes=('_open', '_close'))

        mt5_merged_trades['trade_duration'] = (mt5_merged_trades['time_close'] - mt5_merged_trades['time_open']).dt.total_seconds()
        mt5_short_trades = mt5_merged_trades[mt5_merged_trades['trade_duration'] < 60]
        short_trade_counts = mt5_short_trades.groupby('login').size().reset_index(name='short_trades_count')

        logger.info("Calculating trade pairs for MT5...")
        mt5_open_trades_sorted = mt5_open_trades[['login', 'time', 'action', 'positionid']].dropna(subset=['time'])
        mt5_open_trades_sorted = mt5_open_trades_sorted.sort_values(by=['login', 'time'])

        mt5_open_trades_sorted['next_time'] = mt5_open_trades_sorted.groupby('login')['time'].shift(-1)
        mt5_open_trades_sorted['next_action'] = mt5_open_trades_sorted.groupby('login')['action'].shift(-1)

        mt5_open_trades_sorted['time_diff'] = (mt5_open_trades_sorted['next_time'] - mt5_open_trades_sorted['time']).dt.total_seconds()
        mt5_open_trades_sorted['opposite_direction'] = mt5_open_trades_sorted['action'] != mt5_open_trades_sorted['next_action']
        mt5_open_trades_sorted['within_30_seconds'] = mt5_open_trades_sorted['time_diff'] <= 30

        mt5_trade_pairs = mt5_open_trades_sorted[(mt5_open_trades_sorted['within_30_seconds'] == True) & (mt5_open_trades_sorted['opposite_direction'] == True)]

        trade_pair_counts = mt5_trade_pairs.groupby('login').size().reset_index(name='trade_pairs_count')

    logger.info("Metric calculation completed")
    return short_trade_counts, trade_pair_counts
