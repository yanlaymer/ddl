# tasks/task_3.py

import pandas as pd
import numpy as np

def find_user_pairs(filtered_trades, schema_type='mt4'):
    if schema_type == 'mt4':
        mt4_filtered_open_trades = filtered_trades[['login', 'open_time', 'cmd', 'symbol']].dropna(subset=['open_time'])
        mt4_filtered_open_trades['time_bin'] = mt4_filtered_open_trades['open_time'].dt.floor('30s')

        trade_pairs_mt4 = pd.merge(mt4_filtered_open_trades, mt4_filtered_open_trades, on=['time_bin', 'symbol'], suffixes=('_user1', '_user2'))
        trade_pairs_mt4_filtered = trade_pairs_mt4[(trade_pairs_mt4['login_user1'] != trade_pairs_mt4['login_user2']) & (trade_pairs_mt4['cmd_user1'] != trade_pairs_mt4['cmd_user2'])]
        user_pairs_mt4 = trade_pairs_mt4_filtered.groupby(['login_user1', 'login_user2']).size().reset_index(name='pair_trade_count')

        user_pair_counts = user_pairs_mt4[user_pairs_mt4['pair_trade_count'] > 10]

    elif schema_type == 'mt5':
        mt5_filtered_open_trades = filtered_trades[['login', 'time', 'action', 'symbol']].dropna(subset=['time'])
        mt5_filtered_open_trades['time_bin'] = mt5_filtered_open_trades['time'].dt.floor('30s')

        trade_pairs_mt5 = pd.merge(mt5_filtered_open_trades, mt5_filtered_open_trades, on=['time_bin', 'symbol'], suffixes=('_user1', '_user2'))
        trade_pairs_mt5_filtered = trade_pairs_mt5[(trade_pairs_mt5['login_user1'] != trade_pairs_mt5['login_user2']) & (trade_pairs_mt5['action_user1'] != trade_pairs_mt5['action_user2'])]
        user_pairs_mt5 = trade_pairs_mt5_filtered.groupby(['login_user1', 'login_user2']).size().reset_index(name='pair_trade_count')
        
        user_pair_counts = user_pairs_mt5[user_pairs_mt5['pair_trade_count'] > 10]

    return user_pair_counts
