import ccxt
import numpy as np
import pickle
import pandas as pd
from itertools import product
from strategies import *
from multiprocessing import Pool
import multiprocessing
import time

MIN_INVESTMENT = 0.1
FEE = 0.0025


# open data file ()
with open('bittrex_1h_top_cap.pkl', 'rb') as f:
    ohlcv_table = pickle.load(f)

# add daily change column
close_change = ohlcv_table.groupby('symbol')["close"].apply(lambda x: x.diff() / np.concatenate([[1], x.values[:-1]]))
ohlcv_table['change'] = close_change


timestamps = ohlcv_table['timestamp'].unique()
timestamps.sort()

transaction_log_cols = ['strategy_id', 'symbol', 'invested', 'buy_time', 'sell_time', 'buy_price', 'sell_price', 'profit']


# gen strategies
# min_volumes = [0, 20, 40]
# min_profits = [0.02, 0.06]
# wait_minutes = [60*12, 60*10000]
# max_loss = [0.3, 1]

min_volumes = [20]
min_profits = [0.02,0.05]
wait_minutes = [60*12, 10000]
max_loss = [0.3, 1]


params = list(product(min_volumes, min_profits, wait_minutes, max_loss))
len(params)
strategy = {i: (volume_profit, param) for i, param in enumerate(params)}


# simulate


def run_experiment():
    open_transaction_log = pd.DataFrame(columns=transaction_log_cols)
    closed_transaction_log = pd.DataFrame(columns=transaction_log_cols)
    balance = {strat_id: 1 for strat_id in strategy.keys()}

    with Pool(6) as p:
        # for i in range(len(timestamps)):
        for i in range(3000):
            t = timestamps[i]
            try:

                current_ohlcv = ohlcv_table.loc[(ohlcv_table['timestamp'] == t)]

                # sell
                exec_tuples = []
                for s_id, (func, params) in strategy.items():
                    args_dict = {'strategy_id': s_id,
                                 'params': params,
                                 'buy': False,
                                 'timestamp': t,
                                 'current_ohlcv': current_ohlcv,
                                 'my_open_transactions': open_transaction_log[open_transaction_log['strategy_id'] == s_id].copy(),
                                 'available_balance': balance[s_id]}
                    exec_tuples.append((func, args_dict))

                imap_it = p.imap(run_func, exec_tuples)
                for s_id, balance_res, modified_trans in imap_it:
                    balance[s_id] = balance_res
                    open_transaction_log.drop(modified_trans.index, inplace=True)
                    closed_transaction_log = closed_transaction_log.append(modified_trans, ignore_index=True)

                # buy
                if i < (len(timestamps)-0):
                    exec_tuples = []
                    for s_id, (func, params) in strategy.items():
                        args_dict = {'strategy_id': s_id,
                                     'params': params,
                                     'buy': True,
                                     'timestamp': t,
                                     'current_ohlcv': current_ohlcv,
                                     'my_open_transactions': open_transaction_log[
                                         open_transaction_log['strategy_id'] == s_id].copy(),
                                     'available_balance': balance[s_id]}
                        exec_tuples.append((func, args_dict))

                    imap_it = p.imap(run_func, exec_tuples)
                    for s_id, balance_res, new_trans in imap_it:
                        balance[s_id] = balance_res
                        open_transaction_log = open_transaction_log.append(new_trans, ignore_index=True)
                        # open_transaction_log = pd.concat([open_transaction_log, new_trans], axis=0, ignore_index=True)

            except Exception as e:
                raise e

            if i % 1000 == 0:
                print("step" + str(i))

    return balance, open_transaction_log, closed_transaction_log

if __name__ == '__main__':
    multiprocessing.freeze_support()


    start_time = time.time()
    balance, open_transaction_log, closed_transaction_log = run_experiment()
    print("--- %s seconds ---" % (time.time() - start_time))

    new_balance = {strat_id: 0 for strat_id in strategy.keys()}

    for i in strategy.keys():
        new_balance[i] = np.sum(open_transaction_log.loc[open_transaction_log['strategy_id'] == i, 'invested']) + balance[i]

    print([(strategy[i][1], profit) for i,profit in sorted(new_balance.items(), key=lambda x: x[1])])