import numpy as np
import pandas as pd

transaction_log_cols = ['strategy_id', 'symbol', 'invested', 'buy_time', 'sell_time', 'buy_price', 'sell_price', 'profit']
MIN_INVESTMENT = 0.1
FEE = 0.0025

def run_func(args):
    func = args[0]
    argsdict = args[1]
    return func(**argsdict)


def volume_profit(strategy_id, params, buy, timestamp, current_ohlcv, my_open_transactions, available_balance=None, data=None):
    min_volume, min_profit, wait_mins, max_loss = params
    new_transactions = pd.DataFrame(columns=transaction_log_cols)

    if current_ohlcv.empty:
        return available_balance, new_transactions

    if buy:

        if available_balance > 0:
            amount_to_invest = np.max([0.2 * available_balance, MIN_INVESTMENT])
            available_coins = set(current_ohlcv['symbol'].values)
            exclude_coins = set(my_open_transactions['symbol'])

            available_coins = available_coins.difference(exclude_coins)

            while (amount_to_invest <= available_balance) and available_coins:
                selected_symbol = available_coins.pop()
                coindata = current_ohlcv[(current_ohlcv['symbol'] == selected_symbol)].iloc[0]
                if (coindata['highest']*coindata['volume'] > min_volume) and (coindata['change'] > 0):
                    buy_price = coindata['highest']
                    new_transactions.loc[-1] = [strategy_id, selected_symbol, amount_to_invest * (1 - FEE), timestamp, None, buy_price, None, None]
                    new_transactions.index += 1
                    available_balance -= amount_to_invest

        return strategy_id, available_balance, new_transactions

    else:

        selected_ohlcv = current_ohlcv[(current_ohlcv['symbol'].isin(my_open_transactions['symbol']))]

        close_price = selected_ohlcv[['close', 'symbol']]

        sell_price = close_price.copy()
        sell_price["sell_price"] = sell_price["close"]

        open_index = my_open_transactions.index
        buy_sell = pd.merge(my_open_transactions[['buy_price', 'symbol']], sell_price, on='symbol', how='outer')
        buy_sell.set_index(open_index, inplace=True)

        profit = (buy_sell['sell_price'] - buy_sell['buy_price']) / buy_sell['buy_price']
        profit.name = 'profit'

        sell_time = pd.Series(timestamp, index=profit.index, name='sell_time')

        update_transactions = pd.concat([sell_time, profit, buy_sell['sell_price']], axis=1)

        elapsed_time_min = (timestamp - my_open_transactions['buy_time']) / 1000 / 60
        modify_sell_only = (profit >= min_profit) | (elapsed_time_min > wait_mins) | (profit <= -1*max_loss)

        my_open_transactions.update(update_transactions[modify_sell_only])

        modified = my_open_transactions[my_open_transactions['profit'].notnull()]

        available_balance += np.sum(((1 + modified['profit']) * modified['invested']) * (1 - FEE))

        return strategy_id, available_balance, modified
