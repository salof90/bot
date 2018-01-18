import ccxt
import numpy as np
from time import sleep
import matplotlib.pyplot as plt
import pickle
import pandas as pd
from pandas import Series

topVolumeN = 25
top_cap_num = 30
profit_percent = 1.02

cmc = ccxt.coinmarketcap()
top_market_cap = cmc.fetch_tickers(currency='BTC')
filtered_tickers = [(data['info']['symbol'], data) for (name,data) in list(top_market_cap.items()) if not (data['info']['symbol'].startswith('USDT') or data['info']['symbol'].startswith('BTC'))]
top_market_cap = sorted(filtered_tickers, key=lambda tup: float(tup[1]['info']['market_cap_usd'] if tup[1]['info']['market_cap_usd'] else 0), reverse=True)[:top_cap_num]

top_market_cap_symbols = [symbol+'/BTC' for symbol,data in top_market_cap]

top_market_cap_symbols = ['ETH/BTC', 'XRP/BTC', 'BCH/BTC', 'ADA/BTC', 'XEM/BTC', 'LTC/BTC', 'XLM/BTC', 'DASH/BTC', 'NEO/BTC', 'XMR/BTC', 'BTG/BTC','QTUM/BTC', 'ETC/BTC', 'LSK/BTC', 'XVG/BTC', 'OMG/BTC', 'SC/BTC', 'ZEC/BTC', 'STRAT/BTC']

exchange = ccxt.bittrex()

# tickers = exchange.fetch_tickers()
# filtered_tickers = [(symbol, data) for (symbol,data) in list(tickers.items()) if symbol.endswith('BTC') and not symbol.startswith('USDT')]
# #topVolumeCoins = sorted(filtered_tickers, key=lambda tup: tup[1]['quoteVolume'], reverse=True)[:topVolumeN]
# topVolumeCoins = filtered_tickers


ohlcv_table_new = pd.DataFrame()
ohlcv_columns = ["timestamp","open","highest","lowest","close","volume"]

for symbol in top_market_cap_symbols:
    while True:
        try:
            candles = pd.DataFrame(exchange.fetch_ohlcv(symbol, timeframe='1m'), columns=ohlcv_columns)
            candles['symbol'] = Series(symbol, index=candles.index)
            sleep(exchange.rateLimit * 1.1 / 1000)

            print(symbol + ', datapoints: ' + str(len(candles)))

            # profit_delays = np.zeros((candles.shape[0],))
            # for i_ohlcv in candles.index:  # range from 1 to match close_diff size
            #     i_timestamp = candles.loc[i_ohlcv, "timestamp"]
            #     profit_times = candles.loc[((candles["timestamp"] >= i_timestamp) &
            #                                 (candles["open"] >= profit_percent * candles.loc[
            #                                     i_ohlcv, "open"])), "timestamp"]
            #     profit_delays[i_ohlcv] = profit_times.min() - i_timestamp if profit_times.size != 0 else -1

            # candles['profit_delays'] = Series(profit_delays, index=candles.index)

            ohlcv_table_new = pd.concat([ohlcv_table_new, candles], axis=0, ignore_index=True)
            sleep(exchange.rateLimit * 1.1 / 1000)
            break
        except Exception as e:
            print(e)
            if "does not have market symbol" in str(e):
                break

with open('bittrex_1h_top_cap.pkl', 'rb') as f:
    ohlcv_table_old = pickle.load(f)

ohlcv_table = pd.concat([ohlcv_table_old, ohlcv_table_new], axis=0, ignore_index=True)
ohlcv_table.drop_duplicates(inplace=True)

with open('bittrex_1h_top_cap.pkl', 'wb') as f:
    pickle.dump(ohlcv_table, f)


close_change = ohlcv_table.groupby('symbol')["close"].apply(lambda x: x.diff() / np.concatenate([[1], x.values[:-1]]))
ohlcv_table['change'] = close_change

#ohlcv_pos_change = ohlcv_table[(ohlcv_table["profit_delays"] != -2)]
ohlcv_pos_change = ohlcv_table

unique_timestamps = ohlcv_pos_change['timestamp'].unique()
unique_timestamps.sort()
result = np.zeros((unique_timestamps.shape[0], 4))
for i_ts in range(unique_timestamps.shape[0]):
    selected_ts_ind = (ohlcv_pos_change["timestamp"] == unique_timestamps[i_ts])
    profit_time_ind = (ohlcv_pos_change['profit_delays'] != -1)

    timestamp_count = np.sum(selected_ts_ind)
    prof_delay = ohlcv_pos_change.loc[selected_ts_ind & profit_time_ind, ['profit_delays']]

    prof_mean = np.mean(prof_delay) if prof_delay.size != 0 else None
    prof_std = np.std(prof_delay) if prof_delay.size != 0 else None
    result[i_ts] = [unique_timestamps[i_ts], prof_mean, prof_std, len(prof_delay)/timestamp_count]


time_convert = 1000*60*60 # miliseconds to minutes

# significant when percentage of observed coins (after diff filter) above threshold
significant_ind = result[:, 3] >= 0.8



timestamps_data = pd.to_datetime(result[significant_ind , 0], unit='ms')
mean_data = result[significant_ind , 1]/time_convert
std_data = result[significant_ind, 2]/time_convert

select_date = timestamps_data > pd.to_datetime("2018-01-05")

plt.errorbar(timestamps_data[select_date], mean_data[select_date], std_data[select_date], linestyle='-', marker='.', ecolor='b', color='r')

