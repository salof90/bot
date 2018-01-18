c =0

for index, transaction in closed_transaction_log.iterrows():
    if transaction["sell_time"] != transaction["buy_time"]:
        match_sell = (ohlcv_table["timestamp"] == transaction['sell_time']) & \
                    (ohlcv_table["close"] == transaction['sell_price']) & \
                    (ohlcv_table["symbol"] == transaction['symbol'])
        match_buy = (ohlcv_table["timestamp"] == transaction['buy_time']) & \
                    (ohlcv_table["highest"] == transaction['buy_price']) & \
                    (ohlcv_table["symbol"] == transaction['symbol'])


        if not any(match_sell) or not any(match_buy):
            c+=1



print(c)