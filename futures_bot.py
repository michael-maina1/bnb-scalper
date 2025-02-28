import pandas as pd
import numpy as np
from datetime import datetime
import time
import os
import asyncio

class FuturesBot:
    def __init__(self, symbol='BNB/USDT', bankroll=100, max_daily_loss=0.05, fixed_tp=0.005, 
                 maker_fee=0.0002, taker_fee=0.00055, leverage=10):
        self.symbol = symbol
        self.bankroll = bankroll
        self.max_daily_loss = max_daily_loss
        self.fixed_tp = fixed_tp
        self.maker_fee = maker_fee
        self.taker_fee = taker_fee
        self.leverage = leverage
        self.daily_loss = 0
        self.trades = []
        self.position = None
        self.running = False
        self.perf_file = 'bot_performance.txt'
        self.csv_1m_path = 'bnb_usdt_1m_stream.csv'
        self.csv_5m_path = 'bnb_usdt_5m_stream.csv'
        self.csv_15m_path = 'bnb_usdt_15m_stream.csv'
        self.last_1m_size = 0
        self.last_5m_size = 0
        self.last_15m_size = 0
        self.message_queue = []

    def calculate_atr(self, df, period=14):
        high_low = df['high'] - df['low']
        high_close = np.abs(df['high'] - df['close'].shift())
        low_close = np.abs(df['low'] - df['close'].shift())
        ranges = pd.concat([high_low, high_close, low_close], axis=1)
        true_range = ranges.max(axis=1)
        return true_range.rolling(period).mean()

    def bollinger_bands(self, df, period=20, std_dev=1.5):
        sma = df['close'].rolling(window=period).mean()
        std = df['close'].rolling(window=period).std()
        upper_band = sma + (std * std_dev)
        lower_band = sma - (std * std_dev)
        return upper_band, sma, lower_band

    def is_bullish_15m(self, df_15m):
        df_15m = df_15m.tail(3)
        sma_15m = df_15m['close'].rolling(window=3).mean().iloc[-1]
        higher_highs = all(df_15m['high'].iloc[i] < df_15m['high'].iloc[i+1] for i in range(len(df_15m)-1))
        return df_15m['close'].iloc[-1] > sma_15m and higher_highs

    def is_bearish_15m(self, df_15m):
        df_15m = df_15m.tail(3)
        sma_15m = df_15m['close'].rolling(window=3).mean().iloc[-1]
        lower_lows = all(df_15m['low'].iloc[i] > df_15m['low'].iloc[i+1] for i in range(len(df_15m)-1))
        return df_15m['close'].iloc[-1] < sma_15m and lower_lows

    def place_limit_order(self, side, price, amount, current_price):
        if side == 'buy' and current_price <= price:
            return {'status': 'closed', 'price': price, 'amount': amount}
        elif side == 'sell' and current_price >= price:
            return {'status': 'closed', 'price': price, 'amount': amount}
        adjusted_price = price * (1.001 if side == 'buy' else 0.999)
        return {'status': 'closed', 'price': adjusted_price, 'amount': amount}

    def get_latest_data(self):
        df_1m = pd.read_csv(self.csv_1m_path)
        df_5m = pd.read_csv(self.csv_5m_path)
        df_15m = pd.read_csv(self.csv_15m_path)
        
        df_1m['timestamp'] = pd.to_datetime(df_1m['timestamp'])
        df_5m['timestamp'] = pd.to_datetime(df_5m['timestamp'])
        df_15m['timestamp'] = pd.to_datetime(df_15m['timestamp'])
        df_1m = df_1m.sort_values('timestamp')
        df_5m = df_5m.sort_values('timestamp')
        df_15m = df_15m.sort_values('timestamp')
        
        return df_1m, df_5m, df_15m

    async def execute_trade(self, df_1m, df_5m, df_15m, current_price, timestamp):
        if self.daily_loss >= self.max_daily_loss * self.bankroll:
            self.message_queue.append(f"[{timestamp}] Daily loss limit ($5) reached. Bot stopped.")
            return False

        atr_1m = self.calculate_atr(df_1m).iloc[-1]
        upper_band, middle_band, lower_band = self.bollinger_bands(df_1m)
        
        print(f"[{timestamp}] Price: {current_price:.2f} | Upper BB: {upper_band.iloc[-1]:.2f} | Middle BB: {middle_band.iloc[-1]:.2f} | Lower BB: {lower_band.iloc[-1]:.2f} | Diff (U): {current_price - upper_band.iloc[-1]:.2f} | Diff (L): {current_price - lower_band.iloc[-1]:.2f}")

        is_bullish = self.is_bullish_15m(df_15m)
        is_bearish = self.is_bearish_15m(df_15m)

        if self.position is None:
            if is_bullish and current_price > upper_band.iloc[-1]:  # Buy on bullish breakout
                position_size = (self.bankroll * 0.01 * self.leverage) / current_price
                stop_loss = current_price - (5 / position_size)  # $5 loss cap
                order = self.place_limit_order('buy', current_price, position_size, current_price)
                self.position = {
                    'side': 'buy',
                    'entry_price': order['price'],
                    'size': position_size,
                    'sl': stop_loss,
                    'entry_time': timestamp
                }
                msg = (f"[{timestamp}] Position Opened (Buy)\n"
                       f"Entry Price: {order['price']:.2f}\n"
                       f"Size: {position_size:.4f} BNB\n"
                       f"Stop Loss: {stop_loss:.2f}")
                print(msg)
                self.message_queue.append(msg)
                self.log_performance(timestamp)
            elif is_bearish and current_price < lower_band.iloc[-1]:  # Sell short on bearish breakdown
                position_size = (self.bankroll * 0.01 * self.leverage) / current_price
                stop_loss = current_price + (5 / position_size)  # $5 loss cap (higher for short)
                order = self.place_limit_order('sell', current_price, position_size, current_price)
                self.position = {
                    'side': 'sell',
                    'entry_price': order['price'],
                    'size': position_size,
                    'sl': stop_loss,
                    'entry_time': timestamp
                }
                msg = (f"[{timestamp}] Position Opened (Sell Short)\n"
                       f"Entry Price: {order['price']:.2f}\n"
                       f"Size: {position_size:.4f} BNB\n"
                       f"Stop Loss: {stop_loss:.2f}")
                print(msg)
                self.message_queue.append(msg)
                self.log_performance(timestamp)

        elif self.position:
            entry_price = self.position['entry_price']
            size = self.position['size']
            stop_loss = self.position['sl']
            entry_time = self.position['entry_time']
            side = self.position['side']

            if side == 'buy':
                tp_price = entry_price * (1 + self.fixed_tp)
                if current_price >= tp_price:
                    exit_order = self.place_limit_order('sell', tp_price, size, current_price)
                    profit = (exit_order['price'] - entry_price) * size - (self.maker_fee * exit_order['price'] * size)
                    self.daily_loss -= profit
                    self.trades.append({
                        'entry_time': entry_time,
                        'exit_time': timestamp,
                        'entry_price': entry_price,
                        'exit_price': tp_price,
                        'size': size,
                        'profit': profit,
                        'type': 'Fixed TP'
                    })
                    msg = (f"[{timestamp}] Position Closed (Buy)\n"
                           f"Exit Price: {tp_price:.2f}\n"
                           f"Profit: {profit:.2f} USDT\n"
                           f"Type: Fixed TP")
                    print(msg)
                    self.message_queue.append(msg)
                    self.position = None
                    self.log_performance(timestamp)
                elif current_price <= stop_loss:
                    exit_order = self.place_limit_order('sell', stop_loss, size, current_price)
                    profit = (exit_order['price'] - entry_price) * size - (self.maker_fee * exit_order['price'] * size)
                    self.daily_loss -= profit
                    self.trades.append({
                        'entry_time': entry_time,
                        'exit_time': timestamp,
                        'entry_price': entry_price,
                        'exit_price': stop_loss,
                        'size': size,
                        'profit': profit,
                        'type': 'Stop Loss'
                    })
                    msg = (f"[{timestamp}] Position Closed (Buy)\n"
                           f"Exit Price: {stop_loss:.2f}\n"
                           f"Profit: {profit:.2f} USDT\n"
                           f"Type: Stop Loss")
                    print(msg)
                    self.message_queue.append(msg)
                    self.position = None
                    self.log_performance(timestamp)
            elif side == 'sell':
                tp_price = entry_price * (1 - self.fixed_tp)
                if current_price <= tp_price:
                    exit_order = self.place_limit_order('buy', tp_price, size, current_price)
                    profit = (entry_price - exit_order['price']) * size - (self.maker_fee * exit_order['price'] * size)
                    self.daily_loss -= profit
                    self.trades.append({
                        'entry_time': entry_time,
                        'exit_time': timestamp,
                        'entry_price': entry_price,
                        'exit_price': tp_price,
                        'size': size,
                        'profit': profit,
                        'type': 'Fixed TP'
                    })
                    msg = (f"[{timestamp}] Position Closed (Sell Short)\n"
                           f"Exit Price: {tp_price:.2f}\n"
                           f"Profit: {profit:.2f} USDT\n"
                           f"Type: Fixed TP")
                    print(msg)
                    self.message_queue.append(msg)
                    self.position = None
                    self.log_performance(timestamp)
                elif current_price >= stop_loss:
                    exit_order = self.place_limit_order('buy', stop_loss, size, current_price)
                    profit = (entry_price - exit_order['price']) * size - (self.maker_fee * exit_order['price'] * size)
                    self.daily_loss -= profit
                    self.trades.append({
                        'entry_time': entry_time,
                        'exit_time': timestamp,
                        'entry_price': entry_price,
                        'exit_price': stop_loss,
                        'size': size,
                        'profit': profit,
                        'type': 'Stop Loss'
                    })
                    msg = (f"[{timestamp}] Position Closed (Sell Short)\n"
                           f"Exit Price: {stop_loss:.2f}\n"
                           f"Profit: {profit:.2f} USDT\n"
                           f"Type: Stop Loss")
                    print(msg)
                    self.message_queue.append(msg)
                    self.position = None
                    self.log_performance(timestamp)

        return True

    def log_performance(self, timestamp):
        total_profit = sum(trade['profit'] for trade in self.trades)
        win_rate = len([t for t in self.trades if t['profit'] > 0]) / len(self.trades) if self.trades else 0
        with open(self.perf_file, 'a') as f:
            f.write(f"[{timestamp}] Total Profit: {total_profit:.2f} USDT | Win Rate: {win_rate:.2%} | Trades: {len(self.trades)} | Daily Loss: {self.daily_loss:.2f}\n")
            if self.trades:
                last_trade = self.trades[-1]
                f.write(f"Last Trade - Entry: {last_trade['entry_time']} @ {last_trade['entry_price']} | "
                        f"Exit: {last_trade['exit_time']} @ {last_trade['exit_price']} | "
                        f"Size: {last_trade['size']:.4f} BNB | Profit: {last_trade['profit']:.2f} USDT | "
                        f"Type: {last_trade['type']}\n")

    def daily_report(self):
        now = datetime.now()
        if self.last_report_date is None or now.date() > self.last_report_date:
            if self.trades:
                today_trades = [t for t in self.trades if pd.to_datetime(t['entry_time']).date() == now.date()]
                if today_trades:
                    total_profit = sum(t['profit'] for t in today_trades)
                    win_rate = len([t for t in today_trades if t['profit'] > 0]) / len(today_trades) if today_trades else 0
                    msg = (f"Daily Report - {now.date()}\n"
                           f"Total Profit: {total_profit:.2f} USDT\n"
                           f"Win Rate: {win_rate:.2%}\n"
                           f"Trades: {len(today_trades)}\n"
                           f"Bankroll: {self.bankroll - self.daily_loss:.2f} USDT")
                    self.message_queue.append(msg)
            self.last_report_date = now.date()

    async def run(self):
        print(f"Starting real-time simulation on {datetime.now()} with $100 bankroll and 10x leverage")
        
        while not os.path.exists(self.csv_1m_path) or not os.path.exists(self.csv_5m_path) or not os.path.exists(self.csv_15m_path):
            print("Waiting for CSV files to be created by streamer...")
            await asyncio.sleep(5)

        self.running = True
        while self.running:
            df_1m, df_5m, df_15m = self.get_latest_data()
            current_1m_size = len(df_1m)
            current_5m_size = len(df_5m)
            current_15m_size = len(df_15m)

            if current_1m_size > self.last_1m_size or current_5m_size > self.last_5m_size or current_15m_size > self.last_15m_size:
                if len(df_1m) >= 20 and len(df_5m) >= 5 and len(df_15m) >= 2:
                    current_time = df_1m['timestamp'].iloc[-1]
                    current_price = df_1m['close'].iloc[-1]
                    df_1m_slice = df_1m.tail(50)
                    df_5m_slice = df_5m[df_5m['timestamp'] <= current_time].tail(20)
                    df_15m_slice = df_15m[df_15m['timestamp'] <= current_time].tail(5)
                    
                    if not await self.execute_trade(df_1m_slice, df_5m_slice, df_15m_slice, current_price, current_time):
                        print(f"[{current_time}] Daily loss limit reached ($5). Stopping.")
                        self.running = False
                        break
                else:
                    print(f"[{datetime.now()}] Insufficient data - 1m: {current_1m_size}, 5m: {current_5m_size}, 15m: {current_15m_size}")
            
            self.daily_report()
            self.last_1m_size = current_1m_size
            self.last_5m_size = current_5m_size
            self.last_15m_size = current_15m_size
            await asyncio.sleep(10)

if __name__ == "__main__":
    asyncio.run(FuturesBot().run())