import websocket
import json
import csv
from datetime import datetime
import asyncio

BYBIT_WS_URL = "wss://stream.bybit.com/v5/public/linear"
FILE_1M = "bnb_usdt_1m_stream.csv"
FILE_5M = "bnb_usdt_5m_stream.csv"
FILE_15M = "bnb_usdt_15m_stream.csv"

last_saved_1m = None
last_saved_5m = None
last_saved_15m = None

def init_csv():
    headers = ["timestamp", "open", "close", "high", "low", "volume"]
    for file in [FILE_1M, FILE_5M, FILE_15M]:
        with open(file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(headers)

def transform_timestamp(unix_ms):
    return datetime.fromtimestamp(unix_ms / 1000).strftime('%Y-%m-%d %H:%M:%S')

def get_interval_minutes(interval):
    return int(interval)

def is_correct_interval(timestamp_ms, interval):
    dt = datetime.fromtimestamp(timestamp_ms / 1000)
    minutes = get_interval_minutes(interval)
    return dt.second == 0 and dt.minute % minutes == 0

def on_open(ws):
    print("✅ WebSocket Connected!")
    global last_saved_1m, last_saved_5m, last_saved_15m
    last_saved_1m = None
    last_saved_5m = None
    last_saved_15m = None
    subscribe_payload = {
        "op": "subscribe",
        "args": ["kline.1.BNBUSDT", "kline.5.BNBUSDT", "kline.15.BNBUSDT"]
    }
    ws.send(json.dumps(subscribe_payload))
    print("📡 Subscribed to BNB/USDT K-line data (1m, 5m, & 15m)")

def on_message(ws, message):
    print(f"Raw message: {message}")
    data = json.loads(message)
    
    if "topic" in data and "data" in data:
        kline_data = data["data"][0]
        if not kline_data.get('confirm', False):
            print(f"Skipping unconfirmed candle: {kline_data}")
            return
        
        timestamp = kline_data["start"]
        interval = data["topic"].split('.')[1]
        
        if not is_correct_interval(timestamp, interval):
            print(f"Skipping {interval}m candle—wrong interval: {transform_timestamp(timestamp)}")
            return

        global last_saved_1m, last_saved_5m, last_saved_15m
        current_dt = datetime.fromtimestamp(timestamp / 1000)
        if interval == '1':
            if last_saved_1m and (current_dt - datetime.fromtimestamp(last_saved_1m / 1000)).seconds < 60:
                print(f"Skipping duplicate 1m candle: {transform_timestamp(timestamp)}")
                return
            last_saved_1m = timestamp
            file_path = FILE_1M
        elif interval == '5':
            if last_saved_5m and (current_dt - datetime.fromtimestamp(last_saved_5m / 1000)).seconds < 300:
                print(f"Skipping duplicate 5m candle: {transform_timestamp(timestamp)}")
                return
            last_saved_5m = timestamp
            file_path = FILE_5M
        elif interval == '15':
            if last_saved_15m and (current_dt - datetime.fromtimestamp(last_saved_15m / 1000)).seconds < 900:
                print(f"Skipping duplicate 15m candle: {transform_timestamp(timestamp)}")
                return
            last_saved_15m = timestamp
            file_path = FILE_15M
        else:
            return

        dt_str = transform_timestamp(timestamp)
        row = [
            dt_str,
            kline_data["open"],
            kline_data["close"],
            kline_data["high"],
            kline_data["low"],
            kline_data["volume"]
        ]
        
        with open(file_path, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(row)
        
        print(f"💾 Data saved to {file_path} at {interval}m interval: {row}")

def on_error(ws, error):
    print(f"❌ Error: {error}")

def on_close(ws, code, reason):
    print("🔴 WebSocket Disconnected.")

async def run_streamer():
    init_csv()
    ws = websocket.WebSocketApp(
        BYBIT_WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    await asyncio.get_event_loop().run_in_executor(None, ws.run_forever)

if __name__ == "__main__":
    asyncio.run(run_streamer())