import websocket
import json
import csv
from datetime import datetime

BYBIT_WS_URL = "wss://stream.bybit.com/v5/public/linear"
FILE_1M = "bnb_usdt_1m_stream.csv"
FILE_5M = "bnb_usdt_5m_stream.csv"
FILE_15M = "bnb_usdt_15m_stream.csv"

def init_csv():
    headers = ["timestamp", "open", "close", "high", "low", "volume"]
    for file in [FILE_1M, FILE_5M, FILE_15M]:
        with open(file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(headers)

def transform_timestamp(unix_ms):
    return datetime.fromtimestamp(unix_ms / 1000).strftime('%Y-%m-%d %H:%M:%S')

def on_open(ws):
    print("✅ WebSocket Connected!")
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
        timestamp = transform_timestamp(kline_data["timestamp"])
        row = [
            timestamp,
            kline_data["open"],
            kline_data["close"],
            kline_data["high"],
            kline_data["low"],
            kline_data["volume"]
        ]
        
        if "kline.1." in data["topic"]:
            file_path = FILE_1M
        elif "kline.5." in data["topic"]:
            file_path = FILE_5M
        elif "kline.15." in data["topic"]:
            file_path = FILE_15M
        else:
            return
        
        with open(file_path, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(row)
        
        print(f"💾 Data saved to {file_path}: {row}")

def on_error(ws, error):
    print(f"❌ Error: {error}")

def on_close(ws, code, reason):
    print("🔴 WebSocket Disconnected.")

def stream_kline_data():
    init_csv()
    ws = websocket.WebSocketApp(
        BYBIT_WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.run_forever()

if __name__ == "__main__":
    stream_kline_data()