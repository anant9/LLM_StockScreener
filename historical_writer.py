# historical_writer.py
import os
import schedule
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv
from smartapi import SmartConnect
import mysql.connector
import sqlite3

load_dotenv()

# Setup Angel One login
angel = SmartConnect(api_key=os.getenv("ANGEL_API_KEY"))
session = angel.generateSession(
    os.getenv("ANGEL_USER_ID"),
    os.getenv("ANGEL_PASSWORD"),
    os.getenv("ANGEL_TOTP")
)
jwt_token = session["data"]["jwtToken"]

# Config
DB_TYPE = os.getenv("DB_TYPE", "sqlite")
INTERVAL = "ONE_MINUTE"

# SQLite/MySQL insert functions
def insert_candles(symbol, candles):
    if DB_TYPE == "mysql":
        conn = mysql.connector.connect(
            host=os.getenv("MYSQL_HOST"),
            port=int(os.getenv("MYSQL_PORT")),
            user=os.getenv("MYSQL_USER"),
            password=os.getenv("MYSQL_PASSWORD"),
            database=os.getenv("MYSQL_DATABASE")
        )
        cursor = conn.cursor()
        for c in candles:
            ts = datetime.fromtimestamp(c[0]/1000.0)
            cursor.execute("""
                INSERT INTO historical_candles
                (symbol, timestamp, open, high, low, close, volume, interval)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (symbol, ts, c[1], c[2], c[3], c[4], c[5], "1minute"))
        conn.commit()
        conn.close()

    else:
        conn = sqlite3.connect(os.getenv("SQLITE_PATH", "data/user_prompts.db"))
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS historical_candles (
                symbol TEXT,
                timestamp TEXT,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                interval TEXT
            )
        """)
        for c in candles:
            ts = datetime.fromtimestamp(c[0]/1000.0)
            cursor.execute("""
                INSERT INTO historical_candles
                (symbol, timestamp, open, high, low, close, volume, interval)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (symbol, ts, c[1], c[2], c[3], c[4], c[5], "1minute"))
        conn.commit()
        conn.close()

# Sample symbol token mapping
def get_symbol_token_map():
    # In real use, load from OpenAPIScripMaster.json or Upstash list
    return {
        "SBIN": "3045",
        "RELIANCE": "2885",
        "TCS": "11536"
    }

# Fetch and store

def fetch_for_all():
    print("[Fetch] Running historical candle fetch job...")
    symbol_map = get_symbol_token_map()
    now = datetime.now()
    from_time = (now - timedelta(minutes=15)).strftime('%Y-%m-%d %H:%M')
    to_time = now.strftime('%Y-%m-%d %H:%M')

    for symbol, token in symbol_map.items():
        try:
            candles = angel.getCandleData(
                exchange="NSE",
                symbol=symbol,
                interval=INTERVAL,
                fromdate=from_time,
                todate=to_time,
                token=token
            )
            insert_candles(symbol, candles["data"])
            print(f"Stored {symbol} - {len(candles['data'])} candles")
        except Exception as e:
            print(f"Error fetching {symbol}: {e}")

# Schedule
schedule.every(15).minutes.do(fetch_for_all)

if __name__ == "__main__":
    print("[Scheduler] Starting candle data fetcher...")
    fetch_for_all()
    while True:
        schedule.run_pending()
        time.sleep(60)
