import os
import pandas as pd
import mysql.connector
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

def get_mysql_connection():
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST"),
        port=int(os.getenv("MYSQL_PORT")),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DATABASE")
    )

def get_mysql_change_pct(symbol: str, interval_minutes: int) -> dict:
    conn = get_mysql_connection()
    cursor = conn.cursor(dictionary=True)

    end_time = datetime.utcnow()
    start_time = end_time - timedelta(minutes=interval_minutes)

    query = (
        "SELECT timestamp, close FROM historical_candles "
        "WHERE symbol = %s AND interval = '1minute' "
        "AND timestamp BETWEEN %s AND %s "
        "ORDER BY timestamp ASC"
    )
    cursor.execute(query, (symbol, start_time, end_time))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    if not rows or len(rows) < 2:
        return None

    ltp_then = rows[0]["close"]
    ltp_now = rows[-1]["close"]
    pct_change = ((ltp_now - ltp_then) / ltp_then) * 100

    return {
        "ltp_now": ltp_now,
        "ltp_then": ltp_then,
        "pct_change": pct_change,
        "timestamp": rows[0]["timestamp"].strftime("%Y-%m-%d %H:%M:%S")
    }