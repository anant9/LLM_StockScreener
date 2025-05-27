import pandas as pd
from historical_fetcher import get_mysql_connection

def get_ohlcv(symbol: str, interval: str = "1minute", limit: int = 50):
    conn = get_mysql_connection()
    query = """
        SELECT timestamp, open, high, low, close, volume
        FROM historical_candles
        WHERE symbol = %s AND interval = %s
        ORDER BY timestamp DESC LIMIT %s
    """
    df = pd.read_sql(query, conn, params=(symbol, interval, limit))
    conn.close()
    return df.sort_values("timestamp")

def is_bullish_engulfing(df):
    if len(df) < 2:
        return False
    prev, last = df.iloc[-2], df.iloc[-1]
    return prev["close"] < prev["open"] and last["close"] > last["open"] and            last["open"] < prev["close"] and last["close"] > prev["open"]

def is_breakout_52w(df):
    if df.empty or len(df) < 250:
        return False
    recent_high = df["high"].iloc[:-1].max()
    latest_close = df["close"].iloc[-1]
    return latest_close > recent_high

def filter_by_patterns(symbols: list, patterns: list) -> list:
    matched = []
    for symbol in symbols:
        try:
            df = get_ohlcv(symbol, interval="1minute", limit=50)
            daily_df = get_ohlcv(symbol, interval="1day", limit=252)

            pattern_match = False
            for pattern in patterns:
                if pattern == "bullish engulfing" and is_bullish_engulfing(df):
                    pattern_match = True
                elif pattern == "breakout" and is_breakout_52w(daily_df):
                    pattern_match = True
                # Add more pattern checks as needed

            if pattern_match:
                matched.append(symbol)

        except Exception as e:
            print(f"Pattern detection error for {symbol}: {e}")
    return matched