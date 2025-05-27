import pandas as pd
import pandas_ta as ta
from historical_fetcher import get_mysql_connection

def get_ohlcv(symbol: str, interval: str = "1minute", limit: int = 100):
    conn = get_mysql_connection()
    query = """
        SELECT timestamp, open, high, low, close, volume
        FROM historical_candles
        WHERE symbol = %s AND interval = %s
        ORDER BY timestamp DESC LIMIT %s
    """
    df = pd.read_sql(query, conn, params=(symbol, interval, limit))
    conn.close()
    df = df.sort_values("timestamp")
    return df

def filter_by_indicators(symbols: list, indicators: dict) -> list:
    matched = []
    for symbol in symbols:
        try:
            df = get_ohlcv(symbol)
            if df.empty or len(df) < 20:
                continue

            df.ta.strategy("All")
            pass_all = True
            for ind, rule in indicators.items():
                ind = ind.lower()
                if ind not in df.columns:
                    continue

                val = df[ind].iloc[-1]
                if rule.startswith("<") and not val < float(rule[1:]):
                    pass_all = False
                elif rule.startswith(">") and not val > float(rule[1:]):
                    pass_all = False
                elif rule.startswith("=") and not val == float(rule[1:]):
                    pass_all = False

            if pass_all:
                matched.append(symbol)

        except Exception as e:
            print(f"Indicator filter error for {symbol}: {e}")
    return matched