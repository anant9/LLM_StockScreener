import os
import pandas as pd
from redis_reader import get_redis_change_pct
from historical_fetcher import get_mysql_change_pct
#from fundamental_engine import filter_fundamentals
#from technical_engine import filter_by_indicators
#from chart_pattern_engine import filter_by_patterns
from dotenv import load_dotenv
from stock_list import load_nifty50_symbols

load_dotenv()

USE_REDIS = os.getenv("USE_REDIS", "true").lower() == "true"

def run_screener(parsed: dict) -> pd.DataFrame:
    if not parsed.get("change_pct") or not parsed.get("interval_minutes"):
        return pd.DataFrame()

    symbols = load_nifty50_symbols()

   # if parsed.get("fundamentals"):
   #     symbols = filter_fundamentals(parsed["fundamentals"], symbols)

   # if parsed.get("indicators"):
    #    symbols = filter_by_indicators(symbols, parsed["indicators"])

    #if parsed.get("patterns"):
     #   symbols = filter_by_patterns(symbols, parsed["patterns"])

    matched = []
    for symbol in symbols:
        try:
            if USE_REDIS and parsed["interval_minutes"] <= int(os.getenv("REDIS_TICK_WINDOW_MINUTES", 15)):
                result = get_redis_change_pct(symbol, parsed["interval_minutes"])
            else:
                result = get_mysql_change_pct(symbol, parsed["interval_minutes"])

            if result and abs(result["pct_change"]) >= parsed["change_pct"]:
                matched.append({
                    "Symbol": symbol,
                    "LTP (Now)": result["ltp_now"],
                    "LTP (Then)": result["ltp_then"],
                    "% Change": round(result["pct_change"], 2),
                    "Matched At": result["timestamp"]
                })
        except Exception as e:
            print(f"Error processing {symbol}: {e}")

    return pd.DataFrame(matched)