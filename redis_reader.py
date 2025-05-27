import json
import os
from redis import Redis
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()

redis_client = Redis(
    host=os.getenv("REDIS_HOST"),
    port=int(os.getenv("REDIS_PORT")),
    password=os.getenv("REDIS_PASSWORD"),
    decode_responses=True,
    ssl=True
)

def get_redis_change_pct(symbol: str, interval_minutes: int) -> dict:
    key = f"tickhist:{symbol}"
    ticks = redis_client.lrange(key, 0, -1)

    if not ticks or len(ticks) < 2:
        return None

    now = datetime.utcnow()
    window_start = now - timedelta(minutes=interval_minutes)
    ltp_now, ltp_then = None, None
    then_timestamp = None

    for tick_json in reversed(ticks):
        tick = json.loads(tick_json)
        ts = datetime.fromtimestamp(tick["timestamp"] / 1000)
        if ltp_now is None:
            ltp_now = tick["ltp"]
        if ts <= window_start:
            ltp_then = tick["ltp"]
            then_timestamp = ts
            break

    if ltp_now and ltp_then:
        pct_change = ((ltp_now - ltp_then) / ltp_then) * 100
        return {
            "ltp_now": ltp_now,
            "ltp_then": ltp_then,
            "pct_change": pct_change,
            "timestamp": then_timestamp.strftime("%Y-%m-%d %H:%M:%S")
        }

    return None