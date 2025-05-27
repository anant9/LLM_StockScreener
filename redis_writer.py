import json
import os
import threading
from smartapi.smartWebSocketV2 import SmartWebSocketV2
from redis import Redis
from dotenv import load_dotenv

load_dotenv()

# Redis connection
redis_client = Redis(
    host=os.getenv("REDIS_HOST"),
    port=int(os.getenv("REDIS_PORT")),
    password=os.getenv("REDIS_PASSWORD"),
    decode_responses=True
)

# Token mapping and config
TOKEN_LIST = []
TOKEN_SYMBOL_MAP = {}

def set_tokens(symbol_token_map):
    global TOKEN_LIST, TOKEN_SYMBOL_MAP
    TOKEN_SYMBOL_MAP = symbol_token_map
    TOKEN_LIST = list(symbol_token_map.values())

# WebSocket handling
def on_message(wsapp, message):
    try:
        data = json.loads(message)["data"]
        token = str(data["token"])
        ltp = data["last_traded_price"] / 100.0
        symbol = [sym for sym, tok in TOKEN_SYMBOL_MAP.items() if tok == token]
        if symbol:
            redis_key = f"tickhist:{symbol[0]}"
            redis_client.rpush(redis_key, json.dumps({
                "ltp": ltp,
                "timestamp": data["exchange_time_stamp"]
            }))
            redis_client.ltrim(redis_key, -120, -1)  # Keep last N ticks
            redis_client.set(f"tick:{symbol[0]}", ltp)
    except Exception as e:
        print("Message parse error:", e)

def on_open(wsapp):
    wsapp.subscribe(TOKEN_LIST)

def on_error(wsapp, error):
    print("WebSocket error:", error)

def on_close(wsapp):
    print("WebSocket closed")

def start_socket(api_key, client_code, jwt_token):
    sws = SmartWebSocketV2(api_key, client_code, jwt_token)
    sws.on_open = on_open
    sws.on_message = on_message
    sws.on_error = on_error
    sws.on_close = on_close
    threading.Thread(target=sws.connect).start()