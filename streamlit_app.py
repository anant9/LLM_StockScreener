import streamlit as st
import pandas as pd
from gpt_parser import parse_prompt
from screener_engine import run_screener
from prompt_logger import log_prompt
from user_sessions import log_user_session, get_all_sessions
from db_config import DB_TYPE
from redis_reader import redis_client
import os 

st.set_page_config(page_title="LLM Stock Screener", layout="wide")

st.title("LLM-Powered Stock Screener")

email = st.text_input("Enter your email to continue:", "")
if email:
    log_user_session(email)
    prompt = st.text_area("Enter your natural language prompt:")
    if st.button("Run Screener") and prompt:
        parsed = parse_prompt(prompt)
        df = run_screener(parsed)
        log_prompt(email, prompt, parsed)
        st.success(f"Showing results for: {prompt}")
        st.dataframe(df)

    with st.expander("Prompt Logs (Admin Only)"):
        if email == os.getenv("ADMIN_EMAIL", "anantgoel09admin@gmail.com"):
            logs = get_all_sessions()
            st.subheader("Recent User Sessions")
            st.dataframe(pd.DataFrame(logs, columns=["Email", "Login Time"]))
            from prompt_logger import get_prompt_logs
            st.subheader("Recent Prompts")
            st.dataframe(pd.DataFrame(get_prompt_logs(), columns=["Email", "Prompt", "Parsed", "Timestamp"]))

    with st.expander("Redis Tick Cache (Debug)"):
        symbol = st.text_input("Symbol for tickhist (e.g., SBIN)")
        if symbol:
            key = f"tickhist:{symbol.upper()}"
            ticks = redis_client.lrange(key, 0, -1)
            st.write(f"Found {len(ticks)} entries for {symbol}")
            st.json(ticks[-5:])