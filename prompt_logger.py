import sqlite3
from datetime import datetime
import os
from db_config import DB_TYPE
import mysql.connector

def log_prompt(email, prompt, parsed):
    timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    if DB_TYPE == "mysql":
        conn = mysql.connector.connect(
            host=os.getenv("MYSQL_HOST"),
            port=int(os.getenv("MYSQL_PORT")),
            user=os.getenv("MYSQL_USER"),
            password=os.getenv("MYSQL_PASSWORD"),
            database=os.getenv("MYSQL_DATABASE")
        )
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO prompt_logs (email, prompt, parsed, timestamp) VALUES (%s, %s, %s, %s)",
            (email, prompt, str(parsed), timestamp)
        )
        conn.commit()
        cursor.close()
        conn.close()
    else:
        conn = sqlite3.connect(os.getenv("SQLITE_PATH", "data/user_prompts.db"))
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS prompt_logs (
                email TEXT,
                prompt TEXT,
                parsed TEXT,
                timestamp TEXT
            )
        """)
        cursor.execute("INSERT INTO prompt_logs VALUES (?, ?, ?, ?)", (email, prompt, str(parsed), timestamp))
        conn.commit()
        conn.close()

def get_prompt_logs():
    conn = sqlite3.connect(os.getenv("SQLITE_PATH", "data/user_prompts.db"))
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM prompt_logs ORDER BY timestamp DESC LIMIT 50")
    logs = cursor.fetchall()
    conn.close()
    return logs