import sqlite3
from datetime import datetime
import os
from dotenv import load_dotenv
load_dotenv()

from db_config import DB_TYPE
import mysql.connector

def log_user_session(email):
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
            "INSERT INTO user_sessions (email, login_time) VALUES (%s, %s)",
            (email, timestamp)
        )
        conn.commit()
        cursor.close()
        conn.close()
    else:
        conn = sqlite3.connect(os.getenv("SQLITE_PATH", "data/user_prompts.db"))
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_sessions (
                email TEXT,
                login_time TEXT
            )
        """)
        cursor.execute("INSERT INTO user_sessions VALUES (?, ?)", (email, timestamp))
        conn.commit()
        conn.close()

def get_all_sessions():
    conn = sqlite3.connect(os.getenv("SQLITE_PATH", "data/user_prompts.db"))
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM user_sessions ORDER BY login_time DESC LIMIT 50")
    rows = cursor.fetchall()
    conn.close()
    return rows