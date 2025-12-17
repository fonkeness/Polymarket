import sqlite3
from pathlib import Path

DB_PATH = Path("tg_bot.db")


def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS authorized_users (
            user_id INTEGER PRIMARY KEY,
            role TEXT DEFAULT 'user',
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.commit()
    conn.close()


def authorize(user_id: int):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute(
        "INSERT OR IGNORE INTO authorized_users (user_id) VALUES (?)",
        (user_id,)
    )

    conn.commit()
    conn.close()


def is_authorized(user_id: int) -> bool:
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute(
        "SELECT user_id FROM authorized_users WHERE user_id = ?",
        (user_id,)
    )

    row = cursor.fetchone()
    conn.close()

    return row is not None


def set_role(user_id: int, role: str):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute(
        "UPDATE authorized_users SET role = ? WHERE user_id = ?",
        (role, user_id)
    )

    conn.commit()
    conn.close()


def get_all_users():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("SELECT user_id, role, timestamp FROM authorized_users")
    rows = cursor.fetchall()

    conn.close()
    return rows
