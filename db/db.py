# sqlite_read.py
import pandas as pd
import sqlite3
from pathlib import Path
from typing import Iterable, Tuple

DEFAULT_DB_PATH = Path("db/trading_data.db")

def create_db(path: Path = DEFAULT_DB_PATH):
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    # Tabla única para múltiples timeframes; PK compuesto evita duplicados
    cur.execute("""
    CREATE TABLE IF NOT EXISTS candles (
        symbol TEXT NOT NULL,
        interval TEXT NOT NULL,
        open_time INTEGER NOT NULL,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume REAL,
        rsi REAL,
        local_time TEXT,
        PRIMARY KEY(symbol, interval, open_time)
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS alerts (
    symbol TEXT,
    interval TEXT,
    open_time INTEGER,
    alert_type TEXT,
    PRIMARY KEY(symbol, interval, open_time, alert_type)
    );""")
    conn.commit()
    conn.close()
    print(f"DB creada / verificada en {path}")


INSERT_SQL = """
INSERT INTO candles (
    symbol, interval, open_time, open, high, low, close, volume, rsi, local_time 
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(symbol, interval, open_time) DO UPDATE SET
    open=excluded.open,
    high=excluded.high,
    low=excluded.low,
    close=excluded.close,
    volume=excluded.volume,
    rsi=excluded.rsi,
    local_time=excluded.local_time;
"""

def insert_candles(records: Iterable[Tuple], db_path: Path = DEFAULT_DB_PATH, batch_size=500):
    """
    records: iterable de tuplas (symbol, interval, open_time_epoch_ms, open, high, low, close, volume, close_time_epoch_ms, rsi)
    """
    conn = sqlite3.connect(db_path, timeout=30)
    cur = conn.cursor()
    # mejora rendimiento
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")
    batch = []
    count = 0
    for r in records:
        batch.append(r)
        if len(batch) >= batch_size:
            cur.executemany(INSERT_SQL, batch)
            conn.commit()
            count += len(batch)
            batch = []
    if batch:
        cur.executemany(INSERT_SQL, batch)
        conn.commit()
        count += len(batch)
    conn.close()
    return count


def read_candles(symbol: str, interval: str, start_open_time=None, end_open_time=None, db_path: Path = DEFAULT_DB_PATH):
    conn = sqlite3.connect(db_path)
    query = "SELECT * FROM candles WHERE symbol=? AND interval=?"
    params = [symbol, interval]
    if start_open_time is not None:
        query += " AND open_time >= ?"
        params.append(start_open_time)
    if end_open_time is not None:
        query += " AND open_time <= ?"
        params.append(end_open_time)
    query += " ORDER BY open_time ASC"
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df

def get_latest_open_time(symbol, timeframe, db_path: Path = DEFAULT_DB_PATH):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("""
        SELECT MAX(open_time) FROM candles
        WHERE symbol=? AND interval=?
    """, (symbol, timeframe))
    row = cur.fetchone()
    return row[0] if row[0] is not None else 0


create_db()
