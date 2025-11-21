from datetime import datetime
import pandas as pd
from finance_modules.tickers import acciones
from finance_modules.telegram import send_telegram
from finance_modules.data import get_ohlcv
from db.db import create_db, insert_candles, get_latest_open_time
from pathlib import Path

DB_PATH = Path("db/trading_data.db")

def ensure_db():
    create_db()  # crea si no existe

def df_to_records(df: pd.DataFrame, symbol: str, interval: str, epoch_ms=True):
    """
    normaliza dataframe y devuelve tuplas para insertar
    Espera columnas: open_time, open, high, low, close, volume, rsi
    """
    df2 = df.copy()
    # Asegurar open_time en epoch ms (entero)
    if pd.api.types.is_datetime64_any_dtype(df2['open_time']):
        df2['open_time'] = (df2['open_time'].astype('int64') // 1_000_000)  # ns -> ms
    else:
        # si ya está en segundos, convertir a ms si es necesario
        # asumimos que viene en ms o s; heurística: si valores < 1e11 -> segundos
        sample = int(df2['open_time'].iloc[0])
        if sample < 1e11:
            df2['open_time'] = df2['open_time'].astype(int) * 1000

    records = []
    for _, row in df2.iterrows():
        rec = (
            symbol,
            interval,
            int(row['open_time']),
            float(row.get('open', None)) if not pd.isna(row.get('open', None)) else None,
            float(row.get('high', None)) if not pd.isna(row.get('high', None)) else None,
            float(row.get('low', None)) if not pd.isna(row.get('low', None)) else None,
            float(row.get('close', None)) if not pd.isna(row.get('close', None)) else None,
            float(row.get('volume', None)) if not pd.isna(row.get('volume', None)) else None,
            float(row.get('rsi', None)) if not pd.isna(row.get('rsi', None)) else None,
            row.get('local_time')
        )
        records.append(rec)
    return records


def check_stocks_time():
    if datetime.now().hour >= 18 or datetime.now().hour < 2:
        return True
    elif datetime.now().weekday() in [5,6]:
        return True
    return False

def rsi(df, column='close', period=14):
    # Validación mínima para evitar cálculos basura
    if column not in df.columns:
        raise ValueError(f"La columna '{column}' no existe en el DataFrame.")
    if len(df) < period + 1:
        raise ValueError("No hay suficientes datos para calcular el RSI.")

    delta = df[column].diff()

    # Separar ganancias y pérdidas
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    # Media móvil exponencial según Wilder (similar a EMA con alpha = 1/period)
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))

    df.insert(0, 'rsi', rsi)
    df.dropna(inplace=True)
    return df

import time
import sqlite3

def should_fetch(timeframe, last_open_time_ms):
    interval_min = TIMEFRAME_MIN[timeframe]
    interval_ms = interval_min * 60 * 1000

    # Tiempo actual redondeado hacia abajo
    now_ms = int(time.time() * 1000)

    # Si no ha pasado suficiente tiempo, no hay vela nueva
    return (now_ms - last_open_time_ms) >= interval_ms


def candle_is_final(interval, open_time_ms):
    """Verifica si la vela ya cerró según la hora actual en UTC."""
    from datetime import datetime, timezone
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    duration = TIMEFRAME_MIN[interval] * 60 * 1000
    return now_ms >= open_time_ms + duration

def check_rsi_alerts():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # seleccionar la vela más reciente por simbolo/timeframe
    cur.execute("""
        SELECT c.*
        FROM candles c
        WHERE (symbol, interval, open_time) IN (
            SELECT symbol, interval, MAX(open_time)
            FROM candles
            WHERE open_time < (
                SELECT MAX(open_time)
                FROM candles c2
                WHERE c2.symbol = candles.symbol
                  AND c2.interval = candles.interval
            )
            GROUP BY symbol, interval
        );
    """)

    rows = cur.fetchall()

    for row in rows:
        symbol = row["symbol"]
        interval = row["interval"]
        rsi = row["rsi"]
        open_time = row["open_time"]
        local_time = row['local_time']

        # 1. Solo velas cerradas
        if not candle_is_final(interval, open_time):
            continue

        # 2. Solo si RSI está por debajo del umbral
        if rsi is None or rsi >= 30:
            continue

        # 3. Verificar si ya se envió una alerta para esta vela
        cur.execute("""
            SELECT 1 FROM alerts
            WHERE symbol=? AND interval=? AND open_time=? AND alert_type='RSI_UNDER_30'
        """, (symbol, interval, open_time))

        if cur.fetchone():
            # Ya enviada → evitar spam
            continue

        # 4. Enviar alerta
        message = f"⚠️ RSI {symbol} {interval}\n{local_time}\nRSI = {rsi:.2f}"
        send_telegram(message)

        # 5. Registrar alerta para evitar duplicados
        cur.execute("""
            INSERT INTO alerts(symbol, interval, open_time, alert_type)
            VALUES (?, ?, ?, 'RSI_UNDER_30')
        """, (symbol, interval, open_time))

    conn.commit()
    conn.close()

TIMEFRAME_MIN = {
    '1m': 1,
    '3m': 3,
    '5m': 5,
    '15m': 15,
    '1h': 60,
    '4h': 240,
    '1d': 1440,
    '1w': 10080,  # 7 días
}

if __name__ == "__main__":
    symbols = ['SOL/USDT', 'BTC/USDT'] #, 'VIRTUAL/USDT']
    stocks = acciones
    trackeo = symbols + stocks
    print(f"Script iniciado: trackeo hibrido {trackeo}")

    for activo in symbols:
        # Operando a frecuencia variable
        for timeframe in ['1h', '1d', '4h', '15m', '5m', '3m', '1w']:
            try:
                # 1. Obtener última vela guardada
                base = activo.split('/')[0]
                last_open = get_latest_open_time(base, timeframe)

                # 2. Verificar si ya deberías pedir nuevas velas
                if last_open != 0 and not should_fetch(timeframe, last_open):
                    continue

                try:
                    df = get_ohlcv(symbol=activo, timeframe=timeframe, limit=30)
                except: send_telegram(f'error al buscar informacion de {activo}'); continue
                df = rsi(df)
                records = df_to_records(df, activo.split('/')[0], timeframe)
                inserted = insert_candles(records)
            except Exception as e:
                print(e)

    # después de procesar todos los symbols/timeframes:
    check_rsi_alerts()