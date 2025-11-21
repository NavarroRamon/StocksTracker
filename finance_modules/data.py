import numpy as np
import pandas as pd
import ccxt
from datetime import datetime, timedelta

import yfinance as yf
import pandas as pd
import numpy as np


def lowercase_columns(df):
    """
    Convierte todos los nombres de columnas de un DataFrame a minúsculas.
    """
    df.columns = [col.lower() for col in df.columns]
    return df

def get_stock_ohlcv(symbol='AAPL', interval='1m', period='1mo'):
    """
    Descarga datos OHLCV de una acción usando yfinance.
    - ticker: símbolo bursátil (por ejemplo, 'AAPL' para Apple)
    - interval: resolución ('1m', '5m', '15m', '1h', '1d', etc.)
    - period: cuánto tiempo atrás ('1d', '5d', '1mo', '3mo', etc.)
    """
    df = yf.download(tickers=symbol, interval=interval, period=period, progress=False, auto_adjust=True)
    df.reset_index(inplace=True)
    df.rename(columns={'Date': 'timestamp', 'Close': 'close'}, inplace=True)
    df['returns'] = np.log(df['close'] / df['close'].shift(1))
    df['symbol'] = symbol
    df.columns = [col[0] if col[1] == '' else f"{col[0]}" for col in df.columns]
    df = lowercase_columns(df)
    df.dropna(inplace=True)
    return df


# Obtener datos mediante API
def get_ohlcv(symbol='DYDX/USDT', timeframe='1m', limit=500):
    exchange = ccxt.binance()
    ohlcv = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['open_time'] = df['timestamp']
    df['returns'] = np.log(df['close'] / df['close'].shift(1))
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    # Ajustando la zona horaria a local
    df['local_time'] = (
        df['timestamp']
        .dt.tz_localize('UTC')
        .dt.tz_convert('America/Mexico_City')
        .dt.strftime('%Y-%m-%d %H:%M')
    )
    # Registra que symbol se trabajo
    df['symbol'] = symbol
    df['interval'] = timeframe
    df.dropna(inplace=True)
    return df


def get_historical_data(symbol, timeframe, since_datetime):
    exchange = ccxt.binance()
    since = exchange.parse8601(since_datetime.isoformat() + 'Z')
    bars = []

    stop = 0
    while True:
        batch = exchange.fetch_ohlcv(symbol, timeframe, since, limit=700)
        if not batch:
            break
        bars += batch
        since = batch[-1][0] + 1
        stop +=1
        # Cortar si pasamos la fecha actual
        if since > exchange.milliseconds():
            break

    df = pd.DataFrame(bars, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

    # Procesamiento
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
    df['timestamp_local'] = df['timestamp'].dt.tz_convert('America/Mexico_City')
    df['returns'] = np.log(df['close'] / df['close'].shift(1))
    df['symbol'] = symbol
    df['id'] = df['timestamp'].astype('int64') // 10 ** 6

    # Crear columnas de año, mes y día a partir del timestamp_local
    df['year'] = df['timestamp'].dt.year
    df['month'] = df['timestamp'].dt.month
    df['day'] = df['timestamp'].dt.day
    df['fecha'] = df['timestamp'].dt.date
    return df


import os
import glob
import pandas as pd
from tqdm import tqdm



def extract_metadata_from_path(path):
    """Extrae symbol, timelapse, fecha desde la ruta del archivo"""
    parts = path.split(os.sep)
    symbol = parts[1]
    timelapse = parts[2]
    year, month, day = parts[3:6]
    return symbol, timelapse, f"{year}-{month}-{day}"


def load_from_db(symbol="*", lapse="*", year="*", month="*", day="*"):
    """Carga todos los archivos .parquet de la estructura completa"""
    pattern = os.path.join(os.getcwd(), 'db', symbol, lapse, year, month, day, "*.parquet")
    files = glob.glob(pattern, recursive=True)
    all_dfs = []
    print(f"Cargando {len(files)} archivos...")

    for path in tqdm(files):
        df = pd.read_parquet(path)

        if 'timestamp' in df.columns:
            df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
        elif 'date' in df.columns:
            df['datetime'] = pd.to_datetime(df['date'])

        df['hour'] = df['datetime'].dt.hour
        df['minute'] = df['datetime'].dt.minute

        df = df.reset_index(drop=True)
        all_dfs.append(df)

    if not all_dfs:
        print("No se encontraron archivos.")
        return pd.DataFrame()
    df = pd.concat(all_dfs, ignore_index=True)
    df = df.sort_values('datetime').reset_index(drop=True)
    return df