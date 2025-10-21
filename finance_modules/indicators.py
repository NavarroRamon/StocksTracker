from ta.volatility import BollingerBands
from ta.momentum import RSIIndicator
import pandas as pd
from ta.trend import ADXIndicator


def get_adx(df, window=14):
    adx_indicator = ADXIndicator(high=df['high'], low=df['low'], close=df['close'], window=window)
    df['adx'] = adx_indicator.adx()
    df['plus_di'] = adx_indicator.adx_pos()
    df['minus_di'] = adx_indicator.adx_neg()
    return df


def rsi(df, window=14):
    # Calcular RSI (por defecto usa window de 14)
    rsi = RSIIndicator(close=df['close'], window=window)
    # Agregar al DataFrame
    df['rsi'] = rsi.rsi()
    df.dropna(inplace=True)
    return df.reset_index(drop=True)


def es_minimo_local(serie: pd.Series, ventana: int) -> pd.Series:
    """
    Devuelve una Serie booleana indicando si el valor actual
    es el mínimo en las últimas `ventana` filas (incluyendo la actual).
    Args:
        serie: pd.Series de datos numéricos
        ventana: número de filas a considerar hacia atrás (rolling)
    Returns:
        pd.Series de tipo bool
    """
    return serie == serie.rolling(window=ventana, min_periods=ventana).min()


def get_quantiles(df, column='close', quantiles=(0.25, 0.5, 0.75)):
    try:
        if column not in df.columns:
            raise ValueError(f"Columna '{column}' no encontrada en el DataFrame.")
        values = df[column].dropna()
        return {f"q{int(q*100)}": round(values.quantile(q), 4) for q in quantiles}
    except Exception as e:
        print(f"[ERROR] Quantile error: {e}")
        return {f"q{int(q*100)}": 0 for q in quantiles}


def get_bollinger(df, col='close', window=20):
    bb = BollingerBands(close=df[col], window=window, window_dev=2)
    df['bb_upper'] = bb.bollinger_hband()
    df['bb_lower'] = bb.bollinger_lband()
    return df
