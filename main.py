# Utils
import time
from datetime import datetime

# Estadisticos de data
import numpy as np

# Data
from modules.file_value import read_value, write_value, delete_value
from finance_modules.tickers import acciones
# Alertas
from finance_modules.discord import send_discord
from finance_modules.telegram import send_telegram
# modulos
from finance_modules.indicators import get_quantiles, get_bollinger, es_minimo_local, rsi, get_adx
from finance_modules.data import get_ohlcv, get_stock_ohlcv

# Config
from dotenv import load_dotenv
import os
load_dotenv()
sound = os.getenv('SOUND')
loop = os.getenv('LOOP', False)
if sound == 1:
    import pyttsx3
    engine = pyttsx3.init()


def alerta_rsi(df):
    # Calcula RSI
    df = rsi(df,14)

    # Aplicar bollinger
    df = get_bollinger(df)
    df['minimo_local'] = es_minimo_local(df['close'], 60)

    # Determina la condicion
    df['alerta_rsi'] = (df['rsi'] < 30) & (df['close'] < df['bb_lower'])
    return df

def alerta_percentil(serie, percentil, last_n=50):
    """
    :param serie: Data de entrada en formato de serie
    :param percentil: El percentil que se decea calcular
    :param last_n: La cantidad de rows hacia atras que se usan para el calculo
    :return: Serie con el valor del percentil_x para cada grupo de last_n datos
    """
    percentil_x = (serie.rolling(window=last_n, min_periods=last_n).apply(lambda x: np.percentile(x, percentil), raw=True))
    return percentil_x

def alerta_adx(df, window=14, threshold=25):
    # get calculations
    df = get_adx(df, window)
    # Lógica de alerta
    df['adx_fuerte'] = df['adx'] > threshold
    df['tendencia_alcista'] = (df['plus_di'] > df['minus_di']) & df['adx_fuerte']
    df['tendencia_bajista'] = (df['minus_di'] > df['plus_di']) & df['adx_fuerte']
    return df

def alerta_z(series, n_media_lenta, n_desv):
    # Obtener medias moviles
    media_lenta = series.rolling(window=n_media_lenta).mean()
    # Calcula desviacion estandar y z-score
    desvest = series.rolling(n_desv).std()
    z_score = (series-media_lenta)/desvest
    return z_score

def rsi_multi_tf_cripto_check(symbol, timeframes=('1m', '30m', '1h', '4h', '1d'), threshold=30, window=14):
    rsi_result = {}
    alerta = False
    num_alertas = 0
    for tf in timeframes:
        try:
            df_tf = get_ohlcv(symbol=symbol, timeframe=tf, limit=window+1)
            df_tf = rsi(df_tf, window)  # Aplica tu función rsi que agrega la columna 'rsi'
            current_rsi = round(df_tf['rsi'].iloc[-1], 2)
            if current_rsi < threshold and tf != '1m':
                num_alertas +=1
            rsi_result[tf] = current_rsi
        except Exception as e:
            continue
    if num_alertas >=1:
        alerta = True
    if alerta == False:
        return False, rsi_result
    return True, rsi_result


def rsi_multi_tf_stock_check(symbol, timeframes=None, threshold=30, window=14):
    """
    Calcula el RSI de un símbolo en múltiples temporalidades y genera una alerta si alguno cae por debajo del umbral.
    Args:
        symbol (str): símbolo bursátil (ej. 'AAPL')
        timeframes (dict): diccionario con intervalos y periodos. Ej: {'1m': '1d', '15m': '5d', '1h': '1mo', '1d': '6mo'}
        threshold (float): umbral de alerta para RSI.
        window (int): número de periodos para calcular RSI.
    Returns:
        (bool, dict): (alerta, {tf: valor_rsi})
    """
    if timeframes is None:
        timeframes = {'1m': '1d', '15m': '5d', '1h': '1mo', '1d': '6mo'}

    rsi_result = {}
    alerta = False

    for interval, period in timeframes.items():
        try:
            df_tf = get_stock_ohlcv(symbol=symbol, interval=interval, period=period)
            df_tf = rsi(df_tf, window)  # función rsi debe devolver df con columna 'rsi'
            current_rsi = round(df_tf['rsi'].iloc[-1], 2)
            rsi_result[interval] = current_rsi

            if current_rsi < threshold and interval != '1m':
                alerta = True

        except Exception as e:
            print(f"Error en {interval}: {e}")
            continue
    return alerta, rsi_result

def pct_diff(base, ref):
    return round(100 * (ref / base - 1), 1)

def format_msg(activo, close, quantiles):
    lines = []

    for temporalidad, qdict in quantiles.items():
        line = (
            f"Minimo global en {temporalidad}\n"
            f"{qdict['q5']:.2f} ({pct_diff(close, qdict['q5'])}) | "
            f"{qdict['q50']:.2f} ({pct_diff(close, qdict['q50'])}) | "
            f"{qdict['q75']:.2f} ({pct_diff(close, qdict['q75'])})"
        )
        lines.append(line)

    msg = "\n".join(lines)
    return msg


def check_stocks_time():
    """
    :return: boolean, True if
    """
    if datetime.now().hour >= 18 or datetime.now().hour < 2:
        return True
    elif datetime.now().weekday() in [5,6]:
        return True
    return False


if __name__ == "__main__":
    symbols = ['SOL/USDT'] #, 'VIRTUAL/USDT']
    stocks = acciones
    trackeo = symbols
    print(f"Script iniciado: trackeo hibrido {trackeo}")

    # Variables
    media_lenta = 20
    media_rapida = 7
    n_desv = 15
    z_param = -2
    last_n = 59

    while True:
        for activo in trackeo:
            activo_name = activo.split('/')[0]
            try:
                # ALTA FRECUENCIA OPERANDO CON 1Minuto de muestreo =============
                if activo in symbols:
                    df = get_ohlcv(symbol=activo, timeframe='3m', limit=1000)
                elif activo in stocks:
                    if check_stocks_time():
                        continue
                    df = get_stock_ohlcv(symbol=activo, interval='1m', period='1d')
            except: send_telegram(f'error al buscar informacion de {activo}'); continue

            # Calculos
            # Minimo percentiles: compra segun percentil
            df['percentil_05'] = alerta_percentil(df['close'], 5, last_n)
            df['percentil_50'] = alerta_percentil(df['close'],50, last_n)

            # Z-Score: regresion a la media
            df['z_score'] = alerta_z(df['close'], media_lenta, n_desv)

            # RSI: compra segun RSI
            df = alerta_rsi(df)

            # ADX: mide tendencia
            df = alerta_adx(df)

            # Data Actual
            row = df.iloc[-1]
            msg = f"**{activo_name}** **{row['close']:.2f}**\n"

            # Minimo historico en trackeo
            path = os.path.join('data', f"{activo_name}.txt")
            prev_value = read_value(path)
            if prev_value is None or row['close'] < float(prev_value) or activo in symbols:
                # MINIMO GLOBAL
                if activo in symbols:
                    quantiles_df = df
                    # Identifica la distribucion en las ultima hora ( para ver el rango)
                    q12 = get_quantiles(quantiles_df[250:], quantiles=(0.05, 0.5, 0.75))  # 1 dia
                    q24 = get_quantiles(quantiles_df[500:], quantiles=(0.05, 0.5, 0.75))  # 1 dia
                    q48 = get_quantiles(quantiles_df, quantiles=(0.05, 0.5, 0.75))  # 1 dia
                    q_corte = min([q12['q5'], q24['q5'], q48['q5']])
                else:
                    quantiles_df = get_stock_ohlcv(symbol=activo, interval='5m', period='1mo')
                    # Identifica la distribucion en las ultima hora ( para ver el rango)
                    q24 = get_quantiles(quantiles_df[500:], quantiles=(0.05, 0.5, 0.75))  # 1 dia
                    q48 = get_quantiles(quantiles_df, quantiles=(0.05, 0.5, 0.75))  # 1 dia
                    q_corte = min([q24['q5'], q48['q5']])
                if row['close'] < q_corte:
                    write_value(path, row['close'])
                    if activo in stocks:
                        msg += f"{format_msg(activo_name, row['close'], {'2W': q24, '1MO': q48})}\n"
                    elif activo in symbols:
                        msg += f"{format_msg(activo_name, row['close'], {'12': q12, '24':q24, '48':q48})}\n"
                    if sound == 1:
                        engine.say(f"{activo_name} mínimo global {int(row['close'])}")
                        engine.runAndWait()
                    if activo in symbols:
                        alerta, rsi_vals = rsi_multi_tf_cripto_check(activo)
                    else:
                        alerta, rsi_vals = rsi_multi_tf_stock_check(activo)
                    # Alerta RSI
                    # Devuelve RSI en temporalidades
                    for tf, val in rsi_vals.items():
                        msg += f"RSI: {val:.0f}: {tf}\n"
                    # Alerta ADX
                    if row.get('adx_fuerte', False):
                        if row['tendencia_alcista']:
                            q_vals = get_quantiles(df)
                            rango_total = q_vals['q75'] - q_vals['q25']
                            rango_libre = q_vals['q75'] - df['close'].iloc[-1]
                            # Normaliza
                            rango_libre_pct = rango_libre / rango_total
                            msg += (f"TENDENCIA ALCISTA {row['adx']:.2f}\n"
                                    f"RangoLibrePorcentual: {rango_libre_pct:.1f}, Q75: {q_vals['q75']:.2f}\n")
                        else:
                            msg += f"TENDENCIA BAJISTA {row['adx']:.2f}\n"
                    # Alerta Z-Score
                    if row.get('alerta_z', False):
                        msg += f"Z SCORE\nclose: {row['close']} media: {row['media_lenta']}\nValorZ: {row['z_score']}\n"

                    send_discord(msg)
                    send_telegram(msg)
        if not loop:
            print('Exiting code')
            break
        else:
            time.sleep(60*4)
