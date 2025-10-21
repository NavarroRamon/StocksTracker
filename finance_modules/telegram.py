import requests
import os
from dotenv import load_dotenv

load_dotenv()

def send_telegram(msg, TOKEN=os.getenv('TELEGRAM_TOKEN'), CHAT_ID=os.getenv('TELEGRAM_CHAT')):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    data = {"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"}
    try:
        requests.post(url, data=data, timeout=5)
    except Exception as e:
        print(f"Error al enviar Telegram: {e}")