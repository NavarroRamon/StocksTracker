from discord_webhook import DiscordWebhook
from dotenv import load_dotenv
import os

load_dotenv()

def send_discord(message: str, name: str='BOT_El_Comprador'):
    webhook_url = os.getenv('DISCORD_WEBHOOK')
    webhook = DiscordWebhook(url=webhook_url, username=name)
    webhook.content = message
    webhook.execute()