import os
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("BOT_TOKEN")
PASSWORD = os.getenv("PASSWORD")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))

if not TOKEN:
    raise RuntimeError("BOT_TOKEN is not set")
