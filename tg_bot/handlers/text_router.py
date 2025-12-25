from telegram import Update
from telegram.ext import ContextTypes

from tg_bot.handlers.auth.auth_handler import handle_password
from tg_bot.handlers.reports.event_report_handler import handle_event_report_url


async def text_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.user_data.get("waiting_for_password"):
        return await handle_password(update, context)

    if context.user_data.get("waiting_for_event_url"):
        return await handle_event_report_url(update, context)

    # если ни один режим не активен — игнор
    return
