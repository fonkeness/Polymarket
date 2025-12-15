from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import ContextTypes

from bot.services.db import is_authorized, authorize
from bot.config.settings import PASSWORD
from bot.handlers.menu.main_menu import build_main_menu


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if is_authorized(user_id):
        await update.message.reply_text("Дарова хуесос",
                                        reply_markup=build_main_menu(user_id))
        return

    await update.message.reply_text("Введите пароль для доступа:")
    context.user_data["waiting_for_password"] = True


async def handle_password(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get("waiting_for_password"):
        return

    user_input = update.message.text
    user_id = update.effective_user.id

    if user_input == PASSWORD:
        authorize(user_id)
        context.user_data["waiting_for_password"] = False

        await update.message.reply_text(
            "Пароль верный! Доступ открыт.",
            reply_markup=build_main_menu(user_id)
        )
    else:
        await update.message.reply_text("❌ Неверный пароль, попробуйте ещё раз.")
