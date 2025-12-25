from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from tg_bot.config.settings import ADMIN_ID

def build_main_menu(user_id: int):
    keyboard = [
        [
            InlineKeyboardButton("📈 Мои рынки", callback_data="markets"),
            InlineKeyboardButton("🔔 Алёрты", callback_data="alerts")
        ],
        [
            InlineKeyboardButton("📄 Отчёт по событию", callback_data="event_report")
        ],
        [
            InlineKeyboardButton("⚙ Настройки", callback_data="settings")
        ]
    ]
    if user_id == ADMIN_ID:
        keyboard.append([
            InlineKeyboardButton("👥 Пользователи", callback_data="admin_users")
        ])

    return InlineKeyboardMarkup(keyboard)
