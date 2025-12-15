from telegram import Update
from telegram.ext import ContextTypes
from bot.services.db import get_all_users
from bot.config.settings import ADMIN_ID


async def menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    user_id = update.effective_user.id
    action = query.data

    # ---- Обычные кнопки меню ----
    if action == "markets":
        await query.edit_message_text("📈 Раздел рынков — скоро добавим функционал.")

    elif action == "alerts":
        await query.edit_message_text("🔔 Раздел алёртов — в разработке.")

    elif action == "settings":
        await query.edit_message_text("⚙ Настройки — будут позже.")

    # ---- Админ-секция ----
    elif action == "admin_users":
        if user_id != ADMIN_ID:
            await query.edit_message_text("⛔ У вас нет прав.")
            return

        users = get_all_users()
        if not users:
            await query.edit_message_text("Пользователи не найдены.")
            return

        text = "👥 *Список пользователей:*\n\n"
        for uid, role, ts in users:
            text += f"• `{uid}` — {role} — {ts}\n"

        await query.edit_message_text(text, parse_mode="Markdown")

    # ---- Неизвестная команда ----
    else:
        await query.edit_message_text("❓ Неизвестная команда.")
