from telegram import Update
from telegram.ext import ContextTypes

from tg_bot.services.db import get_all_users, is_authorized
from tg_bot.config.settings import ADMIN_ID


async def menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    user_id = update.effective_user.id
    action = query.data

    # защита от ручных callback-ов
    if not is_authorized(user_id):
        await query.edit_message_text("Сначала /start и авторизация.")
        return

    if action == "markets":
        await query.edit_message_text("📈 Раздел рынков — скоро добавим функционал.")

    elif action == "alerts":
        await query.edit_message_text("🔔 Раздел алёртов — в разработке.")

    elif action == "settings":
        await query.edit_message_text("⚙ Настройки — будут позже.")

    elif action == "event_report":
        context.user_data["waiting_for_event_url"] = True
        await query.edit_message_text("Скинь ссылку на событие (формат: https://polymarket.com/event/...)")

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
    elif action == "cancel_report":
        # отмена генерации отчёта
        task = context.user_data.get("report_task")
        context.user_data["waiting_for_event_url"] = False

        if task and not task.done():
            task.cancel()

        await query.edit_message_text("Ок, отменил.")
        from tg_bot.handlers.menu.main_menu import build_main_menu
        await query.message.reply_text("Меню:", reply_markup=build_main_menu(user_id))
        return

    else:
        await query.edit_message_text("❓ Неизвестная команда.")
