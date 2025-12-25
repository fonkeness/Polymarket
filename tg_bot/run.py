from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters

from tg_bot.services.db import init_db, set_role
from tg_bot.config.settings import TOKEN, ADMIN_ID
from tg_bot.handlers.auth.auth_handler import start
from tg_bot.handlers.menu.callbacks import menu_callback
from tg_bot.handlers.text_router import text_router


def main():
    init_db()
    set_role(ADMIN_ID, "admin")

    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))

    # вместо handle_password напрямую — роутер
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_router))

    app.add_handler(CallbackQueryHandler(menu_callback))

    print("Бот запущен..")
    app.run_polling()


if __name__ == "__main__":
    main()
