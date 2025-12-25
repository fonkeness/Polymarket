from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from datetime import datetime, timezone

from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import ContextTypes

from tg_bot.services.db import is_authorized
from tg_bot.handlers.menu.main_menu import build_main_menu

# чтобы импортировать Polymarket_client/app/...
PROJECT_ROOT = Path(__file__).resolve().parents[3]
POLYMARKET_CLIENT_DIR = PROJECT_ROOT / "Polymarket_client"
if str(POLYMARKET_CLIENT_DIR) not in sys.path:
    sys.path.insert(0, str(POLYMARKET_CLIENT_DIR))

from app.ingestion.event_resolver import resolve_event
from app.ingestion.trades_loader import iter_event_trades
from app.services.event_aggregator import aggregate_event
from app.reporting.excel_exporter import export_event_report_xlsx


def _cancel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="cancel_report")]])


def _build_report_sync(event_url_or_slug: str) -> tuple[str, str, str]:
    """
    Синхронная тяжёлая часть (requests + агрегация + excel).
    Возвращает: (event_title, event_slug, путь к xlsx)
    """
    ev = resolve_event(event_url_or_slug)

    as_of = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    report = aggregate_event(
        ev,
        iter_event_trades(ev.event_id, limit=1000, taker_only=False),  # limit = размер страницы, пагинация до конца
        as_of_utc=as_of,
    )

    out_dir = PROJECT_ROOT / "out"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"event_{ev.slug}_{int(datetime.now().timestamp())}.xlsx"

    export_event_report_xlsx(event=ev, report=report, out_path=str(out_path))
    return ev.title, ev.slug, str(out_path)


async def _run_report_job(
    *,
    chat_id: int,
    user_id: int,
    event_url_or_slug: str,
    context: ContextTypes.DEFAULT_TYPE,
):
    try:
        # ВАЖНО: уводим всё тяжёлое в отдельный поток, чтобы бот не зависал
        title, slug, out_path = await asyncio.to_thread(_build_report_sync, event_url_or_slug)

        # если успели отменить — просто выходим (даже если файл уже сделалcя)
        task = context.user_data.get("report_task")
        if task and task.cancelled():
            return

        with open(out_path, "rb") as f:
            await context.bot.send_document(
                chat_id=chat_id,
                document=f,
                filename=Path(out_path).name,
                caption=title,
            )

        # меню назад
        await context.bot.send_message(chat_id=chat_id, text="Меню:", reply_markup=build_main_menu(user_id))

        # можно чистить файл (если хочешь сохранять — убери это)
        try:
            Path(out_path).unlink(missing_ok=True)
        except Exception:
            pass

    except asyncio.CancelledError:
        # отменили кнопкой
        await context.bot.send_message(chat_id=chat_id, text="Ок, отменено.")
        raise
    except Exception as e:
        await context.bot.send_message(chat_id=chat_id, text=f"Ошибка при генерации отчёта: {e}")
    finally:
        # чистим state
        if context.user_data.get("report_task"):
            context.user_data["report_task"] = None


async def handle_event_report_url(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get("waiting_for_event_url"):
        return

    user_id = update.effective_user.id
    chat_id = update.effective_chat.id

    # сбрасываем режим ожидания ссылки
    context.user_data["waiting_for_event_url"] = False

    if not is_authorized(user_id):
        await update.message.reply_text("Сначала /start и авторизация.")
        return

    text = (update.message.text or "").strip()

    # если у пользователя уже идёт отчёт — сначала отменим/не дадим запускать второй
    prev_task = context.user_data.get("report_task")
    if prev_task and not prev_task.done():
        await update.message.reply_text("У тебя уже идёт отчёт. Нажми ❌ Отмена под сообщением.")
        return

    # Быстрая валидация (resolve_event тоже синхронный и тяжёлый — не делаем тут)
    if "polymarket.com/event/" not in text and not text:
        await update.message.reply_text("Скинь ссылку на событие (формат: https://polymarket.com/event/...)")
        return

    await update.message.reply_text("Ок, собираю отчёт…", reply_markup=_cancel_kb())

    # стартуем фоновую задачу
    task = asyncio.create_task(
        _run_report_job(
            chat_id=chat_id,
            user_id=user_id,
            event_url_or_slug=text,
            context=context,
        )
    )
    context.user_data["report_task"] = task
