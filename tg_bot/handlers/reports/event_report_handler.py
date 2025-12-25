from __future__ import annotations

import sys
from pathlib import Path
from datetime import datetime, timezone

from telegram import Update
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


async def handle_event_report_url(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get("waiting_for_event_url"):
        return

    user_id = update.effective_user.id
    if not is_authorized(user_id):
        context.user_data["waiting_for_event_url"] = False
        await update.message.reply_text("Сначала /start и авторизация.")
        return

    text = (update.message.text or "").strip()
    context.user_data["waiting_for_event_url"] = False

    try:
        ev = resolve_event(text)  # принимает и ссылку, и slug
    except Exception as e:
        await update.message.reply_text(f"Не похоже на event-ссылку. Ошибка: {e}")
        return

    await update.message.reply_text("Ок, собираю отчёт...")

    try:
        as_of = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        report = aggregate_event(
            ev,
            iter_event_trades(ev.event_id, limit=1000, taker_only=False),
            as_of_utc=as_of,
        )

        out_dir = PROJECT_ROOT / "out"
        out_path = out_dir / f"event_{ev.slug}.xlsx"
        export_event_report_xlsx(event=ev, report=report, out_path=str(out_path))

        with open(out_path, "rb") as f:
            await update.message.reply_document(
                document=f,
                filename=out_path.name,
                caption=f"{ev.title}",
            )

        await update.message.reply_text("Меню:", reply_markup=build_main_menu(user_id))

    except Exception as e:
        await update.message.reply_text(f"Ошибка при генерации отчёта: {e}")
