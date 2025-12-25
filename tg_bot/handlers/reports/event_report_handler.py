from __future__ import annotations

import asyncio
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional, Tuple

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


def _build_report_file(
    event_url: str,
    user_id: int,
    progress_cb: Optional[Callable[[int], None]],
) -> Tuple[Path, str, int, int]:
    """
    Запускается в отдельном треде.
    progress_cb вызывается каждые N сделок (настраивается в aggregate_event).
    """
    ev = resolve_event(event_url)

    as_of = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    report = aggregate_event(
        ev,
        iter_event_trades(ev.event_id, limit=1000, taker_only=False),
        as_of_utc=as_of,
        progress_cb=progress_cb,
        progress_every=500,
    )

    out_dir = PROJECT_ROOT / "out"
    out_dir.mkdir(parents=True, exist_ok=True)

    # уникальное имя, чтобы два юзера не перетёрли файл
    stamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"event_{ev.slug}_{user_id}_{stamp}.xlsx"

    export_event_report_xlsx(event=ev, report=report, out_path=str(out_path))
    return out_path, ev.title, report.total_trades, report.unique_traders


async def handle_event_report_url(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get("waiting_for_event_url"):
        return

    user_id = update.effective_user.id
    if not is_authorized(user_id):
        context.user_data["waiting_for_event_url"] = False
        await update.message.reply_text("Сначала /start и авторизация.")
        return

    event_url = (update.message.text or "").strip()
    context.user_data["waiting_for_event_url"] = False

    status_msg = await update.message.reply_text("Собираю отчёт...")

    chat_id = update.effective_chat.id
    msg_id = status_msg.message_id
    loop = asyncio.get_running_loop()

    async def _edit_status(text: str):
        try:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=msg_id,
                text=text,
            )
        except Exception:
            # игнорируем ошибки редактирования (например, слишком часто)
            pass

    last_shown = {"n": 0}

    def progress_cb(n: int):
        # Этот коллбек вызывается из треда.
        if n <= last_shown["n"]:
            return
        last_shown["n"] = n
        try:
            asyncio.run_coroutine_threadsafe(
                _edit_status(f"Обработано: {n} сделок..."),
                loop,
            )
        except Exception:
            pass

    out_path: Path | None = None
    try:
        # Генерим файл в отдельном треде (не блокируем event loop бота)
        out_path, title, trades_cnt, traders_cnt = await asyncio.to_thread(
            _build_report_file,
            event_url,
            user_id,
            progress_cb,
        )

        await _edit_status(f"Готово. trades={trades_cnt}, traders={traders_cnt}. Отправляю файл...")

        with open(out_path, "rb") as f:
            await update.message.reply_document(
                document=f,
                filename=out_path.name,
                caption=title,
            )

        await _edit_status("Отправлено.")
        await update.message.reply_text("Меню:", reply_markup=build_main_menu(user_id))

    except Exception as e:
        await _edit_status(f"Ошибка: {e}")

    finally:
        if out_path and out_path.exists():
            try:
                out_path.unlink()
            except Exception:
                pass
