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
    phase_cb: Optional[Callable[[str], None]],
) -> Tuple[Path, str, int, int]:
    if phase_cb:
        phase_cb("Резолвлю событие...")

    ev = resolve_event(event_url)

    if phase_cb:
        phase_cb("Тяну сделки и считаю статистику...")

    as_of = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    report = aggregate_event(
        ev,
        iter_event_trades(ev.event_id, limit=1000, taker_only=False),
        as_of_utc=as_of,
        progress_cb=progress_cb,
        progress_every=500,
    )

    if phase_cb:
        phase_cb(f"Сделки обработаны: {report.total_trades}. Пишу Excel...")

    out_dir = PROJECT_ROOT / "out"
    out_dir.mkdir(parents=True, exist_ok=True)

    stamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"event_{ev.slug}_{user_id}_{stamp}.xlsx"

    export_event_report_xlsx(event=ev, report=report, out_path=str(out_path))

    if phase_cb:
        phase_cb("Excel готов. Отправляю...")

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

    status_msg = await update.message.reply_text("Стартую...")
    chat_id = update.effective_chat.id
    msg_id = status_msg.message_id
    loop = asyncio.get_running_loop()

    state = {"phase": "Стартую...", "n": 0}

    def _render() -> str:
        if state["n"] > 0:
            return f"{state['phase']}\nОбработано: {state['n']} сделок..."
        return state["phase"]

    async def _edit_status():
        try:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=msg_id,
                text=_render(),
            )
        except Exception:
            pass

    def _schedule_edit():
        try:
            asyncio.run_coroutine_threadsafe(_edit_status(), loop)
        except Exception:
            pass

    last_n = {"n": 0}

    def progress_cb(n: int):
        # вызывается из треда
        if n <= last_n["n"]:
            return
        last_n["n"] = n
        state["n"] = n
        _schedule_edit()

    def phase_cb(text: str):
        # вызывается из треда
        state["phase"] = text
        _schedule_edit()

    out_path: Path | None = None
    try:
        phase_cb("Запускаю сбор отчёта...")

        out_path, title, trades_cnt, traders_cnt = await asyncio.to_thread(
            _build_report_file,
            event_url,
            user_id,
            progress_cb,
            phase_cb,
        )

        phase_cb(f"Готово: trades={trades_cnt}, traders={traders_cnt}. Отправляю файл...")

        with open(out_path, "rb") as f:
            await update.message.reply_document(
                document=f,
                filename=out_path.name,
                caption=title,
            )

        phase_cb("Отправлено.")
        await update.message.reply_text("Меню:", reply_markup=build_main_menu(user_id))

    except Exception as e:
        # важно: шлём отдельным сообщением, чтобы точно было видно
        await update.message.reply_text(f"Ошибка при генерации отчёта: {e}")
        phase_cb(f"Ошибка: {e}")

    finally:
        if out_path and out_path.exists():
            try:
                out_path.unlink()
            except Exception:
                pass
