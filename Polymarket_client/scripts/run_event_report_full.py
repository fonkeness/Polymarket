cat > /root/Polymarket/Polymarket_client/scripts/run_event_report_full.py << 'PY'
from __future__ import annotations

import sys
from pathlib import Path
from datetime import datetime, timezone
import logging

from app.ingestion.event_resolver import resolve_event
from app.ingestion.trades_loader import iter_event_trades, Trade
from app.services.event_aggregator import aggregate_event
from app.reporting.excel_exporter import export_event_report_xlsx


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("run_event_report_full")


MAX_TOTAL_SHARES = 20_000.0        # <-- лимит "на 20000 shares"
PAGE_LIMIT = 500                  # размер страницы trades API
PROGRESS_EVERY_TRADES = 2000      # как часто печатать прогресс


def limited_trades_by_total_shares(event_id: int) -> tuple[iter, dict]:
    """
    Возвращает:
      - итератор трейдов (ограничен по сумме abs(size))
      - dict со статой (будем обновлять во время итерации)
    """
    stats = {"trades": 0, "shares": 0.0}

    def progress_cb(processed_trades: int, offset: int) -> None:
        # offset — это смещение пагинации, чисто для дебага
        log.info("Загружено/обработано: trades=%s, shares≈%.2f, offset=%s", stats["trades"], stats["shares"], offset)

    def gen():
        for tr in iter_event_trades(
            event_id,
            limit=PAGE_LIMIT,
            taker_only=False,
            timeout_s=30,
            max_retries=8,
            min_request_interval_s=0.15,
            progress_cb=progress_cb,
            progress_every=PROGRESS_EVERY_TRADES,
        ):
            stats["trades"] += 1
            stats["shares"] += abs(float(tr.size))

            yield tr

            if stats["shares"] >= MAX_TOTAL_SHARES:
                log.info("Достигли лимита shares: %.2f >= %.2f. Останавливаюсь.", stats["shares"], MAX_TOTAL_SHARES)
                return

    return gen(), stats


def main():
    if len(sys.argv) < 2:
        print('Usage: run_event_report_full.py "https://polymarket.com/event/..."')
        raise SystemExit(2)

    url = sys.argv[1].strip()
    ev = resolve_event(url)

    as_of = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    log.info("Event: id=%s slug=%s title=%s", ev.event_id, ev.slug, ev.title)
    log.info("As of (UTC): %s", as_of)
    log.info("Лимит: total_shares=%.2f", MAX_TOTAL_SHARES)

    trades_iter, stats = limited_trades_by_total_shares(ev.event_id)

    log.info("Считаю агрегаты...")
    report = aggregate_event(ev, trades_iter, as_of_utc=as_of)
    log.info("Агрегация готова. trades=%s shares≈%.2f", stats["trades"], stats["shares"])

    out_dir = Path("/root/Polymarket/out")
    out_dir.mkdir(parents=True, exist_ok=True)

    final_path = out_dir / f"event_{ev.slug}.xlsx"
    tmp_path = out_dir / f".{final_path.name}.tmp"

    log.info("Пишу Excel во временный файл: %s", tmp_path)
    export_event_report_xlsx(event=ev, report=report, out_path=str(tmp_path))

    # атомарная подмена (чтобы WinSCP не видел "занятый" финальный файл)
    tmp_path.replace(final_path)

    log.info("Готово: %s", final_path)


if __name__ == "__main__":
    main()
PY
