from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable, Optional


@dataclass
class ProgressConfig:
    # как часто писать "Обработано X трейдов"
    trades_step: int = 500

    # минимальный интервал между сообщениями (защита от спама)
    min_interval_s: float = 1.5


class ProgressReporter:
    """
    Универсальный репортер прогресса.

    emit: функция, которая принимает строку и куда-то её отправляет:
      - print(...) для консоли
      - позже подключим в Telegram (send_message / edit_message_text)
    """
    def __init__(self, emit: Callable[[str], None], cfg: Optional[ProgressConfig] = None) -> None:
        self.emit = emit
        self.cfg = cfg or ProgressConfig()

        self._last_emit_ts: float = 0.0
        self._last_trades_notified: int = 0

        self._current_stage: str = ""

    def stage(self, name: str) -> None:
        self._current_stage = name
        self._emit_now(f"Этап: {name}")

    def info(self, text: str) -> None:
        self._emit_now(text)

    def trades_progress(self, processed_total: int, inserted_total: int | None = None) -> None:
        """
        Вызывай после каждой пачки.
        Пишет прогресс каждые cfg.trades_step обработанных трейдов.
        """
        if processed_total < self._last_trades_notified + self.cfg.trades_step:
            return

        if not self._can_emit():
            return

        self._last_trades_notified = processed_total

        if inserted_total is None:
            self._emit_now(f"Обработано: {processed_total} трейдов…")
        else:
            self._emit_now(f"Обработано: {processed_total} трейдов… (в БД добавлено новых: {inserted_total})")

    def done(self, excel_path: str | None = None) -> None:
        if excel_path:
            self._emit_now(f"Готово. Excel сохранён: {excel_path}")
        else:
            self._emit_now("Готово.")

    def error(self, text: str) -> None:
        self._emit_now(f"Ошибка: {text}")

    # ---------- internal ----------
    def _can_emit(self) -> bool:
        now = time.time()
        return (now - self._last_emit_ts) >= self.cfg.min_interval_s

    def _emit_now(self, text: str) -> None:
        self._last_emit_ts = time.time()
        prefix = f"[{self._current_stage}] " if self._current_stage else ""
        self.emit(prefix + text)
