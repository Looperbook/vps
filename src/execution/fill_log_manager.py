"""
Fill log manager that runs a background compaction/rotation loop.

Starts an asyncio.Task that periodically compacts each coin's `FillLog`, keeping
only recent events as configured by `Settings.fill_log_retention_days`.
"""

from __future__ import annotations

import asyncio
from typing import List

from src.execution.fill_log import FillLog
from src.config.config import Settings
from src.core.utils import now_ms


class FillLogManager:
    def __init__(self, cfg: Settings) -> None:
        self.cfg = cfg
        self._task: asyncio.Task | None = None
        self._running = False

    async def _compact_once(self) -> None:
        # compute keep_since_ms
        keep_since = now_ms() - int(self.cfg.fill_log_retention_days * 24 * 60 * 60 * 1000)
        # compact per coin
        for coin in self.cfg.coins:
            fl = FillLog(coin, self.cfg.state_dir)
            try:
                await fl.compact(keep_since)
            except Exception:
                # best-effort
                pass

    async def _loop(self) -> None:
        self._running = True
        interval = max(60, int(self.cfg.fill_log_compact_interval_sec))
        while self._running:
            try:
                await self._compact_once()
            except Exception:
                pass
            await asyncio.sleep(interval)

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None


def start_compactor(cfg: Settings) -> FillLogManager:
    mgr = FillLogManager(cfg)
    mgr.start()
    return mgr
