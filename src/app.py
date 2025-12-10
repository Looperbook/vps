"""
Orchestrator with per-coin isolation and supervision.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import List

from src.bot import GridBot
from src.per_coin_config import load_per_coin_overrides
from src.nonce import NonceCoordinator


class BotRunner:
    def __init__(self, bot: GridBot) -> None:
        self.bot = bot
        self.task: asyncio.Task | None = None
        self.error: Exception | None = None

    async def start(self) -> None:
        try:
            await self.bot.initialize()
            self.task = asyncio.create_task(self.bot.run())
        except Exception as exc:
            self.error = exc
            import traceback
            tb = traceback.format_exc()
            logging.getLogger("gridbot").error(json.dumps({"event": "bot_init_error", "coin": self.bot.coin, "err": str(exc), "traceback": tb}))
            # cleanup partially started components
            try:
                self.bot.stop()
                if getattr(self.bot, "router", None):
                    await self.bot.router.stop()
                if getattr(self.bot, "market", None):
                    await self.bot.market.stop()
            except Exception:
                pass

    async def stop(self) -> None:
        self.bot.stop()
        if self.task:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)


async def run_all(coins: List[str], cfg, info, async_info, exchange, metrics, status_board) -> None:
    coordinator = NonceCoordinator()
    runners: List[BotRunner] = []
    # Create one shared lock per account via the coordinator to avoid nonce collisions
    overrides = load_per_coin_overrides()
    if not isinstance(overrides, dict):
        overrides = {}
    for c in coins:
        account = cfg.resolve_account()
        lock = await coordinator.get_lock(account)
        per_coin_cfg = overrides.get(c, {})
        runners.append(BotRunner(GridBot(c, cfg, info, async_info, exchange, metrics, lock, status_board, per_coin_cfg)))
    for r in runners:
        await r.start()

    async with asyncio.TaskGroup() as tg:
        for r in runners:
            if r.task:
                tg.create_task(_watch_bot(r, runners))


async def _watch_bot(runner: BotRunner, runners: List[BotRunner]) -> None:
    try:
        if runner.task:
            await runner.task
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        runner.error = exc
        logging.getLogger("gridbot").error(json.dumps({"event": "bot_run_error", "coin": runner.bot.coin, "err": str(exc)}))
        runner.bot.stop()
        # Cancel siblings to avoid orphaned bots when one fails.
        for r in runners:
            if r is runner:
                continue
            try:
                r.bot.stop()
                if r.task:
                    r.task.cancel()
            except Exception:
                pass
        raise
