"""
Async wrapper around the blocking Hyperliquid Exchange using a shared thread pool.
This reduces thread churn and presents async methods for order/ cancel flows.
"""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Optional

from hyperliquid.utils.types import Cloid
import time
import random


class AsyncExchange:
    def __init__(self, exchange, timeout: float = 2.0, max_workers: int = 8) -> None:
        self._exchange = exchange
        self._timeout = timeout
        self._executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="hl-exec")

    async def order(self, *args, **kwargs) -> Any:
        return await self._call(lambda: self._exchange.order(*args, **kwargs))

    async def bulk_orders(self, *args, **kwargs) -> Any:
        return await self._call(lambda: self._exchange.bulk_orders(*args, **kwargs))

    async def cancel(self, coin: str, oid: int) -> Any:
        return await self._call(lambda: self._exchange.cancel(coin, oid))

    async def cancel_by_cloid(self, coin: str, cloid: Cloid) -> Any:
        return await self._call(lambda: self._exchange.cancel_by_cloid(coin, cloid))

    async def bulk_cancel(self, *args, **kwargs) -> Any:
        return await self._call(lambda: self._exchange.bulk_cancel(*args, **kwargs))

    async def bulk_cancel_by_cloid(self, *args, **kwargs) -> Any:
        return await self._call(lambda: self._exchange.bulk_cancel_by_cloid(*args, **kwargs))

    async def close(self, wait: bool = True) -> None:
        # prefer graceful shutdown to avoid leaking threads between restarts
        self._executor.shutdown(wait=wait)

    async def _call(self, fn, retries: int = 2) -> Any:
        loop = asyncio.get_running_loop()
        backoff = 0.2
        for attempt in range(retries + 1):
            try:
                return await asyncio.wait_for(loop.run_in_executor(self._executor, fn), timeout=self._timeout)
            except Exception:
                if attempt >= retries:
                    raise
                await asyncio.sleep(backoff + random.uniform(0, backoff * 0.5))
                backoff *= 2
