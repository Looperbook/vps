"""
Async/atomic wrapper around StateStore for safe concurrent access.

Provides async `load` and `save` that run file IO in an executor and serialize
access with an `asyncio.Lock` to avoid concurrent writes and interleaved state.
"""

from __future__ import annotations

import asyncio
from typing import Any, Dict

from src.state import StateStore


class AtomicStateStore:
    def __init__(self, coin: str, state_dir: str) -> None:
        self._store = StateStore(coin, state_dir)
        self._lock = asyncio.Lock()

    async def load(self) -> Dict[str, Any]:
        async with self._lock:
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(None, self._store.load)

    async def save(self, data: Dict[str, Any]) -> None:
        async with self._lock:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, lambda: self._store.save(data))
