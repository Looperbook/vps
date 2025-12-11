"""
Account-level nonce coordinator.

Provides a single asyncio.Lock per account so multiple per-coin bots
sharing the same account coordinate nonce usage.
"""

from __future__ import annotations

import asyncio
from typing import Dict


class NonceCoordinator:
    def __init__(self) -> None:
        # map account -> asyncio.Lock
        self._locks: Dict[str, asyncio.Lock] = {}
        # guard for creating locks
        self._guard = asyncio.Lock()

    async def get_lock(self, account: str) -> asyncio.Lock:
        """Return a shared asyncio.Lock for the given account.

        This ensures all bots using the same account will serialize
        order/cancel submissions using a single lock.
        """
        # keep creation atomic
        async with self._guard:
            lock = self._locks.get(account)
            if lock is None:
                lock = asyncio.Lock()
                self._locks[account] = lock
            return lock
