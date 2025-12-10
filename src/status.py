"""
In-memory status board for lightweight dashboards.
"""

from __future__ import annotations

import asyncio
from typing import Any, Dict


class StatusBoard:
    def __init__(self) -> None:
        self._data: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()

    async def update(self, coin: str, payload: Dict[str, Any]) -> None:
        async with self._lock:
            self._data[coin] = payload

    async def snapshot(self) -> Dict[str, Dict[str, Any]]:
        async with self._lock:
            return dict(self._data)
