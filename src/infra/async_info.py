"""
Minimal async HTTP client for Hyperliquid info endpoints using HTTP/2.
"""

from __future__ import annotations

import httpx
from typing import Any, Optional


class AsyncInfo:
    def __init__(self, base_url: str, timeout: float = 2.0, client: Optional[httpx.AsyncClient] = None) -> None:
        self.base_url = base_url.rstrip("/")
        # If a shared client is passed in, we won't close it in close(); otherwise we own the client.
        if client is not None:
            self.client = client
            self._owns_client = False
        else:
            self.client = httpx.AsyncClient(base_url=self.base_url, http2=True, timeout=timeout)
            self._owns_client = True

    async def close(self) -> None:
        if self._owns_client:
            await self.client.aclose()

    async def meta(self, dex: Optional[str] = None) -> Any:
        payload: dict[str, Any] = {"type": "meta"}
        if dex:
            # REST meta expects "dex" (not perpDex) to select builder / alternate venues.
            payload["dex"] = dex
        return await self._post_info(payload)

    async def all_mids(self, dex: Optional[str] = None) -> Any:
        payload: dict[str, Any] = {"type": "allMids"}
        if dex:
            # REST allMids also uses "dex" to route to builder perps.
            payload["dex"] = dex
        return await self._post_info(payload)

    async def user_state(self, account: str, dex: Optional[str] = None) -> Any:
        # Match Info.user_state: type clearinghouseState, key "dex"
        payload: dict[str, Any] = {"type": "clearinghouseState", "user": account}
        if dex:
            payload["dex"] = dex
        return await self._post_info(payload)

    async def frontend_open_orders(self, account: str, dex: Optional[str] = None) -> Any:
        payload: dict[str, Any] = {"type": "frontendOpenOrders", "user": account}
        if dex:
            payload["dex"] = dex
        return await self._post_info(payload)

    async def user_fills_by_time(self, account: str, start_ms: int) -> Any:
        """
        Fetch fills by time. Per Hyperliquid docs: userFillsByTime does NOT support dex filtering.
        Returns all fills for the user - caller must filter by coin if needed.
        """
        payload: dict[str, Any] = {"type": "userFillsByTime", "user": account, "startTime": start_ms}
        return await self._post_info(payload)

    async def query_order_by_oid(self, account: str, oid: int) -> Any:
        """
        Query order status by order ID.
        SDK Reference: info.query_order_by_oid(user, oid)
        """
        payload: dict[str, Any] = {"type": "orderStatus", "user": account, "oid": oid}
        return await self._post_info(payload)

    async def query_order_by_cloid(self, account: str, cloid: str) -> Any:
        """
        Query order status by client order ID.
        SDK Reference: info.query_order_by_cloid(user, cloid)
        """
        payload: dict[str, Any] = {"type": "orderStatus", "user": account, "oid": cloid}
        return await self._post_info(payload)

    async def user_rate_limit(self, account: str) -> Any:
        """
        Query current rate limit status for user.
        SDK Reference: info.user_rate_limit(user)
        """
        payload: dict[str, Any] = {"type": "userRateLimit", "user": account}
        return await self._post_info(payload)

    async def _post_info(self, payload: dict[str, Any]) -> Any:
        resp = await self.client.post("/info", json=payload)
        resp.raise_for_status()
        data = resp.json()
        # unwrap {status:'ok', response:{data:{...}}} patterns
        if isinstance(data, dict):
            payload = data
            if "response" in payload and isinstance(payload["response"], dict):
                payload = payload["response"]
            if "data" in payload and isinstance(payload["data"], dict):
                payload = payload["data"]
            # Special-case allMids nesting
            if "allMids" in payload and isinstance(payload["allMids"], dict):
                payload = payload["allMids"]
            return payload
        return data
