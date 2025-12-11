"""
RestPoller: REST fill polling service extracted from bot.py.

This module handles periodic REST API polling for fills to ensure
no fills are missed when WebSocket connection drops or has gaps.

Architecture:
    RestPoller runs on a configurable interval and calls the exchange
    to get recent fills. It uses a high-water-mark approach to avoid
    re-processing fills and coordinates with the FillDeduplicator.

Thread Safety:
    Uses internal tracking for polling intervals.
    Fill processing should be serialized by the caller.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from src.market_data.market_data import MarketData
    from src.execution.fill_deduplicator import FillDeduplicator

import logging

log = logging.getLogger("gridbot")


@dataclass
class RestPollerConfig:
    """Configuration for RestPoller."""
    # Polling intervals
    poll_interval_sec: float = 30.0
    degraded_poll_interval_sec: float = 7.5  # When WS is degraded
    
    # Fill rescan window
    fill_rescan_ms: int = 60000
    
    # HTTP timeout
    http_timeout: float = 5.0
    
    # Logging callback
    log_event_callback: Optional[Callable[..., None]] = None


@dataclass
class PollResult:
    """Result of a REST poll operation."""
    success: bool
    fills_found: int = 0
    new_fills: int = 0
    error: Optional[str] = None
    duration_ms: float = 0.0


class RestPoller:
    """
    REST fill polling service.
    
    Provides periodic REST API polling to ensure fill completeness
    when WebSocket may have gaps or disconnections.
    
    Usage:
        poller = RestPoller(
            coin="BTC",
            market=market_data,
            account=account_address,
            config=config,
        )
        
        # In main loop
        result = await poller.poll_if_due(
            force=False,
            ws_degraded=market.is_halted(),
        )
        
        for fill in result.fills:
            await handle_fill(fill)
    """
    
    def __init__(
        self,
        coin: str,
        market: "MarketData",
        account: str,
        fill_deduplicator: Optional["FillDeduplicator"] = None,
        config: Optional[RestPollerConfig] = None,
    ) -> None:
        """
        Initialize RestPoller.
        
        Args:
            coin: Trading symbol
            market: MarketData instance for API calls
            account: User account address
            fill_deduplicator: Optional deduplicator for fill keys
            config: Optional configuration
        """
        self.coin = coin
        self.market = market
        self.account = account
        self.fill_deduplicator = fill_deduplicator
        self.config = config or RestPollerConfig()
        
        # Polling state
        self._last_poll_time: float = 0.0
        self._rest_poll_hwm_ms: int = 0  # High water mark
        self._last_fill_time_ms: int = 0
        
        # Logging
        self._log_event = self.config.log_event_callback or self._default_log
    
    def _default_log(self, event: str, **kwargs: Any) -> None:
        """Default logging."""
        import json
        payload = {"event": event, "coin": self.coin, **kwargs}
        log.info(json.dumps(payload))
    
    def set_last_fill_time(self, ts_ms: int) -> None:
        """Update last fill time from external source."""
        self._last_fill_time_ms = max(self._last_fill_time_ms, ts_ms)
    
    def set_high_water_mark(self, ts_ms: int) -> None:
        """Set high water mark for polling."""
        self._rest_poll_hwm_ms = max(self._rest_poll_hwm_ms, ts_ms)
    
    @property
    def is_poll_due(self) -> bool:
        """Check if a poll is due based on interval."""
        return time.time() - self._last_poll_time >= self.config.poll_interval_sec
    
    async def poll_if_due(
        self,
        force: bool = False,
        ws_degraded: bool = False,
        on_fill: Optional[Callable[[Dict[str, Any]], Any]] = None,
    ) -> PollResult:
        """
        Poll REST API for fills if interval has elapsed.
        
        Args:
            force: Force poll regardless of interval
            ws_degraded: If True, use shorter poll interval
            on_fill: Callback for each fill found
            
        Returns:
            PollResult with outcome
        """
        now = time.time()
        
        # Determine effective interval
        effective_interval = (
            self.config.degraded_poll_interval_sec
            if ws_degraded
            else self.config.poll_interval_sec
        )
        
        # Check if poll is due
        if not force and (now - self._last_poll_time) < effective_interval:
            return PollResult(success=True, fills_found=0)
        
        start_time = time.time()
        
        try:
            fills = await self._fetch_fills()
            
            new_fills = 0
            max_fill_ts = self._rest_poll_hwm_ms
            
            # Sort by timestamp
            sorted_fills = sorted(fills, key=lambda x: int(x.get("time", 0)))
            
            for fill in sorted_fills:
                if fill.get("coin") != self.coin:
                    continue
                    
                fill_ts = int(fill.get("time", 0))
                max_fill_ts = max(max_fill_ts, fill_ts)
                
                # Check deduplication if available
                if self.fill_deduplicator:
                    if not self.fill_deduplicator.check_and_add(fill):
                        continue  # Already processed
                
                new_fills += 1
                
                # Call handler if provided
                if on_fill:
                    try:
                        result = on_fill(fill)
                        if asyncio.iscoroutine(result):
                            await result
                    except Exception as exc:
                        self._log_event("rest_fill_handler_error", error=str(exc))
            
            # Update high water mark
            self._rest_poll_hwm_ms = max(self._rest_poll_hwm_ms, max_fill_ts)
            self._last_poll_time = now
            
            duration_ms = (time.time() - start_time) * 1000
            
            self._log_event(
                "rest_fills_polled",
                count=len(sorted_fills),
                new_fills=new_fills,
                hwm_ms=self._rest_poll_hwm_ms,
                duration_ms=round(duration_ms, 1),
                ws_degraded=ws_degraded,
            )
            
            return PollResult(
                success=True,
                fills_found=len(sorted_fills),
                new_fills=new_fills,
                duration_ms=duration_ms,
            )
            
        except Exception as exc:
            self._log_event("rest_poll_error", error=str(exc))
            return PollResult(
                success=False,
                error=str(exc),
                duration_ms=(time.time() - start_time) * 1000,
            )
    
    async def _fetch_fills(self) -> List[Dict[str, Any]]:
        """Fetch fills from REST API."""
        # Use HWM if set, otherwise fall back to last_fill_time_ms
        hwm_start = (
            self._rest_poll_hwm_ms
            if self._rest_poll_hwm_ms > 0
            else self._last_fill_time_ms
        )
        start_ms = max(0, hwm_start - self.config.fill_rescan_ms)
        
        return await self.market.user_fills_since(self.account, start_ms)
    
    async def force_poll(
        self,
        on_fill: Optional[Callable[[Dict[str, Any]], Any]] = None,
    ) -> PollResult:
        """Force an immediate poll regardless of interval."""
        return await self.poll_if_due(force=True, on_fill=on_fill)
    
    def reset(self) -> None:
        """Reset polling state."""
        self._last_poll_time = 0.0
        self._rest_poll_hwm_ms = 0
