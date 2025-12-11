"""
Dead-Man-Switch: Automatic order cancellation if bot becomes unresponsive.

Uses Hyperliquid's schedule_cancel API to ensure all orders are cancelled
if the bot stops sending heartbeats. This prevents orphaned orders from
accumulating unexpected positions.

Features:
- Periodic heartbeat refresh
- Automatic activation on startup
- Graceful deactivation on shutdown
- Configurable timeout
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Optional, Callable

log = logging.getLogger("gridbot")


class DeadManSwitch:
    """
    Dead-man-switch using Hyperliquid's schedule_cancel API.
    
    Schedules automatic cancellation of all orders if no heartbeat
    is received within the timeout period. The bot must periodically
    call refresh() to push the cancellation time forward.
    
    Usage:
        dms = DeadManSwitch(exchange, timeout_sec=300)
        await dms.activate()
        
        # In main loop
        await dms.refresh()
        
        # On shutdown
        await dms.deactivate()
    """
    
    # Minimum timeout (Hyperliquid requires at least 5 seconds)
    MIN_TIMEOUT_SEC = 10
    # Default timeout (5 minutes)
    DEFAULT_TIMEOUT_SEC = 300
    # Refresh interval (refresh when 50% of timeout has elapsed)
    REFRESH_RATIO = 0.5
    
    def __init__(
        self,
        exchange: Any,
        timeout_sec: int = DEFAULT_TIMEOUT_SEC,
        on_trigger: Optional[Callable[[], None]] = None,
        log_event: Optional[Callable[..., None]] = None,
    ) -> None:
        """
        Initialize DeadManSwitch.
        
        Args:
            exchange: AsyncExchange instance
            timeout_sec: Seconds until cancel-all triggers
            on_trigger: Callback when switch would trigger (for testing)
            log_event: Structured logging callback
        """
        self._exchange = exchange
        self._timeout_sec = max(self.MIN_TIMEOUT_SEC, timeout_sec)
        self._on_trigger = on_trigger
        self._log = log_event or self._default_log
        
        self._active = False
        self._last_refresh_ms: int = 0
        self._scheduled_cancel_ms: int = 0
        self._refresh_task: Optional[asyncio.Task] = None
        
        # Statistics
        self._stats = {
            "activations": 0,
            "refreshes": 0,
            "deactivations": 0,
            "errors": 0,
        }
    
    def _default_log(self, event: str, **kwargs: Any) -> None:
        log.info(json.dumps({"event": event, **kwargs}))
    
    def _now_ms(self) -> int:
        return int(time.time() * 1000)
    
    async def activate(self) -> bool:
        """
        Activate the dead-man-switch.
        
        Schedules initial cancel-all and starts refresh task.
        Returns True if successful.
        """
        if self._active:
            return True
        
        try:
            cancel_time_ms = self._now_ms() + (self._timeout_sec * 1000)
            
            # Call Hyperliquid schedule_cancel API
            await self._schedule_cancel(cancel_time_ms)
            
            self._scheduled_cancel_ms = cancel_time_ms
            self._last_refresh_ms = self._now_ms()
            self._active = True
            self._stats["activations"] += 1
            
            self._log(
                "dead_man_switch_activated",
                timeout_sec=self._timeout_sec,
                cancel_at_ms=cancel_time_ms,
            )
            
            # Start background refresh task
            self._refresh_task = asyncio.create_task(self._refresh_loop())
            
            return True
            
        except Exception as e:
            self._stats["errors"] += 1
            self._log("dead_man_switch_activation_error", error=str(e))
            return False
    
    async def deactivate(self) -> bool:
        """
        Deactivate the dead-man-switch.
        
        Cancels the scheduled cancel-all. Call this on graceful shutdown.
        Returns True if successful.
        """
        if not self._active:
            return True
        
        # Stop refresh task
        if self._refresh_task:
            self._refresh_task.cancel()
            try:
                await self._refresh_task
            except asyncio.CancelledError:
                pass
            self._refresh_task = None
        
        try:
            # Clear the scheduled cancel by not including time
            await self._clear_schedule()
            
            self._active = False
            self._stats["deactivations"] += 1
            
            self._log("dead_man_switch_deactivated")
            
            return True
            
        except Exception as e:
            self._stats["errors"] += 1
            self._log("dead_man_switch_deactivation_error", error=str(e))
            return False
    
    async def refresh(self) -> bool:
        """
        Refresh the dead-man-switch, pushing cancel time forward.
        
        Should be called periodically from the main loop.
        Returns True if successful.
        """
        if not self._active:
            return False
        
        try:
            cancel_time_ms = self._now_ms() + (self._timeout_sec * 1000)
            
            await self._schedule_cancel(cancel_time_ms)
            
            self._scheduled_cancel_ms = cancel_time_ms
            self._last_refresh_ms = self._now_ms()
            self._stats["refreshes"] += 1
            
            return True
            
        except Exception as e:
            self._stats["errors"] += 1
            self._log("dead_man_switch_refresh_error", error=str(e))
            return False
    
    async def _refresh_loop(self) -> None:
        """Background task to refresh periodically."""
        refresh_interval = self._timeout_sec * self.REFRESH_RATIO
        
        while self._active:
            try:
                await asyncio.sleep(refresh_interval)
                
                if self._active:
                    await self.refresh()
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._log("dead_man_switch_refresh_loop_error", error=str(e))
                await asyncio.sleep(5)  # Brief pause on error
    
    async def _schedule_cancel(self, cancel_time_ms: int) -> None:
        """
        Schedule cancel-all via Hyperliquid API.
        
        This wraps the exchange's schedule_cancel method.
        """
        loop = asyncio.get_running_loop()
        
        # The exchange.schedule_cancel method takes time in ms
        def _call():
            # Access the underlying exchange object
            base_exchange = getattr(self._exchange, '_exchange', self._exchange)
            return base_exchange.schedule_cancel(cancel_time_ms)
        
        await loop.run_in_executor(None, _call)
    
    async def _clear_schedule(self) -> None:
        """Clear scheduled cancel (pass None/no time)."""
        loop = asyncio.get_running_loop()
        
        def _call():
            base_exchange = getattr(self._exchange, '_exchange', self._exchange)
            # Passing None clears the scheduled cancel
            return base_exchange.schedule_cancel(None)
        
        await loop.run_in_executor(None, _call)
    
    @property
    def is_active(self) -> bool:
        return self._active
    
    @property
    def time_until_trigger_ms(self) -> int:
        """Milliseconds until the switch would trigger."""
        if not self._active:
            return 0
        return max(0, self._scheduled_cancel_ms - self._now_ms())
    
    @property
    def time_since_refresh_ms(self) -> int:
        """Milliseconds since last refresh."""
        if not self._active:
            return 0
        return self._now_ms() - self._last_refresh_ms
    
    def get_stats(self) -> dict:
        """Get statistics."""
        return {
            **self._stats,
            "active": self._active,
            "timeout_sec": self._timeout_sec,
            "time_until_trigger_ms": self.time_until_trigger_ms,
            "time_since_refresh_ms": self.time_since_refresh_ms,
        }
    
    async def cancel(self) -> bool:
        """Alias for deactivate() - cancels the scheduled cancel-all."""
        return await self.deactivate()


class DeadManSwitchStub(DeadManSwitch):
    """
    Stub implementation for testing or when exchange doesn't support schedule_cancel.
    
    Logs actions but doesn't make API calls.
    """
    
    async def _schedule_cancel(self, cancel_time_ms: int) -> None:
        self._log("dead_man_switch_stub_schedule", cancel_at_ms=cancel_time_ms)
    
    async def _clear_schedule(self) -> None:
        self._log("dead_man_switch_stub_clear")
