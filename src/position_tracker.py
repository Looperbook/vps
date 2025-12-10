"""
PositionTracker: Centralized position management with atomic updates and drift detection.

Extracted from bot.py to improve maintainability and testability.
Handles:
- Thread-safe position updates with asyncio.Lock
- Realized PnL tracking
- Position drift detection
- Skew ratio calculation
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

log = logging.getLogger("gridbot")


@dataclass
class PositionSnapshot:
    """Immutable snapshot of position state at a point in time."""
    position: float
    realized_pnl: float
    timestamp_ms: int


@dataclass
class DriftResult:
    """Result of position drift check."""
    has_drift: bool
    drift_amount: float
    drift_pct: float
    snapshot_position: float
    current_position: float
    
    @property
    def is_significant(self) -> bool:
        """Check if drift exceeds 10% threshold."""
        return self.drift_pct > 0.10


class PositionTracker:
    """
    Thread-safe position tracker with atomic updates.
    
    Provides:
    - Atomic position updates under lock
    - Realized PnL tracking
    - Position drift detection between snapshots
    - Skew ratio calculation
    
    Thread-safe for asyncio usage via internal Lock.
    """
    
    def __init__(self, 
                 initial_position: float = 0.0,
                 initial_realized_pnl: float = 0.0,
                 log_event: Optional[Callable[..., None]] = None) -> None:
        self._position: float = initial_position
        self._realized_pnl: float = initial_realized_pnl
        self._lock = asyncio.Lock()
        self._log_event = log_event or self._default_log
        self._last_update_ms: int = 0
    
    def _default_log(self, event: str, **kwargs) -> None:
        """Default logging implementation."""
        log.debug(f'{{"event":"{event}",{",".join(f"{k}:{v}" for k,v in kwargs.items())}}}')
    
    @property
    def position(self) -> float:
        """Current position (not locked - use snapshot for consistency)."""
        return self._position
    
    @position.setter
    def position(self, value: float) -> None:
        """Set position directly (use with caution - prefer atomic methods)."""
        self._position = value
    
    @property
    def realized_pnl(self) -> float:
        """Current realized PnL."""
        return self._realized_pnl
    
    @realized_pnl.setter
    def realized_pnl(self, value: float) -> None:
        """Set realized PnL directly."""
        self._realized_pnl = value
    
    @property
    def lock(self) -> asyncio.Lock:
        """Access to the internal lock for external coordination."""
        return self._lock
    
    async def get_snapshot(self) -> PositionSnapshot:
        """Get atomic snapshot of position state."""
        async with self._lock:
            return PositionSnapshot(
                position=self._position,
                realized_pnl=self._realized_pnl,
                timestamp_ms=self._last_update_ms
            )
    
    async def update_from_fill(self, side: str, size: float, fill_px: float, 
                               order_px: float, timestamp_ms: int = 0) -> float:
        """
        Update position from a fill.
        
        Args:
            side: 'buy' or 'sell' (or starts with 'b'/'s')
            size: Fill size
            fill_px: Price at which fill occurred
            order_px: Original order price (for PnL calculation)
            timestamp_ms: Fill timestamp
            
        Returns:
            PnL from this fill
        """
        async with self._lock:
            # Update position
            if side.lower().startswith("b"):
                self._position += size
                pnl = (fill_px - order_px) * size
            else:
                self._position -= size
                pnl = (order_px - fill_px) * size
            
            self._realized_pnl += pnl
            if timestamp_ms > 0:
                self._last_update_ms = max(self._last_update_ms, timestamp_ms)
            
            return pnl
    
    async def set_position(self, position: float) -> None:
        """Set position atomically (e.g., from reconciliation)."""
        async with self._lock:
            old_pos = self._position
            self._position = position
            if abs(old_pos - position) > 1e-9:
                self._log_event("position_set", old=old_pos, new=position)
    
    async def check_drift(self, snapshot_position: float) -> DriftResult:
        """
        Check position drift since a snapshot.
        
        Args:
            snapshot_position: Position at time of snapshot
            
        Returns:
            DriftResult with drift details
        """
        async with self._lock:
            current = self._position
        
        drift_amount = abs(current - snapshot_position)
        # Avoid division by zero - use absolute snapshot or 1e-9
        base = max(abs(snapshot_position), 1e-9)
        drift_pct = drift_amount / base
        
        result = DriftResult(
            has_drift=drift_amount > 1e-9,
            drift_amount=drift_amount,
            drift_pct=drift_pct,
            snapshot_position=snapshot_position,
            current_position=current
        )
        
        if result.has_drift:
            self._log_event("position_drift_check", 
                          snapshot=snapshot_position, 
                          current=current, 
                          drift=drift_amount,
                          drift_pct=drift_pct)
        
        return result
    
    def calculate_skew_ratio(self, target_position: float) -> float:
        """
        Calculate skew ratio (current position vs target).
        
        Args:
            target_position: Target/max position for the strategy
            
        Returns:
            Ratio of current position to target (0.0 if target is 0)
        """
        if target_position <= 0:
            return 0.0
        return abs(self._position) / target_position
    
    def get_disallowed_side(self) -> Optional[str]:
        """
        Get side that should be disallowed to reduce exposure.
        
        Returns:
            'buy' if long, 'sell' if short, None if flat
        """
        if self._position > 0:
            return "buy"
        elif self._position < 0:
            return "sell"
        return None
    
    def get_reduce_side(self) -> Optional[str]:
        """
        Get side that reduces current position.
        
        Returns:
            'sell' if long, 'buy' if short, None if flat
        """
        if self._position > 0:
            return "sell"
        elif self._position < 0:
            return "buy"
        return None
    
    def get_state(self) -> dict:
        """Get current state for persistence/monitoring."""
        return {
            "position": self._position,
            "realized_pnl": self._realized_pnl,
            "last_update_ms": self._last_update_ms,
        }
    
    def load_state(self, state: dict) -> None:
        """Load state from persistence."""
        self._position = float(state.get("position", 0.0))
        self._realized_pnl = float(state.get("realized_pnl", 0.0))
        self._last_update_ms = int(state.get("last_update_ms", 0))
