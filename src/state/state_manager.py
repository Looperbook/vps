"""
StateManager: Centralized state ownership for grid trading bot.

This module provides a single source of truth for all bot state:
- Position and PnL (via PositionTracker)
- Fill timestamps
- Grid state
- Session tracking
- State persistence

Architecture:
    StateManager owns all state. Bot and FillProcessor read/write via
    StateManager rather than maintaining their own copies. This prevents
    state divergence and simplifies reasoning about system state.

Thread Safety:
    All mutations go through async methods that use internal locking.
    Read-only properties are safe for concurrent access.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from src.state.state_atomic import AtomicStateStore
    from src.state.position_tracker import PositionTracker
    from src.state.shadow_ledger import ShadowLedger
    from src.risk.risk import RiskEngine

import logging

log = logging.getLogger("gridbot")


@dataclass
class StateSnapshot:
    """
    Immutable snapshot of complete bot state at a point in time.
    
    Used for atomic state reads and persistence.
    """
    # Position state
    position: float
    session_realized_pnl: float
    alltime_realized_pnl: float
    
    # Fill tracking
    last_fill_time_ms: int
    
    # Grid state
    grid_center: Optional[float]
    
    # Session info
    session_start_time: float
    
    # Metadata
    state_version: int = 3
    timestamp_ms: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for persistence."""
        return {
            "position": self.position,
            "realized_pnl": self.alltime_realized_pnl,  # Persist all-time, not session
            "session_pnl": self.session_realized_pnl,
            "last_fill_time_ms": self.last_fill_time_ms,
            "grid_center": self.grid_center,
            "state_version": self.state_version,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any], session_start: float = 0.0) -> "StateSnapshot":
        """Create from persisted dictionary."""
        return cls(
            position=float(data.get("position", 0.0)),
            session_realized_pnl=0.0,  # Always starts at 0 on load
            alltime_realized_pnl=float(data.get("realized_pnl", 0.0)),
            last_fill_time_ms=int(data.get("last_fill_time_ms", 0)),
            grid_center=data.get("grid_center"),
            session_start_time=session_start or time.time(),
            state_version=int(data.get("state_version", 1)),
        )


@dataclass
class StateManagerConfig:
    """Configuration for StateManager."""
    # Fill timestamp handling
    fill_rescan_ms: int = 60000  # Window for fill replay
    
    # Logging
    log_event_callback: Optional[Callable[..., None]] = None


class StateManager:
    """
    Single source of truth for all bot state.
    
    Centralizes state that was previously scattered across:
    - GridBot (position, last_fill_time_ms, realized_pnl)
    - PositionTracker (position, realized_pnl)
    - FillProcessor (session_pnl, last_fill_time)
    
    Benefits:
    - No state divergence between components
    - Clear ownership model
    - Simplified persistence
    - Atomic state snapshots
    """
    
    def __init__(
        self,
        coin: str,
        state_store: "AtomicStateStore",
        position_tracker: "PositionTracker",
        shadow_ledger: "ShadowLedger",
        risk_engine: "RiskEngine",
        config: Optional[StateManagerConfig] = None,
    ) -> None:
        """
        Initialize StateManager with dependencies.
        
        Args:
            coin: Trading symbol
            state_store: Persistence layer
            position_tracker: Position/PnL tracking
            shadow_ledger: Exchange reconciliation
            risk_engine: Risk limits
            config: Optional configuration
        """
        self.coin = coin
        self.state_store = state_store
        self.position_tracker = position_tracker
        self.shadow_ledger = shadow_ledger
        self.risk_engine = risk_engine
        self.config = config or StateManagerConfig()
        
        # Internal state
        self._lock = asyncio.Lock()
        self._session_realized_pnl: float = 0.0
        self._alltime_realized_pnl: float = 0.0
        self._last_fill_time_ms: int = 0
        self._grid_center: Optional[float] = None
        self._session_start_time: float = time.time()
        
        # Logging
        self.log = log  # Module-level logger for DEBUG messages
        self._log_event = self.config.log_event_callback or self._default_log
    
    def _default_log(self, event: str, **kwargs: Any) -> None:
        """Default logging."""
        import json
        payload = {"event": event, "coin": self.coin, **kwargs}
        log.info(json.dumps(payload))
    
    # ========== Position Properties ==========
    
    @property
    def position(self) -> float:
        """Current position (delegates to PositionTracker)."""
        return self.position_tracker.position
    
    async def set_position(self, value: float, source: str = "unknown") -> None:
        """
        Set position with proper synchronization.
        
        Updates PositionTracker, ShadowLedger, and RiskEngine.
        """
        async with self._lock:
            old_position = self.position_tracker.position
            self.position_tracker.position = value
            self.shadow_ledger.local_position = value
            self.risk_engine.set_position(value)
            
            if abs(old_position - value) > 1e-9:
                self._log_event(
                    "position_set",
                    source=source,
                    old=old_position,
                    new=value,
                )
    
    # ========== PnL Properties ==========
    
    @property
    def session_realized_pnl(self) -> float:
        """Session realized PnL (resets each run)."""
        return self._session_realized_pnl
    
    @property
    def alltime_realized_pnl(self) -> float:
        """All-time realized PnL (persisted)."""
        return self._alltime_realized_pnl
    
    async def add_realized_pnl(self, pnl: float) -> None:
        """Add to realized PnL (both session and all-time)."""
        async with self._lock:
            self._session_realized_pnl += pnl
            self._alltime_realized_pnl += pnl
            # Keep position tracker in sync
            self.position_tracker.realized_pnl = self._session_realized_pnl
    
    # ========== Fill Tracking ==========
    
    @property
    def last_fill_time_ms(self) -> int:
        """Timestamp of last processed fill."""
        return self._last_fill_time_ms
    
    async def update_last_fill_time(self, ts_ms: int) -> None:
        """Update last fill timestamp (max of current and new)."""
        async with self._lock:
            self._last_fill_time_ms = max(self._last_fill_time_ms, ts_ms)
    
    # ========== Grid State ==========
    
    @property
    def grid_center(self) -> Optional[float]:
        """Current grid center price."""
        return self._grid_center
    
    async def set_grid_center(self, value: Optional[float]) -> None:
        """Set grid center."""
        async with self._lock:
            self._grid_center = value
    
    # ========== Session Info ==========
    
    @property
    def session_start_time(self) -> float:
        """Session start timestamp."""
        return self._session_start_time
    
    @property
    def session_duration_sec(self) -> float:
        """Session duration in seconds."""
        return time.time() - self._session_start_time
    
    # ========== Atomic Operations ==========
    
    async def get_snapshot(self) -> StateSnapshot:
        """Get atomic snapshot of all state."""
        async with self._lock:
            return StateSnapshot(
                position=self.position_tracker.position,
                session_realized_pnl=self._session_realized_pnl,
                alltime_realized_pnl=self._alltime_realized_pnl,
                last_fill_time_ms=self._last_fill_time_ms,
                grid_center=self._grid_center,
                session_start_time=self._session_start_time,
                timestamp_ms=int(time.time() * 1000),
            )
    
    async def apply_fill(
        self,
        side: str,
        size: float,
        pnl: float,
        timestamp_ms: int,
    ) -> None:
        """
        Apply fill to state atomically.
        
        Updates position, PnL, and fill timestamp in one operation.
        """
        async with self._lock:
            # Update position
            delta = size if side.startswith("b") else -size
            old_pos = self.position_tracker.position
            new_pos = old_pos + delta
            
            self.position_tracker.position = new_pos
            self.shadow_ledger.local_position = new_pos
            self.risk_engine.set_position(new_pos)
            
            # Update PnL
            self._session_realized_pnl += pnl
            self._alltime_realized_pnl += pnl
            self.position_tracker.realized_pnl = self._session_realized_pnl
            
            # Update fill timestamp
            self._last_fill_time_ms = max(self._last_fill_time_ms, timestamp_ms)
    
    # ========== Persistence ==========
    
    async def persist(self) -> None:
        """Persist current state to storage."""
        snapshot = await self.get_snapshot()
        await self.state_store.save(snapshot.to_dict())
    
    async def load(self, current_time_ms: int) -> StateSnapshot:
        """
        Load state from storage.
        
        Args:
            current_time_ms: Current timestamp for fill time adjustment
            
        Returns:
            Loaded state snapshot
        """
        data = await self.state_store.load()
        
        async with self._lock:
            # Load all-time PnL
            self._alltime_realized_pnl = float(data.get("realized_pnl", 0.0))
            
            # Session PnL starts at 0
            self._session_realized_pnl = 0.0
            self._session_start_time = time.time()
            
            # Load position
            position = float(data.get("position", 0.0))
            self.position_tracker.position = position
            self.position_tracker.realized_pnl = 0.0
            self.shadow_ledger.local_position = position
            self.shadow_ledger.confirmed_position = position
            self.risk_engine.set_position(position)
            
            # Load fill timestamp with rescan window
            saved_fill = int(data.get("last_fill_time_ms", 0))
            self._last_fill_time_ms = max(
                saved_fill, 
                current_time_ms - self.config.fill_rescan_ms
            )
            
            # Load grid center
            self._grid_center = data.get("grid_center")
            
            # DEBUG level - fires every loop iteration
            self.log.debug(
                "state_loaded pos=%s pnl=%s grid_center=%s",
                position, self._alltime_realized_pnl, self._grid_center
            )
            
            return StateSnapshot(
                position=position,
                session_realized_pnl=0.0,
                alltime_realized_pnl=self._alltime_realized_pnl,
                last_fill_time_ms=self._last_fill_time_ms,
                grid_center=self._grid_center,
                session_start_time=self._session_start_time,
            )
    
    async def reset_session(self) -> None:
        """Reset session state (for testing or restart)."""
        async with self._lock:
            self._session_realized_pnl = 0.0
            self._session_start_time = time.time()
            self.position_tracker.realized_pnl = 0.0
            self._log_event("session_reset")
    
    # ========== Reconciliation ==========
    
    async def reconcile_position(self, exchange_position: float) -> float:
        """
        Reconcile position with exchange.
        
        Args:
            exchange_position: Position from exchange
            
        Returns:
            Drift amount (positive)
        """
        async with self._lock:
            old_pos = self.position_tracker.position
            drift = abs(old_pos - exchange_position)
            
            if drift > 1e-9:
                self._log_event(
                    "position_reconciled",
                    old=old_pos,
                    new=exchange_position,
                    drift=drift,
                )
                
                self.position_tracker.position = exchange_position
                self.shadow_ledger.local_position = exchange_position
                self.shadow_ledger.confirmed_position = exchange_position
                self.risk_engine.set_position(exchange_position)
            
            return drift
    
    def get_state_for_fill_processor(self) -> Dict[str, Any]:
        """
        Get state dict for FillProcessor sync.
        
        Returns minimal state needed by FillProcessor.
        """
        return {
            "position": self.position_tracker.position,
            "session_pnl": self._session_realized_pnl,
            "alltime_pnl": self._alltime_realized_pnl,
            "last_fill_time_ms": self._last_fill_time_ms,
        }
    
    def sync_from_fill_processor(
        self,
        position: float,
        session_pnl: float,
        alltime_pnl: float,
        last_fill_time_ms: int,
    ) -> None:
        """
        Sync state from FillProcessor.
        
        Called after FillProcessor.process_fill() to update central state.
        Note: This is synchronous for performance - caller must hold lock.
        """
        self.position_tracker.position = position
        self._session_realized_pnl = session_pnl
        self._alltime_realized_pnl = alltime_pnl
        self._last_fill_time_ms = last_fill_time_ms
        self.position_tracker.realized_pnl = session_pnl
        self.shadow_ledger.local_position = position
        self.risk_engine.set_position(position)
    
    def apply_fill_sync(
        self,
        side: str,
        size: float,
        pnl: float,
        timestamp_ms: int,
    ) -> float:
        """
        Apply fill to state synchronously - for use within position lock.
        
        This is the preferred method for fill processing as it:
        1. Updates all state in one atomic operation
        2. Returns the new position for immediate use
        3. Avoids the overhead of async lock acquisition
        
        IMPORTANT: Caller must already hold position_lock!
        
        Args:
            side: 'buy' or 'sell'
            size: Fill size
            pnl: Realized PnL from this fill
            timestamp_ms: Fill timestamp
            
        Returns:
            New position after fill
        """
        # Update position
        delta = size if side.lower().startswith("b") else -size
        old_pos = self.position_tracker.position
        new_pos = old_pos + delta
        
        self.position_tracker.position = new_pos
        self.shadow_ledger.local_position = new_pos
        self.risk_engine.set_position(new_pos)
        
        # Update PnL
        self._session_realized_pnl += pnl
        self._alltime_realized_pnl += pnl
        self.position_tracker.realized_pnl = self._session_realized_pnl
        
        # Update fill timestamp
        self._last_fill_time_ms = max(self._last_fill_time_ms, timestamp_ms)
        
        return new_pos
    
    def get_fill_state_sync(self) -> tuple:
        """
        Get state needed for fill processing synchronously.
        
        Returns tuple of (position, session_pnl, alltime_pnl, last_fill_time_ms)
        for use in fill processing where async overhead is undesirable.
        """
        return (
            self.position_tracker.position,
            self._session_realized_pnl,
            self._alltime_realized_pnl,
            self._last_fill_time_ms,
        )
