"""
Shadow Ledger: Centralized position tracking with exchange reconciliation.

S-1 Structural Improvement: Maintains local position shadow with in-flight 
order tracking to detect drift between local and exchange positions.

Features:
- Confirmed position (last reconciled with exchange)
- Local position (real-time from fills)
- Pending exposure (from resting orders)
- Drift detection with alerting
- Automatic reconciliation
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

log = logging.getLogger("gridbot")


@dataclass
class PendingOrder:
    """Represents an expected fill from a resting order."""
    cloid: Optional[str]
    oid: Optional[int]
    side: str
    size: float
    price: float
    created_ms: int
    
    @property
    def signed_size(self) -> float:
        """Return signed size: positive for buy, negative for sell."""
        return self.size if self.side.lower().startswith('b') else -self.size


@dataclass
class LedgerSnapshot:
    """Immutable snapshot of ledger state at a point in time."""
    confirmed_position: float
    local_position: float
    pending_exposure: float
    effective_position: float
    last_reconcile_ms: int
    last_fill_ms: int
    pending_count: int
    timestamp_ms: int


@dataclass
class ReconcileResult:
    """Result of a reconciliation with exchange."""
    drift: float
    drift_pct: float
    is_significant: bool
    local_position: float
    exchange_position: float
    pending_exposure: float
    timestamp_ms: int
    
    @property
    def adjusted_drift(self) -> float:
        """Drift adjusted for pending orders."""
        return abs(self.exchange_position - (self.local_position + self.pending_exposure))


class ShadowLedger:
    """
    Maintains local position shadow with in-flight order tracking.
    
    Position states:
    - confirmed_position: Last reconciled with exchange (truth)
    - local_position: Real-time local tracking from fills
    - pending_exposure: Expected position change from resting orders
    - effective_position: local_position + pending_exposure
    
    Drift detection:
    - Compares local_position against exchange on reconciliation
    - Alerts when drift exceeds threshold (default 10%)
    - Provides metrics for monitoring
    
    Thread-safe for asyncio via internal Lock.
    """
    
    # Drift threshold for alerting (10%)
    DRIFT_ALERT_THRESHOLD = 0.10
    # Maximum age for pending orders before warning (5 minutes)
    MAX_PENDING_AGE_MS = 300_000
    # Minimum time between reconciliations (30 seconds)
    MIN_RECONCILE_INTERVAL_MS = 30_000
    
    def __init__(
        self,
        coin: str,
        log_event: Optional[Callable[..., None]] = None,
        on_drift: Optional[Callable[[ReconcileResult], None]] = None,
    ) -> None:
        """
        Initialize ShadowLedger.
        
        Args:
            coin: Trading symbol
            log_event: Callback for structured logging
            on_drift: Callback when significant drift detected
        """
        self.coin = coin
        self._log = log_event or self._default_log
        self._on_drift = on_drift
        
        # Core positions
        self.confirmed_position: float = 0.0  # From exchange
        self.local_position: float = 0.0       # From local fills
        
        # Pending orders by cloid
        self._pending: Dict[str, PendingOrder] = {}
        # Secondary index by oid for faster lookup
        self._pending_by_oid: Dict[int, str] = {}
        
        # Timestamps
        self.last_reconcile_ms: int = 0
        self.last_fill_ms: int = 0
        
        # Statistics
        self._stats = {
            "total_fills": 0,
            "total_reconciles": 0,
            "drift_alerts": 0,
            "max_drift_seen": 0.0,
            "pending_orders_added": 0,
            "pending_orders_removed": 0,
        }
        
        # Thread safety
        self._lock = asyncio.Lock()
    
    def _default_log(self, event: str, **kwargs: Any) -> None:
        """Default logging when no callback provided."""
        log.debug(f'{{"event":"{event}",{",".join(f"{k}:{v}" for k,v in kwargs.items())}}}')
    
    # -------------------------------------------------------------------------
    # Pending Order Management
    # -------------------------------------------------------------------------
    
    async def add_pending_order(
        self,
        cloid: str,
        side: str,
        size: float,
        price: float,
        oid: Optional[int] = None,
    ) -> None:
        """
        Track a submitted order as pending.
        
        Call this when placing an order to track expected position change.
        
        Args:
            cloid: Client order ID
            side: 'buy' or 'sell'
            size: Order size
            price: Order price
            oid: Exchange order ID (if known)
        """
        async with self._lock:
            pending = PendingOrder(
                cloid=cloid,
                oid=oid,
                side=side,
                size=size,
                price=price,
                created_ms=int(time.time() * 1000),
            )
            self._pending[cloid] = pending
            if oid is not None:
                self._pending_by_oid[oid] = cloid
            
            self._stats["pending_orders_added"] += 1
            
            self._log(
                "shadow_ledger_pending_add",
                coin=self.coin,
                cloid=cloid,
                oid=oid,
                side=side,
                size=size,
                pending_count=len(self._pending),
            )
    
    async def update_pending_oid(self, cloid: str, oid: int) -> bool:
        """
        Update the exchange order ID for a pending order.
        
        Args:
            cloid: Client order ID
            oid: Exchange order ID
            
        Returns:
            True if updated, False if not found
        """
        async with self._lock:
            pending = self._pending.get(cloid)
            if pending:
                # Remove old oid index if present
                if pending.oid is not None and pending.oid in self._pending_by_oid:
                    del self._pending_by_oid[pending.oid]
                # Update
                pending.oid = oid
                self._pending_by_oid[oid] = cloid
                return True
            return False
    
    async def remove_pending_order(
        self,
        cloid: Optional[str] = None,
        oid: Optional[int] = None,
    ) -> Optional[PendingOrder]:
        """
        Remove a pending order (filled or cancelled).
        
        Args:
            cloid: Client order ID
            oid: Exchange order ID (used if cloid not provided)
            
        Returns:
            Removed PendingOrder or None if not found
        """
        async with self._lock:
            # Resolve cloid from oid if needed
            if cloid is None and oid is not None:
                cloid = self._pending_by_oid.get(oid)
            
            if cloid is None:
                return None
            
            pending = self._pending.pop(cloid, None)
            if pending:
                # Clean up oid index
                if pending.oid is not None and pending.oid in self._pending_by_oid:
                    del self._pending_by_oid[pending.oid]
                
                self._stats["pending_orders_removed"] += 1
                
                self._log(
                    "shadow_ledger_pending_remove",
                    coin=self.coin,
                    cloid=cloid,
                    oid=pending.oid,
                    pending_count=len(self._pending),
                )
            
            return pending
    
    async def get_pending_order(
        self,
        cloid: Optional[str] = None,
        oid: Optional[int] = None,
    ) -> Optional[PendingOrder]:
        """Look up a pending order."""
        async with self._lock:
            if cloid:
                return self._pending.get(cloid)
            if oid is not None:
                cloid = self._pending_by_oid.get(oid)
                if cloid:
                    return self._pending.get(cloid)
            return None
    
    # -------------------------------------------------------------------------
    # Fill Processing
    # -------------------------------------------------------------------------
    
    async def apply_fill(
        self,
        side: str,
        size: float,
        cloid: Optional[str] = None,
        oid: Optional[int] = None,
        timestamp_ms: int = 0,
    ) -> None:
        """
        Apply a fill to local position.
        
        Updates local_position and removes from pending if matched.
        
        Args:
            side: 'buy' or 'sell'
            size: Fill size
            cloid: Client order ID (optional, for pending removal)
            oid: Exchange order ID (optional, for pending removal)
            timestamp_ms: Fill timestamp
        """
        async with self._lock:
            # Update local position
            if side.lower().startswith('b'):
                self.local_position += size
            else:
                self.local_position -= size
            
            # Remove from pending
            resolved_cloid = cloid
            if resolved_cloid is None and oid is not None:
                resolved_cloid = self._pending_by_oid.get(oid)
            
            if resolved_cloid and resolved_cloid in self._pending:
                pending = self._pending.pop(resolved_cloid)
                if pending.oid is not None and pending.oid in self._pending_by_oid:
                    del self._pending_by_oid[pending.oid]
            
            # Update timestamp
            now = int(time.time() * 1000)
            self.last_fill_ms = timestamp_ms if timestamp_ms > 0 else now
            
            self._stats["total_fills"] += 1
            
            self._log(
                "shadow_ledger_fill",
                coin=self.coin,
                side=side,
                size=size,
                cloid=cloid,
                local_position=self.local_position,
            )
    
    # -------------------------------------------------------------------------
    # Reconciliation
    # -------------------------------------------------------------------------
    
    async def reconcile(self, exchange_position: float) -> ReconcileResult:
        """
        Reconcile with exchange position.
        
        Compares local tracking against exchange truth, calculates drift,
        and resets local to match exchange.
        
        Args:
            exchange_position: Current position from exchange
            
        Returns:
            ReconcileResult with drift details
        """
        async with self._lock:
            now = int(time.time() * 1000)
            
            # Calculate drift (before any adjustments)
            drift = exchange_position - self.local_position
            
            # Calculate drift percentage
            base = max(abs(self.local_position), abs(exchange_position), 1e-9)
            drift_pct = abs(drift) / base
            
            # Check if significant
            is_significant = drift_pct > self.DRIFT_ALERT_THRESHOLD
            
            # Calculate pending exposure for context
            pending_exposure = sum(p.signed_size for p in self._pending.values())
            
            result = ReconcileResult(
                drift=drift,
                drift_pct=drift_pct,
                is_significant=is_significant,
                local_position=self.local_position,
                exchange_position=exchange_position,
                pending_exposure=pending_exposure,
                timestamp_ms=now,
            )
            
            # Update stats
            self._stats["total_reconciles"] += 1
            self._stats["max_drift_seen"] = max(self._stats["max_drift_seen"], abs(drift))
            
            # Log drift if present
            if abs(drift) > 1e-9:
                self._log(
                    "shadow_ledger_drift",
                    coin=self.coin,
                    local=self.local_position,
                    exchange=exchange_position,
                    drift=drift,
                    drift_pct=drift_pct,
                    pending_exposure=pending_exposure,
                    is_significant=is_significant,
                )
            
            # Alert on significant drift
            if is_significant:
                self._stats["drift_alerts"] += 1
                if self._on_drift:
                    try:
                        self._on_drift(result)
                    except Exception as e:
                        self._log(
                            "shadow_ledger_drift_callback_error",
                            coin=self.coin,
                            error=str(e),
                        )
            
            # Reset local to exchange truth
            self.confirmed_position = exchange_position
            self.local_position = exchange_position
            self.last_reconcile_ms = now
            
            return result
    
    async def should_reconcile(self) -> bool:
        """Check if reconciliation is due (based on time interval)."""
        now = int(time.time() * 1000)
        return (now - self.last_reconcile_ms) > self.MIN_RECONCILE_INTERVAL_MS
    
    # -------------------------------------------------------------------------
    # Position Queries
    # -------------------------------------------------------------------------
    
    @property
    def pending_exposure(self) -> float:
        """Total pending exposure from resting orders (not locked)."""
        return sum(p.signed_size for p in self._pending.values())
    
    @property
    def effective_position(self) -> float:
        """Position including pending orders (not locked)."""
        return self.local_position + self.pending_exposure
    
    async def get_snapshot(self) -> LedgerSnapshot:
        """Get atomic snapshot of ledger state."""
        async with self._lock:
            pending_exposure = sum(p.signed_size for p in self._pending.values())
            return LedgerSnapshot(
                confirmed_position=self.confirmed_position,
                local_position=self.local_position,
                pending_exposure=pending_exposure,
                effective_position=self.local_position + pending_exposure,
                last_reconcile_ms=self.last_reconcile_ms,
                last_fill_ms=self.last_fill_ms,
                pending_count=len(self._pending),
                timestamp_ms=int(time.time() * 1000),
            )
    
    def get_drift_age_ms(self) -> int:
        """Time since last reconciliation (ms)."""
        if self.last_reconcile_ms == 0:
            return 0
        return int(time.time() * 1000) - self.last_reconcile_ms
    
    # -------------------------------------------------------------------------
    # Stale Order Management
    # -------------------------------------------------------------------------
    
    async def get_stale_pending_orders(self, max_age_ms: Optional[int] = None) -> List[PendingOrder]:
        """
        Get pending orders older than threshold.
        
        Stale orders may indicate lost fills or stuck orders.
        
        Args:
            max_age_ms: Maximum age threshold (default: MAX_PENDING_AGE_MS)
            
        Returns:
            List of stale PendingOrder objects
        """
        if max_age_ms is None:
            max_age_ms = self.MAX_PENDING_AGE_MS
        
        async with self._lock:
            now = int(time.time() * 1000)
            cutoff = now - max_age_ms
            return [p for p in self._pending.values() if p.created_ms < cutoff]
    
    async def prune_stale_pending(self, max_age_ms: Optional[int] = None) -> int:
        """
        Remove stale pending orders.
        
        Args:
            max_age_ms: Maximum age threshold
            
        Returns:
            Number of orders pruned
        """
        stale = await self.get_stale_pending_orders(max_age_ms)
        count = 0
        for order in stale:
            removed = await self.remove_pending_order(cloid=order.cloid)
            if removed:
                count += 1
                self._log(
                    "shadow_ledger_pending_pruned",
                    coin=self.coin,
                    cloid=order.cloid,
                    age_ms=int(time.time() * 1000) - order.created_ms,
                )
        return count
    
    # -------------------------------------------------------------------------
    # State Persistence
    # -------------------------------------------------------------------------
    
    def get_state(self) -> Dict[str, Any]:
        """Get ledger state for persistence."""
        return {
            "confirmed_position": self.confirmed_position,
            "local_position": self.local_position,
            "last_reconcile_ms": self.last_reconcile_ms,
            "last_fill_ms": self.last_fill_ms,
            "stats": dict(self._stats),
        }
    
    def load_state(self, state: Dict[str, Any]) -> None:
        """Load ledger state from persistence."""
        self.confirmed_position = float(state.get("confirmed_position", 0.0))
        self.local_position = float(state.get("local_position", 0.0))
        self.last_reconcile_ms = int(state.get("last_reconcile_ms", 0))
        self.last_fill_ms = int(state.get("last_fill_ms", 0))
        if "stats" in state:
            for k, v in state["stats"].items():
                if k in self._stats:
                    self._stats[k] = v
    
    def get_stats(self) -> Dict[str, Any]:
        """Get ledger statistics."""
        return {
            **self._stats,
            "pending_count": len(self._pending),
            "pending_exposure": self.pending_exposure,
            "drift_age_ms": self.get_drift_age_ms(),
        }
    
    async def clear(self) -> None:
        """Clear all state (for testing/reset)."""
        async with self._lock:
            self.confirmed_position = 0.0
            self.local_position = 0.0
            self._pending.clear()
            self._pending_by_oid.clear()
            self.last_reconcile_ms = 0
            self.last_fill_ms = 0
