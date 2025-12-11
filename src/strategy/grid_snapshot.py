"""
Atomic Grid Snapshot for race-condition-free grid rebuilds.

Provides immutable snapshots of all state needed for grid computation,
ensuring that fills arriving during grid rebuild don't cause inconsistencies.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set
from decimal import Decimal

from src.strategy.strategy import GridLevel


@dataclass(frozen=True)
class OrderSnapshot:
    """Immutable snapshot of a single order."""
    cloid: Optional[str]
    oid: Optional[int]
    side: str
    price: float
    size: float
    filled_qty: float
    price_key: str


@dataclass(frozen=True)
class GridSnapshot:
    """
    Immutable snapshot of all state needed for grid computation.
    
    This is captured atomically under lock, then grid computation
    proceeds using only snapshot data. Any fills that arrive during
    computation will trigger a re-snapshot on the next cycle.
    
    Attributes:
        position: Position at snapshot time
        mid_price: Mid price at snapshot time
        open_orders: Dict of price_key -> OrderSnapshot
        grid_center: Current grid center
        timestamp_ms: When snapshot was taken
        sequence: Monotonic sequence for staleness detection
    """
    position: float
    mid_price: float
    open_orders: Dict[str, OrderSnapshot]
    grid_center: Optional[float]
    timestamp_ms: int
    sequence: int
    
    # Derived fields
    open_order_keys: frozenset = field(default=frozenset(), compare=False)
    buy_exposure: float = 0.0
    sell_exposure: float = 0.0
    
    def __post_init__(self):
        # Compute derived fields (frozen=True requires object.__setattr__)
        if not self.open_order_keys:
            object.__setattr__(self, 'open_order_keys', frozenset(self.open_orders.keys()))
        
        buy_exp = sum(o.size - o.filled_qty for o in self.open_orders.values() if o.side == "buy")
        sell_exp = sum(o.size - o.filled_qty for o in self.open_orders.values() if o.side == "sell")
        object.__setattr__(self, 'buy_exposure', buy_exp)
        object.__setattr__(self, 'sell_exposure', sell_exp)
    
    def is_stale(self, current_position: float, epsilon: float = 1e-9) -> bool:
        """Check if snapshot is stale due to position change."""
        return abs(current_position - self.position) > epsilon
    
    def position_drift(self, current_position: float) -> float:
        """Calculate position drift since snapshot."""
        return abs(current_position - self.position)
    
    def get_order_by_key(self, key: str) -> Optional[OrderSnapshot]:
        """Get order by price key."""
        return self.open_orders.get(key)


@dataclass
class GridDiffResult:
    """Result of computing grid diff from snapshot."""
    to_cancel: List[OrderSnapshot]  # Orders to cancel
    to_place: List[GridLevel]       # New levels to place
    unchanged: List[str]            # Price keys of unchanged orders
    snapshot_seq: int               # Sequence of snapshot used
    computed_at_ms: int             # When diff was computed
    
    @property
    def has_changes(self) -> bool:
        return bool(self.to_cancel) or bool(self.to_place)
    
    @property
    def cancel_count(self) -> int:
        return len(self.to_cancel)
    
    @property
    def place_count(self) -> int:
        return len(self.to_place)


class SnapshotManager:
    """
    Manages atomic snapshots for grid computation.
    
    Provides:
    - Atomic snapshot capture under lock
    - Staleness detection
    - Sequence tracking for concurrent rebuild detection
    """
    
    def __init__(self) -> None:
        self._sequence: int = 0
        self._last_snapshot: Optional[GridSnapshot] = None
        self._last_diff_seq: int = 0
    
    def capture(
        self,
        position: float,
        mid_price: float,
        orders_by_price: Dict[str, Any],  # ActiveOrder objects
        grid_center: Optional[float],
        price_key_fn: callable,
    ) -> GridSnapshot:
        """
        Capture atomic snapshot of current state.
        
        Args:
            position: Current position
            mid_price: Current mid price
            orders_by_price: Dict of price_key -> ActiveOrder
            grid_center: Current grid center
            price_key_fn: Function to generate price keys
            
        Returns:
            Immutable GridSnapshot
        """
        self._sequence += 1
        
        # Deep copy orders to immutable snapshots
        order_snapshots = {}
        for key, order in orders_by_price.items():
            order_snapshots[key] = OrderSnapshot(
                cloid=order.cloid,
                oid=order.oid,
                side=order.level.side,
                price=order.level.px,
                size=order.original_qty,
                filled_qty=order.filled_qty,
                price_key=key,
            )
        
        snapshot = GridSnapshot(
            position=position,
            mid_price=mid_price,
            open_orders=order_snapshots,
            grid_center=grid_center,
            timestamp_ms=int(time.time() * 1000),
            sequence=self._sequence,
        )
        
        self._last_snapshot = snapshot
        return snapshot
    
    def is_diff_valid(self, diff: GridDiffResult, current_sequence: int) -> bool:
        """
        Check if a diff is still valid.
        
        A diff is invalid if another snapshot was taken since the diff
        was computed (indicates another rebuild started).
        """
        return diff.snapshot_seq == current_sequence
    
    def mark_diff_executed(self, diff: GridDiffResult) -> None:
        """Mark that a diff was executed."""
        self._last_diff_seq = diff.snapshot_seq
    
    @property
    def current_sequence(self) -> int:
        return self._sequence
    
    @property
    def last_snapshot(self) -> Optional[GridSnapshot]:
        return self._last_snapshot


def compute_grid_diff_from_snapshot(
    snapshot: GridSnapshot,
    desired_levels: List[GridLevel],
    tick_sz: float,
    px_decimals: int,
    reprice_threshold: int,
) -> GridDiffResult:
    """
    Compute grid diff using only snapshot data.
    
    This is a pure function - no side effects, no external state access.
    Safe to call without locks.
    
    Args:
        snapshot: Immutable state snapshot
        desired_levels: Target grid levels
        tick_sz: Price tick size
        px_decimals: Price decimal places
        reprice_threshold: Ticks of drift before repricing
        
    Returns:
        GridDiffResult with orders to cancel/place
    """
    from decimal import Decimal, ROUND_HALF_UP
    
    def price_key(side: str, px: float) -> str:
        px_dec = Decimal(str(px)).quantize(
            Decimal(10) ** -px_decimals,
            rounding=ROUND_HALF_UP
        )
        return f"{side}:{px_dec}"
    
    # Build desired keys
    desired_by_key: Dict[str, GridLevel] = {}
    for lvl in desired_levels:
        key = price_key(lvl.side, lvl.px)
        desired_by_key[key] = lvl
    
    existing_keys = snapshot.open_order_keys
    desired_keys = frozenset(desired_by_key.keys())
    
    # Compute diff
    to_cancel: List[OrderSnapshot] = []
    to_place: List[GridLevel] = []
    unchanged: List[str] = []
    
    # Orders to cancel: exist but not in desired
    for key in existing_keys:
        if key not in desired_keys:
            order = snapshot.get_order_by_key(key)
            if order:
                to_cancel.append(order)
        else:
            # Check if needs repricing
            order = snapshot.get_order_by_key(key)
            desired = desired_by_key[key]
            
            if order and reprice_threshold > 0:
                price_diff = abs(order.price - desired.px)
                tick_diff = price_diff / tick_sz if tick_sz > 0 else 0
                
                if tick_diff > reprice_threshold:
                    to_cancel.append(order)
                    to_place.append(desired)
                else:
                    unchanged.append(key)
            else:
                unchanged.append(key)
    
    # Orders to place: desired but don't exist
    for key, lvl in desired_by_key.items():
        if key not in existing_keys and key not in [price_key(l.side, l.px) for l in to_place]:
            to_place.append(lvl)
    
    return GridDiffResult(
        to_cancel=to_cancel,
        to_place=to_place,
        unchanged=unchanged,
        snapshot_seq=snapshot.sequence,
        computed_at_ms=int(time.time() * 1000),
    )
