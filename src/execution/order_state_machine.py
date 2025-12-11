"""
Order State Machine - Explicit order lifecycle management.

Provides a formal state machine for order lifecycle tracking with:
- Explicit states: PENDING, OPEN, PARTIALLY_FILLED, FILLED, CANCELLED, REJECTED, EXPIRED
- Valid state transitions with guards
- Audit trail of state changes
- Prevents invalid transitions

This module ensures orders cannot end up in inconsistent states and provides
clear visibility into order lifecycle for debugging and monitoring.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Callable, Dict, List, Optional, Any

log = logging.getLogger("gridbot")


class OrderState(Enum):
    """
    Order lifecycle states.
    
    State Diagram:
    
    PENDING ─────┬──────────> OPEN ─────────┬──────> FILLED
                 │             │              │
                 │             ▼              │
                 │      PARTIALLY_FILLED ────┘
                 │             │
                 ▼             ▼
             REJECTED      CANCELLED
                 │             │
                 └─────────────┴──────> EXPIRED (timeout)
    """
    PENDING = auto()           # Order submitted, awaiting exchange acknowledgement
    OPEN = auto()              # Order accepted by exchange, resting on book
    PARTIALLY_FILLED = auto()  # Order partially filled, remainder on book
    FILLED = auto()            # Order completely filled (terminal)
    CANCELLED = auto()         # Order cancelled (terminal)
    REJECTED = auto()          # Order rejected by exchange (terminal)
    EXPIRED = auto()           # Order expired/timed out (terminal)


@dataclass
class StateTransition:
    """Record of a state transition."""
    from_state: OrderState
    to_state: OrderState
    timestamp_ms: int
    reason: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass 
class OrderStateRecord:
    """
    Complete state record for an order.
    
    Tracks current state, all transitions, and accumulated fill info.
    """
    cloid: Optional[str]
    oid: Optional[int]
    side: str
    price: float
    original_qty: float
    
    state: OrderState = OrderState.PENDING
    filled_qty: float = 0.0
    remaining_qty: float = 0.0
    
    created_at_ms: int = 0
    last_updated_ms: int = 0
    
    # Transition history for audit trail
    transitions: List[StateTransition] = field(default_factory=list)
    
    # Error info if rejected/failed
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    
    def __post_init__(self):
        now = int(time.time() * 1000)
        if self.created_at_ms == 0:
            self.created_at_ms = now
        self.last_updated_ms = now
        self.remaining_qty = self.original_qty


# Valid state transitions
VALID_TRANSITIONS: Dict[OrderState, List[OrderState]] = {
    OrderState.PENDING: [
        OrderState.OPEN,           # Exchange accepted
        OrderState.REJECTED,       # Exchange rejected
        OrderState.FILLED,         # Immediately filled (market order)
        OrderState.CANCELLED,      # Cancelled before ack
        OrderState.EXPIRED,        # Timeout waiting for ack
    ],
    OrderState.OPEN: [
        OrderState.PARTIALLY_FILLED,  # First partial fill
        OrderState.FILLED,            # Fully filled
        OrderState.CANCELLED,         # User/system cancelled
        OrderState.EXPIRED,           # TTL expired
    ],
    OrderState.PARTIALLY_FILLED: [
        OrderState.PARTIALLY_FILLED,  # Additional partial fill
        OrderState.FILLED,            # Final fill
        OrderState.CANCELLED,         # Cancel remainder
    ],
    # Terminal states - no transitions allowed
    OrderState.FILLED: [],
    OrderState.CANCELLED: [],
    OrderState.REJECTED: [],
    OrderState.EXPIRED: [],
}


class OrderStateMachine:
    """
    Manages order state transitions with validation.
    
    Provides:
    - State transition validation
    - Callback hooks for state changes
    - Audit trail of all transitions
    - Metrics/logging integration
    
    Thread-safety: Individual order updates should be externally synchronized.
    This class provides logical consistency, not thread synchronization.
    """
    
    def __init__(
        self,
        log_event: Optional[Callable[..., None]] = None,
        on_state_change: Optional[Callable[[OrderStateRecord, OrderState, OrderState], None]] = None,
    ) -> None:
        """
        Initialize OrderStateMachine.
        
        Args:
            log_event: Callback for structured logging
            on_state_change: Callback when any order changes state
        """
        self._log_event = log_event or self._default_log
        self._on_state_change = on_state_change
        
        # Order records by cloid and oid
        self._orders_by_cloid: Dict[str, OrderStateRecord] = {}
        self._orders_by_oid: Dict[int, OrderStateRecord] = {}
        
        # Statistics
        self._stats = {
            "total_created": 0,
            "total_filled": 0,
            "total_cancelled": 0,
            "total_rejected": 0,
            "invalid_transitions_blocked": 0,
        }
    
    def _default_log(self, event: str, **kwargs) -> None:
        """Default logging when no callback provided."""
        import json
        log.info(json.dumps({"event": event, **kwargs}))
    
    def create_order(
        self,
        cloid: Optional[str],
        oid: Optional[int],
        side: str,
        price: float,
        qty: float,
    ) -> OrderStateRecord:
        """
        Create a new order in PENDING state.
        
        Args:
            cloid: Client order ID
            oid: Exchange order ID (may be None initially)
            side: 'buy' or 'sell'
            price: Order price
            qty: Order quantity
            
        Returns:
            New OrderStateRecord in PENDING state
        """
        record = OrderStateRecord(
            cloid=cloid,
            oid=oid,
            side=side,
            price=price,
            original_qty=qty,
        )
        
        # Index the order
        if cloid:
            self._orders_by_cloid[cloid] = record
        if oid is not None:
            self._orders_by_oid[oid] = record
        
        self._stats["total_created"] += 1
        
        # Demote to DEBUG - too noisy during grid builds
        log.debug(
            "order_created cloid=%s side=%s px=%s sz=%s",
            cloid, side, price, qty
        )
        
        return record
    
    def get_order(
        self,
        cloid: Optional[str] = None,
        oid: Optional[int] = None,
    ) -> Optional[OrderStateRecord]:
        """
        Look up order by cloid or oid.
        
        Args:
            cloid: Client order ID
            oid: Exchange order ID
            
        Returns:
            OrderStateRecord if found, None otherwise
        """
        if cloid and cloid in self._orders_by_cloid:
            return self._orders_by_cloid[cloid]
        if oid is not None and oid in self._orders_by_oid:
            return self._orders_by_oid[oid]
        return None
    
    def update_oid(self, cloid: str, oid: int) -> bool:
        """
        Update the exchange order ID for an order.
        
        Called when exchange returns oid after order submission.
        
        Args:
            cloid: Client order ID
            oid: Exchange order ID
            
        Returns:
            True if updated, False if order not found
        """
        record = self._orders_by_cloid.get(cloid)
        if not record:
            return False
        
        record.oid = oid
        self._orders_by_oid[oid] = record
        record.last_updated_ms = int(time.time() * 1000)
        
        return True
    
    def transition(
        self,
        cloid: Optional[str] = None,
        oid: Optional[int] = None,
        to_state: OrderState = None,
        reason: Optional[str] = None,
        fill_qty: float = 0.0,
        error_code: Optional[str] = None,
        error_message: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Attempt to transition an order to a new state.
        
        Validates the transition is allowed, updates the record,
        and logs/callbacks as appropriate.
        
        Args:
            cloid: Client order ID
            oid: Exchange order ID
            to_state: Target state
            reason: Human-readable reason for transition
            fill_qty: Quantity filled (for PARTIALLY_FILLED/FILLED)
            error_code: Error code (for REJECTED)
            error_message: Error message (for REJECTED)
            metadata: Additional metadata for audit trail
            
        Returns:
            True if transition succeeded, False if blocked
        """
        record = self.get_order(cloid=cloid, oid=oid)
        if not record:
            self._log_event(
                "order_state_transition_failed",
                cloid=cloid,
                oid=oid,
                reason="order_not_found",
                to_state=to_state.name if to_state else None,
            )
            return False
        
        if to_state is None:
            return False
        
        from_state = record.state
        
        # Check if transition is valid
        if not self._is_valid_transition(from_state, to_state):
            self._stats["invalid_transitions_blocked"] += 1
            self._log_event(
                "order_state_invalid_transition",
                cloid=cloid,
                oid=oid,
                from_state=from_state.name,
                to_state=to_state.name,
                reason=reason,
            )
            return False
        
        # Record transition
        now_ms = int(time.time() * 1000)
        transition = StateTransition(
            from_state=from_state,
            to_state=to_state,
            timestamp_ms=now_ms,
            reason=reason,
            metadata=metadata or {},
        )
        record.transitions.append(transition)
        
        # Update state
        record.state = to_state
        record.last_updated_ms = now_ms
        
        # Handle fill updates
        if fill_qty > 0:
            record.filled_qty += fill_qty
            record.remaining_qty = max(0, record.original_qty - record.filled_qty)
        
        # Handle error info
        if error_code:
            record.error_code = error_code
        if error_message:
            record.error_message = error_message
        
        # Update statistics
        if to_state == OrderState.FILLED:
            self._stats["total_filled"] += 1
        elif to_state == OrderState.CANCELLED:
            self._stats["total_cancelled"] += 1
        elif to_state == OrderState.REJECTED:
            self._stats["total_rejected"] += 1
        
        # Log transition - demote ACKs to DEBUG level
        if from_state == OrderState.PENDING and to_state == OrderState.OPEN:
            # Routine ACK - just log at DEBUG level
            log.debug(
                "order_ack cloid=%s oid=%s",
                cloid, oid
            )
        else:
            # Important transitions (fills, cancels, rejects) - keep at INFO
            self._log_event(
                "order_state_transition",
                cloid=cloid,
                oid=oid,
                from_state=from_state.name,
                to_state=to_state.name,
                reason=reason,
                fill_qty=fill_qty,
                filled_qty=record.filled_qty,
                remaining_qty=record.remaining_qty,
            )
        
        # Fire callback
        if self._on_state_change:
            try:
                self._on_state_change(record, from_state, to_state)
            except Exception as e:
                self._log_event(
                    "order_state_callback_error",
                    error=str(e),
                    cloid=cloid,
                    oid=oid,
                )
        
        return True
    
    def acknowledge(
        self,
        cloid: Optional[str] = None,
        oid: Optional[int] = None,
    ) -> bool:
        """
        Acknowledge order accepted by exchange (PENDING -> OPEN).
        
        Convenience method for common transition.
        """
        return self.transition(
            cloid=cloid,
            oid=oid,
            to_state=OrderState.OPEN,
            reason="exchange_ack",
        )
    
    def fill(
        self,
        cloid: Optional[str] = None,
        oid: Optional[int] = None,
        fill_qty: float = 0.0,
        is_complete: bool = True,
    ) -> bool:
        """
        Record a fill on the order.
        
        Args:
            cloid: Client order ID
            oid: Exchange order ID
            fill_qty: Quantity filled
            is_complete: True if fully filled, False if partial
            
        Returns:
            True if transition succeeded
        """
        to_state = OrderState.FILLED if is_complete else OrderState.PARTIALLY_FILLED
        return self.transition(
            cloid=cloid,
            oid=oid,
            to_state=to_state,
            reason="fill",
            fill_qty=fill_qty,
        )
    
    def cancel(
        self,
        cloid: Optional[str] = None,
        oid: Optional[int] = None,
        reason: str = "user_cancel",
    ) -> bool:
        """
        Cancel the order (transition to CANCELLED).
        """
        return self.transition(
            cloid=cloid,
            oid=oid,
            to_state=OrderState.CANCELLED,
            reason=reason,
        )
    
    def reject(
        self,
        cloid: Optional[str] = None,
        oid: Optional[int] = None,
        error_code: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> bool:
        """
        Reject the order (transition to REJECTED).
        """
        return self.transition(
            cloid=cloid,
            oid=oid,
            to_state=OrderState.REJECTED,
            reason="rejected",
            error_code=error_code,
            error_message=error_message,
        )
    
    def expire(
        self,
        cloid: Optional[str] = None,
        oid: Optional[int] = None,
        reason: str = "timeout",
    ) -> bool:
        """
        Expire the order (transition to EXPIRED).
        """
        return self.transition(
            cloid=cloid,
            oid=oid,
            to_state=OrderState.EXPIRED,
            reason=reason,
        )
    
    def _is_valid_transition(self, from_state: OrderState, to_state: OrderState) -> bool:
        """Check if transition is allowed."""
        valid_targets = VALID_TRANSITIONS.get(from_state, [])
        return to_state in valid_targets
    
    def is_terminal(self, cloid: Optional[str] = None, oid: Optional[int] = None) -> bool:
        """Check if order is in a terminal state."""
        record = self.get_order(cloid=cloid, oid=oid)
        if not record:
            return True  # Non-existent orders are effectively terminal
        return record.state in (
            OrderState.FILLED,
            OrderState.CANCELLED,
            OrderState.REJECTED,
            OrderState.EXPIRED,
        )
    
    def is_active(self, cloid: Optional[str] = None, oid: Optional[int] = None) -> bool:
        """Check if order is still active (not terminal)."""
        return not self.is_terminal(cloid=cloid, oid=oid)
    
    def remove_order(self, cloid: Optional[str] = None, oid: Optional[int] = None) -> bool:
        """
        Remove order from tracking (typically after terminal state).
        
        Returns True if removed, False if not found.
        """
        record = self.get_order(cloid=cloid, oid=oid)
        if not record:
            return False
        
        if record.cloid and record.cloid in self._orders_by_cloid:
            del self._orders_by_cloid[record.cloid]
        if record.oid is not None and record.oid in self._orders_by_oid:
            del self._orders_by_oid[record.oid]
        
        return True
    
    def get_orders_by_state(self, state: OrderState) -> List[OrderStateRecord]:
        """Get all orders in a specific state."""
        return [
            rec for rec in self._orders_by_cloid.values()
            if rec.state == state
        ]
    
    def get_active_orders(self) -> List[OrderStateRecord]:
        """Get all non-terminal orders."""
        terminal = {
            OrderState.FILLED,
            OrderState.CANCELLED,
            OrderState.REJECTED,
            OrderState.EXPIRED,
        }
        return [
            rec for rec in self._orders_by_cloid.values()
            if rec.state not in terminal
        ]
    
    def get_stats(self) -> Dict[str, Any]:
        """Get state machine statistics."""
        return {
            **self._stats,
            "active_orders": len(self.get_active_orders()),
            "pending": len(self.get_orders_by_state(OrderState.PENDING)),
            "open": len(self.get_orders_by_state(OrderState.OPEN)),
            "partially_filled": len(self.get_orders_by_state(OrderState.PARTIALLY_FILLED)),
        }
    
    def clear(self) -> None:
        """Clear all orders (for testing/reset)."""
        self._orders_by_cloid.clear()
        self._orders_by_oid.clear()
    
    def prune_terminal(self, max_age_ms: int = 300_000) -> int:
        """
        Remove terminal orders older than max_age_ms.
        
        Keeps memory bounded by cleaning up completed orders.
        
        Args:
            max_age_ms: Maximum age in milliseconds (default 5 minutes)
            
        Returns:
            Number of orders pruned
        """
        now_ms = int(time.time() * 1000)
        cutoff_ms = now_ms - max_age_ms
        
        terminal_states = {
            OrderState.FILLED,
            OrderState.CANCELLED,
            OrderState.REJECTED,
            OrderState.EXPIRED,
        }
        
        to_prune = []
        for cloid, record in self._orders_by_cloid.items():
            if record.state in terminal_states and record.last_updated_ms < cutoff_ms:
                to_prune.append(cloid)
        
        for cloid in to_prune:
            self.remove_order(cloid=cloid)
        
        if to_prune:
            self._log_event("order_state_pruned", count=len(to_prune))
        
        return len(to_prune)

    def get_stuck_orders(self, max_pending_age_ms: int = 30_000) -> List[OrderStateRecord]:
        """
        Get orders stuck in PENDING state longer than threshold.
        
        Orders in PENDING state should transition to OPEN (ack) or REJECTED
        within a few seconds. Orders stuck longer may indicate network issues
        or lost acknowledgements.
        
        Args:
            max_pending_age_ms: Maximum time in PENDING before considered stuck (default 30s)
            
        Returns:
            List of stuck OrderStateRecords
        """
        now_ms = int(time.time() * 1000)
        cutoff_ms = now_ms - max_pending_age_ms
        
        stuck = []
        for record in self._orders_by_cloid.values():
            if record.state == OrderState.PENDING and record.created_at_ms < cutoff_ms:
                stuck.append(record)
        
        return stuck

