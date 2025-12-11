"""
Order Sync: Synchronization layer between OrderManager and OrderStateMachine.

S-3 Structural Improvement: Ensures OrderManager (active order tracking) and 
OrderStateMachine (lifecycle state) are always in sync. Provides a unified 
interface for order operations that updates both systems atomically.

Features:
- Unified order registration updating both systems
- Automatic state machine transitions on order events
- Consistency validation between systems
- Audit trail for divergence detection
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Optional, Tuple

from src.execution.order_manager import OrderManager, ActiveOrder
from src.execution.order_state_machine import OrderStateMachine, OrderState, OrderStateRecord
from src.strategy.strategy import GridLevel

log = logging.getLogger("gridbot")


class OrderSyncError(Exception):
    """Raised when order sync detects inconsistency."""
    pass


class OrderSync:
    """
    Synchronization layer ensuring OrderManager and OrderStateMachine stay in sync.
    
    All order mutations should go through this class to ensure both systems
    are updated atomically. Direct mutations to OrderManager or OrderStateMachine
    may cause divergence.
    
    Usage:
        sync = OrderSync(order_manager, order_state_machine)
        
        # Register new order (updates both systems)
        record, state = sync.register_order(level, cloid, oid)
        
        # Update oid after exchange ack
        sync.acknowledge_order(cloid, oid)
        
        # Record fill
        sync.record_fill(cloid, oid, fill_sz, is_complete=False)
        
        # Cancel order
        sync.cancel_order(cloid, oid)
        
        # Validate consistency
        errors = sync.validate_consistency()
    """
    
    def __init__(
        self,
        order_manager: OrderManager,
        order_state_machine: OrderStateMachine,
        log_event: Optional[Callable[..., None]] = None,
    ) -> None:
        """
        Initialize OrderSync.
        
        Args:
            order_manager: OrderManager instance
            order_state_machine: OrderStateMachine instance
            log_event: Callback for structured logging
        """
        self.order_manager = order_manager
        self.order_state_machine = order_state_machine
        self._log = log_event or self._default_log
        
        # Statistics
        self._stats = {
            "orders_registered": 0,
            "orders_acknowledged": 0,
            "orders_filled": 0,
            "orders_cancelled": 0,
            "orders_rejected": 0,
            "consistency_checks": 0,
            "inconsistencies_found": 0,
        }
    
    def _default_log(self, event: str, **kwargs: Any) -> None:
        """Default logging when no callback provided."""
        log.debug(f'{{"event":"{event}",{",".join(f"{k}:{v}" for k,v in kwargs.items())}}}')
    
    # -------------------------------------------------------------------------
    # Order Registration
    # -------------------------------------------------------------------------
    
    def register_order(
        self,
        level: GridLevel,
        cloid: Optional[str],
        oid: Optional[int] = None,
    ) -> Tuple[ActiveOrder, Optional[OrderStateRecord]]:
        """
        Register order in both OrderManager and OrderStateMachine.
        
        Args:
            level: Grid level with order details
            cloid: Client order ID
            oid: Exchange order ID (optional at submission)
            
        Returns:
            Tuple of (ActiveOrder, OrderStateRecord)
        """
        # 1. Register in OrderManager
        active_order = self.order_manager.register(level, cloid, oid)
        
        # 2. Register in OrderStateMachine (if has cloid)
        state_record: Optional[OrderStateRecord] = None
        if cloid:
            # Check if already exists
            existing = self.order_state_machine.get_order(cloid=cloid)
            if not existing:
                state_record = self.order_state_machine.create_order(
                    cloid=cloid,
                    oid=oid,
                    side=level.side,
                    price=level.px,
                    qty=level.sz,
                )
            else:
                state_record = existing
        
        self._stats["orders_registered"] += 1
        
        self._log(
            "order_sync_register",
            cloid=cloid,
            oid=oid,
            side=level.side,
            price=level.px,
            size=level.sz,
        )
        
        return active_order, state_record
    
    # -------------------------------------------------------------------------
    # Order State Transitions
    # -------------------------------------------------------------------------
    
    def acknowledge_order(
        self,
        cloid: Optional[str],
        oid: Optional[int] = None,
        new_oid: Optional[int] = None,
    ) -> bool:
        """
        Acknowledge order (exchange accepted it).
        
        Updates oid if provided and transitions state machine to OPEN.
        
        Args:
            cloid: Client order ID
            oid: Existing exchange order ID (for lookup)
            new_oid: New exchange order ID from exchange
            
        Returns:
            True if acknowledged, False if not found
        """
        # Update oid in OrderManager if needed
        if new_oid is not None and cloid:
            active = self.order_manager.lookup(cloid=cloid)
            if active and active.oid != new_oid:
                # Need to update the oid index
                if active.oid is not None:
                    self.order_manager.orders_by_oid.pop(active.oid, None)
                active.oid = new_oid
                self.order_manager.orders_by_oid[new_oid] = active
        
        # Update oid in state machine
        if new_oid is not None and cloid:
            self.order_state_machine.update_oid(cloid, new_oid)
        
        # Transition state machine
        result = self.order_state_machine.acknowledge(cloid=cloid, oid=oid or new_oid)
        
        if result:
            self._stats["orders_acknowledged"] += 1
            self._log(
                "order_sync_acknowledge",
                cloid=cloid,
                oid=new_oid or oid,
            )
        
        return result
    
    def record_fill(
        self,
        cloid: Optional[str],
        oid: Optional[int],
        fill_sz: float,
        is_complete: bool = True,
    ) -> Optional[ActiveOrder]:
        """
        Record fill in both systems.
        
        Args:
            cloid: Client order ID
            oid: Exchange order ID
            fill_sz: Fill size
            is_complete: True if order fully filled
            
        Returns:
            ActiveOrder if found, None otherwise
        """
        # 1. Update OrderManager (handles partial fill logic)
        if is_complete:
            active = self.order_manager.pop_by_ids(cloid, oid)
        else:
            active = self.order_manager.handle_partial_fill(cloid, oid, fill_sz)
        
        # 2. Update state machine
        self.order_state_machine.fill(
            cloid=cloid,
            oid=oid,
            fill_qty=fill_sz,
            is_complete=is_complete,
        )
        
        if is_complete:
            self._stats["orders_filled"] += 1
        
        self._log(
            "order_sync_fill",
            cloid=cloid,
            oid=oid,
            fill_sz=fill_sz,
            is_complete=is_complete,
        )
        
        return active
    
    def cancel_order(
        self,
        cloid: Optional[str],
        oid: Optional[int],
        reason: str = "user_cancel",
    ) -> Optional[ActiveOrder]:
        """
        Cancel order in both systems.
        
        Args:
            cloid: Client order ID
            oid: Exchange order ID
            reason: Cancel reason for audit
            
        Returns:
            ActiveOrder if found and removed, None otherwise
        """
        # 1. Remove from OrderManager
        active = self.order_manager.pop_by_ids(cloid, oid)
        
        # 2. Transition state machine
        self.order_state_machine.cancel(cloid=cloid, oid=oid, reason=reason)
        
        self._stats["orders_cancelled"] += 1
        
        self._log(
            "order_sync_cancel",
            cloid=cloid,
            oid=oid,
            reason=reason,
        )
        
        return active
    
    def reject_order(
        self,
        cloid: Optional[str],
        oid: Optional[int] = None,
        error_code: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> Optional[ActiveOrder]:
        """
        Reject order in both systems.
        
        Args:
            cloid: Client order ID
            oid: Exchange order ID
            error_code: Error code from exchange
            error_message: Error message
            
        Returns:
            ActiveOrder if found and removed, None otherwise
        """
        # 1. Remove from OrderManager
        active = self.order_manager.pop_by_ids(cloid, oid)
        
        # 2. Transition state machine
        self.order_state_machine.reject(
            cloid=cloid,
            oid=oid,
            error_code=error_code,
            error_message=error_message,
        )
        
        self._stats["orders_rejected"] += 1
        
        self._log(
            "order_sync_reject",
            cloid=cloid,
            oid=oid,
            error_code=error_code,
        )
        
        return active
    
    def expire_order(
        self,
        cloid: Optional[str],
        oid: Optional[int],
        reason: str = "timeout",
    ) -> Optional[ActiveOrder]:
        """
        Expire order in both systems.
        
        Args:
            cloid: Client order ID
            oid: Exchange order ID  
            reason: Expiry reason
            
        Returns:
            ActiveOrder if found and removed
        """
        # 1. Remove from OrderManager
        active = self.order_manager.pop_by_ids(cloid, oid)
        
        # 2. Transition state machine
        self.order_state_machine.expire(cloid=cloid, oid=oid, reason=reason)
        
        self._log(
            "order_sync_expire",
            cloid=cloid,
            oid=oid,
            reason=reason,
        )
        
        return active
    
    # -------------------------------------------------------------------------
    # Removal Operations
    # -------------------------------------------------------------------------
    
    def remove_order(
        self,
        cloid: Optional[str] = None,
        oid: Optional[int] = None,
    ) -> bool:
        """
        Remove order from both systems without state transition.
        
        Use this for cleanup when order is already in terminal state
        or for forced removal.
        
        Args:
            cloid: Client order ID
            oid: Exchange order ID
            
        Returns:
            True if removed from at least one system
        """
        removed_manager = False
        removed_state = False
        
        # Remove from OrderManager
        active = self.order_manager.pop_by_ids(cloid, oid)
        if active:
            removed_manager = True
        
        # Remove from state machine
        removed_state = self.order_state_machine.remove_order(cloid=cloid, oid=oid)
        
        if removed_manager or removed_state:
            self._log(
                "order_sync_remove",
                cloid=cloid,
                oid=oid,
                removed_manager=removed_manager,
                removed_state=removed_state,
            )
        
        return removed_manager or removed_state
    
    # -------------------------------------------------------------------------
    # Consistency Validation
    # -------------------------------------------------------------------------
    
    def validate_consistency(self) -> List[Dict[str, Any]]:
        """
        Check consistency between OrderManager and OrderStateMachine.
        
        Returns list of inconsistencies found.
        
        Checks:
        1. Orders in manager but not in state machine
        2. Active orders in state machine but not in manager
        3. State mismatches (e.g., manager has order, state machine says filled)
        """
        errors: List[Dict[str, Any]] = []
        self._stats["consistency_checks"] += 1
        
        # Get all orders from each system
        manager_orders = set()
        for cloid in self.order_manager.orders_by_cloid:
            manager_orders.add(("cloid", cloid))
        for oid in self.order_manager.orders_by_oid:
            manager_orders.add(("oid", oid))
        
        active_state_orders = set()
        for record in self.order_state_machine.get_active_orders():
            if record.cloid:
                active_state_orders.add(("cloid", record.cloid))
            if record.oid is not None:
                active_state_orders.add(("oid", record.oid))
        
        # Check 1: Orders in manager but not in active state machine orders
        # (these might be filled/cancelled in state machine but still in manager)
        for id_type, id_val in manager_orders:
            if id_type == "cloid":
                state_rec = self.order_state_machine.get_order(cloid=id_val)
            else:
                state_rec = self.order_state_machine.get_order(oid=id_val)
            
            if not state_rec:
                errors.append({
                    "type": "manager_only",
                    "id_type": id_type,
                    "id_val": id_val,
                    "message": "Order in manager but not in state machine",
                })
            elif self.order_state_machine.is_terminal(cloid=state_rec.cloid, oid=state_rec.oid):
                errors.append({
                    "type": "terminal_in_manager",
                    "id_type": id_type,
                    "id_val": id_val,
                    "state": state_rec.state.name,
                    "message": "Terminal order still in manager",
                })
        
        # Check 2: Active orders in state machine but not in manager
        for id_type, id_val in active_state_orders:
            in_manager = False
            if id_type == "cloid":
                in_manager = id_val in self.order_manager.orders_by_cloid
            else:
                in_manager = id_val in self.order_manager.orders_by_oid
            
            if not in_manager:
                errors.append({
                    "type": "state_only",
                    "id_type": id_type,
                    "id_val": id_val,
                    "message": "Active order in state machine but not in manager",
                })
        
        if errors:
            self._stats["inconsistencies_found"] += len(errors)
            self._log(
                "order_sync_inconsistency",
                error_count=len(errors),
                errors=errors[:5],  # Log first 5 to avoid spam
            )
        
        return errors
    
    def reconcile(self) -> int:
        """
        Fix inconsistencies between systems.
        
        Strategy: 
        - Remove terminal orders from manager
        - Remove orphaned orders from state machine
        
        Returns:
            Number of corrections made
        """
        corrections = 0
        errors = self.validate_consistency()
        
        for error in errors:
            if error["type"] == "terminal_in_manager":
                # Remove terminal order from manager
                id_type = error["id_type"]
                id_val = error["id_val"]
                if id_type == "cloid":
                    active = self.order_manager.pop_by_ids(id_val, None)
                else:
                    active = self.order_manager.pop_by_ids(None, id_val)
                if active:
                    corrections += 1
                    self._log(
                        "order_sync_reconcile_remove",
                        id_type=id_type,
                        id_val=id_val,
                        reason="terminal_in_manager",
                    )
            
            elif error["type"] == "manager_only":
                # Order exists in manager but not state machine - create state record
                id_type = error["id_type"]
                id_val = error["id_val"]
                if id_type == "cloid":
                    active = self.order_manager.lookup(cloid=id_val)
                else:
                    active = self.order_manager.lookup(oid=id_val)
                
                if active and active.cloid:
                    self.order_state_machine.create_order(
                        cloid=active.cloid,
                        oid=active.oid,
                        side=active.level.side,
                        price=active.level.px,
                        qty=active.original_qty,
                    )
                    # Acknowledge if has oid
                    if active.oid is not None:
                        self.order_state_machine.acknowledge(cloid=active.cloid)
                    corrections += 1
                    self._log(
                        "order_sync_reconcile_create",
                        cloid=active.cloid,
                        oid=active.oid,
                        reason="manager_only",
                    )
        
        if corrections:
            self._log(
                "order_sync_reconciled",
                corrections=corrections,
            )
        
        return corrections
    
    # -------------------------------------------------------------------------
    # Query Methods
    # -------------------------------------------------------------------------
    
    def get_order(
        self,
        cloid: Optional[str] = None,
        oid: Optional[int] = None,
    ) -> Optional[Tuple[Optional[ActiveOrder], Optional[OrderStateRecord]]]:
        """
        Get order from both systems.
        
        Returns:
            Tuple of (ActiveOrder or None, OrderStateRecord or None)
        """
        active = self.order_manager.lookup(cloid=cloid, oid=oid)
        state = self.order_state_machine.get_order(cloid=cloid, oid=oid)
        
        if active is None and state is None:
            return None
        
        return (active, state)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get synchronization statistics."""
        return {
            **self._stats,
            "manager_count": self.order_manager.open_count(),
            "state_active_count": len(self.order_state_machine.get_active_orders()),
        }
    
    def clear(self) -> None:
        """Clear both systems (for testing/reset)."""
        self.order_manager.clear()
        self.order_state_machine.clear()
        self._log("order_sync_cleared")
