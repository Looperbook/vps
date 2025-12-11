"""
ExecutionGateway: Unified order execution interface for grid trading.

This module provides a single entry point for all order operations:
- Order submission (single and batch)
- Order cancellation (single, side, all)
- Order lifecycle state management
- Event emission for order events

Architecture:
    ExecutionGateway wraps OrderRouter, OrderManager, and OrderStateMachine
    to provide a clean, high-level interface for order operations. The bot
    no longer needs to coordinate these components directly.

Thread Safety:
    Uses internal locking for order index operations.
    Delegates async execution to OrderRouter.
"""

from __future__ import annotations

import asyncio
import secrets
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, TYPE_CHECKING

from src.core.utils import to_int_safe

if TYPE_CHECKING:
    from src.execution.order_router import OrderRouter, OrderRequest
    from src.execution.order_manager import OrderManager, ActiveOrder
    from src.execution.order_state_machine import OrderStateMachine
    from src.state.shadow_ledger import ShadowLedger
    from src.core.event_bus import EventBus
    from src.monitoring.metrics_rich import RichMetrics
    from src.risk.circuit_breaker import CircuitBreaker
    from src.strategy.strategy import GridLevel

import logging

log = logging.getLogger("gridbot")


@dataclass
class SubmitResult:
    """Result of order submission."""
    success: bool
    cloid: Optional[str] = None
    oid: Optional[int] = None
    error: Optional[str] = None
    level: Optional[Any] = None  # GridLevel
    
    @property
    def has_ids(self) -> bool:
        """Has at least one valid ID."""
        return self.cloid is not None or self.oid is not None


@dataclass
class CancelResult:
    """Result of order cancellation."""
    success: bool
    cancelled_count: int = 0
    errors: List[str] = field(default_factory=list)
    order_gone: bool = False  # C-6 FIX: True if order confirmed gone from exchange


@dataclass
class ExecutionGatewayConfig:
    """Configuration for ExecutionGateway."""
    # Order settings
    use_cloid: bool = True
    
    # C-6 FIX: HTTP timeout for exchange queries
    http_timeout: float = 10.0
    
    # DEX setting for API calls
    dex: str = "hl"
    
    # Logging
    log_order_intent: bool = True
    log_event_callback: Optional[Callable[..., None]] = None


class ExecutionGateway:
    """
    Unified order execution interface.
    
    Encapsulates coordination between:
    - OrderRouter: Low-level order submission
    - OrderManager: Order indexing and tracking
    - OrderStateMachine: Order lifecycle states
    - ShadowLedger: Pending order tracking
    - EventBus: Order event publication
    - CircuitBreaker: Error tracking
    
    Benefits:
    - Single entry point for all order operations
    - Consistent state management across components
    - Clean separation from bot orchestration
    """
    
    def __init__(
        self,
        coin: str,
        router: "OrderRouter",
        order_manager: "OrderManager",
        order_state_machine: "OrderStateMachine",
        shadow_ledger: "ShadowLedger",
        event_bus: "EventBus",
        rich_metrics: "RichMetrics",
        circuit_breaker: "CircuitBreaker",
        config: Optional[ExecutionGatewayConfig] = None,
        # C-6 FIX: Optional dependencies for order verification
        info: Optional[Any] = None,
        async_info: Optional[Any] = None,
        account: Optional[str] = None,
    ) -> None:
        """
        Initialize ExecutionGateway with dependencies.
        
        Args:
            coin: Trading symbol
            router: Order submission/cancellation
            order_manager: Order indexing
            order_state_machine: Order lifecycle tracking
            shadow_ledger: Pending order tracking
            event_bus: Event publication
            rich_metrics: Prometheus metrics
            circuit_breaker: Error tracking
            config: Optional configuration
            info: Hyperliquid Info API (for C-6 FIX verification)
            async_info: Async Info API (for C-6 FIX verification)
            account: Trading account address
        """
        self.coin = coin
        self.router = router
        self.order_manager = order_manager
        self.order_state_machine = order_state_machine
        self.shadow_ledger = shadow_ledger
        self.event_bus = event_bus
        self.rich_metrics = rich_metrics
        self.circuit_breaker = circuit_breaker
        self.config = config or ExecutionGatewayConfig()
        
        # C-6 FIX dependencies
        self.info = info
        self.async_info = async_info
        self.account = account
        
        # Flatten mode flag (prevents opening new exposure)
        self._flatten_mode = False
        
        # Logging callback
        self._log_event = self.config.log_event_callback or self._default_log
    
    def _default_log(self, event: str, **kwargs: Any) -> None:
        """Default logging."""
        import json
        payload = {"event": event, "coin": self.coin, **kwargs}
        log.info(json.dumps(payload))
    
    def _generate_cloid(self) -> str:
        """Generate a new client order ID."""
        return f"0x{secrets.token_hex(16)}"
    
    @property
    def flatten_mode(self) -> bool:
        """Whether flatten mode is active."""
        return self._flatten_mode
    
    @flatten_mode.setter
    def flatten_mode(self, value: bool) -> None:
        """Set flatten mode."""
        self._flatten_mode = value
        if value:
            self._log_event("flatten_mode_enabled")
        else:
            self._log_event("flatten_mode_disabled")
    
    # ========== Order Submission ==========
    
    async def submit_level(
        self,
        level: "GridLevel",
        mode: str = "grid",
        position: float = 0.0,
    ) -> SubmitResult:
        """
        Submit a single order level.
        
        Args:
            level: GridLevel to submit
            mode: Order mode for logging (grid, replace, etc.)
            position: Current position (for flatten mode checks)
            
        Returns:
            SubmitResult with outcome
        """
        if not self.router:
            return SubmitResult(success=False, error="no_router")
        
        # Flatten mode check
        if self._flatten_mode:
            disallowed = "buy" if position > 0 else "sell"
            if level.side == disallowed:
                self._log_event(
                    "flatten_skip_level",
                    side=level.side,
                    px=level.px,
                    sz=level.sz,
                    mode=mode,
                )
                return SubmitResult(success=False, error="flatten_blocked")
        
        # Generate cloid if enabled
        pre_cloid = self._generate_cloid() if self.config.use_cloid else None
        
        # Create order request
        from src.execution.order_router import OrderRequest
        req = OrderRequest(
            is_buy=level.side == "buy",
            sz=level.sz,
            px=level.px,
            reduce_only=False,
            cloid=pre_cloid,
        )
        
        # Create in state machine (PENDING)
        self.order_state_machine.create_order(
            cloid=pre_cloid,
            oid=None,
            side=level.side,
            price=level.px,
            qty=level.sz,
        )
        
        if self.config.log_order_intent:
            self._log_event(
                "order_intent",
                side=level.side,
                px=level.px,
                sz=level.sz,
                mode=mode,
            )
        
        # Submit
        try:
            resp = await self.router.submit(req)
        except Exception as exc:
            self._log_event(
                "order_submit_error",
                side=level.side,
                error=str(exc),
                mode=mode,
            )
            self.order_state_machine.reject(
                cloid=pre_cloid,
                error_code="submit_error",
                error_message=str(exc),
            )
            self.circuit_breaker.record_error("submit", exc)
            return SubmitResult(success=False, error=str(exc), level=level)
        
        # Process response
        return await self._process_submit_response(resp, level, pre_cloid, mode)
    
    async def _process_submit_response(
        self,
        resp: Any,
        level: "GridLevel",
        pre_cloid: Optional[str],
        mode: str,
    ) -> SubmitResult:
        """Process order submission response."""
        if not isinstance(resp, dict):
            self._log_event(
                "order_submit_ack",
                side=level.side,
                px=level.px,
                sz=level.sz,
                oid=None,
                cloid=None,
                mode=mode,
                status="noop",
            )
            return SubmitResult(success=False, error="invalid_response", level=level)
        
        oid_val = to_int_safe(resp.get("oid"))
        cloid_val = resp.get("cloid") or pre_cloid
        cloid_str = str(cloid_val) if cloid_val else None
        
        # Update level
        level.cloid = cloid_str
        level.oid = oid_val
        
        # Register in order manager
        self.order_manager.register(level, cloid_str, oid_val)
        
        # Update state machine
        if cloid_str:
            if oid_val is not None:
                self.order_state_machine.update_oid(cloid_str, oid_val)
            self.order_state_machine.acknowledge(cloid=cloid_str)
        
        # Track in shadow ledger
        try:
            self.shadow_ledger.add_pending_order(
                cloid=cloid_str or str(oid_val),
                side=level.side,
                qty=level.sz,
            )
        except Exception:
            pass
        
        # Emit event
        await self._emit_order_submitted(level, oid_val, cloid_str)
        
        # Update metrics
        try:
            self.rich_metrics.orders_submitted.labels(
                coin=self.coin, side=level.side
            ).inc()
        except Exception:
            pass
        
        # Log
        if oid_val is None:
            self._log_event(
                "order_ack_missing_oid",
                side=level.side,
                px=level.px,
                sz=level.sz,
                cloid=cloid_str,
                mode=mode,
                status="cloid_only" if cloid_str else "missing_ids",
            )
        else:
            self._log_event(
                "order_submit_ack",
                side=level.side,
                px=level.px,
                sz=level.sz,
                oid=oid_val,
                cloid=cloid_str,
                mode=mode,
            )
        
        return SubmitResult(
            success=True,
            cloid=cloid_str,
            oid=oid_val,
            level=level,
        )
    
    async def submit_levels_batch(
        self,
        levels: List["GridLevel"],
        mode: str = "grid",
        position: float = 0.0,
    ) -> List[SubmitResult]:
        """
        Submit multiple order levels in batch.
        
        Args:
            levels: List of GridLevels to submit
            mode: Order mode for logging
            position: Current position (for flatten mode)
            
        Returns:
            List of SubmitResults
        """
        if not levels or not self.router:
            return []
        
        from src.execution.order_router import OrderRequest
        
        tasks = []
        metas = []  # (level, pre_cloid)
        
        for level in levels:
            # Flatten mode check
            if self._flatten_mode:
                disallowed = "buy" if position > 0 else "sell"
                if level.side == disallowed:
                    continue
            
            # Generate cloid
            pre_cloid = self._generate_cloid() if self.config.use_cloid else None
            level.cloid = pre_cloid
            
            # Create request
            req = OrderRequest(
                is_buy=level.side == "buy",
                sz=level.sz,
                px=level.px,
                reduce_only=False,
                cloid=pre_cloid,
            )
            
            # Create in state machine
            self.order_state_machine.create_order(
                cloid=pre_cloid,
                oid=None,
                side=level.side,
                price=level.px,
                qty=level.sz,
            )
            
            if self.config.log_order_intent:
                self._log_event(
                    "order_intent",
                    side=level.side,
                    px=level.px,
                    sz=level.sz,
                    mode=mode,
                )
            
            tasks.append(asyncio.create_task(self.router.submit(req)))
            metas.append((level, pre_cloid))
        
        if not tasks:
            return []
        
        # Execute all submissions
        results_raw = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        results = []
        batch_errors = 0
        
        for (level, pre_cloid), resp in zip(metas, results_raw):
            if isinstance(resp, Exception):
                batch_errors += 1
                self._log_event(
                    "order_submit_error",
                    side=level.side,
                    error=str(resp),
                    mode=mode,
                )
                self.order_state_machine.reject(
                    cloid=pre_cloid,
                    error_code="submit_error",
                    error_message=str(resp),
                )
                self.circuit_breaker.record_error("batch_submit", resp)
                try:
                    self.rich_metrics.orders_rejected.labels(
                        coin=self.coin, reason="submit_error"
                    ).inc()
                except Exception:
                    pass
                results.append(SubmitResult(
                    success=False,
                    error=str(resp),
                    level=level,
                ))
                continue
            
            result = await self._process_submit_response(resp, level, pre_cloid, mode)
            results.append(result)
        
        if batch_errors > 0:
            self._log_event(
                "batch_submit_errors",
                total=len(levels),
                errors=batch_errors,
                mode=mode,
            )
        
        return results
    
    # ========== Order Cancellation ==========
    
    async def cancel_all(self, reason: str = "") -> CancelResult:
        """
        Cancel all open orders.
        
        Args:
            reason: Reason for cancellation (for logging)
            
        Returns:
            CancelResult with outcome
        """
        if not self.router:
            return CancelResult(success=False, errors=["no_router"])
        
        open_count = self.order_manager.open_count()
        self._log_event(
            "cancel_all_start",
            open_orders=open_count,
            reason=reason or None,
        )
        
        errors = []
        try:
            await self.router.cancel_all()
            self._log_event("cancel_all_complete", reason=reason or None)
        except Exception as exc:
            self._log_event(
                "cancel_all_error",
                error=str(exc),
                reason=reason or None,
            )
            errors.append(str(exc))
            
            # Fallback: cancel individually
            for rec in self.order_manager.all_orders():
                try:
                    await self.router.safe_cancel(cloid=rec.cloid, oid=rec.oid)
                except Exception as inner_exc:
                    errors.append(f"{rec.cloid or rec.oid}: {inner_exc}")
        finally:
            # Clear all order indices
            self.order_manager.clear()
        
        return CancelResult(
            success=len(errors) == 0,
            cancelled_count=open_count,
            errors=errors,
        )
    
    async def cancel_side(
        self,
        side: str,
        reason: str = "flatten",
    ) -> CancelResult:
        """
        Cancel all orders on one side.
        
        Args:
            side: "buy" or "sell"
            reason: Reason for cancellation
            
        Returns:
            CancelResult with outcome
        """
        to_cancel = self.order_manager.orders_by_side(side)
        errors = []
        cancelled = 0
        
        for rec in to_cancel:
            try:
                await self._cancel_record(rec, reason)
                self.order_manager.unindex(rec)
                cancelled += 1
            except Exception as exc:
                self._log_event(
                    "cancel_error",
                    error=str(exc),
                    side=side,
                    reason=reason,
                )
                errors.append(str(exc))
        
        return CancelResult(
            success=len(errors) == 0,
            cancelled_count=cancelled,
            errors=errors,
        )
    
    async def cancel_order(
        self,
        cloid: Optional[str] = None,
        oid: Optional[int] = None,
        reason: str = "",
    ) -> CancelResult:
        """
        Cancel a single order by ID.
        
        Args:
            cloid: Client order ID
            oid: Exchange order ID
            reason: Reason for cancellation
            
        Returns:
            CancelResult with outcome
        """
        rec = self.order_manager.lookup(cloid=cloid, oid=oid)
        if not rec:
            return CancelResult(success=False, errors=["order_not_found"])
        
        try:
            await self._cancel_record(rec, reason)
            self.order_manager.unindex(rec)
            return CancelResult(success=True, cancelled_count=1)
        except Exception as exc:
            return CancelResult(success=False, errors=[str(exc)])
    
    async def _cancel_record(
        self,
        rec: "ActiveOrder",
        reason: str = "",
    ) -> None:
        """Cancel an order record via router."""
        await self.router.safe_cancel(cloid=rec.cloid, oid=rec.oid)
        
        # Update state machine
        cloid_str = str(rec.cloid) if rec.cloid else None
        if cloid_str or rec.oid is not None:
            self.order_state_machine.cancel(
                cloid=cloid_str,
                oid=rec.oid,
            )
        
        # Remove from shadow ledger
        order_id = cloid_str or str(rec.oid) if rec.oid else None
        if order_id:
            try:
                self.shadow_ledger.remove_pending_order(order_id)
            except Exception:
                pass
        
        # Emit event
        await self._emit_order_cancelled(rec, reason)
        
        # Log
        self._log_event(
            "order_cancelled",
            cloid=rec.cloid,
            oid=rec.oid,
            side=rec.level.side,
            reason=reason,
        )
    
    async def cancel_order_with_verification(
        self,
        cloid: Optional[str] = None,
        oid: Optional[int] = None,
        reason: str = "",
    ) -> CancelResult:
        """
        Cancel order with C-6 FIX verification.
        
        If cancel fails, checks if order actually still exists on exchange.
        This prevents stale orders from accumulating when cancels fail but
        orders were already filled/cancelled elsewhere.
        
        Args:
            cloid: Client order ID
            oid: Exchange order ID
            reason: Reason for cancellation
            
        Returns:
            CancelResult with order_gone=True if verified removed
        """
        rec = self.order_manager.lookup(cloid=cloid, oid=oid)
        if not rec:
            return CancelResult(success=False, errors=["order_not_found"])
        
        try:
            # Normal cancel path
            await self._cancel_record(rec, reason)
            self.order_manager.unindex(rec)
            
            # Success metrics
            try:
                self.rich_metrics.orders_cancelled.labels(coin=self.coin, reason=reason).inc()
            except Exception:
                pass
            
            return CancelResult(success=True, cancelled_count=1, order_gone=True)
            
        except Exception as exc:
            # Cancel failed - apply C-6 FIX
            self._log_event("cancel_error", err=str(exc), cloid=cloid, oid=oid, reason=reason)
            
            # Track error
            try:
                self.rich_metrics.api_errors_total.labels(
                    coin=self.coin, error_type="cancel_error"
                ).inc()
            except Exception:
                pass
            
            # C-6 FIX: Check if order actually still exists on exchange
            try:
                still_open = await self._is_order_still_open(cloid, oid)
                if not still_open:
                    # Order is gone - clean up local state
                    self._log_event(
                        "cancel_error_order_gone",
                        cloid=cloid,
                        oid=oid,
                        reason="order_not_found_on_exchange",
                    )
                    self.order_manager.unindex(rec)
                    
                    # Update state machine
                    cloid_str = str(cloid) if cloid else None
                    if cloid_str or oid is not None:
                        self.order_state_machine.cancel(
                            cloid=cloid_str,
                            oid=oid,
                            reason="external_cancel_or_fill",
                        )
                    
                    return CancelResult(success=True, cancelled_count=1, order_gone=True)
            except Exception as check_exc:
                self._log_event("cancel_error_check_failed", err=str(check_exc))
            
            # Order may still be resting
            return CancelResult(success=False, errors=[str(exc)], order_gone=False)
    
    async def _is_order_still_open(
        self,
        cloid: Optional[str],
        oid: Optional[int],
    ) -> bool:
        """
        C-6 FIX: Check if order still exists on exchange.
        
        Returns True if order is still open, False if it's gone (filled/cancelled).
        On error, returns True (safer to assume order exists).
        
        Args:
            cloid: Client order ID
            oid: Exchange order ID
            
        Returns:
            True if order exists on exchange, False otherwise
        """
        if not self.info and not self.async_info:
            return True  # Can't verify without API access
        
        if not self.account:
            return True  # Need account to query
        
        try:
            # Query open orders from exchange
            if self.async_info:
                remote = await self.async_info.frontend_open_orders(
                    self.account,
                    self.config.dex
                )
            else:
                import asyncio
                def _call():
                    return self.info.frontend_open_orders(
                        self.account,
                        dex=self.config.dex
                    )
                remote = await asyncio.wait_for(
                    asyncio.get_running_loop().run_in_executor(None, _call),
                    timeout=self.config.http_timeout
                )
            
            # Handle both list and dict response formats
            if isinstance(remote, dict):
                remote = remote.get("openOrders", [])
            
            # Search for our order
            for o in remote:
                if o.get("coin") != self.coin:
                    continue
                    
                remote_cloid = o.get("cloid")
                remote_oid = o.get("oid")
                
                # Match by cloid if available
                if cloid and remote_cloid and str(remote_cloid) == str(cloid):
                    return True
                    
                # Match by oid if available
                if oid is not None and remote_oid is not None:
                    try:
                        if int(remote_oid) == int(oid):
                            return True
                    except (ValueError, TypeError):
                        pass
            
            return False  # Order not found in open orders
            
        except Exception as exc:
            self._log_event(
                "is_order_still_open_error",
                err=str(exc),
                cloid=cloid,
                oid=oid
            )
            return True  # Assume open on error (safer)
    
    async def cancel_stuck_orders(
        self,
        stuck: List[Any],
        stuck_order_first_seen: Dict[str, float],
        cancel_threshold_sec: float = 120.0,
    ) -> int:
        """
        Cancel orders that have been stuck for too long.
        
        Tracks when each stuck order was first seen and cancels it if it's been
        stuck longer than the threshold.
        
        Args:
            stuck: List of OrderStateRecord for stuck orders
            stuck_order_first_seen: Dict tracking when each order was first seen stuck
            cancel_threshold_sec: Time in seconds before cancelling stuck order
            
        Returns:
            Number of orders cancelled
        """
        import time
        now = time.time()
        cancelled_count = 0
        current_cloids = {r.cloid for r in stuck if r.cloid}
        
        # Clean up tracking for orders no longer stuck
        to_remove = [cloid for cloid in stuck_order_first_seen if cloid not in current_cloids]
        for cloid in to_remove:
            del stuck_order_first_seen[cloid]
        
        # Check each stuck order
        for record in stuck:
            if not record.cloid:
                continue
                
            # Track first time we saw this order as stuck
            if record.cloid not in stuck_order_first_seen:
                stuck_order_first_seen[record.cloid] = now
                continue
            
            # Check if it's been stuck too long
            first_seen = stuck_order_first_seen[record.cloid]
            stuck_duration = now - first_seen
            
            if stuck_duration >= cancel_threshold_sec:
                try:
                    self._log_event(
                        "stuck_order_auto_cancel",
                        cloid=record.cloid,
                        oid=record.oid,
                        stuck_duration_sec=round(stuck_duration, 1),
                        side=record.side,
                        price=record.price,
                        qty=record.original_qty,
                    )
                    
                    # Cancel via order router
                    await self.router.safe_cancel(cloid=record.cloid, oid=record.oid)
                    
                    # Transition to EXPIRED in state machine
                    from src.execution.order_state_machine import OrderState
                    self.order_state_machine.transition(
                        cloid=record.cloid,
                        to_state=OrderState.EXPIRED,
                        reason="stuck_order_auto_cancel",
                    )
                    
                    # Remove from tracking
                    del stuck_order_first_seen[record.cloid]
                    cancelled_count += 1
                    
                except Exception as exc:
                    self._log_event(
                        "stuck_order_cancel_error",
                        cloid=record.cloid,
                        err=str(exc),
                    )
        
        return cancelled_count
    
    # ========== Event Emission ==========
    
    async def _emit_order_submitted(
        self,
        level: "GridLevel",
        oid: Optional[int],
        cloid: Optional[str],
    ) -> None:
        """Emit order submitted event."""
        try:
            from src.core.event_bus import EventType
            await self.event_bus.emit(
                EventType.ORDER_SUBMITTED,
                source="execution_gateway",
                side=level.side,
                price=level.px,
                size=level.sz,
                oid=oid,
                cloid=cloid,
            )
        except Exception:
            pass
    
    async def _emit_order_cancelled(
        self,
        rec: "ActiveOrder",
        reason: str,
    ) -> None:
        """Emit order cancelled event."""
        try:
            from src.core.event_bus import EventType
            await self.event_bus.emit(
                EventType.ORDER_CANCELLED,
                source="execution_gateway",
                side=rec.level.side,
                price=rec.level.px,
                size=rec.level.sz,
                oid=rec.oid,
                cloid=rec.cloid,
                reason=reason,
            )
        except Exception:
            pass
    
    # ========== Order Queries ==========
    
    def open_count(self) -> int:
        """Number of open orders."""
        return self.order_manager.open_count()
    
    def has_order_at_price(self, price_key: str) -> bool:
        """Check if order exists at price."""
        return self.order_manager.has_price_key(price_key)
    
    def get_order(
        self,
        cloid: Optional[str] = None,
        oid: Optional[int] = None,
        price_key: Optional[str] = None,
    ) -> Optional["ActiveOrder"]:
        """Look up an order by any ID."""
        return self.order_manager.lookup(cloid=cloid, oid=oid, price_key=price_key)
    
    def all_orders(self) -> List["ActiveOrder"]:
        """Get all active orders."""
        return self.order_manager.all_orders()
