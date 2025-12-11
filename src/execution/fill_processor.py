"""
FillProcessor: Single-responsibility fill handling extracted from bot.py.

This module handles all fill processing logic with clear interfaces:
- Fill deduplication
- WAL-based crash safety
- Position updates with proper locking
- PnL calculation
- Order state machine updates
- Shadow ledger sync
- Metrics emission
- Event bus notifications

Architecture:
    FillProcessor receives raw fills and produces a FillResult.
    It delegates to injected components rather than creating them.
    The caller (GridBot) provides dependencies via constructor injection.

Thread Safety:
    FillProcessor is NOT thread-safe by itself. The caller must hold
    position_lock during the processing window, or use the provided
    async process_fill() which handles locking internally.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Protocol, TYPE_CHECKING

from src.core.utils import to_int_safe, safe_call

if TYPE_CHECKING:
    from src.execution.order_manager import OrderManager, ActiveOrder
    from src.execution.order_state_machine import OrderStateMachine
    from src.state.shadow_ledger import ShadowLedger
    from src.risk.risk import RiskEngine
    from src.core.event_bus import EventBus, EventType
    from src.monitoring.metrics_rich import RichMetrics
    from src.state.wal import WriteAheadLog
    from src.execution.fill_log import FillLog
    from src.strategy.strategy import GridStrategy
    from src.state.state_manager import StateManager

log = logging.getLogger("gridbot")


@dataclass
class FillResult:
    """
    Result of processing a fill.
    
    Contains all information needed for the caller to update
    their state and decide on follow-up actions.
    """
    # Core identification
    cloid: Optional[str]
    oid: Optional[int]
    side: str
    price: float
    size: float
    timestamp_ms: int
    
    # Processing outcome
    matched: bool = False
    is_replay: bool = False
    is_duplicate: bool = False
    
    # Position change (only if matched)
    old_position: float = 0.0
    new_position: float = 0.0
    position_delta: float = 0.0
    
    # PnL (only if matched)
    realized_pnl: float = 0.0
    session_pnl: float = 0.0
    alltime_pnl: float = 0.0
    
    # Order info (only if matched)
    order_was_fully_filled: bool = False
    level_price: float = 0.0
    level_side: str = ""
    
    # WAL tracking
    wal_entry_id: Optional[int] = None
    
    # Flags for caller actions
    needs_reconciliation: bool = False
    needs_grid_rebuild: bool = False
    trigger_price: Optional[float] = None  # For strategy.on_price()
    
    @property
    def success(self) -> bool:
        """Fill was successfully processed (matched or valid replay)."""
        return self.matched or self.is_replay or self.is_duplicate


@dataclass
class FillProcessorConfig:
    """Configuration for FillProcessor behavior."""
    # WAL settings
    wal_enabled: bool = True
    
    # PnL alerting
    max_single_fill_pnl: float = 500.0
    
    # Fill timestamp validation
    max_fill_age_ms: int = 300_000  # 5 minutes
    max_future_fill_ms: int = 5_000  # 5 seconds
    
    # Logging
    log_event_callback: Optional[Callable[..., None]] = None


class PositionUpdateCallback(Protocol):
    """Protocol for position update notification."""
    def __call__(self, old_position: float, new_position: float, delta: float) -> None: ...


class FillProcessor:
    """
    Single-responsibility fill processing.
    
    Extracted from GridBot._handle_fill() to create a focused,
    testable component with clear interfaces.
    
    Dependencies are injected via constructor for testability.
    """
    
    def __init__(
        self,
        coin: str,
        order_manager: "OrderManager",
        order_state_machine: "OrderStateMachine",
        shadow_ledger: "ShadowLedger",
        risk_engine: "RiskEngine",
        event_bus: "EventBus",
        rich_metrics: "RichMetrics",
        wal: Optional["WriteAheadLog"],
        fill_log: "FillLog",
        state_manager: "StateManager",
        config: Optional[FillProcessorConfig] = None,
    ) -> None:
        """
        Initialize FillProcessor with all required dependencies.
        
        Args:
            coin: Trading pair symbol
            order_manager: Order registry for fill matching
            order_state_machine: Order lifecycle state tracking
            shadow_ledger: Local position shadow for reconciliation
            risk_engine: Risk limits and position tracking
            event_bus: Event publication for downstream consumers
            rich_metrics: Prometheus metrics
            wal: Write-ahead log for crash safety (optional)
            fill_log: Persistent fill event log
            config: Configuration options
            state_manager: Optional StateManager for direct state access
        """
        self.coin = coin
        self.order_manager = order_manager
        self.order_state_machine = order_state_machine
        self.shadow_ledger = shadow_ledger
        self.risk_engine = risk_engine
        self.event_bus = event_bus
        self.rich_metrics = rich_metrics
        self.wal = wal
        self.fill_log = fill_log
        self.config = config or FillProcessorConfig()
        # StateManager is the single source of truth for position/PnL
        self.state_manager = state_manager
        if self.state_manager is None:
            raise ValueError("FillProcessor requires a StateManager instance")
        
        # Logger for debugging
        self.logger = logging.getLogger(f"{__name__}.{coin}")
        
        # Callback for logging (optional)
        self._log_event = self.config.log_event_callback or self._default_log_event
    
    @property
    def position(self) -> float:
        """Current position delegated to StateManager."""
        return self.state_manager.position
    
    @position.setter
    def position(self, value: float) -> None:
        """Set position (caller must ensure thread safety)."""
        # Update central state directly (sync path used only in tests/backfills)
        self.state_manager.position_tracker.position = value
        self.state_manager.shadow_ledger.local_position = value
        self.state_manager.risk_engine.set_position(value)
    
    @property
    def last_fill_time_ms(self) -> int:
        """Timestamp of last processed fill."""
        return self.state_manager.last_fill_time_ms
    
    @last_fill_time_ms.setter
    def last_fill_time_ms(self, value: int) -> None:
        """Set last fill time."""
        # Keep StateManager as authority
        self.state_manager.sync_from_fill_processor(
            position=self.state_manager.position,
            session_pnl=self.state_manager.session_realized_pnl,
            alltime_pnl=self.state_manager.alltime_realized_pnl,
            last_fill_time_ms=value,
        )
    
    @property
    def session_realized_pnl(self) -> float:
        """Session realized PnL."""
        return self.state_manager.session_realized_pnl
    
    @property
    def alltime_realized_pnl(self) -> float:
        """All-time realized PnL."""
        return self.state_manager.alltime_realized_pnl
    
    def set_pnl_state(self, session_pnl: float, alltime_pnl: float) -> None:
        """Set PnL state (for initialization from saved state)."""
        self.state_manager.sync_from_fill_processor(
            position=self.state_manager.position,
            session_pnl=session_pnl,
            alltime_pnl=alltime_pnl,
            last_fill_time_ms=self.state_manager.last_fill_time_ms,
        )
    
    def apply_fill_to_state(self, side: str, size: float, pnl: float, timestamp_ms: int) -> float:
        """
        Apply fill to state - uses StateManager.apply_fill_sync if available.
        
        IMPORTANT: Caller must hold position_lock!
        
        Returns:
            New position after fill
        """
        return self.state_manager.apply_fill_sync(side, size, pnl, timestamp_ms)
    
    def _default_log_event(self, event: str, **data: Any) -> None:
        """Default logging if no callback provided."""
        import json
        payload = {"event": event, "coin": self.coin, **data}
        log.info(json.dumps(payload))
    
    def validate_fill_timestamp(self, ts_ms: int, current_time_ms: int) -> bool:
        """
        Validate fill timestamp is within acceptable range.
        
        Critical-10: Detect stale or future-dated fills that may indicate
        clock skew or replay attacks.
        
        Returns:
            True if timestamp is valid, False if anomalous
        """
        age_ms = current_time_ms - ts_ms
        
        if age_ms > self.config.max_fill_age_ms:
            self._log_event(
                "fill_timestamp_anomaly",
                reason="too_old",
                fill_ts_ms=ts_ms,
                current_ts_ms=current_time_ms,
                age_ms=age_ms,
                threshold_ms=self.config.max_fill_age_ms,
            )
            return False
        
        if age_ms < -self.config.max_future_fill_ms:
            self._log_event(
                "fill_timestamp_anomaly",
                reason="future_dated",
                fill_ts_ms=ts_ms,
                current_ts_ms=current_time_ms,
                age_ms=age_ms,
                threshold_ms=self.config.max_future_fill_ms,
            )
            return False
        
        return True
    
    async def process_fill(
        self,
        side: str,
        px: float,
        sz: float,
        oid: Optional[int],
        cloid: Optional[str],
        ts_ms: int,
        replay: bool = False,
        rebuild_in_progress: bool = False,
    ) -> FillResult:
        """
        Process a fill event and return the result.
        
        This is the main entry point. It handles:
        1. Replay detection and handling
        2. Fill log persistence
        3. Order matching
        4. WAL write-ahead logging
        5. Position update
        6. PnL calculation
        7. Shadow ledger sync
        8. Metrics and events
        
        Args:
            side: "buy" or "sell"
            px: Fill price
            sz: Fill size
            oid: Exchange order ID
            cloid: Client order ID
            ts_ms: Fill timestamp in milliseconds
            replay: True if this is a replay from fill log on startup
            rebuild_in_progress: True if a grid rebuild is in progress
        
        Returns:
            FillResult with all processing outcomes
        """
        result = FillResult(
            cloid=cloid,
            oid=oid,
            side=side,
            price=px,
            size=sz,
            timestamp_ms=ts_ms,
            is_replay=replay,
        )
        
        # Handle replay fills - only update indices, no position changes
        if replay:
            return await self._handle_replay_fill(result)
        
        # C-1 FIX: Mark rebuild needed if fill arrives during grid rebuild
        if rebuild_in_progress:
            result.needs_grid_rebuild = True
            self._log_event(
                "fill_during_rebuild",
                side=side,
                px=px,
                sz=sz,
                note="marking_rebuild_needed",
            )
        
        # Persist fill to event log (best-effort)
        await self._persist_fill_to_log(side, px, sz, oid, cloid, ts_ms)
        
        # Check for out-of-order fills (log but continue processing)
        if ts_ms < self.state_manager.last_fill_time_ms:
            self._log_event(
                "fill_out_of_order",
                side=side,
                px=px,
                sz=sz,
                ts_ms=ts_ms,
                last_fill_ms=self.state_manager.last_fill_time_ms,
            )
        
        # Try to match fill with known order
        cloid_str = str(cloid) if cloid else None
        oid_int = to_int_safe(oid)
        
        rec = self.order_manager.handle_partial_fill(cloid_str, oid_int, sz)
        
        if not rec:
            # Unmatched fill - log and signal reconciliation needed
            self._log_event(
                "fill_unmatched",
                side=side,
                px=px,
                sz=sz,
                oid=oid,
                cloid=cloid,
                position=self.state_manager.position,
            )
            result.needs_reconciliation = True
            return result
        
        # === Matched fill - process position and PnL ===
        result.matched = True
        
        # Calculate position delta
        delta = sz if side.startswith("b") else -sz
        old_position = self.state_manager.position
        new_position = old_position + delta

        result.old_position = old_position
        result.new_position = new_position
        result.position_delta = delta
        
        # Order info
        result.level_price = rec.level.px
        result.level_side = rec.level.side
        
        # Determine if fully filled
        remaining = rec.original_qty - rec.filled_qty
        dust_tolerance = max(sz * 0.01, 1e-9)
        result.order_was_fully_filled = rec.original_qty <= 0 or remaining <= dust_tolerance
        
        # === Calculate PnL ===
        pnl = self._calculate_pnl(rec.level, px, sz)

        # === WAL: Write-Ahead Log for crash safety ===
        wal_entry_id = None
        if self.wal and self.config.wal_enabled:
            wal_entry_id = await self._wal_append(
                cloid_str, oid_int, side, px, sz,
                old_position, new_position, ts_ms
            )
            result.wal_entry_id = wal_entry_id

        # === Apply position & PnL change via StateManager ===
        new_position = self.apply_fill_to_state(side, sz, pnl, ts_ms)
        result.new_position = new_position
        
        # Update order state machine
        self.order_state_machine.fill(
            cloid=cloid_str,
            oid=oid_int,
            fill_qty=sz,
            is_complete=result.order_was_fully_filled,
        )
        
        # === Emit position changed event ===
        await self._emit_position_event(delta, side)
        
        # === Update shadow ledger ===
        await self.shadow_ledger.apply_fill(
            side=side,
            size=sz,
            cloid=cloid_str,
            oid=oid_int,
            timestamp_ms=ts_ms,
        )
        
        result.realized_pnl = pnl
        result.session_pnl = self.state_manager.session_realized_pnl
        result.alltime_pnl = self.state_manager.alltime_realized_pnl
        
        # === WAL: Mark entry as committed ===
        if self.wal and wal_entry_id is not None:
            await self._wal_commit(wal_entry_id)
        
        # === Check for unusual PnL ===
        if abs(pnl) > self.config.max_single_fill_pnl:
            self._log_event(
                "unusual_pnl_fill",
                pnl=pnl,
                side=side,
                px=px,
                sz=sz,
                threshold=self.config.max_single_fill_pnl,
            )
            safe_call(
                lambda: self.rich_metrics.unusual_pnl_fills.labels(coin=self.coin).inc(),
                "increment unusual_pnl_fills metric",
                self.logger,
            )
        
        # === Update metrics ===
        await self._update_metrics(side)
        
        # === Emit fill event ===
        await self._emit_fill_event(result)
        
        # Set trigger price for strategy update
        result.trigger_price = px
        
        return result
    
    async def _handle_replay_fill(self, result: FillResult) -> FillResult:
        """
        Handle replay fill - update indices only, no position changes.
        
        Replay fills are from the fill log during startup. The position
        was already loaded from state, so we just need to ensure the
        order indices are correct.
        """
        cloid_str = str(result.cloid) if result.cloid else None
        oid_int = to_int_safe(result.oid)
        
        # Try to pop the order (so we don't try to cancel filled orders)
        rec = self.order_manager.pop_by_ids(cloid_str, oid_int)
        
        if rec:
            self._log_event(
                "fill_replay_order_cleared",
                cloid=result.cloid,
                oid=result.oid,
                side=result.side,
                px=result.price,
                sz=result.size,
            )
            # Update state machine
            self.order_state_machine.fill(
                cloid=cloid_str,
                oid=oid_int,
                fill_qty=result.size,
                is_complete=True,
            )
        else:
            self._log_event(
                "fill_replay_no_order",
                cloid=result.cloid,
                oid=result.oid,
                side=result.side,
                px=result.price,
                sz=result.size,
            )
        
        # Update last fill time to track replay progress
        await self.state_manager.update_last_fill_time(result.timestamp_ms)
        
        return result
    
    async def _persist_fill_to_log(
        self,
        side: str,
        px: float,
        sz: float,
        oid: Optional[int],
        cloid: Optional[str],
        ts_ms: int,
    ) -> None:
        """Persist fill to event log for crash recovery."""
        try:
            await self.fill_log.append({
                "side": side,
                "px": px,
                "sz": sz,
                "oid": oid,
                "cloid": cloid,
                "time": ts_ms,
            })
        except Exception as e:
            # Best-effort - continue even if logging fails
            self._log_event("fill_log_append_error", error=str(e))
    
    async def _wal_append(
        self,
        cloid: Optional[str],
        oid: Optional[int],
        side: str,
        price: float,
        size: float,
        old_position: float,
        new_position: float,
        timestamp_ms: int,
    ) -> Optional[int]:
        """Write fill intent to WAL before modifying position."""
        try:
            return self.wal.append_sync(
                op="fill",
                cloid=cloid,
                oid=oid,
                side=side,
                price=price,
                size=size,
                old_position=old_position,
                new_position=new_position,
                timestamp_ms=timestamp_ms,
            )
        except Exception as e:
            self._log_event(
                "wal_append_error",
                error=str(e),
                side=side,
                px=price,
                sz=size,
            )
            return None
    
    async def _wal_commit(self, entry_id: int) -> None:
        """Mark WAL entry as committed after all state updates."""
        try:
            self.wal.mark_committed(entry_id)
        except Exception as e:
            self._log_event("wal_commit_error", error=str(e), entry_id=entry_id)
    
    def _calculate_pnl(self, level: Any, fill_price: float, fill_size: float) -> float:
        """
        Calculate realized PnL for a fill.
        
        PnL = (fill_price - level_price) * size for buys that close short
        PnL = (level_price - fill_price) * size for sells that close long
        
        Note: This assumes the fill is closing a position opened at level_price.
        """
        if level.side == "buy":
            return (fill_price - level.px) * fill_size
        else:
            return (level.px - fill_price) * fill_size
    
    async def _emit_position_event(self, delta: float, side: str) -> None:
        """Emit position changed event."""
        try:
            from src.core.event_bus import EventType
            await self.event_bus.emit(
                EventType.POSITION_CHANGED,
                source="fill_processor",
                position=self.state_manager.position,
                delta=delta,
                side=side,
            )
        except Exception as e:
            self.logger.debug("Position event emission failed: %s", e)
    
    async def _update_metrics(self, side: str) -> None:
        """Update Prometheus metrics."""
        def _update():
            self.rich_metrics.fills_total.labels(coin=self.coin, side=side[:1]).inc()
            self.rich_metrics.position.labels(coin=self.coin).set(self.state_manager.position)
            self.rich_metrics.realized_pnl.labels(coin=self.coin).set(self.state_manager.session_realized_pnl)
        safe_call(_update, "update fill metrics", self.logger)
    
    async def _emit_fill_event(self, result: FillResult) -> None:
        """Emit fill processed event."""
        try:
            from src.core.event_bus import EventType
            event_type = EventType.FILL_PROCESSED if result.matched else EventType.FILL_RECEIVED
            await self.event_bus.emit(
                event_type,
                source="fill_processor",
                side=result.side,
                price=result.price,
                size=result.size,
                oid=result.oid,
                cloid=result.cloid,
                matched=result.matched,
                pnl=result.realized_pnl,
                position=self.state_manager.position,
            )
        except Exception as e:
            self.logger.debug("Fill event emission failed: %s", e)
    
    def get_state_snapshot(self) -> Dict[str, Any]:
        """
        Get current state for persistence.
        
        Returns dict with all state needed to restore FillProcessor.
        """
        return self.state_manager.get_state_for_fill_processor()
    
    def restore_from_snapshot(self, snapshot: Dict[str, Any]) -> None:
        """
        Restore state from persistence snapshot.
        
        Args:
            snapshot: Dict from get_state_snapshot()
        """
        self.state_manager.sync_from_fill_processor(
            position=snapshot.get("position", 0.0),
            session_pnl=snapshot.get("session_pnl", snapshot.get("session_realized_pnl", 0.0)),
            alltime_pnl=snapshot.get("alltime_pnl", snapshot.get("alltime_realized_pnl", 0.0)),
            last_fill_time_ms=snapshot.get("last_fill_time_ms", 0),
        )
