"""
BotOrchestrator: Thin coordination layer for grid bot components.

This module provides a coordinator that ties together the extracted
execution layer components (FillProcessor, StateManager, ExecutionGateway,
GridBuilder) with the bot's main loop.

Architecture:
    The orchestrator acts as a facade, delegating to specialized components:
    - FillProcessor: Fill handling, PnL calculation
    - StateManager: Centralized state (position, PnL, fills)
    - ExecutionGateway: Order submission and lifecycle
    - GridBuilder: Grid construction and placement

    The orchestrator owns the main control flow but not the business logic.

Usage:
    # In bot.py, create orchestrator in __init__
    self.orchestrator = BotOrchestrator(
        coin=coin,
        fill_processor=self.fill_processor,
        state_manager=self.state_manager,
        execution_gateway=self.execution_gateway,
        grid_builder=self.grid_builder,
    )
    
    # In run() loop, delegate to orchestrator
    result = await self.orchestrator.run_cycle(callbacks)
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Awaitable, Callable, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from src.execution.fill_processor import FillProcessor
    from src.state.state_manager import StateManager
    from src.execution.execution_gateway import ExecutionGateway
    from src.execution.grid_builder import GridBuilder

import logging

log = logging.getLogger("gridbot")


class CycleAction(Enum):
    """Actions that can result from a cycle."""
    CONTINUE = auto()
    SLEEP_SHORT = auto()
    SLEEP_NORMAL = auto()
    STOP = auto()
    SKIP_CYCLE = auto()


@dataclass
class CycleResult:
    """Result of a single orchestrator cycle."""
    success: bool
    action: CycleAction = CycleAction.CONTINUE
    mid: Optional[float] = None
    fills_processed: int = 0
    grid_rebuilt: bool = False
    error: Optional[str] = None
    duration_ms: float = 0.0
    reason: Optional[str] = None


@dataclass
class CycleCallbacks:
    """
    Callbacks for orchestrator to delegate operations to bot.py.
    
    This allows the orchestrator to coordinate without owning the business logic.
    All callbacks are async functions that the bot provides.
    
    Phase 5 Simplification: Grouped into logical categories with direct
    component references where possible.
    """
    # === Market Data (from MarketData component) ===
    get_mid_price: Optional[Callable[[], Awaitable[float]]] = None
    get_data_age: Optional[Callable[[], float]] = None
    is_market_halted: Optional[Callable[[], bool]] = None
    
    # === State (from StateManager or bot) ===
    get_position: Optional[Callable[[], float]] = None
    get_position_lock: Optional[Callable[[], asyncio.Lock]] = None
    
    # === Safety Systems ===
    refresh_dead_man_switch: Optional[Callable[[], Awaitable[None]]] = None
    is_risk_halted: Optional[Callable[[], bool]] = None
    is_circuit_tripped: Optional[Callable[[], bool]] = None
    
    # === Grid Operations ===
    should_rebuild_grid: Optional[Callable[[], bool]] = None
    build_and_place_grid: Optional[Callable[[float], Awaitable[None]]] = None
    clear_rebuild_flag: Optional[Callable[[], None]] = None
    check_trailing_trigger: Optional[Callable[[float], bool]] = None
    
    # === Periodic Operations (intervals managed by orchestrator) ===
    poll_rest_fills: Optional[Callable[[], Awaitable[None]]] = None
    reconcile_position: Optional[Callable[[], Awaitable[None]]] = None
    reconcile_orders: Optional[Callable[[], Awaitable[None]]] = None
    check_and_cancel_stuck: Optional[Callable[[], Awaitable[int]]] = None
    
    # === Skew/Flatten ===
    handle_skew: Optional[Callable[[], Awaitable[bool]]] = None
    
    # === Logging/Status ===
    log_pnl: Optional[Callable[[], Awaitable[None]]] = None
    push_status: Optional[Callable[[], Awaitable[None]]] = None
    
    # === Order Management ===
    cancel_all: Optional[Callable[[str], Awaitable[None]]] = None
    
    # === State Persistence ===
    persist_state: Optional[Callable[[], Awaitable[None]]] = None


@dataclass
class ComponentRefs:
    """
    Direct component references for the orchestrator.
    
    Phase 5: Instead of callback indirection for simple operations,
    provide direct references to components. This reduces boilerplate
    while keeping the orchestrator thin.
    """
    # Market data component
    market: Optional[Any] = None
    
    # Risk engine
    risk: Optional[Any] = None
    
    # Circuit breaker
    circuit_breaker: Optional[Any] = None
    
    # Dead man switch
    dead_man_switch: Optional[Any] = None
    
    # Reconciliation service
    reconciliation_service: Optional[Any] = None
    
    # Rest poller
    rest_poller: Optional[Any] = None


@dataclass
class OrchestratorConfig:
    """Configuration for BotOrchestrator."""
    # Rebuild interval
    rebuild_interval_sec: float = 30.0
    
    # State save interval
    state_save_interval_sec: float = 10.0
    
    # PnL log interval
    pnl_log_interval_sec: float = 60.0
    
    # REST fill poll interval
    rest_fill_poll_interval_sec: float = 30.0
    
    # Position reconcile interval
    position_reconcile_interval_sec: float = 60.0
    
    # Order reconcile interval  
    order_reconcile_interval_sec: float = 60.0
    
    # Stuck order check interval
    stuck_order_check_interval_sec: float = 30.0
    
    # Loop interval (sleep time)
    loop_interval_sec: float = 1.0
    
    # Short sleep (when paused)
    short_sleep_sec: float = 2.0
    
    # Enable state manager integration
    use_state_manager: bool = False
    
    # Enable execution gateway integration
    use_execution_gateway: bool = False
    
    # Enable grid builder integration
    use_grid_builder: bool = False
    
    # Orchestrator main loop is now always used (legacy loop removed)
    # This flag is deprecated and will be removed in a future version
    use_orchestrator_loop: bool = True
    
    # Data halt threshold
    data_halt_sec: float = 30.0
    
    # Logging callback
    log_event_callback: Optional[Callable[..., None]] = None


class BotOrchestrator:
    """
    Thin coordination layer for grid bot components.
    
    This class coordinates the interactions between specialized components
    without owning any business logic itself. It's designed to replace
    the monolithic bot.py main loop over time.
    
    The orchestrator supports gradual migration:
    - Initially, it just coordinates FillProcessor
    - Additional components can be integrated one at a time
    - Eventually, the main loop logic moves here entirely
    
    Phase 5: Added ComponentRefs for direct component access to reduce
    callback indirection where appropriate.
    """
    
    def __init__(
        self,
        coin: str,
        fill_processor: "FillProcessor",
        state_manager: Optional["StateManager"] = None,
        execution_gateway: Optional["ExecutionGateway"] = None,
        grid_builder: Optional["GridBuilder"] = None,
        config: Optional[OrchestratorConfig] = None,
        components: Optional[ComponentRefs] = None,
    ) -> None:
        """
        Initialize orchestrator with components.
        
        Args:
            coin: Trading symbol
            fill_processor: Fill handling component
            state_manager: State management component (optional)
            execution_gateway: Order execution component (optional)
            grid_builder: Grid building component (optional)
            config: Optional configuration
            components: Optional direct component references
        """
        self.coin = coin
        self.fill_processor = fill_processor
        self.state_manager = state_manager
        self.execution_gateway = execution_gateway
        self.grid_builder = grid_builder
        self.config = config or OrchestratorConfig()
        self.components = components or ComponentRefs()
        
        # Timing trackers
        self._last_rebuild = 0.0
        self._last_state_save = 0.0
        self._last_pnl_log = 0.0
        self._last_rest_poll = 0.0
        self._last_position_reconcile = 0.0
        self._last_order_reconcile = 0.0
        self._last_stuck_check = 0.0
        
        # State
        self._running = True
        self._cycle_count = 0
        self._data_paused = False
        self._risk_paused = False
        self._circuit_orders_cancelled = False
        
        # Logging
        self._log_event = self.config.log_event_callback or self._default_log
    
    def _default_log(self, event: str, **kwargs: Any) -> None:
        """Default logging."""
        import json
        payload = {"event": event, "coin": self.coin, **kwargs}
        log.info(json.dumps(payload))
    
    # === Component Access Helpers (Phase 5) ===
    # These methods use ComponentRefs with callback fallback
    
    def _get_data_age(self, callbacks: CycleCallbacks) -> float:
        """Get data age from component or callback."""
        if self.components.market:
            return self.components.market.data_age()
        if callbacks.get_data_age:
            return callbacks.get_data_age()
        return 0.0
    
    def _is_market_halted(self, callbacks: CycleCallbacks) -> bool:
        """Check if market is halted from component or callback."""
        if self.components.market:
            return self.components.market.is_halted()
        if callbacks.is_market_halted:
            return callbacks.is_market_halted()
        return False
    
    def _is_risk_halted(self, callbacks: CycleCallbacks) -> bool:
        """Check if risk is halted from component or callback."""
        if self.components.risk:
            return self.components.risk.is_halted()
        if callbacks.is_risk_halted:
            return callbacks.is_risk_halted()
        return False
    
    def _is_circuit_tripped(self, callbacks: CycleCallbacks) -> bool:
        """Check if circuit breaker is tripped from component or callback."""
        if self.components.circuit_breaker:
            return self.components.circuit_breaker.is_tripped
        if callbacks.is_circuit_tripped:
            return callbacks.is_circuit_tripped()
        return False
    
    async def _refresh_dms(self, callbacks: CycleCallbacks) -> None:
        """Refresh dead-man-switch from component or callback."""
        if self.components.dead_man_switch and self.components.dead_man_switch.is_active:
            await self.components.dead_man_switch.refresh()
        elif callbacks.refresh_dead_man_switch:
            await callbacks.refresh_dead_man_switch()
    
    @property
    def is_running(self) -> bool:
        """Check if orchestrator is running."""
        return self._running
    
    def stop(self) -> None:
        """Signal orchestrator to stop."""
        self._running = False
        self._log_event("orchestrator_stop")
    
    async def run_cycle(self, callbacks: CycleCallbacks) -> CycleResult:
        """
        Execute one main loop cycle using callbacks for all operations.
        
        This is the primary method for orchestrator-driven main loop.
        The orchestrator coordinates timing and flow, but delegates all
        business logic to the bot via callbacks.
        
        Phase 5: Uses component references where available for reduced indirection.
        
        Args:
            callbacks: CycleCallbacks with bot methods
            
        Returns:
            CycleResult indicating what happened and what action to take
        """
        start_time = time.perf_counter()
        self._cycle_count += 1
        now = time.time()
        
        grid_rebuilt = False
        mid = None
        error = None
        
        try:
            # Get current position (from StateManager if available)
            if self.state_manager:
                position = self.state_manager.position
            elif callbacks.get_position:
                position = callbacks.get_position()
            else:
                position = 0.0
            
            # Log cycle start
            self._log_event(
                "cycle_start",
                position=position,
                cycle=self._cycle_count,
            )
            
            # 1. Refresh dead-man-switch (using helper)
            try:
                await self._refresh_dms(callbacks)
            except Exception as exc:
                self._log_event("dms_refresh_error", error=str(exc))
            
            # 2. Check data health (using helpers)
            data_age = self._get_data_age(callbacks)
            is_halted = self._is_market_halted(callbacks)
            
            if is_halted or data_age > self.config.data_halt_sec:
                if not self._data_paused:
                    self._data_paused = True
                    self._log_event("data_stale_halt", age_sec=data_age)
                    if callbacks.cancel_all:
                        try:
                            await callbacks.cancel_all("data_stale")
                        except Exception as exc:
                            self._log_event("data_stale_cancel_error", error=str(exc))
                
                return CycleResult(
                    success=True,
                    action=CycleAction.SLEEP_SHORT,
                    reason="data_paused",
                    duration_ms=(time.perf_counter() - start_time) * 1000,
                )
            
            # Data resumed
            if self._data_paused:
                self._data_paused = False
                self._log_event("data_resume", age_sec=data_age)
                if callbacks.get_mid_price and callbacks.build_and_place_grid:
                    mid = await callbacks.get_mid_price()
                    await callbacks.build_and_place_grid(mid)
                return CycleResult(
                    success=True,
                    action=CycleAction.SLEEP_NORMAL,
                    reason="data_resumed",
                    mid=mid,
                    duration_ms=(time.perf_counter() - start_time) * 1000,
                )
            
            # 3. Check risk halt (using helper)
            if self._is_risk_halted(callbacks):
                if not self._risk_paused:
                    self._risk_paused = True
                    self._log_event("risk_halt")
                    if callbacks.cancel_all:
                        try:
                            await callbacks.cancel_all("risk_halt")
                        except Exception as exc:
                            self._log_event("risk_halt_cancel_error", error=str(exc))
                    if callbacks.push_status:
                        await callbacks.push_status()
                
                return CycleResult(
                    success=True,
                    action=CycleAction.STOP,
                    reason="risk_halt",
                    duration_ms=(time.perf_counter() - start_time) * 1000,
                )
            
            # 4. Check circuit breaker (using helper)
            if self._is_circuit_tripped(callbacks):
                if not self._circuit_orders_cancelled:
                    self._log_event("circuit_breaker_trip")
                    if callbacks.cancel_all:
                        try:
                            await callbacks.cancel_all("circuit_breaker")
                            self._circuit_orders_cancelled = True
                        except Exception as exc:
                            self._log_event("circuit_breaker_cancel_error", error=str(exc))
                
                return CycleResult(
                    success=True,
                    action=CycleAction.SLEEP_NORMAL,
                    reason="circuit_tripped",
                    duration_ms=(time.perf_counter() - start_time) * 1000,
                )
            else:
                # Circuit reset - rebuild grid
                if self._circuit_orders_cancelled:
                    self._circuit_orders_cancelled = False
                    if callbacks.clear_rebuild_flag:
                        # Actually need to SET rebuild flag, not clear
                        pass  # Bot handles this via rebuild_needed = True
            
            # 5. Handle skew/flatten mode
            if callbacks.handle_skew:
                try:
                    await callbacks.handle_skew()
                except Exception as exc:
                    self._log_event("skew_handle_error", error=str(exc))
            
            # 6. REST fill polling (with adaptive interval)
            if callbacks.poll_rest_fills:
                if (now - self._last_rest_poll) > self.config.rest_fill_poll_interval_sec:
                    try:
                        await callbacks.poll_rest_fills()
                        self._last_rest_poll = now
                    except Exception as exc:
                        self._log_event("rest_poll_error", error=str(exc))
            
            # 7. Position reconciliation
            if callbacks.reconcile_position:
                if (now - self._last_position_reconcile) > self.config.position_reconcile_interval_sec:
                    try:
                        await callbacks.reconcile_position()
                        self._last_position_reconcile = now
                    except Exception as exc:
                        self._log_event("position_reconcile_error", error=str(exc))
            
            # 8. Order reconciliation
            if callbacks.reconcile_orders:
                if (now - self._last_order_reconcile) > self.config.order_reconcile_interval_sec:
                    try:
                        await callbacks.reconcile_orders()
                        self._last_order_reconcile = now
                    except Exception as exc:
                        self._log_event("order_reconcile_error", error=str(exc))
            
            # 9. Stuck order check
            if callbacks.check_and_cancel_stuck:
                if (now - self._last_stuck_check) > self.config.stuck_order_check_interval_sec:
                    try:
                        cancelled = await callbacks.check_and_cancel_stuck()
                        if cancelled > 0:
                            self._log_event("stuck_orders_cancelled", count=cancelled)
                        self._last_stuck_check = now
                    except Exception as exc:
                        self._log_event("stuck_check_error", error=str(exc))
            
            # 10. PnL logging
            if callbacks.log_pnl:
                if (now - self._last_pnl_log) > self.config.pnl_log_interval_sec:
                    try:
                        await callbacks.log_pnl()
                        self._last_pnl_log = now
                    except Exception as exc:
                        self._log_event("pnl_log_error", error=str(exc))
            
            # 11. Get mid price
            if callbacks.get_mid_price:
                try:
                    mid = await callbacks.get_mid_price()
                except Exception as exc:
                    self._log_event("mid_price_error", error=str(exc))
                    return CycleResult(
                        success=True,
                        action=CycleAction.SLEEP_SHORT,
                        reason="mid_price_error",
                        error=str(exc),
                        duration_ms=(time.perf_counter() - start_time) * 1000,
                    )
            
            # 12. Grid rebuild check
            should_rebuild = callbacks.should_rebuild_grid() if callbacks.should_rebuild_grid else False
            trailing_trigger = callbacks.check_trailing_trigger(mid) if callbacks.check_trailing_trigger and mid else False
            
            if should_rebuild or trailing_trigger:
                if callbacks.build_and_place_grid and mid:
                    try:
                        await callbacks.build_and_place_grid(mid)
                        grid_rebuilt = True
                        self._last_rebuild = now
                        if callbacks.clear_rebuild_flag:
                            callbacks.clear_rebuild_flag()
                    except Exception as exc:
                        self._log_event("grid_rebuild_error", error=str(exc))
            
        except Exception as exc:
            error = str(exc)
            self._log_event("cycle_error", error=error)
        
        duration_ms = (time.perf_counter() - start_time) * 1000
        
        return CycleResult(
            success=error is None,
            action=CycleAction.SLEEP_NORMAL,
            mid=mid,
            grid_rebuilt=grid_rebuilt,
            error=error,
            duration_ms=duration_ms,
        )

    async def process_fill(
        self,
        fill: Dict[str, Any],
        position: float,
        pre_fee_cost: Optional[float] = None,
        **kwargs: Any,
    ) -> Any:
        """
        Process a fill through FillProcessor.
        
        Args:
            fill: Fill data dictionary
            position: Current position
            pre_fee_cost: Optional pre-calculated fee cost
            **kwargs: Additional arguments for FillProcessor
            
        Returns:
            FillResult from FillProcessor
        """
        return await self.fill_processor.process_fill(
            fill=fill,
            position=position,
            pre_fee_cost=pre_fee_cost,
            **kwargs,
        )
    
    async def cycle(
        self,
        mid: float,
        position: float,
        position_lock: Optional[asyncio.Lock] = None,
        get_current_position: Optional[Callable[[], float]] = None,
        fill_queue: Optional[asyncio.Queue] = None,
    ) -> CycleResult:
        """
        Execute one orchestrator cycle.
        
        This method coordinates periodic tasks:
        - Grid rebuild (if needed)
        - State persistence
        - PnL logging
        - Fill processing from queue
        
        Args:
            mid: Current mid price
            position: Current position
            position_lock: Lock for position access
            get_current_position: Callback to get current position
            fill_queue: Optional queue for fill processing
            
        Returns:
            CycleResult with outcome
        """
        start_time = time.perf_counter()
        self._cycle_count += 1
        
        fills_processed = 0
        grid_rebuilt = False
        error = None
        
        try:
            # Process pending fills from queue
            if fill_queue:
                fills_processed = await self._process_fill_queue(
                    fill_queue, position
                )
            
            # Check if grid rebuild needed
            now = time.time()
            rebuild_due = (now - self._last_rebuild) > self.config.rebuild_interval_sec
            builder_needs_rebuild = (
                self.grid_builder and self.grid_builder.should_rebuild()
            )
            
            if (rebuild_due or builder_needs_rebuild) and self.grid_builder:
                if position_lock and get_current_position:
                    result = await self.grid_builder.build_and_place_grid(
                        mid=mid,
                        position=position,
                        position_lock=position_lock,
                        get_current_position=get_current_position,
                    )
                    if result.success:
                        grid_rebuilt = True
                        self._last_rebuild = now
                        self.grid_builder.clear_rebuild_flag()
            
            # Save state if using state manager
            if self.state_manager and self.config.use_state_manager:
                if (now - self._last_state_save) > self.config.state_save_interval_sec:
                    await self.state_manager.save_state()
                    self._last_state_save = now
            
            # Log PnL periodically
            if (now - self._last_pnl_log) > self.config.pnl_log_interval_sec:
                self._log_pnl_summary()
                self._last_pnl_log = now
                
        except Exception as exc:
            error = str(exc)
            self._log_event("orchestrator_cycle_error", error=error)
        
        duration_ms = (time.perf_counter() - start_time) * 1000
        
        return CycleResult(
            success=error is None,
            mid=mid,
            fills_processed=fills_processed,
            grid_rebuilt=grid_rebuilt,
            error=error,
            duration_ms=duration_ms,
        )
    
    async def _process_fill_queue(
        self,
        fill_queue: asyncio.Queue,
        position: float,
    ) -> int:
        """Process fills from queue without blocking."""
        processed = 0
        
        while not fill_queue.empty():
            try:
                fill = fill_queue.get_nowait()
                await self.fill_processor.process_fill(
                    fill=fill,
                    position=position,
                )
                fill_queue.task_done()
                processed += 1
            except asyncio.QueueEmpty:
                break
            except Exception as exc:
                self._log_event("fill_queue_process_error", error=str(exc))
        
        return processed
    
    def _log_pnl_summary(self) -> None:
        """Log PnL summary."""
        if self.state_manager and self.config.use_state_manager:
            snapshot = self.state_manager.snapshot()
            self._log_event(
                "pnl_summary",
                session_pnl=snapshot.session_realized_pnl,
                alltime_pnl=snapshot.alltime_realized_pnl,
                position=snapshot.position,
            )
        elif self.fill_processor:
            self._log_event(
                "pnl_summary_basic",
                note="state_manager_not_integrated",
            )
    
    async def shutdown(self) -> None:
        """Graceful shutdown of orchestrator."""
        self._log_event("orchestrator_shutdown_start")
        self._running = False
        
        # Cancel all orders if execution gateway available
        if self.execution_gateway and self.config.use_execution_gateway:
            try:
                await self.execution_gateway.cancel_all(reason="shutdown")
            except Exception as exc:
                self._log_event("shutdown_cancel_error", error=str(exc))
        
        # Save final state
        if self.state_manager and self.config.use_state_manager:
            try:
                await self.state_manager.save_state()
            except Exception as exc:
                self._log_event("shutdown_state_save_error", error=str(exc))
        
        self._log_event("orchestrator_shutdown_complete", cycles=self._cycle_count)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get orchestrator statistics."""
        return {
            "coin": self.coin,
            "running": self._running,
            "cycle_count": self._cycle_count,
            "last_rebuild": self._last_rebuild,
            "last_state_save": self._last_state_save,
            "last_pnl_log": self._last_pnl_log,
            "components": {
                "fill_processor": self.fill_processor is not None,
                "state_manager": self.state_manager is not None,
                "execution_gateway": self.execution_gateway is not None,
                "grid_builder": self.grid_builder is not None,
            },
        }


class OrchestratorFactory:
    """Factory for creating configured orchestrators."""
    
    @staticmethod
    def create_minimal(
        coin: str,
        fill_processor: "FillProcessor",
        log_callback: Optional[Callable] = None,
    ) -> BotOrchestrator:
        """
        Create minimal orchestrator with just FillProcessor.
        
        This is the first step in migration - just wrap existing
        fill processing in the orchestrator pattern.
        """
        config = OrchestratorConfig(
            log_event_callback=log_callback,
        )
        return BotOrchestrator(
            coin=coin,
            fill_processor=fill_processor,
            config=config,
        )
    
    @staticmethod
    def create_full(
        coin: str,
        fill_processor: "FillProcessor",
        state_manager: "StateManager",
        execution_gateway: "ExecutionGateway",
        grid_builder: "GridBuilder",
        log_callback: Optional[Callable] = None,
    ) -> BotOrchestrator:
        """
        Create full orchestrator with all components.
        
        Use this after all components are integrated.
        """
        config = OrchestratorConfig(
            use_state_manager=True,
            use_execution_gateway=True,
            use_grid_builder=True,
            log_event_callback=log_callback,
        )
        return BotOrchestrator(
            coin=coin,
            fill_processor=fill_processor,
            state_manager=state_manager,
            execution_gateway=execution_gateway,
            grid_builder=grid_builder,
            config=config,
        )
