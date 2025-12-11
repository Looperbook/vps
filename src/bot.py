"""
Core grid bot wiring components.

Production-hardened grid trading bot with:
- Write-ahead logging for crash-safe fill processing
- Atomic grid snapshots to prevent race conditions
- Dead-man-switch for orphan order protection
- Comprehensive state validation and recovery
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import random
import secrets
import time
from typing import Any, Dict, List, Optional, Tuple

from dataclasses import dataclass

from hyperliquid.exchange import Exchange
from hyperliquid.info import Info

from src.config.config import Settings
from src.infra.logging_cfg import build_logger
from src.market_data.market_data import MarketData
from src.monitoring.metrics import Metrics
from src.monitoring.metrics_rich import RichMetrics
from src.core.bot_context import BotContext
from src.execution.order_router import OrderRequest, OrderRouter
from src.execution.order_manager import OrderManager, ActiveOrder
from src.risk.circuit_breaker import CircuitBreaker, CircuitBreakerConfig
from src.execution.fill_deduplicator import FillDeduplicator
from src.state.position_tracker import PositionTracker
from src.strategy.grid_calculator import GridCalculator, GridBuildResult, GridDiff
from src.execution.order_state_machine import OrderStateMachine, OrderState
from src.risk.risk import RiskEngine
from src.state.state_atomic import AtomicStateStore
from src.execution.fill_log import FillLog, BatchedFillLog
from src.strategy.strategy import GridLevel, GridStrategy
from src.strategy.strategy_factory import StrategyFactory
from src.core.utils import now_ms, quantize, tick_to_decimals
from src.monitoring.status import StatusBoard
# S-1, S-2, S-3 Structural Improvements
from src.state.shadow_ledger import ShadowLedger
from src.core.event_bus import EventBus, EventType, Event
from src.execution.order_sync import OrderSync
# Production hardening improvements
from src.state.wal import WriteAheadLog, WALEntryType
from src.strategy.grid_snapshot import SnapshotManager, GridSnapshot, compute_grid_diff_from_snapshot
from src.risk.dead_man_switch import DeadManSwitch, DeadManSwitchStub
# Architecture refactor: Extracted modules
from src.execution.fill_processor import FillProcessor, FillProcessorConfig, FillResult
from src.state.state_manager import StateManager, StateManagerConfig, StateSnapshot
from src.execution.execution_gateway import ExecutionGateway, ExecutionGatewayConfig
from src.execution.grid_builder import GridBuilder, GridBuilderConfig
from src.orchestrator.bot_orchestrator import (
    BotOrchestrator, OrchestratorConfig, CycleCallbacks, CycleAction, ComponentRefs
)
from src.execution.rest_poller import RestPoller, RestPollerConfig, PollResult
from src.execution.reconciliation_service import (
    ReconciliationService, ReconciliationConfig,
    PositionReconcileResult, OrderReconcileResult
)
from src.bot_logger import BotLogger, BotLoggerConfig
from src.meta_loader import MetaLoader, MetaResult

log = build_logger("gridbot")
# Current state schema version for migration support
STATE_SCHEMA_VERSION = 3


class GridBot:
    def __init__(
        self,
        coin: str,
        cfg: Settings,
        info: Info,
        async_info,
        exchange,
        metrics: Metrics,
        nonce_lock: asyncio.Lock,
        status_board: StatusBoard,
        per_coin_cfg: dict | None = None,
    ) -> None:
        self.coin = coin
        self.cfg = cfg
        self.info = info
        self.async_info = async_info
        self.exchange = exchange
        self.metrics = metrics
        self.account = cfg.resolve_account()
        
        # Initialize BotLogger early - used by all components
        logger_config = BotLoggerConfig(
            order_intent_sample_rate=getattr(cfg, 'log_order_intent_sample', 0.0),
            submit_sample_rate=getattr(cfg, 'log_submit_sample', 0.05),
        )
        self._logger = BotLogger(coin=coin, config=logger_config)
        
        # C-3 FIX: Pass reconnect callback to trigger REST poll when WS reconnects
        # This ensures fills during WS disconnect gap are captured
        self.market = MarketData(
            info, coin, self.account, cfg, 
            async_info=self.async_info,
            on_reconnect=self._on_ws_reconnect
        )
        # Use atomic async state store to avoid concurrent save/load races and blocking IO
        self.state_store = AtomicStateStore(coin, cfg.state_dir)
        # Phase 3 observability - create metrics early so fill log can use callback
        self.rich_metrics = RichMetrics()
        # Optimization-2: Event-sourced fill log with batched writes for reduced I/O
        # Wire up error callback to track fill log write failures
        self.fill_log: FillLog = BatchedFillLog(
            coin,
            cfg.state_dir,
            flush_interval=cfg.fill_log_batch_flush_sec,
            on_write_error=self._on_fill_log_write_error,
        )
        self.risk = RiskEngine(cfg)
        self.px_decimals = 2
        self.sz_decimals = 3
        self.tick_sz = 0.01
        self.strategy: Optional[GridStrategy] = None
        self.router: Optional[OrderRouter] = None
        # Struct-1: Use OrderManager for centralized order indexing
        self.order_manager = OrderManager(tick_sz=self.tick_sz, px_decimals=self.px_decimals)
        # Legacy aliases for backward compatibility during transition
        self.orders_by_cloid = self.order_manager.orders_by_cloid
        self.orders_by_oid = self.order_manager.orders_by_oid
        self.orders_by_price = self.order_manager.orders_by_price
        # Struct-4: Centralized position tracking with drift detection
        self.position_tracker = PositionTracker(log_event=self._log_event)
        self.running = True
        self.last_fill_time_ms = 0
        self.last_rest_fill_poll = 0.0
        self.last_resync = 0.0
        self.last_pnl_log = 0.0
        self.nonce_lock = nonce_lock
        self.status_board = status_board
        # Optional per-coin overrides loaded by the orchestrator
        self.per_coin_cfg = per_coin_cfg or {}
        self.rebuild_needed = False
        self.last_order_resync = 0.0
        self.data_paused = False
        self.risk_paused = False
        self._http_timeout = cfg.http_timeout
        self._skew_paused = False
        self._flatten_mode = False
        # Struct-2: Centralized circuit breaker for API errors
        try:
            _error_threshold = int(getattr(cfg, 'api_error_threshold', 5) or 5)
        except (TypeError, ValueError):
            _error_threshold = 5
        try:
            _backoff_sec = float(getattr(cfg, 'rest_mid_backoff_sec', 5) or 5)
            _cooldown = max(5.0, _backoff_sec * 2)
        except (TypeError, ValueError):
            _cooldown = 10.0
        self.circuit_breaker = CircuitBreaker(
            config=CircuitBreakerConfig(
                error_threshold=_error_threshold,
                cooldown_sec=_cooldown,
            ),
            on_reset=self._on_circuit_reset,
            log_event=self._log_event
        )
        # Struct-3: Centralized fill deduplication
        self.fill_deduplicator = FillDeduplicator(
            max_fills=10000,
            log_event=self._log_event
        )
        # Struct-5: Centralized grid calculations
        # Note: tick_sz/decimals initialized with defaults, updated in _load_meta
        try:
            _init_grids = int(getattr(cfg, 'grids', 5) or 5)
        except (TypeError, ValueError):
            _init_grids = 5
        try:
            _init_investment = float(getattr(cfg, 'investment_usd', 1000) or 1000)
        except (TypeError, ValueError):
            _init_investment = 1000.0
        self.grid_calculator = GridCalculator(
            cfg=cfg,
            tick_sz=self.tick_sz,
            px_decimals=self.px_decimals,
            sz_decimals=self.sz_decimals,
            effective_grids=_init_grids,  # Will be updated per-coin in initialize()
            effective_investment_usd=_init_investment
        )
        # Struct-6: Order State Machine for lifecycle tracking
        self.order_state_machine = OrderStateMachine(
            log_event=self._log_event,
            on_state_change=self._on_order_state_change
        )
        # S-1: Shadow Ledger for position tracking with exchange reconciliation
        self.shadow_ledger = ShadowLedger(
            coin=coin,
            log_event=self._log_event,
            on_drift=self._on_position_drift
        )
        # S-2: Event Bus for decoupled component communication
        self.event_bus = EventBus(
            history_size=1000,
            log_event=self._log_event
        )
        # S-3: Order Sync to keep OrderManager and OrderStateMachine in sync
        self.order_sync = OrderSync(
            order_manager=self.order_manager,
            order_state_machine=self.order_state_machine,
            log_event=self._log_event
        )
        # Structured logging context with trace ids
        self.ctx = BotContext(coin=self.coin, logger=log)
        # Hardening: Track last stuck order check time
        self._last_stuck_order_check = 0.0
        # Hardening: Track stuck orders for auto-cancellation
        # Maps cloid -> first_detected_at_timestamp
        self._stuck_order_first_seen: Dict[str, float] = {}
        # Auto-cancel stuck orders after this many seconds (default 90s)
        self._stuck_order_cancel_threshold_sec = float(os.getenv("HL_STUCK_ORDER_CANCEL_SEC", "90"))
        # Hardening: PnL sanity check threshold (configurable via per-coin or env)
        self._max_single_fill_pnl = float(os.getenv("HL_MAX_SINGLE_FILL_PNL", "500"))
        # Per-run (session) PnL tracking - reset each startup
        self._session_realized_pnl: float = 0.0
        self._session_start_time: float = time.time()
        self._alltime_realized_pnl: float = 0.0  # Loaded from state, never reset
        # C-1 FIX: Serialize fills during grid rebuild to prevent race condition
        # where fills use stale position snapshot
        self._rebuild_lock = asyncio.Lock()
        self._rebuild_in_progress = False
        # M-2 FIX: Dedicated high watermark for REST polling
        # Prevents skipping fills when last_fill_time_ms jumps during grid rebuild
        self._rest_poll_hwm_ms: int = 0
        
        # PRODUCTION HARDENING: Write-Ahead Log for crash-safe fill processing
        # Use config setting for WAL enablement
        self.wal: Optional[WriteAheadLog] = None
        if getattr(cfg, 'wal_enabled', True):
            wal_dir = getattr(cfg, 'wal_dir', f"{cfg.state_dir}/wal")
            self.wal = WriteAheadLog(
                coin=coin,
                state_dir=wal_dir,
                on_error=self._on_wal_error,
                fsync=True,  # Enable fsync for durability
            )
        
        # PRODUCTION HARDENING: Atomic snapshot manager for race-free grid rebuilds
        self._snapshot_manager = SnapshotManager()
        self._rebuild_epoch: int = 0  # Monotonic counter for rebuild detection
        
        # PRODUCTION HARDENING: Dead-man-switch for orphan order protection
        # Initialized in initialize() after exchange is available
        self.dead_man_switch: Optional[DeadManSwitch] = None
        self._dms_timeout_sec = getattr(cfg, 'dead_man_switch_sec', 300)
        
        # PRODUCTION HARDENING: Circuit breaker order cancellation state
        self._circuit_orders_cancelled = False
        
        # PRODUCTION HARDENING: State integrity flags
        self._needs_full_reconcile = False
        self._state_corrupted = False
        
        # Order TTL for expiresAfter
        self._order_ttl_ms = getattr(cfg, 'order_ttl_ms', 60000)
        
        # ARCHITECTURE REFACTOR: StateManager for centralized state ownership
        # StateManager is now the SINGLE SOURCE OF TRUTH for position/PnL
        state_config = StateManagerConfig(
            fill_rescan_ms=self.cfg.fill_rescan_ms if hasattr(self.cfg, 'fill_rescan_ms') else 60000,
            log_event_callback=self._log_event,
        )
        self.state_manager = StateManager(
            coin=coin,
            state_store=self.state_store,
            position_tracker=self.position_tracker,
            shadow_ledger=self.shadow_ledger,
            risk_engine=self.risk,
            config=state_config,
        )
        
        # ARCHITECTURE REFACTOR: FillProcessor for single-responsibility fill handling
        # Encapsulates fill logic previously embedded in _handle_fill()
        fill_config = FillProcessorConfig(
            wal_enabled=getattr(cfg, 'wal_enabled', True),
            max_single_fill_pnl=self._max_single_fill_pnl,
            log_event_callback=self._log_event,
        )
        self.fill_processor = FillProcessor(
            coin=coin,
            order_manager=self.order_manager,
            order_state_machine=self.order_state_machine,
            shadow_ledger=self.shadow_ledger,
            risk_engine=self.risk,
            event_bus=self.event_bus,
            rich_metrics=self.rich_metrics,
            wal=self.wal,
            fill_log=self.fill_log,
            config=fill_config,
            state_manager=self.state_manager,
        )
        
        # Flag to use StateManager as authoritative source
        self._use_state_manager = True
        
        # ARCHITECTURE REFACTOR: ExecutionGateway initialized after router is available
        # Will be set up in initialize() method
        self.execution_gateway: Optional[ExecutionGateway] = None
        
        # ARCHITECTURE REFACTOR: GridBuilder initialized after strategy is available
        # Will be set up in initialize() method
        self.grid_builder: Optional[GridBuilder] = None
        
        # ARCHITECTURE REFACTOR: BotOrchestrator for coordination (future use)
        # Will be set up after all components are available
        self.orchestrator: Optional[BotOrchestrator] = None
        
        # ARCHITECTURE REFACTOR: RestPoller for REST fill polling
        # Initialized here, but market not available until initialize()
        self.rest_poller: Optional[RestPoller] = None
        
        # ARCHITECTURE REFACTOR: ReconciliationService for position/order reconciliation
        # Will be set up after market is available
        self.reconciliation_service: Optional[ReconciliationService] = None

    def _on_wal_error(self, error: str) -> None:
        """Callback for WAL write errors - critical failure."""
        self._log_event("wal_write_error", error=error, severity="CRITICAL")
        try:
            self.rich_metrics.wal_write_errors.labels(coin=self.coin).inc()
        except Exception:
            pass

    # Struct-4: Backward-compatible properties delegating to StateManager (or PositionTracker fallback)
    @property
    def position(self) -> float:
        """Current position delegated to StateManager."""
        return self.state_manager.position
    
    @position.setter
    def position(self, value: float) -> None:
        """Set position - delegates to StateManager if enabled."""
        # Synchronous setter for compatibility - StateManager keeps components in sync
        self.state_manager.position_tracker.position = value
        self.state_manager.shadow_ledger.local_position = value
        self.state_manager.risk_engine.set_position(value)
    
    @property
    def realized_pnl(self) -> float:
        """Realized PnL - delegates to StateManager if enabled."""
        return self.state_manager.session_realized_pnl
    
    @realized_pnl.setter
    def realized_pnl(self, value: float) -> None:
        """Set realized PnL - delegates to PositionTracker (session only)."""
        self.state_manager.position_tracker.realized_pnl = value
    
    @property
    def position_lock(self) -> asyncio.Lock:
        """Position lock - delegates to PositionTracker."""
        return self.position_tracker.lock

    def _on_fill_log_write_error(self, error_msg: str) -> None:
        """Callback when fill log write fails - track in metrics and log."""
        try:
            self.rich_metrics.fill_log_write_errors.labels(coin=self.coin).inc()
            self._log_event("fill_log_write_error", error=error_msg)
        except Exception:
            pass  # Must not raise

    async def _on_ws_reconnect(self) -> None:
        """
        C-3 FIX: Callback triggered when WS reconnects after disconnect.
        
        Immediately poll REST fills to catch any fills missed during the 
        WebSocket disconnect gap. This prevents fill loss scenarios where
        WS was down but orders were still being matched on the exchange.
        """
        try:
            self._log_event("ws_reconnect_rest_poll", reason="fill_gap_recovery")
            await self._poll_rest_fills(force=True)
            try:
                self.rich_metrics.ws_reconnect_rest_polls.labels(coin=self.coin).inc()
            except Exception:
                pass
        except Exception as exc:
            self._log_event("ws_reconnect_rest_poll_error", err=str(exc))

    def _on_order_state_change(self, record, from_state: OrderState, to_state: OrderState) -> None:
        """Callback when order state changes - update metrics and publish event."""
        try:
            if to_state == OrderState.FILLED:
                self.rich_metrics.orders_filled.labels(coin=self.coin, side=record.side).inc()
            elif to_state == OrderState.CANCELLED:
                self.rich_metrics.orders_cancelled.labels(coin=self.coin).inc()
            elif to_state == OrderState.REJECTED:
                self.rich_metrics.orders_rejected.labels(coin=self.coin, reason=record.error_code or "unknown").inc()
        except Exception:
            pass  # Metrics must not break execution
        
        # S-2: Publish order state change event
        try:
            event_type = {
                OrderState.OPEN: EventType.ORDER_ACKNOWLEDGED,
                OrderState.FILLED: EventType.ORDER_FILLED,
                OrderState.PARTIALLY_FILLED: EventType.ORDER_PARTIAL,
                OrderState.CANCELLED: EventType.ORDER_CANCELLED,
                OrderState.REJECTED: EventType.ORDER_REJECTED,
            }.get(to_state)
            if event_type:
                self.event_bus.publish_sync(Event(
                    type=event_type,
                    data={
                        "cloid": record.cloid,
                        "oid": record.oid,
                        "side": record.side,
                        "price": record.price,
                        "from_state": from_state.name,
                        "to_state": to_state.name,
                    },
                    source="order_state_machine",
                ))
        except Exception:
            pass  # Events must not break execution

    def _on_position_drift(self, result) -> None:
        """
        S-1: Callback when shadow ledger detects significant position drift.
        
        Logs the drift and publishes event for monitoring/alerting.
        """
        try:
            self._log_event(
                "position_drift_alert",
                local=result.local_position,
                exchange=result.exchange_position,
                drift=result.drift,
                drift_pct=result.drift_pct,
                pending_exposure=result.pending_exposure,
            )
            self.rich_metrics.position_drift_alerts.labels(coin=self.coin).inc()
            
            # Publish drift event
            self.event_bus.publish_sync(Event(
                type=EventType.POSITION_DRIFT,
                data={
                    "drift": result.drift,
                    "drift_pct": result.drift_pct,
                    "local_position": result.local_position,
                    "exchange_position": result.exchange_position,
                },
                source="shadow_ledger",
            ))
        except Exception:
            pass  # Must not raise

    def _validate_fill_timestamp(self, ts_ms: int) -> Optional[str]:
        """
        Validate fill timestamp for anomalies.
        
        Returns anomaly type string if suspicious, None if OK.
        """
        now = now_ms()
        # Check for future timestamp (>1 minute ahead - possible clock skew)
        if ts_ms > now + 60_000:
            return "future"
        # Check for very old timestamp (>1 day old - suspicious)
        if ts_ms < now - 86_400_000:
            return "very_old"
        return None

    def _check_stuck_orders(self) -> List[Any]:
        """
        Check for orders stuck in PENDING state and update metrics.
        
        Returns list of stuck orders for logging/alerting.
        """
        stuck = self.order_state_machine.get_stuck_orders(max_pending_age_ms=30_000)
        try:
            self.rich_metrics.stuck_orders_count.labels(coin=self.coin).set(len(stuck))
        except Exception:
            pass
        return stuck

    async def _cancel_stuck_orders(self, stuck: List[Any]) -> int:
        """Cancel orders that have been stuck for too long. Delegates to ExecutionGateway."""
        return await self.execution_gateway.cancel_stuck_orders(
            stuck=stuck,
            stuck_order_first_seen=self._stuck_order_first_seen,
            cancel_threshold_sec=self._stuck_order_cancel_threshold_sec,
        )

    async def _check_and_cancel_stuck(self) -> int:
        """Check for stuck orders and cancel them if they've been stuck too long."""
        stuck = self._check_stuck_orders()
        if stuck:
            return await self._cancel_stuck_orders(stuck)
        return 0

    def _get_config_value(self, key: str, default=None):
        """Get config value with per-coin override support."""
        # Check per_coin_cfg first, then fall back to global cfg
        if isinstance(self.per_coin_cfg, dict) and key in self.per_coin_cfg:
            return self.per_coin_cfg[key]
        return getattr(self.cfg, key, default)

    @property
    def effective_investment_usd(self) -> float:
        """Investment USD with per-coin override support."""
        return float(self._get_config_value("investment_usd", self.cfg.investment_usd))

    @property
    def effective_grids(self) -> int:
        """Number of grids with per-coin override support."""
        return int(self._get_config_value("grids", self.cfg.grids))

    @property
    def effective_grid_spacing_pct(self) -> float:
        """Grid spacing with per-coin override support."""
        return float(self._get_config_value("base_spacing_pct", self.cfg.base_spacing_pct))

    async def initialize(self) -> None:
        await self._load_meta()
        # Ensure per_coin_cfg is a dict
        if not isinstance(self.per_coin_cfg, dict):
            self.per_coin_cfg = {}
        # Struct-5: Update GridCalculator with per-coin config values
        self.grid_calculator.update_config(
            effective_grids=self.effective_grids,
            effective_investment_usd=self.effective_investment_usd
        )
        # Use StrategyFactory to create the configured strategy (default 'grid')
        strat_name = self.per_coin_cfg.get("default_strategy", getattr(self.cfg, "default_strategy", "grid"))
        self.strategy = StrategyFactory.create(strat_name, self.cfg, self.tick_sz, self.px_decimals, self.sz_decimals, log_sample=self.cfg.log_strategy_sample, per_coin_cfg=self.per_coin_cfg)
        self.router = OrderRouter(
            self.coin, self.exchange, self.info, self.cfg, self.px_decimals, self.tick_sz, self.sz_decimals, self.nonce_lock
        )
        self.router.start()
        # Apply per-coin overrides to router tuning (e.g., coalescing)
        try:
            if isinstance(self.per_coin_cfg, dict) and "coalesce_ms" in self.per_coin_cfg:
                val = int(self.per_coin_cfg.get("coalesce_ms") or 0)
                if val > 0:
                    self.router.coalesce_ms = val
                    self.router.coalesce_min_ms = max(1, int(val / 4))
                    self.router.coalesce_max_ms = max(val, 200)
        except Exception:
            pass
        
        # ARCHITECTURE REFACTOR: Initialize ExecutionGateway now that router is available
        exec_config = ExecutionGatewayConfig(
            use_cloid=self.cfg.use_cloid,
            log_event_callback=self._log_event,
            dex=self.cfg.dex if hasattr(self.cfg, 'dex') else "hl",
            http_timeout=self._http_timeout,
        )
        self.execution_gateway = ExecutionGateway(
            coin=self.coin,
            router=self.router,
            order_manager=self.order_manager,
            order_state_machine=self.order_state_machine,
            shadow_ledger=self.shadow_ledger,
            event_bus=self.event_bus,
            rich_metrics=self.rich_metrics,
            circuit_breaker=self.circuit_breaker,
            config=exec_config,
            # C-6 FIX: Pass info APIs for order verification
            info=self.info,
            async_info=self.async_info,
            account=self.account,
        )
        
        # ARCHITECTURE REFACTOR: Initialize GridBuilder now that strategy is available
        grid_builder_config = GridBuilderConfig(
            drift_abort_threshold=0.10,
            flatten_mode=self._flatten_mode,
            log_event_callback=self._log_event,
        )
        self.grid_builder = GridBuilder(
            coin=self.coin,
            grid_calculator=self.grid_calculator,
            strategy=self.strategy,
            risk_engine=self.risk,
            execution_gateway=self.execution_gateway,
            rich_metrics=self.rich_metrics,
            router=self.router,
            config=grid_builder_config,
        )
        
        # ARCHITECTURE REFACTOR: Initialize BotOrchestrator for coordination
        orch_config = OrchestratorConfig(
            rebuild_interval_sec=self.cfg.grid_refresh_sec if hasattr(self.cfg, 'grid_refresh_sec') else 30.0,
            state_save_interval_sec=30.0,
            pnl_log_interval_sec=self.cfg.pnl_log_interval if hasattr(self.cfg, 'pnl_log_interval') else 60.0,
            rest_fill_poll_interval_sec=self.cfg.rest_fill_interval if hasattr(self.cfg, 'rest_fill_interval') else 30.0,
            position_reconcile_interval_sec=self.cfg.rest_audit_interval if hasattr(self.cfg, 'rest_audit_interval') else 60.0,
            order_reconcile_interval_sec=self.cfg.rest_audit_interval if hasattr(self.cfg, 'rest_audit_interval') else 60.0,
            stuck_order_check_interval_sec=30.0,
            loop_interval_sec=self.cfg.loop_interval if hasattr(self.cfg, 'loop_interval') else 1.0,
            short_sleep_sec=min(self.cfg.loop_interval if hasattr(self.cfg, 'loop_interval') else 1.0, 2.0),
            data_halt_sec=self.cfg.data_halt_sec if hasattr(self.cfg, 'data_halt_sec') else 30.0,
            use_state_manager=True,  # StateManager is now the single source of truth
            use_execution_gateway=True,  # ExecutionGateway now used for order submission
            use_grid_builder=True,  # GridBuilder now used for level replacement
            use_orchestrator_loop=True,  # Orchestrator now drives the main loop
            log_event_callback=self._log_event,
        )
        # Phase 5: Provide direct component references to reduce callback indirection
        component_refs = ComponentRefs(
            market=self.market,
            risk=self.risk,
            circuit_breaker=self.circuit_breaker,
            dead_man_switch=None,  # Set after initialization in initialize()
        )
        self.orchestrator = BotOrchestrator(
            coin=self.coin,
            fill_processor=self.fill_processor,
            state_manager=self.state_manager,
            execution_gateway=self.execution_gateway,
            grid_builder=self.grid_builder,
            config=orch_config,
            components=component_refs,
        )
        
        # ARCHITECTURE REFACTOR: Initialize RestPoller for REST fill polling
        rest_poller_config = RestPollerConfig(
            poll_interval_sec=self.cfg.rest_fill_interval if hasattr(self.cfg, 'rest_fill_interval') else 30.0,
            degraded_poll_interval_sec=self.cfg.degraded_rest_interval if hasattr(self.cfg, 'degraded_rest_interval') else 7.5,
            fill_rescan_ms=self.cfg.fill_rescan_ms if hasattr(self.cfg, 'fill_rescan_ms') else 60000,
            log_event_callback=self._log_event,
        )
        self.rest_poller = RestPoller(
            coin=self.coin,
            market=self.market,
            account=self.account,
            fill_deduplicator=self.fill_deduplicator,
            config=rest_poller_config,
        )
        
        # ARCHITECTURE REFACTOR: Initialize ReconciliationService
        reconcile_config = ReconciliationConfig(
            position_reconcile_interval_sec=self.cfg.rest_audit_interval if hasattr(self.cfg, 'rest_audit_interval') else 60.0,
            order_reconcile_interval_sec=self.cfg.rest_audit_interval if hasattr(self.cfg, 'rest_audit_interval') else 60.0,
            position_drift_alert_pct=0.01,  # 1% drift tolerance
            dex=self.cfg.dex if hasattr(self.cfg, 'dex') else "hl",
            log_event_callback=self._log_event,
        )
        self.reconciliation_service = ReconciliationService(
            coin=self.coin,
            async_info=self.async_info,
            info=self.info,
            account=self.account,
            order_manager=self.order_manager,
            state_manager=self.state_manager,
            shadow_ledger=self.shadow_ledger,
            risk_engine=self.risk,
            rich_metrics=self.rich_metrics,
            event_bus=self.event_bus,
            config=reconcile_config,
        )
        
        # PRODUCTION HARDENING: Initialize WAL and recover state
        if self.wal:
            await self.wal.initialize()
            await self._recover_from_wal()
        
        await self._load_state()
        
        # PRODUCTION HARDENING: Validate state integrity
        await self._validate_state_integrity()
        
        # S-1: Initialize shadow ledger from loaded position
        self.shadow_ledger.local_position = self.position
        self.shadow_ledger.confirmed_position = self.position
        # Set min fill time on market to filter stale WS fills from previous sessions
        if self.market:
            self.market.min_fill_time_ms = self.last_fill_time_ms
        # Replay any fills from the event log that occurred after the saved last_fill_time_ms
        try:
            events = await self.fill_log.read_since(self.last_fill_time_ms)
            for ev in events:
                try:
                    await self._handle_fill(ev, replay=True)
                except Exception:
                    pass
        except Exception:
            pass
        self.market.start_ws(asyncio.get_running_loop())
        # Log loaded config once for visibility of env overrides.
        self._log_event("config_loaded", cfg=self.cfg.dump())
        
        # ALWAYS reconcile with exchange on startup to detect state corruption
        await self._reconcile_position()
        
        # PRODUCTION HARDENING: Full reconcile if state might be corrupted
        if self._needs_full_reconcile or self._state_corrupted:
            self._log_event("full_reconcile_required", 
                           needs_full=self._needs_full_reconcile,
                           corrupted=self._state_corrupted)
            # Trust exchange position, don't replay fills
            exchange_pos = await self._fetch_exchange_position()
            if exchange_pos is not None:
                await self.state_manager.set_position(exchange_pos, source="startup_exchange_reconcile")
                self._needs_full_reconcile = False
        
        await self._poll_rest_fills(force=True)
        await self._cancel_all(reason="startup_reset")
        mid = 0.0
        for _ in range(10):
            mid = await self.market.mid_price()
            if mid > 0:
                break
            await asyncio.sleep(0.5)
        if mid <= 0:
            self._log_event("init_mid_unavailable", mid=mid)
            raise RuntimeError("mid_price_unavailable")
        await self._build_and_place_grid(mid)
        await self._push_status()
        
        # PRODUCTION HARDENING: Activate dead-man-switch
        if self._dms_timeout_sec > 0:
            try:
                self.dead_man_switch = DeadManSwitch(
                    exchange=self.exchange,
                    timeout_sec=self._dms_timeout_sec,
                    log_event=self._log_event,
                )
                await self.dead_man_switch.activate()
                # Phase 5: Update component refs with dead man switch
                if self.orchestrator and self.orchestrator.components:
                    self.orchestrator.components.dead_man_switch = self.dead_man_switch
                self._log_event("dead_man_switch_active", timeout_sec=self._dms_timeout_sec)
            except Exception as exc:
                # Dead-man-switch is safety feature, log but continue
                self._log_event("dead_man_switch_init_error", error=str(exc))
                self.dead_man_switch = None
        
        self._log_event("init", mid=mid)
        try:
            self.rich_metrics.bot_started.labels(coin=self.coin).inc()
        except Exception:
            pass

    async def _recover_from_wal(self) -> None:
        """Recover state from WAL on startup."""
        if not self.wal:
            return
        try:
            checkpoint, entries = await self.wal.recover()
            
            if checkpoint:
                self._log_event("wal_recovery_checkpoint",
                               seq=checkpoint.sequence,
                               position=checkpoint.position,
                               pnl=checkpoint.realized_pnl)
                # Use checkpoint as baseline (state file should match)
            
            if entries:
                self._log_event("wal_recovery_entries", count=len(entries))
                # Entries are fills since checkpoint - they should be in fill_log too
                # This is a consistency check, not replay
                fill_entries = [e for e in entries if e.entry_type == WALEntryType.FILL]
                if fill_entries:
                    self._log_event("wal_fill_entries_found", count=len(fill_entries))
                    
        except Exception as exc:
            self._log_event("wal_recovery_error", error=str(exc))
    
    async def _validate_state_integrity(self) -> None:
        """Validate loaded state integrity, flag for full reconcile if issues found."""
        # Check for state corruption indicators
        issues = []
        
        # Issue 1: Has fill history but zero position
        if self.last_fill_time_ms > 0 and abs(self.position) < 1e-9:
            # This could indicate state corruption OR legitimate flat position
            # Check fill log for recent activity
            try:
                recent_fills = await self.fill_log.read_since(self.last_fill_time_ms - 3600000)  # 1 hour
                if len(recent_fills) > 0:
                    issues.append("fill_history_but_zero_position")
            except Exception:
                pass
        
        # Issue 2: State version mismatch
        # (handled in _load_state)
        
        if issues:
            self._log_event("state_integrity_issues", issues=issues)
            self._needs_full_reconcile = True
    
    def _build_cycle_callbacks(self) -> CycleCallbacks:
        """
        Build CycleCallbacks for orchestrator main loop.
        
        Phase 5: Some callbacks are now optional since orchestrator can use
        ComponentRefs directly (market, risk, circuit_breaker, dead_man_switch).
        These are still provided as fallback for testing/backwards compat.
        """
        return CycleCallbacks(
            # Market data (fallback - orchestrator uses ComponentRefs.market)
            get_mid_price=self.market.mid_price,
            get_data_age=lambda: self.market.data_age() if self.market else 0.0,
            is_market_halted=lambda: self.market.is_halted() if self.market else False,
            
            # Position/state (fallback - orchestrator uses StateManager)
            get_position=lambda: self.position,
            get_position_lock=lambda: self.position_lock,
            
            # Safety (fallback - orchestrator uses ComponentRefs)
            refresh_dead_man_switch=self._refresh_dead_man_switch,
            is_risk_halted=lambda: self.risk.is_halted(),
            is_circuit_tripped=lambda: self.circuit_breaker.is_tripped,
            
            # Grid operations
            should_rebuild_grid=lambda: self.rebuild_needed,
            build_and_place_grid=self._build_and_place_grid,
            clear_rebuild_flag=self._clear_rebuild_flag,
            check_trailing_trigger=self._check_trailing_trigger,
            
            # Periodic operations
            poll_rest_fills=self._poll_rest_fills,
            reconcile_position=self._reconcile_position,
            reconcile_orders=self._reconcile_orders_with_prune,
            check_and_cancel_stuck=self._check_and_cancel_stuck,
            
            # Skew/flatten
            handle_skew=self._handle_skew,
            
            # Logging/status
            log_pnl=self._log_pnl,
            push_status=self._push_status,
            
            # Order management
            cancel_all=self._cancel_all,
            
            # State persistence
            persist_state=self._persist,
        )
    
    async def _refresh_dead_man_switch(self) -> None:
        """Refresh dead-man-switch if active."""
        if self.dead_man_switch and self.dead_man_switch.is_active:
            await self.dead_man_switch.refresh()
    
    def _clear_rebuild_flag(self) -> None:
        """Clear the rebuild needed flag."""
        self.rebuild_needed = False
    
    def _check_trailing_trigger(self, mid: float) -> bool:
        """Check if trailing trigger requires grid rebuild."""
        if not self.strategy or not self.strategy.grid_center or not mid:
            return False
        drift = abs(mid - self.strategy.grid_center) / self.strategy.grid_center
        if drift > self.cfg.trailing_pct:
            self.rebuild_needed = True
            return True
        return False
    
    async def _reconcile_orders_with_prune(self) -> None:
        """Reconcile orders and prune old terminal states."""
        await self._reconcile_orders()
        self._reset_api_errors()
        # Struct-6: Prune old terminal orders from state machine
        self.order_state_machine.prune_terminal(max_age_ms=300_000)  # 5 minutes
    
    async def _handle_skew(self) -> bool:
        snapshot = await self.state_manager.load(now_ms())
        self._alltime_realized_pnl = snapshot.alltime_realized_pnl
        self._session_realized_pnl = 0.0  # Always starts at 0
        self._session_start_time = snapshot.session_start_time
        self.last_fill_time_ms = snapshot.last_fill_time_ms
        gc = snapshot.grid_center
        if self.strategy and gc:
            self.strategy.grid_center = gc
        self._log_event(
            "session_start",
            alltime_pnl=self._alltime_realized_pnl,
            session_pnl=0.0,
            session_start=self._session_start_time,
            via="state_manager",
        )
        """Fetch current position from exchange."""
        try:
            builder_asset = self.cfg.dex.lower() == "xyz" or self.coin.startswith("xyz:")
            if self.async_info and not builder_asset:
                state = await self.async_info.user_state(self.account, self.cfg.dex)
            else:
                def _call():
                    return self.info.user_state(self.account, dex=self.cfg.dex)
                state = await self._call_with_retry(_call, "user_state")
            
            for ap in state.get("assetPositions", []):
                p = ap.get("position", {})
                if p.get("coin") == self.coin:
                    return float(p.get("szi", 0.0))
            return 0.0
        except Exception as exc:
            self._log_event("fetch_exchange_position_error", error=str(exc))
            return None

    async def run(self) -> None:
        # Optimization-2: Start batched fill log background flush
        if hasattr(self.fill_log, "start"):
            await self.fill_log.start()
        # S-2: Start event bus in background
        event_bus_task = asyncio.create_task(self.event_bus.start())
        # Publish bot started event
        await self.event_bus.emit(EventType.BOT_STARTED, source="bot", coin=self.coin)
        fill_task = asyncio.create_task(self._fill_worker())
        
        # Build callbacks for orchestrator
        callbacks = self._build_cycle_callbacks()
        
        try:
            # ARCHITECTURE: Always use orchestrator-driven main loop
            # Legacy loop has been removed - orchestrator is the single execution path
            await self._run_orchestrator_loop(callbacks)
        except asyncio.CancelledError:
            pass
        finally:
            fill_task.cancel()
            await asyncio.gather(fill_task, return_exceptions=True)
        
        # ===== GRACEFUL SHUTDOWN SEQUENCE =====
        await self._graceful_shutdown()
    
    async def _run_orchestrator_loop(self, callbacks: CycleCallbacks) -> None:
        """Main loop driven by BotOrchestrator."""
        self._log_event("orchestrator_loop_start")
        
        while self.running:
            # Check for stop file
            if os.path.exists("STOP.txt"):
                self.stop()
                continue
            
            # Run one cycle through orchestrator
            result = await self.orchestrator.run_cycle(callbacks)
            
            # Handle cycle result
            if result.action == CycleAction.STOP:
                self.stop()
                continue
            elif result.action == CycleAction.SLEEP_SHORT:
                await asyncio.sleep(self.orchestrator.config.short_sleep_sec)
            else:  # SLEEP_NORMAL or CONTINUE
                await asyncio.sleep(self.orchestrator.config.loop_interval_sec)
        
        self._log_event("orchestrator_loop_end", cycles=self.orchestrator._cycle_count)
    
    # NOTE: _run_legacy_loop() has been removed
    # All execution now goes through _run_orchestrator_loop()
    # This eliminates dual code paths and simplifies maintenance
    
    async def _graceful_shutdown(self) -> None:
        """Graceful shutdown sequence."""
        # Order matters for crash safety:
        # 1. Cancel dead-man-switch (we're shutting down gracefully)
        # 2. Cancel all orders
        # 3. WAL checkpoint (flush all pending entries)
        # 4. Persist final state
        # 5. Stop subsystems
        
        # 1. Cancel dead-man-switch scheduled cancellation
        if self.dead_man_switch:
            try:
                await self.dead_man_switch.cancel()
                self._log_event("dead_man_switch_cancelled", reason="graceful_shutdown")
            except Exception as e:
                self._log_event("dead_man_switch_cancel_error", error=str(e))
        
        # S-2: Publish bot stopped event and stop event bus
        await self.event_bus.emit(EventType.BOT_STOPPED, source="bot", coin=self.coin)
        self.event_bus.stop()
        await self.event_bus.drain(timeout=2.0)
        
        # 2. Cancel all open orders
        await self._cancel_all(reason="shutdown")
        
        # 3. WAL checkpoint - flush all pending writes to disk
        if self.wal:
            try:
                self.wal.checkpoint()
                self._log_event("wal_checkpoint_complete", reason="shutdown")
            except Exception as e:
                self._log_event("wal_checkpoint_error", error=str(e))
        
        # 4. Persist final state
        await self._persist()
        
        # 5. Stop subsystems
        if self.router:
            await self.router.stop()
        if self.market:
            await self.market.stop()
        # Optimization-2: Flush and stop batched fill log
        if hasattr(self.fill_log, "stop"):
            await self.fill_log.stop()
        await self._push_status()
        
        # Notify orchestrator shutdown
        if self.orchestrator:
            await self.orchestrator.shutdown()

    def stop(self) -> None:
        try:
            self.rich_metrics.bot_stopped.labels(coin=self.coin).inc()
        except Exception:
            pass
        self.running = False

    async def _fill_worker(self) -> None:
        while self.running:
            try:
                f = await self.market.next_fill()
                await self._handle_fill(f)
                self.market.fill_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                self._log_event("fill_error", err=str(exc))

    async def _handle_fill(self, f: Dict[str, Any], replay: bool = False) -> None:
        """Process a fill dictionary `f`. If `replay` is False the fill is appended
        to the event-sourced fill log before processing to guarantee persistence.
        
        ARCHITECTURE REFACTOR: Core processing delegated to FillProcessor.
        FillProcessor uses StateManager directly for state sync.
        """
        # Prepare canonical fields
        side = f.get("side", "").lower()
        px = float(f.get("px", 0.0))
        sz = float(f.get("sz", 0.0))
        oid = f.get("oid")
        cloid = f.get("cloid")
        ts_ms = int(f.get("time", now_ms()))

        # Hardening: Validate fill timestamp for anomalies
        ts_anomaly = self._validate_fill_timestamp(ts_ms)
        if ts_anomaly:
            self._log_event("fill_timestamp_anomaly", type=ts_anomaly, ts_ms=ts_ms, side=side, px=px, sz=sz)
            try:
                self.rich_metrics.fill_timestamp_anomalies.labels(coin=self.coin, type=ts_anomaly).inc()
            except Exception:
                pass

        # Struct-3: Global deduplication via centralized FillDeduplicator
        if not self.fill_deduplicator.check_and_add(f):
            self._log_event("fill_dedup_skip", key=self.fill_deduplicator.make_fill_key(f), replay=replay)
            return

        # Delegate core fill processing to FillProcessor (uses StateManager directly)
        async with self.position_lock:
            result = await self.fill_processor.process_fill(
                side=side,
                px=px,
                sz=sz,
                oid=oid,
                cloid=cloid,
                ts_ms=ts_ms,
                replay=replay,
                rebuild_in_progress=self._rebuild_in_progress,
            )
        
        self._session_realized_pnl = self.state_manager.session_realized_pnl
        self._alltime_realized_pnl = self.state_manager.alltime_realized_pnl
        self.last_fill_time_ms = self.state_manager.last_fill_time_ms
        self.realized_pnl = self._session_realized_pnl  # Keep property in sync
        
        # Handle results
        if result.is_replay:
            return
        
        if result.needs_grid_rebuild:
            self.rebuild_needed = True
        
        if result.needs_reconciliation:
            try:
                await self._reconcile_orders()
            except Exception as exc:
                self._log_event("fill_unmatched_reconcile_error", err=str(exc))
            await self._reconcile_position()
            await self._push_status()
            return
        
        if not result.matched:
            return
        
        # === Matched fill post-processing ===
        
        # Structured log for tracing and correlation
        try:
            self.ctx.log(
                "fill",
                side=side,
                px=px,
                sz=sz,
                oid=oid,
                cloid=cloid,
                matched=result.matched,
                pnl_add=result.realized_pnl,
                position=self.position,
                session_pnl=self.state_manager.session_realized_pnl,
                alltime_pnl=self.state_manager.alltime_realized_pnl,
            )
        except Exception:
            pass
        
        # Legacy/simple event log
        self._log_event(
            "fill",
            side=side,
            px=px,
            sz=sz,
            oid=oid,
            cloid=cloid,
            matched=result.matched,
            pnl_add=result.realized_pnl,
            position=self.position,
            session_pnl=self.state_manager.session_realized_pnl,
            alltime_pnl=self.state_manager.alltime_realized_pnl,
        )
        
        # Strategy and grid updates for matched fills
        if self.strategy and result.trigger_price:
            self.strategy.on_price(result.trigger_price)
            
            # M-5 FIX: Flush fill log before any other processing for crash safety
            if hasattr(self.fill_log, 'flush_sync'):
                try:
                    await self.fill_log.flush_sync()
                except Exception:
                    pass  # Best effort - don't block fill processing
            
            # Immediately recycle only the filled level (no full-grid rebuild)
            await self._replace_after_fill(side, px, sz)
            await self._persist()
            await self._push_status()
            
            # Update unrealized skew/guards using latest mid if available
            try:
                mid_latest = await self.market.mid_price()
                self.risk.set_unrealized(self.position * (mid_latest - px))
            except Exception:
                pass

    async def _call_with_retry(self, fn, label: str, retries: int = 1):
        """
        Run a blocking Info call in a thread with timeout and minimal backoff.
        """
        backoff = 0.25
        for attempt in range(retries + 1):
            try:
                return await asyncio.wait_for(asyncio.get_running_loop().run_in_executor(None, fn), timeout=self._http_timeout)
            except Exception as exc:
                self._log_event("http_retry", where=label, err=str(exc), attempt=attempt)
                if attempt >= retries:
                    raise
                await asyncio.sleep(backoff)
                backoff *= 2

    async def _load_meta(self) -> None:
        """Load asset metadata using MetaLoader."""
        loader = MetaLoader(
            info=self.info,
            async_info=self.async_info,
            coin=self.coin,
            dex=self.cfg.dex,
            http_timeout=self._http_timeout,
            log_event=self._log_event,
        )
        result = await loader.load()
        
        # Apply results
        self.tick_sz = result.tick_sz
        self.px_decimals = result.px_decimals
        self.sz_decimals = result.sz_decimals
        
        # Update dependent components
        self.order_manager.update_tick_info(self.tick_sz, self.px_decimals)
        self.grid_calculator.tick_sz = self.tick_sz
        self.grid_calculator.px_decimals = self.px_decimals
        self.grid_calculator.sz_decimals = self.sz_decimals

    async def _reconcile_position(self) -> None:
        """Reconcile position with exchange - delegates to ReconciliationService."""
        # ARCHITECTURE: ReconciliationService is required
        if not self.reconciliation_service:
            raise RuntimeError("ReconciliationService not initialized. Call setup_test_services() for tests.")
        
        # Delegate to ReconciliationService
        result = await self.reconciliation_service.reconcile_position(
            position_lock=self.position_lock,
        )
        
        if not result.success:
            self._log_event("reconcile_position_failed", error=result.error)
            return
            
        # Bot-specific: Update unrealized PnL from entry price if available
        # (ReconciliationService handles position, equity, daily_pnl, funding)
        try:
            mids = await asyncio.get_running_loop().run_in_executor(
                None, lambda: self.info.all_mids(dex=self.cfg.dex)
            )
            mid_for_unreal = float(mids.get(self.coin, 0.0))
            if mid_for_unreal and result.exchange_position:
                # Fetch entry price for unrealized calc
                builder_asset = self.cfg.dex.lower() == "xyz" or self.coin.startswith("xyz:")
                if self.async_info and not builder_asset:
                    state = await self.async_info.user_state(self.account, self.cfg.dex)
                else:
                    def _call():
                        return self.info.user_state(self.account, dex=self.cfg.dex)
                    state = await self._call_with_retry(_call, "user_state")
                for ap in state.get("assetPositions", []):
                    p = ap.get("position", {})
                    if p.get("coin") == self.coin:
                        entry_px = float(p.get("entryPx", 0.0) or 0.0)
                        if entry_px:
                            self.risk.set_unrealized(result.exchange_position * (mid_for_unreal - entry_px))
                        break
        except Exception:
            pass  # Unrealized calc is best-effort
            
        # Bot-specific: Emit event bus event on significant drift
        if result.drift_pct > 0.01:  # >1% drift
            try:
                await self.event_bus.emit(
                    EventType.POSITION_DRIFT,
                    source="reconcile",
                    drift_amount=result.drift,
                    drift_percent=result.drift_pct,
                )
            except Exception:
                pass
                
        # Bot-specific: Update risk_halted metric
        try:
            self.rich_metrics.risk_halted.labels(coin=self.coin).set(1.0 if self.risk.is_halted() else 0.0)
        except Exception:
            pass

    async def _reconcile_orders(self) -> None:
        """
        Compare local registry with exchange open orders and repair drift.
        Delegates to ReconciliationService.
        """
        # ARCHITECTURE: ReconciliationService is required
        if not self.reconciliation_service:
            raise RuntimeError("ReconciliationService not initialized. Call setup_test_services() for tests.")
        
        # Delegate to ReconciliationService
        result = await self.reconciliation_service.reconcile_orders()
        if not result.success:
            self._log_event("reconcile_orders_failed", error=result.error)

    async def _poll_rest_fills(self, force: bool = False) -> None:
        """Poll REST API for fills - delegates to RestPoller service."""
        if not self.rest_poller:
            # Fall back to legacy behavior if service not initialized
            self._log_event("rest_poll_no_service", force=force)
            return
            
        # Sync initial state to RestPoller if needed
        if self.rest_poller._last_fill_time_ms == 0 and self.last_fill_time_ms > 0:
            self.rest_poller.set_last_fill_time(self.last_fill_time_ms)
        if self.rest_poller._rest_poll_hwm_ms == 0 and self._rest_poll_hwm_ms > 0:
            self.rest_poller.set_high_water_mark(self._rest_poll_hwm_ms)
            
        # Poll using service, with fill handler callback
        result = await self.rest_poller.poll_if_due(
            force=force,
            ws_degraded=self.market.is_halted() if self.market else False,
            on_fill=self._handle_fill,  # Delegate fill processing
        )
        
        # Sync state back from RestPoller
        if result.success:
            self._rest_poll_hwm_ms = self.rest_poller._rest_poll_hwm_ms
            self.last_rest_fill_poll = time.time()

    async def _build_and_place_grid(self, mid: float) -> None:
        """Build and place grid - delegates to GridBuilder."""
        if not self.strategy or not self.router:
            return
        if mid <= 0:
            self._log_event("grid_build_skip_mid", mid=mid)
            return
        
        # ARCHITECTURE: GridBuilder is required - use setup_test_services() for tests
        if not self.grid_builder:
            raise RuntimeError("GridBuilder not initialized. Call setup_test_services() for tests.")
        
        # Snapshot position atomically
        async with self.position_lock:
            position_snapshot = self.position
        
        # Sync flatten mode
        self.grid_builder.flatten_mode = self._flatten_mode
        
        result = await self.grid_builder.build_and_place_grid(
            mid=mid,
            position=position_snapshot,
            position_lock=self.position_lock,
            get_current_position=lambda: self.position,
        )
        
        # Handle result
        if result.was_aborted:
            self._log_event("grid_rebuild_aborted", reason=result.aborted_reason)
        
        # Check rebuild_needed flag on GridBuilder (set during execution)
        if self.grid_builder.rebuild_needed:
            self.rebuild_needed = True
            self.grid_builder.rebuild_needed = False
        
        # Post-rebuild actions
        await self._push_status()
        await self._maybe_flatten(mid)

    async def _replace_after_fill(self, fill_side: str, fill_px: float, fill_sz: float) -> None:
        """Place replacement order after fill using GridBuilder."""
        if not self.grid_builder:
            # Fall back to old behavior if GridBuilder not initialized
            if not self.strategy or not self.router:
                return
            lvl = self.grid_calculator.calculate_replacement_level(
                strategy=self.strategy,
                fill_side=fill_side,
                fill_px=fill_px,
                fill_sz=fill_sz,
                position=self.position,
                flatten_mode=self._flatten_mode
            )
            if lvl is None:
                self._log_event("flatten_skip_refill", side=fill_side, px=fill_px, sz=fill_sz)
                return
            if not self.risk.allow_order(lvl.side, lvl.sz, lvl.px):
                self._log_event("skip_replacement_risk", side=lvl.side, px=lvl.px, sz=lvl.sz, position=self.position)
                return
            await self._submit_level(lvl)
            return
        
        # Use GridBuilder for replacement
        result = await self.grid_builder.replace_after_fill(
            fill_side=fill_side,
            fill_px=fill_px,
            fill_sz=fill_sz,
            position=self.position,
        )
        
        if not result.success and result.skipped_reason:
            self._log_event(
                "replacement_skipped",
                reason=result.skipped_reason,
                fill_side=fill_side,
                fill_px=fill_px,
            )

    async def _submit_level(self, lvl: GridLevel, mode: str = "refill") -> None:
        """Submit a single order level using ExecutionGateway."""
        # ARCHITECTURE: ExecutionGateway is required
        if not self.execution_gateway:
            raise RuntimeError("ExecutionGateway not initialized. Call setup_test_services() for tests.")
        
        # Quantize price before submission
        px = quantize(lvl.px, self.tick_sz, lvl.side, self.px_decimals)
        lvl.px = px
        
        # Sync flatten mode
        self.execution_gateway.flatten_mode = self._flatten_mode
        
        result = await self.execution_gateway.submit_level(
            level=lvl,
            mode=mode,
            position=self.position,
        )
        
        # Register the order if successful
        if result.success and result.has_ids:
            self._register_order(lvl, result.cloid, result.oid)

    async def _submit_levels_batch(self, levels: List[GridLevel], mode: str = "grid") -> None:
        """Submit multiple order levels using ExecutionGateway."""
        # Skip if nothing to place
        if not levels:
            return
            
        # ARCHITECTURE: ExecutionGateway is required
        if not self.execution_gateway:
            raise RuntimeError("ExecutionGateway not initialized. Call setup_test_services() for tests.")
        
        # Quantize prices before submission
        for lvl in levels:
            px = quantize(lvl.px, self.tick_sz, lvl.side, self.px_decimals)
            lvl.px = px
        
        # Sync flatten mode
        self.execution_gateway.flatten_mode = self._flatten_mode
        
        results = await self.execution_gateway.submit_levels_batch(
            levels=levels,
            mode=mode,
            position=self.position,
        )
        
        # Register successful orders
        success_count = 0
        error_count = 0
        for lvl, result in zip(levels, results):
            if result.success and result.has_ids:
                self._register_order(lvl, result.cloid, result.oid)
                success_count += 1
            elif not result.success:
                error_count += 1
        
        # Handle batch failures
        if error_count > 0:
            self._log_event("batch_submit_errors", errors=error_count, total=len(levels))
            error_rate = error_count / len(levels) if levels else 0
            if error_rate >= 0.5:
                self._log_event("batch_majority_failed", error_rate=error_rate,
                               note="triggering_full_reconcile")
                self.rebuild_needed = True
                self._needs_full_reconcile = True
            elif error_count >= 3:
                self._log_event("batch_multiple_failures", count=error_count,
                               note="triggering_rebuild")
                self.rebuild_needed = True
        elif success_count > 0:
            self._reset_api_errors()

    async def _cancel_record(self, rec: ActiveOrder, reason: str = "unspecified") -> bool:
        """
        Cancel an order record with C-6 FIX verification.
        
        Delegates to ExecutionGateway which handles:
        - Normal cancel flow
        - C-6 FIX: Verification if order still exists on exchange after cancel failure
        - State machine updates
        - Shadow ledger cleanup
        
        Returns True if order is confirmed gone, False if still active.
        """
        # ARCHITECTURE: Delegate to ExecutionGateway
        if not self.execution_gateway:
            raise RuntimeError("ExecutionGateway not initialized. Call setup_test_services() for tests.")
        
        result = await self.execution_gateway.cancel_order_with_verification(
            cloid=rec.cloid,
            oid=rec.oid,
            reason=reason,
        )
        
        if result.order_gone:
            # Order confirmed removed (either by cancel or C-6 FIX verification)
            self._unindex(rec)
            return True
        else:
            # Order may still be resting - mark for reconciliation
            self.rebuild_needed = True
            return False

    async def _cancel_all(self, reason: str = "") -> None:
        if not self.router:
            return
        self._log_event("cancel_all_start", open_orders=self._open_count(), reason=reason or None)
        try:
            await self.router.cancel_all()
            self._log_event("cancel_all_complete", reason=reason or None)
        except Exception as exc:
            self._log_event("cancel_all_error", err=str(exc), reason=reason or None)
            # best effort fallback
            # Optimization-3: Iterate directly since we don't modify dict in this loop
            # Note: _clear_orders() in finally handles all cleanup
            for rec in self.orders_by_price.values():
                try:
                    await self.router.safe_cancel(cloid=rec.cloid, oid=rec.oid)
                except Exception as exc_inner:
                    self._log_event(
                        "cancel_error",
                        err=str(exc_inner),
                        cloid=rec.cloid,
                        oid=rec.oid,
                        reason="cancel_all_fallback",
                        cloid_type=str(type(rec.cloid)),
                        oid_type=str(type(rec.oid)),
                    )
        finally:
            self._clear_orders()

    async def _cancel_exposure_side(self, side: str, reason: str = "flatten") -> None:
        """
        Cancel only the side that would increase exposure (e.g., buys when long).
        """
        # Optimization-3: Iterate directly - list comprehension creates the needed copy
        to_cancel = [rec for rec in self.orders_by_price.values() if rec.level.side == side]
        for rec in to_cancel:
            try:
                await self._cancel_record(rec, reason=reason)
            except Exception as exc:
                self._log_event("cancel_error", err=str(exc), side=side, reason=reason)
        # clear indexes for canceled records
        for rec in to_cancel:
            self._unindex(rec)

    async def _log_pnl(self) -> None:
        session_pnl = self.state_manager.session_realized_pnl
        alltime_pnl = self.state_manager.alltime_realized_pnl
        session_duration = self.state_manager.session_duration_sec
        
        await self.metrics.set_gauge(f"position{{coin='{self.coin}'}}", self.position)
        await self.metrics.set_gauge(f"realized_pnl{{coin='{self.coin}'}}", session_pnl)
        await self.metrics.set_gauge(f"alltime_pnl{{coin='{self.coin}'}}", alltime_pnl)
        self._log_event(
            "pnl",
            pos=self.position,
            session_pnl=session_pnl,
            alltime_pnl=alltime_pnl,
            session_duration_sec=round(session_duration, 1),
        )
        await self._push_status()

    async def _persist(self) -> None:
        # Critical-12: Persist state atomically with fill log
        await self.state_manager.persist()
        # Also set grid center if strategy is available
        if self.strategy:
            await self.state_manager.set_grid_center(self.strategy.grid_center)

    async def _load_state(self) -> None:
        # Use StateManager when enabled for single source of truth
        if self._use_state_manager and self.state_manager:
            snapshot = await self.state_manager.load(now_ms())
            self._alltime_realized_pnl = snapshot.alltime_realized_pnl
            self._session_realized_pnl = 0.0  # Always starts at 0
            self._session_start_time = snapshot.session_start_time
            self.last_fill_time_ms = snapshot.last_fill_time_ms
            gc = snapshot.grid_center
            if self.strategy and gc:
                self.strategy.grid_center = gc
            self._log_event(
                "session_start",
                alltime_pnl=self._alltime_realized_pnl,
                session_pnl=0.0,
                session_start=self._session_start_time,
                via="state_manager",
            )
        else:
            # Legacy path
            data = await self.state_store.load()
            # Load all-time PnL from state (never reset)
            self._alltime_realized_pnl = float(data.get("realized_pnl", 0.0))
            # Session PnL starts at 0 each run
            self._session_realized_pnl = 0.0
            self._session_start_time = time.time()
            # For backward compatibility, realized_pnl property returns session PnL
            self.realized_pnl = 0.0
            saved_fill = int(data.get("last_fill_time_ms", 0))
            # Prevent stale fill replay: use max of (saved timestamp, now - rescan window)
            # This ensures fills during restart are not double-counted (Bug #4)
            self.last_fill_time_ms = max(saved_fill, now_ms() - self.cfg.fill_rescan_ms)
            gc = data.get("grid_center")
            if self.strategy:
                self.strategy.grid_center = gc
            self._log_event(
                "session_start",
                alltime_pnl=self._alltime_realized_pnl,
                session_pnl=0.0,
                session_start=self._session_start_time,
            )

    async def _push_status(self) -> None:
        try:
            await self.metrics.set_gauge(f"risk_halt{{coin='{self.coin}'}}", 1.0 if self.risk.is_halted() else 0.0)
            await self.metrics.set_gauge(f"data_age_seconds{{coin='{self.coin}'}}", self.market.data_age() if self.market else 0.0)
            await self.status_board.update(
                self.coin,
                {
                    "position": self.position,
                    "realized_pnl": self.realized_pnl,
                    "open_orders": self._open_count(),
                    "grid_center": self.strategy.grid_center if self.strategy else None,
                    "last_fill_ms": self.last_fill_time_ms,
                    "spacing": self.strategy.compute_spacing(self.strategy.grid_center, position=self.position, grid_center=self.strategy.grid_center) if self.strategy and self.strategy.grid_center else None,
                    "skew_ratio": None if not self.strategy or not self.strategy.grid_center else abs(self.position) / max((self.effective_investment_usd * self.cfg.leverage) / max(self.strategy.grid_center, 1e-9), 1e-9),
                    "risk_halted": self.risk.halt_reason if self.risk.is_halted() else None,
                    "api_error_streak": self.api_error_streak,
                },
            )
        except Exception:
            # Update rich metrics for operational state (best-effort)
            try:
                # fill log file size and entry count
                try:
                    p = self.fill_log.path
                    if p.exists():
                        try:
                            size = p.stat().st_size
                        except Exception:
                            size = 0
                        # count lines (best-effort)
                        try:
                            with p.open("r", encoding="utf-8") as fh:
                                entries = sum(1 for _ in fh)
                        except Exception:
                            entries = 0
                        try:
                            self.rich_metrics.fill_log_size_bytes.labels(coin=self.coin).set(size)
                        except Exception:
                            pass
                        try:
                            self.rich_metrics.fill_log_entries.labels(coin=self.coin).set(entries)
                        except Exception:
                            pass
                except Exception:
                    pass
                # api error streak
                try:
                    self.rich_metrics.api_error_streak.labels(coin=self.coin).set(self.api_error_streak)
                except Exception:
                    pass
            except Exception:
                pass
            pass

    async def _maybe_flatten(self, mid: float) -> None:
        if abs(self.position) <= 0:
            return
        target_notional = self.effective_investment_usd * self.cfg.leverage
        target_pos = target_notional / max(mid, 1e-9)
        ratio = abs(self.position) / max(target_pos, 1e-9)
        if ratio < self.cfg.skew_hard:
            return
        side = "sell" if self.position > 0 else "buy"
        px = mid * (0.999 if side == "sell" else 1.001)
        sz = min(abs(self.position), target_pos * 0.25)
        sz = math.floor(sz * (10 ** self.sz_decimals)) / (10 ** self.sz_decimals)
        lvl = GridLevel(side, px, sz)
        if not self.risk.allow_order(lvl.side, lvl.sz, lvl.px):
            self._log_event("skip_flatten_risk", side=lvl.side, px=px, sz=lvl.sz, position=self.position)
            return
        await self._submit_level(lvl, mode="flatten")

    def _log_order_intent(self) -> bool:
        """Sample order_intent logs to reduce noise."""
        rate = max(0.0, min(1.0, self.cfg.log_order_intent_sample))
        if rate <= 0.0:
            return False
        if rate >= 1.0:
            return True
        return secrets.randbelow(10_000) < int(rate * 10_000)

    def _open_count(self) -> int:
        """Get count of open orders - delegates to OrderManager."""
        return self.order_manager.open_count()

    def _on_circuit_reset(self) -> None:
        """Callback when circuit breaker resets after cooldown."""
        self.rebuild_needed = True

    @property
    def api_error_streak(self) -> int:
        """Legacy property - delegates to circuit breaker."""
        return self.circuit_breaker.error_streak

    def _reset_api_errors(self) -> None:
        """Reset API errors - delegates to circuit breaker."""
        self.circuit_breaker.record_success()

    async def _handle_api_error(self, where: str, exc: Exception) -> None:
        """Handle API error - delegates to circuit breaker."""
        tripped = self.circuit_breaker.record_error(where, exc)
        if tripped:
            try:
                await self._cancel_all(reason="api_circuit_break")
            except Exception as exc_cancel:
                self._log_event("api_circuit_cancel_error", err=str(exc_cancel))

    def _price_key(self, lvl: GridLevel) -> str:
        """Generate price key - delegates to OrderManager."""
        return self.order_manager.price_key(lvl)

    def _unindex(self, rec: ActiveOrder) -> None:
        """Remove order from indices - delegates to OrderManager."""
        self.order_manager.unindex(rec)

    def _register_order(self, lvl: GridLevel, cloid: Optional[str], oid: Optional[int]) -> ActiveOrder:
        """Register order - delegates to OrderManager."""
        return self.order_manager.register(lvl, cloid, oid)

    def _pop_order_by_ids(self, cloid: Optional[str], oid: Optional[int]) -> Optional[ActiveOrder]:
        """Pop order by IDs - delegates to OrderManager."""
        return self.order_manager.pop_by_ids(cloid, oid)

    def _handle_partial_fill(self, cloid: Optional[str], oid: Optional[int], fill_sz: float) -> Optional[ActiveOrder]:
        """Critical-5: Handle partial fills - delegates to OrderManager."""
        return self.order_manager.handle_partial_fill(cloid, oid, fill_sz)

    def _clear_orders(self) -> None:
        """Clear all orders - delegates to OrderManager."""
        self.order_manager.clear()

    def _log_event(self, event: str, **data: Any) -> None:
        """Log event - delegates to BotLogger."""
        self._logger.log(event, **data)
