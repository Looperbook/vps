"""
Core grid bot wiring components.
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import random
import secrets
import time
from typing import Any, Dict, List, Optional

from dataclasses import dataclass

from hyperliquid.exchange import Exchange
from hyperliquid.info import Info

from src.config import Settings
from src.logging_cfg import build_logger
from src.market_data import MarketData
from src.metrics import Metrics
from src.metrics_rich import RichMetrics
from src.bot_context import BotContext
from src.order_router import OrderRequest, OrderRouter
from src.order_manager import OrderManager, ActiveOrder
from src.circuit_breaker import CircuitBreaker, CircuitBreakerConfig
from src.fill_deduplicator import FillDeduplicator
from src.position_tracker import PositionTracker
from src.grid_calculator import GridCalculator, GridBuildResult, GridDiff
from src.order_state_machine import OrderStateMachine, OrderState
from src.risk import RiskEngine
from src.state_atomic import AtomicStateStore
from src.fill_log import FillLog, BatchedFillLog
from src.strategy import GridLevel, GridStrategy
from src.strategy_factory import StrategyFactory
from src.utils import now_ms, quantize, tick_to_decimals
from src.status import StatusBoard
# S-1, S-2, S-3 Structural Improvements
from src.shadow_ledger import ShadowLedger
from src.event_bus import EventBus, EventType, Event
from src.order_sync import OrderSync

log = build_logger("gridbot")


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

    # Struct-4: Backward-compatible properties delegating to PositionTracker
    @property
    def position(self) -> float:
        """Current position - delegates to PositionTracker."""
        return self.position_tracker.position
    
    @position.setter
    def position(self, value: float) -> None:
        """Set position - delegates to PositionTracker."""
        self.position_tracker.position = value
    
    @property
    def realized_pnl(self) -> float:
        """Realized PnL - delegates to PositionTracker."""
        return self.position_tracker.realized_pnl
    
    @realized_pnl.setter
    def realized_pnl(self, value: float) -> None:
        """Set realized PnL - delegates to PositionTracker."""
        self.position_tracker.realized_pnl = value
    
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
        """
        Cancel orders that have been stuck for too long.
        
        Tracks when each stuck order was first seen and cancels it if it's been
        stuck longer than the threshold.
        
        Args:
            stuck: List of OrderStateRecord for stuck orders
            
        Returns:
            Number of orders cancelled
        """
        now = time.time()
        cancelled_count = 0
        current_cloids = {r.cloid for r in stuck if r.cloid}
        
        # Clean up tracking for orders no longer stuck
        to_remove = [cloid for cloid in self._stuck_order_first_seen if cloid not in current_cloids]
        for cloid in to_remove:
            del self._stuck_order_first_seen[cloid]
        
        # Check each stuck order
        for record in stuck:
            if not record.cloid:
                continue
                
            # Track first time we saw this order as stuck
            if record.cloid not in self._stuck_order_first_seen:
                self._stuck_order_first_seen[record.cloid] = now
                continue
            
            # Check if it's been stuck too long
            first_seen = self._stuck_order_first_seen[record.cloid]
            stuck_duration = now - first_seen
            
            if stuck_duration >= self._stuck_order_cancel_threshold_sec:
                try:
                    self._log_event("stuck_order_auto_cancel", 
                                   cloid=record.cloid, 
                                   oid=record.oid,
                                   stuck_duration_sec=round(stuck_duration, 1),
                                   side=record.side,
                                   price=record.price,
                                   qty=record.original_qty)
                    
                    # Cancel via order router
                    await self.router.safe_cancel(cloid=record.cloid, oid=record.oid)
                    
                    # Transition to EXPIRED in state machine
                    self.order_state_machine.transition(
                        cloid=record.cloid,
                        to_state=OrderState.EXPIRED,
                        reason="stuck_order_auto_cancel"
                    )
                    
                    # Remove from tracking
                    del self._stuck_order_first_seen[record.cloid]
                    cancelled_count += 1
                    
                except Exception as exc:
                    self._log_event("stuck_order_cancel_error",
                                   cloid=record.cloid,
                                   err=str(exc))
        
        return cancelled_count

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
        await self._load_state()
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
        await self._reconcile_position()
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
        self._log_event("init", mid=mid)
        try:
            self.rich_metrics.bot_started.labels(coin=self.coin).inc()
        except Exception:
            pass

    async def run(self) -> None:
        # Optimization-2: Start batched fill log background flush
        if hasattr(self.fill_log, "start"):
            await self.fill_log.start()
        # S-2: Start event bus in background
        event_bus_task = asyncio.create_task(self.event_bus.start())
        # Publish bot started event
        await self.event_bus.emit(EventType.BOT_STARTED, source="bot", coin=self.coin)
        fill_task = asyncio.create_task(self._fill_worker())
        try:
            while self.running:
                self._log_event(
                    "cycle_start",
                    position=self.position,
                    realized_pnl=self.realized_pnl,
                    open_orders=self._open_count(),
                    grid_center=self.strategy.grid_center if self.strategy else None,
                    last_fill_ms=self.last_fill_time_ms,
                )
                # HARDENED: Data health guard to pause trading when feeds are stale to avoid blind quoting.
                # Capture data_age once to ensure consistent checking (Bug #13)
                data_age = self.market.data_age() if self.market else 0.0
                is_halted = self.market.is_halted() if self.market else False
                if self.market and (is_halted or data_age > self.cfg.data_halt_sec):
                    if not self.data_paused:
                        self.data_paused = True
                        self._log_event("data_stale_halt", age_sec=data_age, open_orders=self._open_count())
                        try:
                            await self._cancel_all(reason="data_stale")
                        except Exception as exc:
                            self._log_event("data_stale_cancel_error", err=str(exc))
                    await asyncio.sleep(min(self.cfg.loop_interval, 2.0))
                    continue
                if self.risk.is_halted() and not self.risk_paused:
                    self.risk_paused = True
                    self._log_event("risk_halt", reason=self.risk.halt_reason, equity=self.risk.state.equity, hwm=self.risk.state.equity_hwm, daily_pnl=self.risk.state.daily_pnl, funding=self.risk.state.funding)
                    try:
                        await self._cancel_all(reason="risk_halt")
                    except Exception as exc:
                        self._log_event("risk_halt_cancel_error", err=str(exc))
                    await self._push_status()
                    self.stop()
                    continue
                # Struct-2: Circuit breaker check using centralized module
                if self.circuit_breaker.is_tripped:
                    await asyncio.sleep(self.cfg.loop_interval)
                    continue
                if self.data_paused and data_age <= self.cfg.ws_stale_after:
                    self.data_paused = False
                    self._log_event("data_resume", age_sec=data_age, open_orders=self._open_count())
                    mid = await self.market.mid_price()
                    await self._build_and_place_grid(mid)
                    await asyncio.sleep(self.cfg.loop_interval)
                    continue
                if os.path.exists("STOP.txt"):
                    self.stop()
                # Skew handling: enter flatten mode to reduce inventory; only quote reducing side.
                skew_ratio = None
                if self.strategy and self.strategy.grid_center:
                    target_pos = (self.effective_investment_usd * self.cfg.leverage) / max(self.strategy.grid_center, 1e-9)
                    if target_pos > 0:
                        skew_ratio = abs(self.position) / target_pos
                disallowed_side = "buy" if self.position > 0 else "sell" if self.position < 0 else None
                if self.cfg.max_skew_ratio > 0 and skew_ratio is not None and skew_ratio > self.cfg.max_skew_ratio and disallowed_side:
                    if not self._flatten_mode:
                        self._flatten_mode = True
                        self._log_event("flatten_mode_enter", skew=skew_ratio, disallowed_side=disallowed_side)
                        try:
                            await self._cancel_exposure_side(disallowed_side, reason="skew_flatten")
                            self.rebuild_needed = True
                        except Exception as exc:
                            self._log_event("flatten_cancel_error", err=str(exc))
                    # stay in flatten mode until skew drops sufficiently
                elif self._flatten_mode and skew_ratio is not None and skew_ratio <= self.cfg.max_skew_ratio * 0.8:
                    self._flatten_mode = False
                    self._log_event("flatten_mode_exit", skew=skew_ratio)
                    self.rebuild_needed = True
                now_t = time.time()
                
                # Hardening: WS degradation mode - poll REST more frequently if WS is halted
                ws_degraded = self.market._halted if hasattr(self.market, '_halted') else False
                effective_rest_fill_interval = self.cfg.rest_fill_interval / 4 if ws_degraded else self.cfg.rest_fill_interval
                try:
                    self.rich_metrics.ws_degraded_mode.labels(coin=self.coin).set(1.0 if ws_degraded else 0.0)
                except Exception:
                    pass
                
                if now_t - self.last_rest_fill_poll > effective_rest_fill_interval:
                    try:
                        await self._poll_rest_fills()
                        self._reset_api_errors()
                    except Exception as exc:
                        await self._handle_api_error("rest_fills", exc)
                    self.last_rest_fill_poll = now_t
                if now_t - self.last_resync > self.cfg.rest_audit_interval:
                    try:
                        await self._reconcile_position()
                        self._reset_api_errors()
                    except Exception as exc:
                        await self._handle_api_error("reconcile_position", exc)
                    self.last_resync = now_t
                # PRODUCTION EXCELLENCE NOTE:
                # Added periodic order reconciliation to repair drift between local state and exchange.
                if now_t - self.last_order_resync > self.cfg.rest_audit_interval:
                    try:
                        await self._reconcile_orders()
                        self._reset_api_errors()
                        # Struct-6: Prune old terminal orders from state machine
                        self.order_state_machine.prune_terminal(max_age_ms=300_000)  # 5 minutes
                    except Exception as exc:
                        await self._handle_api_error("reconcile_orders", exc)
                    self.last_order_resync = now_t
                
                # Hardening: Check for stuck orders periodically (every 30 seconds)
                if now_t - self._last_stuck_order_check > 30.0:
                    stuck = self._check_stuck_orders()
                    if stuck:
                        self._log_event("stuck_orders_detected", count=len(stuck), 
                                       cloids=[r.cloid for r in stuck[:5]])  # Log first 5
                        # Auto-cancel orders stuck too long
                        cancelled = await self._cancel_stuck_orders(stuck)
                        if cancelled > 0:
                            self._log_event("stuck_orders_auto_cancelled", count=cancelled)
                    self._last_stuck_order_check = now_t
                
                if now_t - self.last_pnl_log > self.cfg.pnl_log_interval:
                    await self._log_pnl()
                    self.last_pnl_log = now_t
                try:
                    mid = await self.market.mid_price()
                except Exception as exc:
                    self._log_event("mid_price_error", err=str(exc))
                    await asyncio.sleep(min(self.cfg.loop_interval, 2.0))
                    continue
                if self.rebuild_needed:
                    await self._build_and_place_grid(mid)
                    self.rebuild_needed = False
                elif self.strategy and self.strategy.grid_center and abs(mid - self.strategy.grid_center) / self.strategy.grid_center > self.cfg.trailing_pct:
                    self.rebuild_needed = True
                # mid logging throttled in MarketData; skip per-loop mid spam here
                await asyncio.sleep(self.cfg.loop_interval)
        except asyncio.CancelledError:
            pass
        finally:
            fill_task.cancel()
            await asyncio.gather(fill_task, return_exceptions=True)
        # S-2: Publish bot stopped event and stop event bus
        await self.event_bus.emit(EventType.BOT_STOPPED, source="bot", coin=self.coin)
        self.event_bus.stop()
        await self.event_bus.drain(timeout=2.0)
        await self._cancel_all(reason="shutdown")
        await self._persist()
        if self.router:
            await self.router.stop()
        if self.market:
            await self.market.stop()
        # Optimization-2: Flush and stop batched fill log
        if hasattr(self.fill_log, "stop"):
            await self.fill_log.stop()
        await self._push_status()

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

        # C-5 FIX: On replay, only clear order state - position was already persisted
        # reflecting these fills. Updating position again would cause double-counting.
        if replay:
            # Just unindex the order if it exists (so we don't try to cancel filled orders)
            rec = self.order_manager.pop_by_ids(str(cloid) if cloid else None, self._to_int_safe(oid))
            if rec:
                self._log_event("fill_replay_order_cleared", cloid=cloid, oid=oid, side=side, px=px, sz=sz)
                # Update state machine to mark order as filled
                self.order_state_machine.fill(
                    cloid=str(cloid) if cloid else None,
                    oid=self._to_int_safe(oid),
                    fill_qty=sz,
                    is_complete=True,
                )
            else:
                self._log_event("fill_replay_no_order", cloid=cloid, oid=oid, side=side, px=px, sz=sz)
            # Update last_fill_time_ms to track replay progress
            self.last_fill_time_ms = max(self.last_fill_time_ms, ts_ms)
            return

        # C-1 FIX: If fill arrives during grid rebuild, mark rebuild_needed
        # so the next iteration uses updated position. The fill still gets processed
        # normally, but the grid will be rebuilt with fresh data after completion.
        if self._rebuild_in_progress:
            self.rebuild_needed = True
            self._log_event("fill_during_rebuild", side=side, px=px, sz=sz, 
                           note="marking_rebuild_needed")

        # Persist live fills to the event log to prevent double-counting on restart
        if not replay:
            try:
                await self.fill_log.append({"side": side, "px": px, "sz": sz, "oid": oid, "cloid": cloid, "time": ts_ms})
            except Exception:
                # best-effort; continue processing even if logging fails
                pass

        pnl = 0.0
        matched = False
        # Allow out-of-order fills but log them (Critical-10)
        if ts_ms < self.last_fill_time_ms:
            self._log_event("fill_out_of_order", side=side, px=px, sz=sz, ts_ms=ts_ms, last_fill_ms=self.last_fill_time_ms)
            # Continue processing - it may be a reconciliation fill
        
        async with self.position_lock:
            # Critical-5: Handle partial fills - use _handle_partial_fill instead of _pop_order_by_ids
            rec = self._handle_partial_fill(str(cloid) if cloid else None, self._to_int_safe(oid), sz)
            if not rec:
                self._log_event(
                    "fill_unmatched",
                    side=side,
                    px=px,
                    sz=sz,
                    oid=oid,
                    cloid=cloid,
                    position=self.position,
                )
                # Harden: reconcile open orders and position when we see an unexpected fill
                try:
                    await self._reconcile_orders()
                except Exception as exc:
                    self._log_event("fill_unmatched_reconcile_error", err=str(exc))
                await self._reconcile_position()
                await self._push_status()
                return
            
            # Struct-6: Update order state machine with fill
            cloid_str = str(cloid) if cloid else None
            # C-2 FIX: Use absolute tolerance instead of percentage for is_fully_filled
            remaining = rec.original_qty - rec.filled_qty
            dust_tolerance = max(sz * 0.01, 1e-9)  # 1% of fill or epsilon
            is_fully_filled = rec.original_qty <= 0 or remaining <= dust_tolerance
            self.order_state_machine.fill(
                cloid=cloid_str,
                oid=self._to_int_safe(oid),
                fill_qty=sz,
                is_complete=is_fully_filled,
            )
            
            # Only trust fills that match known resting orders
            if side.startswith("b"):
                self.position += sz
            else:
                self.position -= sz
            self.risk.set_position(self.position)
            
            # S-2: Publish position changed event
            try:
                await self.event_bus.emit(
                    EventType.POSITION_CHANGED,
                    source="fill_handler",
                    position=self.position,
                    delta=sz if side.startswith("b") else -sz,
                    side=side,
                )
            except Exception:
                pass  # Events must not break execution
            
            # S-1: Update shadow ledger with fill
            await self.shadow_ledger.apply_fill(
                side=side,
                size=sz,
                cloid=cloid_str,
                oid=self._to_int_safe(oid),
                timestamp_ms=ts_ms,
            )
            lvl = rec.level
            if lvl.side == "buy":
                pnl = (px - lvl.px) * sz
            else:
                pnl = (lvl.px - px) * sz
            # Track both session and all-time PnL
            self._session_realized_pnl += pnl
            self._alltime_realized_pnl += pnl
            self.realized_pnl = self._session_realized_pnl  # Keep property in sync
            matched = True
        
        # Hardening: PnL sanity check - alert on unusually large single-fill PnL
        if matched and abs(pnl) > self._max_single_fill_pnl:
            self._log_event("unusual_pnl_fill", pnl=pnl, side=side, px=px, sz=sz, threshold=self._max_single_fill_pnl)
            try:
                self.rich_metrics.unusual_pnl_fills.labels(coin=self.coin).inc()
            except Exception:
                pass
        
        # Update Phase 3 Prometheus metrics
        try:
            self.rich_metrics.fills_total.labels(coin=self.coin, side=side[:1]).inc()
            self.rich_metrics.position.labels(coin=self.coin).set(self.position)
            self.rich_metrics.realized_pnl.labels(coin=self.coin).set(self._session_realized_pnl)
        except Exception:
            # best-effort: metrics must not break execution
            pass
        self.last_fill_time_ms = max(self.last_fill_time_ms, ts_ms)
        # Structured log for tracing and correlation
        try:
            self.ctx.log(
                "fill",
                side=side,
                px=px,
                sz=sz,
                oid=oid,
                cloid=cloid,
                matched=matched,
                pnl_add=pnl,
                position=self.position,
                session_pnl=self._session_realized_pnl,
                alltime_pnl=self._alltime_realized_pnl,
            )
        except Exception:
            pass
        # legacy/simple event log
        self._log_event(
            "fill",
            side=side,
            px=px,
            sz=sz,
            oid=oid,
            cloid=cloid,
            matched=matched,
            pnl_add=pnl,
            position=self.position,
            session_pnl=self._session_realized_pnl,
            alltime_pnl=self._alltime_realized_pnl,
        )
        # S-2: Publish fill event
        try:
            await self.event_bus.emit(
                EventType.FILL_RECEIVED if not matched else EventType.FILL_PROCESSED,
                source="fill_handler",
                side=side,
                price=px,
                size=sz,
                oid=oid,
                cloid=cloid,
                matched=matched,
                pnl=pnl,
                position=self.position,
            )
        except Exception:
            pass  # Events must not break execution
        if matched and self.strategy:
            self.strategy.on_price(px)
            # M-5 FIX: Flush fill log before any other processing for crash safety
            # This ensures the fill is durably recorded before we update other state
            if hasattr(self.fill_log, 'flush_sync'):
                try:
                    await self.fill_log.flush_sync()
                except Exception:
                    pass  # Best effort - don't block fill processing
            # Immediately recycle only the filled level (no full-grid rebuild).
            await self._replace_after_fill(side, px, sz)
            await self._persist()
            await self._push_status()
            # update unrealized skew/guards using latest mid if available
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
        def _call() -> Dict[str, Any]:
            if self.async_info:
                return {}  # placeholder; async path used below
            return self.info.meta(dex=self.cfg.dex)

        builder_asset = self.cfg.dex.lower() == "xyz" or self.coin.startswith("xyz:")
        if self.async_info and not builder_asset:
            try:
                meta = await self.async_info.meta(self.cfg.dex)
            except Exception as exc:
                self._log_event("meta_async_error", err=str(exc))
                meta = await self._call_with_retry(_call, "meta")
        else:
            meta = await self._call_with_retry(_call, "meta")
        universe = meta.get("universe", [])

        def match(name: str) -> bool:
            if name == self.coin:
                return True
            short = self.coin.split(":")[-1]
            return name == short or name == f"{short}-PERP"

        asset = next((a for a in universe if match(a.get("name", ""))), None)
        tick_candidates: List[float] = []
        tick_primary: Optional[float] = None
        builder_asset = self.cfg.dex.lower() == "xyz" or self.coin.startswith("xyz:")
        if asset:
            self.sz_decimals = int(asset.get("szDecimals", self.sz_decimals))
            try:
                tick_val = float(asset.get("tickSz", 0.0))
                if tick_val > 0:
                    tick_primary = tick_val  # trust on-chain metadata first
            except Exception:
                tick_primary = None

        # Try CCXT market metadata first (covers main HL perps with precise increments).
        if not builder_asset:
            try:
                import hyperliquid.ccxt.hyperliquid as ccxt_hl

                cc = ccxt_hl.hyperliquid()
                markets = cc.load_markets()
                short = self.coin.split(":")[-1]
                symbols_to_try = [f"{short}/USDC:USDC", short, self.coin]
                for sym in symbols_to_try:
                    if sym in markets:
                        m = markets[sym]
                        price_inc = m.get("precision", {}).get("price")
                        amount_inc = m.get("precision", {}).get("amount")
                        if price_inc and not tick_primary:
                            tick_primary = float(price_inc)
                        if amount_inc and amount_inc < 1:
                            try:
                                self.sz_decimals = max(self.sz_decimals, int(round(-math.log10(amount_inc))))
                            except Exception:
                                pass
                        break
            except Exception:
                pass

        # Fallback: infer from asset ctx bid/ask spacing; skip for builder-perp since metaAndAssetCtxs lacks builder data.
        if not builder_asset:
            try:
                ctxs = await asyncio.get_running_loop().run_in_executor(None, self.info.meta_and_asset_ctxs)
                asset_ctxs = ctxs[1] if isinstance(ctxs, list) and len(ctxs) > 1 else []
                for idx, ctx in enumerate(asset_ctxs):
                    coin_name = universe[idx].get("name") if idx < len(universe) else None
                    if coin_name and match(coin_name):
                        impact = ctx.get("impactPxs") or []
                        if isinstance(impact, list) and len(impact) == 2:
                            try:
                                diff = abs(float(impact[1]) - float(impact[0]))
                                if diff > 0 and not tick_primary:
                                    # Normalize to a reasonable exchange tick (avoid overfitting to spread).
                                    for nice in (1.0, 0.5, 0.25, 0.1, 0.05, 0.01, 0.001, 0.0001):
                                        if diff >= nice:
                                            tick_candidates.append(nice)
                                            break
                                    else:
                                        tick_candidates.append(diff)
                            except Exception:
                                pass
                        break
            except Exception:
                pass

        # Selection priority: explicit tick from metadata, then ccxt, else best-effort fallback.
        if tick_primary and tick_primary > 0:
            self.tick_sz = tick_primary
        elif tick_candidates:
            tick_candidates = sorted([t for t in tick_candidates if t > 0])
            self.tick_sz = tick_candidates[0]
        else:
            self.tick_sz = 0.0
        # Builder perp heuristic: builder assets omit tickSz; use coarse ticks from midprice so we don't over-quantize.
        try:
            if self.async_info and not builder_asset:
                mids = await self.async_info.all_mids(self.cfg.dex)
            else:
                mids = self.info.all_mids(dex=self.cfg.dex)
            mid_val = float(mids.get(self.coin, 0.0))
        except Exception:
            mid_val = 0.0
        if builder_asset:
            if mid_val >= 1000:
                self.tick_sz = max(self.tick_sz, 1.0)
            elif mid_val >= 10:
                self.tick_sz = max(self.tick_sz, 0.01)
            elif mid_val > 0:
                self.tick_sz = max(self.tick_sz, 0.001)
            # If tick is still extremely small, drop to zero to avoid false invalid_price gating.
            if self.tick_sz < 1e-4:
                self.tick_sz = 0.0
        # Use exchange tick if provided; otherwise fall back to Hyperliquid rounding rule (max 6 - szDecimals decimals for perps).
        if self.tick_sz > 0:
            self.px_decimals = min(tick_to_decimals(self.tick_sz), 6)
        else:
            max_decimals = max(0, 6 - self.sz_decimals)
            if mid_val > 0:
                try:
                    sig_step = 10 ** (int(math.floor(math.log10(mid_val))) - 4)
                    max_decimals = min(max_decimals, tick_to_decimals(sig_step))
                except Exception:
                    pass
            self.px_decimals = min(6, max_decimals)
        # Struct-1: Update OrderManager with final tick/decimal info
        self.order_manager.update_tick_info(self.tick_sz, self.px_decimals)
        # Struct-5: Update GridCalculator with final tick/decimal info
        self.grid_calculator.tick_sz = self.tick_sz
        self.grid_calculator.px_decimals = self.px_decimals
        self.grid_calculator.sz_decimals = self.sz_decimals
        self._log_event("meta_loaded", tick_sz=self.tick_sz, px_decimals=self.px_decimals, sz_decimals=self.sz_decimals)

    async def _reconcile_position(self) -> None:
        builder_asset = self.cfg.dex.lower() == "xyz" or self.coin.startswith("xyz:")
        if self.async_info and not builder_asset:
            state = await self.async_info.user_state(self.account, self.cfg.dex)
        else:
            def _call() -> Dict[str, Any]:
                return self.info.user_state(self.account, dex=self.cfg.dex)
            state = await self._call_with_retry(_call, "user_state")
        self._log_event("reconcile_start")
        
        # Capture local position before fetching exchange position for drift detection
        local_position = self.position
        
        pos = 0.0
        margin_summary = state.get("marginSummary", {})
        equity = float(margin_summary.get("accountValue", 0.0))
        self.risk.update_equity(equity)
        daily_pnl = margin_summary.get("sessionPnl") or margin_summary.get("dailyPnl")
        if daily_pnl is not None:
            try:
                self.risk.set_daily_pnl(float(daily_pnl))
            except Exception:
                pass
        funding_val = margin_summary.get("fundingAccrued") or margin_summary.get("funding")
        if funding_val is not None:
            try:
                self.risk.set_funding(float(funding_val))
            except Exception:
                pass
        # update unrealized from entryPx if available
        mid_for_unreal = None
        try:
            mids = await asyncio.get_running_loop().run_in_executor(None, lambda: self.info.all_mids(dex=self.cfg.dex))
            mid_for_unreal = float(mids.get(self.coin, 0.0))
        except Exception:
            mid_for_unreal = None
        for ap in state.get("assetPositions", []):
            p = ap.get("position", {})
            if p.get("coin") == self.coin:
                pos = float(p.get("szi", 0.0))
                entry_px = float(p.get("entryPx", 0.0) or 0.0)
                if mid_for_unreal and entry_px:
                    self.risk.set_unrealized(pos * (mid_for_unreal - entry_px))
                break
        
        # Hardening: Detect position drift between local shadow ledger and exchange
        drift_amount = abs(pos - local_position)
        drift_pct = drift_amount / max(abs(local_position), abs(pos), 0.001)
        if drift_amount > 1e-6 and drift_pct > 0.01:  # >1% drift
            self._log_event(
                "position_drift_detected",
                local_position=local_position,
                exchange_position=pos,
                drift_amount=drift_amount,
                drift_pct=drift_pct,
            )
            try:
                self.rich_metrics.position_drift_detected.labels(coin=self.coin).inc()
                self.rich_metrics.position_drift_amount.labels(coin=self.coin).set(drift_amount)
            except Exception:
                pass
        
        self.position = pos
        self.risk.set_position(pos)
        
        # S-1: Reconcile shadow ledger with exchange position
        try:
            result = await self.shadow_ledger.reconcile_with_exchange(pos)
            if result.drift_detected:
                self._log_event(
                    "shadow_ledger_drift",
                    drift_amount=result.drift_amount,
                    drift_percent=result.drift_percent,
                    confirmed=result.confirmed_position,
                    local=result.local_position,
                    exchange=pos,
                )
                await self.event_bus.emit(
                    EventType.POSITION_DRIFT,
                    source="reconcile",
                    drift_amount=result.drift_amount,
                    drift_percent=result.drift_percent,
                )
        except Exception:
            pass  # Shadow ledger reconciliation must not break main flow
        
        self._log_event("reconcile_complete", position=self.position, equity=equity, daily_pnl=self.risk.state.daily_pnl, funding=self.risk.state.funding)
        # Update Phase 3 metrics (best-effort)
        try:
            self.rich_metrics.position.labels(coin=self.coin).set(self.position)
        except Exception:
            pass
        try:
            self.rich_metrics.daily_pnl.labels(coin=self.coin).set(self.risk.state.daily_pnl or 0.0)
        except Exception:
            pass
        try:
            # funding is cumulative; increment counter if value present
            if self.risk.state.funding:
                self.rich_metrics.funding_paid.labels(coin=self.coin).inc(self.risk.state.funding)
        except Exception:
            pass
        try:
            self.rich_metrics.risk_halted.labels(coin=self.coin).set(1.0 if self.risk.is_halted() else 0.0)
        except Exception:
            pass

    async def _reconcile_orders(self) -> None:
        """
        Compare local registry with exchange open orders and repair drift.
        Includes validation to prevent re-adding orders during active fill processing (Bug #7).
        """
        builder_asset = self.cfg.dex.lower() == "xyz" or self.coin.startswith("xyz:")
        if self.async_info and not builder_asset:
            remote = await self.async_info.frontend_open_orders(self.account, self.cfg.dex)
        else:
            def _call() -> list[dict[str, Any]]:
                try:
                    return self.info.frontend_open_orders(self.account, dex=self.cfg.dex)
                except Exception:
                    return []
            remote = await self._call_with_retry(_call, "frontend_open_orders")
        if isinstance(remote, dict):
            remote = remote.get("openOrders", [])
        if not isinstance(remote, list):
            remote = []
        remote_pairs = set()
        added = 0
        for o in remote:
            if o.get("coin") != self.coin:
                continue
            side_raw = o.get("side", "")
            side = "buy" if str(side_raw).lower().startswith("b") else "sell"
            try:
                px = float(o.get("limitPx", 0.0))
                sz = float(o.get("sz", 0.0))
            except Exception:
                continue
            oid = o.get("oid")
            cloid = o.get("cloid")
            remote_pairs.add((str(cloid) if cloid else None, int(oid) if oid is not None else None))
            price_key = f"{side}:{quantize(px, self.tick_sz, side, self.px_decimals):.{self.px_decimals}f}"
            if price_key in self.orders_by_price:
                continue
            lvl = GridLevel(side=side, px=px, sz=sz, oid=int(oid) if oid is not None else None, cloid=str(cloid) if cloid else None)
            self._register_order(lvl, lvl.cloid, lvl.oid)
            added += 1
        removed = 0
        # Optimization-3: Copy only keys instead of full values to reduce memory allocation
        for key in list(self.orders_by_price.keys()):
            rec = self.orders_by_price.get(key)
            if rec is None:
                continue
            id_pair = (rec.cloid, rec.oid)
            if id_pair not in remote_pairs:
                self._unindex(rec)
                removed += 1
        self._log_event("order_reconcile", added=added, removed=removed, remote=len(remote), local=self._open_count())

    async def _poll_rest_fills(self, force: bool = False) -> None:
        now_t = time.time()
        if not force and now_t - self.last_rest_fill_poll < self.cfg.rest_fill_interval:
            return
        # M-2 FIX: Use dedicated high watermark instead of last_fill_time_ms
        # This prevents skipping fills when last_fill_time_ms jumps during rapid fills
        buffer_ms = int(self.cfg.rest_fill_interval * 1000 * 2)  # 2x poll interval as buffer
        
        # Use HWM if set, otherwise fall back to last_fill_time_ms for first poll
        hwm_start = self._rest_poll_hwm_ms if self._rest_poll_hwm_ms > 0 else self.last_fill_time_ms
        start_ms = max(0, hwm_start - self.cfg.fill_rescan_ms)
        
        fills = await self.market.user_fills_since(self.account, start_ms)
        max_fill_ts = start_ms  # Track highest fill timestamp seen
        
        for f in sorted(fills, key=lambda x: int(x.get("time", 0))):
            if f.get("coin") != self.coin:
                continue
            fill_ts = int(f.get("time", 0))
            max_fill_ts = max(max_fill_ts, fill_ts)
            await self._handle_fill(f)
        
        # M-2 FIX: Update HWM to highest timestamp seen in this poll
        self._rest_poll_hwm_ms = max(self._rest_poll_hwm_ms, max_fill_ts)
        
        self._log_event("rest_fills_polled", count=len(fills), start_ms=start_ms, 
                       hwm_ms=self._rest_poll_hwm_ms, last_fill_ms=self.last_fill_time_ms)
        self.last_rest_fill_poll = now_t

    async def _build_and_place_grid(self, mid: float) -> None:
        if not self.strategy or not self.router:
            return
        if mid <= 0:
            self._log_event("grid_build_skip_mid", mid=mid)
            return
        
        # C-1 FIX: Acquire rebuild lock to serialize with fill processing
        # This prevents race condition where fills during rebuild use stale position
        async with self._rebuild_lock:
            self._rebuild_in_progress = True
            try:
                await self._build_and_place_grid_inner(mid)
            finally:
                self._rebuild_in_progress = False
    
    async def _build_and_place_grid_inner(self, mid: float) -> None:
        """Inner grid building logic, called while holding rebuild lock."""
        # Snapshot position atomically to prevent race during rebuild (Bug #6)
        async with self.position_lock:
            position_snapshot = self.position
        
        self._log_event("grid_rebuild_start", mid=mid, position=position_snapshot, open_orders=self._open_count())
        # Update strategy internal models and export metrics
        try:
            self.strategy.on_price(mid)
            # capture recent volatility/atr for metrics
            try:
                self.rich_metrics.volatility_estimate.labels(coin=self.coin).set(self.strategy.last_vol)
            except Exception:
                pass
            try:
                self.rich_metrics.atr_value.labels(coin=self.coin).set(self.strategy.last_atr)
            except Exception:
                pass
            # M-3 FIX: Adjust router coalescing based on volatility
            if self.router and hasattr(self.router, 'adjust_for_volatility'):
                self.router.adjust_for_volatility(self.strategy.last_vol)
        except Exception:
            pass
        
        # Struct-5: Use GridCalculator for order size computation
        base_size = self.grid_calculator.calculate_order_size(mid)
        
        # Struct-5: Use GridCalculator for level building with risk filtering
        build_result = self.grid_calculator.build_filtered_levels(
            strategy=self.strategy,
            risk=self.risk,
            mid=mid,
            position=position_snapshot,
            base_size=base_size,
            flatten_mode=self._flatten_mode
        )
        
        spacing = build_result.spacing
        levels = build_result.levels
        
        # Export grid metrics
        try:
            self.rich_metrics.grid_width_pct.labels(coin=self.coin).set(spacing * 100.0)
        except Exception:
            pass
        
        # Instrument strategy outputs
        try:
            # grid_center set by strategy.build_grid
            if self.strategy.grid_center:
                try:
                    self.rich_metrics.grid_center.labels(coin=self.coin).set(self.strategy.grid_center)
                except Exception:
                    pass
            try:
                bias = self.strategy.trend_bias(mid)
                self.rich_metrics.trend_bias.labels(coin=self.coin).set(bias)
            except Exception:
                pass
            try:
                target_notional = self.effective_investment_usd * self.cfg.leverage
                target_pos = target_notional / max(self.strategy.grid_center or mid, 1e-9)
                skew = abs(self.position) / max(target_pos, 1e-9) if target_pos > 0 else 0.0
                self.rich_metrics.skew_ratio.labels(coin=self.coin).set(skew)
            except Exception:
                pass
        except Exception:
            pass
        
        # Struct-5: Use GridCalculator for diff computation
        existing_keys = set(self.orders_by_price.keys())
        grid_diff = self.grid_calculator.compute_grid_diff(
            desired_levels=levels,
            existing_keys=existing_keys,
            orders_by_price=self.orders_by_price,
            reprice_tick_threshold=self.cfg.reprice_tick_threshold
        )
        
        to_cancel_keys = grid_diff.to_cancel_keys
        to_place = grid_diff.to_place
        
        # cancel only diffs
        for key in to_cancel_keys:
            rec = self.orders_by_price.get(key)
            if not rec:
                continue
            try:
                await self._cancel_record(rec, reason="grid_diff")
            except Exception as exc:
                self._log_event("cancel_diff_error", err=str(exc), key=key)
        
        # Critical-3: Re-check position before placing to detect drift during grid computation
        async with self.position_lock:
            current_pos = self.position
        position_drift = abs(current_pos - position_snapshot)
        if position_drift > 1e-9:
            self._log_event("grid_position_drift", snapshot=position_snapshot, current=current_pos, drift=position_drift)
            # If significant drift occurred, trigger another rebuild instead of placing stale orders
            if position_drift / max(abs(position_snapshot), 1e-9) > 0.1:  # >10% drift
                self._log_event("grid_rebuild_aborted_drift", drift_pct=position_drift / max(abs(position_snapshot), 1e-9))
                self.rebuild_needed = True
                return
        
        await self._submit_levels_batch(to_place, mode="grid")
        self._log_event(
            "grid_rebuild_complete",
            mid=mid,
            spacing=spacing,
            placed=len(to_place),
            canceled=len(to_cancel_keys),
            remaining_open=self._open_count(),
        )
        await self._push_status()
        await self._maybe_flatten(mid)

    async def _replace_after_fill(self, fill_side: str, fill_px: float, fill_sz: float) -> None:
        if not self.strategy or not self.router:
            return
        
        # Struct-5: Use GridCalculator for replacement level computation
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

    async def _submit_level(self, lvl: GridLevel, mode: str = "refill") -> None:
        if not self.router:
            return
        if self._flatten_mode:
            disallowed = "buy" if self.position > 0 else "sell"
            if lvl.side == disallowed:
                self._log_event("flatten_skip_level", side=lvl.side, px=lvl.px, sz=lvl.sz, mode=mode)
                return
        px = quantize(lvl.px, self.tick_sz, lvl.side, self.px_decimals)
        lvl.px = px
        
        # Pre-generate cloid for state machine tracking (if use_cloid enabled)
        pre_cloid = f"0x{secrets.token_hex(16)}" if self.cfg.use_cloid else None
        req = OrderRequest(is_buy=lvl.side == "buy", sz=lvl.sz, px=px, reduce_only=False, cloid=pre_cloid)
        
        # Struct-6: Create order in state machine (PENDING state)
        self.order_state_machine.create_order(
            cloid=pre_cloid,
            oid=None,
            side=lvl.side,
            price=px,
            qty=lvl.sz,
        )
        
        self._log_event("order_intent", side=lvl.side, px=px, sz=lvl.sz, mode=mode)
        resp = await self.router.submit(req)
        if isinstance(resp, dict):
            oid_val = self._to_int_safe(resp.get("oid"))
            cloid_val = resp.get("cloid") or req.cloid
            cloid_str = str(cloid_val) if cloid_val else None
            if oid_val is None:
                lvl.cloid = cloid_str
                lvl.oid = None
                if cloid_str:
                    self._register_order(lvl, cloid_str, None)
                    # Struct-6: Still acknowledge even without oid
                    self.order_state_machine.acknowledge(cloid=cloid_str)
                try:
                    self.rich_metrics.orders_submitted.labels(coin=self.coin, side=lvl.side).inc()
                except Exception:
                    pass
                self._log_event("order_ack_missing_oid", side=lvl.side, px=px, sz=lvl.sz, cloid=cloid_str, mode=mode, status="cloid_only" if cloid_str else "missing_ids")
                return
            lvl.cloid = cloid_str
            lvl.oid = oid_val
            self._register_order(lvl, cloid_str, oid_val)
            
            # Struct-6: Update oid and transition to OPEN
            if cloid_str:
                self.order_state_machine.update_oid(cloid_str, oid_val)
                self.order_state_machine.acknowledge(cloid=cloid_str)
            
            # S-1: Track pending order in shadow ledger
            try:
                self.shadow_ledger.add_pending_order(
                    cloid=cloid_str or str(oid_val),
                    side=lvl.side,
                    qty=lvl.sz,
                )
            except Exception:
                pass  # Must not break order flow
            
            # S-2: Publish order submitted event
            try:
                await self.event_bus.emit(
                    EventType.ORDER_SUBMITTED,
                    source="submit_level",
                    side=lvl.side,
                    price=px,
                    size=lvl.sz,
                    oid=oid_val,
                    cloid=cloid_str,
                )
            except Exception:
                pass  # Events must not break execution
            
            try:
                self.rich_metrics.orders_submitted.labels(coin=self.coin, side=lvl.side).inc()
            except Exception:
                pass
            self._log_event("order_submit_ack", side=lvl.side, px=px, sz=lvl.sz, oid=lvl.oid, cloid=lvl.cloid, mode=mode)
        else:
            self._log_event("order_submit_ack", side=lvl.side, px=px, sz=lvl.sz, oid=None, cloid=None, mode=mode, status="noop")

    async def _submit_levels_batch(self, levels: List[GridLevel], mode: str = "grid") -> None:
        # Skip if nothing to place
        if not levels:
            return
        tasks = []
        metas = []
        for lvl in levels:
            if not self.router:
                continue
            px = quantize(lvl.px, self.tick_sz, lvl.side, self.px_decimals)
            lvl.px = px
            
            # Pre-generate cloid for state machine tracking (if use_cloid enabled)
            pre_cloid = f"0x{secrets.token_hex(16)}" if self.cfg.use_cloid else None
            req = OrderRequest(is_buy=lvl.side == "buy", sz=lvl.sz, px=px, reduce_only=False, cloid=pre_cloid)
            lvl.cloid = pre_cloid  # Store pre-generated cloid on level for later matching
            
            # Struct-6: Create order in state machine (PENDING state) before submission
            self.order_state_machine.create_order(
                cloid=pre_cloid,
                oid=None,
                side=lvl.side,
                price=px,
                qty=lvl.sz,
            )
            
            if self._log_order_intent():
                self._log_event("order_intent", side=lvl.side, px=px, sz=lvl.sz, mode=mode)
            tasks.append(asyncio.create_task(self.router.submit(req)))
            metas.append((lvl, px, pre_cloid))
        if not tasks:
            return
        results = await asyncio.gather(*tasks, return_exceptions=True)
        batch_errors = 0
        for (lvl, px, pre_cloid), res in zip(metas, results):
            if isinstance(res, Exception):
                batch_errors += 1
                try:
                    self.rich_metrics.orders_rejected.labels(coin=self.coin, reason="submit_error").inc()
                except Exception:
                    pass
                self._log_event("order_submit_error", side=lvl.side, err=str(res), mode=mode)
                # Critical-11: Track batch errors via circuit breaker
                self.circuit_breaker.record_error("batch_submit", res)
                # Mark order as rejected in state machine
                self.order_state_machine.reject(cloid=pre_cloid, error_code="submit_error", error_message=str(res))
                continue
            if isinstance(res, dict):
                oid_val = self._to_int_safe(res.get("oid"))
                cloid_val = res.get("cloid") or lvl.cloid or pre_cloid
                cloid_str = str(cloid_val) if cloid_val else None
                if oid_val is None:
                    lvl.cloid = cloid_str
                    lvl.oid = None
                    if cloid_str:
                        self._register_order(lvl, cloid_str, None)
                        # Struct-6: Acknowledge without oid
                        self.order_state_machine.acknowledge(cloid=cloid_str)
                    try:
                        self.rich_metrics.orders_submitted.labels(coin=self.coin, side=lvl.side).inc()
                    except Exception:
                        pass
                    self._log_event("order_ack_missing_oid", side=lvl.side, px=px, sz=lvl.sz, cloid=cloid_str, mode=mode, status="cloid_only" if cloid_str else "missing_ids")
                    continue
                lvl.cloid = cloid_str
                lvl.oid = oid_val
                self._register_order(lvl, cloid_str, oid_val)
                
                # Struct-6: Update oid and transition to OPEN
                if cloid_str:
                    self.order_state_machine.update_oid(cloid_str, oid_val)
                    self.order_state_machine.acknowledge(cloid=cloid_str)
                
                # S-1: Track pending order in shadow ledger
                try:
                    self.shadow_ledger.add_pending_order(
                        cloid=cloid_str or str(oid_val),
                        side=lvl.side,
                        qty=lvl.sz,
                    )
                except Exception:
                    pass  # Must not break order flow
                
                # S-2: Publish order submitted event (batch)
                try:
                    await self.event_bus.emit(
                        EventType.ORDER_SUBMITTED,
                        source="batch_submit",
                        side=lvl.side,
                        price=px,
                        size=lvl.sz,
                        oid=oid_val,
                        cloid=cloid_str,
                    )
                except Exception:
                    pass  # Events must not break execution
                
                try:
                    self.rich_metrics.orders_submitted.labels(coin=self.coin, side=lvl.side).inc()
                except Exception:
                    pass
                self._log_event("order_submit_ack", side=lvl.side, px=px, sz=lvl.sz, oid=lvl.oid, cloid=lvl.cloid, mode=mode)
            else:
                self._log_event("order_submit_ack", side=lvl.side, px=px, sz=lvl.sz, oid=None, cloid=None, mode=mode, status="noop")
        
        # Critical-11: Check batch results and update circuit breaker
        if batch_errors > 0:
            self._log_event("batch_submit_errors", errors=batch_errors, total=len(tasks))
        elif len(tasks) > 0:
            # Reset error streak on successful batch
            self._reset_api_errors()

    async def _cancel_record(self, rec: ActiveOrder, reason: str = "unspecified") -> bool:
        """Critical-8: Return True if order is confirmed gone, False if still active.
        
        C-6 FIX: On cancel failure, check if order actually still exists on exchange
        before leaving it in the registry. This prevents stale orders from accumulating
        when cancels fail but orders were already filled/cancelled elsewhere.
        """
        if not self.router:
            return False
        success = False
        try:
            await self.router.safe_cancel(cloid=rec.cloid, oid=rec.oid)
            success = True
            
            # Struct-6: Update order state machine
            self.order_state_machine.cancel(
                cloid=rec.cloid,
                oid=rec.oid,
                reason=reason,
            )
            
            # S-1: Remove pending order from shadow ledger
            try:
                order_id = rec.cloid or str(rec.oid) if rec.oid else None
                if order_id:
                    self.shadow_ledger.remove_pending_order(order_id)
            except Exception:
                pass  # Must not break cancel flow
            
            # S-2: Publish order cancelled event
            try:
                await self.event_bus.emit(
                    EventType.ORDER_CANCELLED,
                    source="cancel_record",
                    cloid=rec.cloid,
                    oid=rec.oid,
                    reason=reason,
                )
            except Exception:
                pass  # Events must not break execution
            
            try:
                self.rich_metrics.orders_cancelled.labels(coin=self.coin, reason=reason).inc()
            except Exception:
                pass
            self._log_event(
                "cancel_ok",
                cloid=rec.cloid,
                oid=rec.oid,
                reason=reason,
                cloid_type=str(type(rec.cloid)),
                oid_type=str(type(rec.oid)),
            )
        except Exception as exc:
            try:
                self.rich_metrics.api_errors_total.labels(coin=self.coin, error_type="cancel_error").inc()
            except Exception:
                pass
            try:
                self.rich_metrics.api_error_streak.labels(coin=self.coin).set(self.api_error_streak)
            except Exception:
                pass
            self._log_event(
                "cancel_error",
                err=str(exc),
                cloid=rec.cloid,
                oid=rec.oid,
                reason=reason,
                cloid_type=str(type(rec.cloid)),
                oid_type=str(type(rec.oid)),
            )
            
            # C-6 FIX: Check if order is actually still open on exchange
            # If order was filled or cancelled elsewhere, unindex it to prevent stale entries
            try:
                still_open = await self._is_order_still_open(rec.cloid, rec.oid)
                if not still_open:
                    self._log_event(
                        "cancel_error_order_gone",
                        cloid=rec.cloid,
                        oid=rec.oid,
                        reason="order_not_found_on_exchange",
                    )
                    self._unindex(rec)
                    # Update state machine - order was filled or cancelled externally
                    self.order_state_machine.cancel(
                        cloid=rec.cloid,
                        oid=rec.oid,
                        reason="external_cancel_or_fill",
                    )
                    return True  # Order is gone, mission accomplished
            except Exception as check_exc:
                self._log_event("cancel_error_check_failed", err=str(check_exc))
            
            # Order may still be resting - mark for reconciliation
            self.rebuild_needed = True
            return False
        
        # Critical-8: Only unindex on confirmed cancel
        if success:
            self._unindex(rec)
        return success
    
    async def _is_order_still_open(self, cloid: Optional[str], oid: Optional[int]) -> bool:
        """C-6 FIX: Check if order still exists on exchange.
        
        Returns True if order is still open, False if it's gone (filled/cancelled).
        On error, returns True (safer to assume order exists).
        """
        try:
            # Use async_info if available for better performance
            if self.async_info:
                remote = await self.async_info.frontend_open_orders(self.account, self.cfg.dex)
            else:
                def _call():
                    return self.info.frontend_open_orders(self.account, dex=self.cfg.dex)
                remote = await asyncio.wait_for(
                    asyncio.get_running_loop().run_in_executor(None, _call),
                    timeout=self._http_timeout
                )
            
            # Handle both list and dict response formats
            if isinstance(remote, dict):
                remote = remote.get("openOrders", [])
            
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
            self._log_event("is_order_still_open_error", err=str(exc), cloid=cloid, oid=oid)
            return True  # Assume open on error (safer)

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
        await self.metrics.set_gauge(f"position{{coin='{self.coin}'}}", self.position)
        await self.metrics.set_gauge(f"realized_pnl{{coin='{self.coin}'}}", self._session_realized_pnl)
        await self.metrics.set_gauge(f"alltime_pnl{{coin='{self.coin}'}}", self._alltime_realized_pnl)
        session_duration = time.time() - self._session_start_time
        self._log_event(
            "pnl",
            pos=self.position,
            session_pnl=self._session_realized_pnl,
            alltime_pnl=self._alltime_realized_pnl,
            session_duration_sec=round(session_duration, 1),
        )
        await self._push_status()

    async def _persist(self) -> None:
        # Critical-12: Persist state atomically with fill log
        # Save all-time PnL (accumulated across sessions)
        await self.state_store.save(
            {
                "realized_pnl": self._alltime_realized_pnl,
                "session_pnl": self._session_realized_pnl,
                "grid_center": self.strategy.grid_center if self.strategy else None,
                "position": self.position,
                "last_fill_time_ms": self.last_fill_time_ms,
                "state_version": 2,  # Schema version for future migrations
            }
        )

    async def _load_state(self) -> None:
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
    def _to_int_safe(self, value: Any) -> Optional[int]:
        try:
            if value is None:
                return None
            return int(value)
        except Exception:
            try:
                return int(str(value), 0)
            except Exception:
                return None

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
        """
        Production-optimized structured logging with proper levels and sampling.
        
        Log Level Hierarchy:
        - CRITICAL: Risk breaches, position drift, data loss (always logged)
        - ERROR: Failures requiring attention (always logged)  
        - WARNING: Recoverable issues (throttled for repetitive events)
        - INFO: Key lifecycle events (fills, reconcile, grid builds)
        - DEBUG: High-frequency events (order intents, mid prices, acks)
        """
        import logging
        
        # Event -> log level mapping
        # CRITICAL: Safety/risk events that must never be missed
        critical_events = {
            "position_drift_detected", "fill_log_write_error", "unusual_pnl_fill",
            "risk_halt", "risk_breach", "data_halt",
        }
        # ERROR: Failures needing attention
        error_events = {
            "order_submit_error", "router_invalid_price", "router_worker_error",
            "api_circuit_break", "api_circuit_cancel_error", "fill_unmatched_reconcile_error",
            "ws_watchdog_error", "state_load_error", "state_save_error",
        }
        # WARNING: Recoverable issues (throttled by logging_cfg)
        warning_events = {
            "ws_stale_detected", "ws_stale_halt", "http_retry", "fill_out_of_order",
            "fill_timestamp_anomaly", "stuck_orders_detected", "flatten_cancel_error",
            "order_ack_missing_oid", "skip_flatten_risk",
        }
        # DEBUG: High-frequency noisy events (sampled)
        debug_events = {
            "order_intent", "order_submit_ack", "order_state_created", 
            "order_state_transition", "mid_price_ws", "mid_price_rest", "mid_price_cache",
            "router_enqueue", "router_send", "router_skip_notional",
            "cycle_start", "rest_fills_polled",
        }
        # INFO: Everything else (key lifecycle events)
        
        # Determine log level
        if event in critical_events:
            level = logging.CRITICAL
        elif event in error_events:
            level = logging.ERROR
        elif event in warning_events:
            level = logging.WARNING
        elif event in debug_events:
            level = logging.DEBUG
        else:
            level = logging.INFO
        
        # Sampling for debug-level noisy events
        if level == logging.DEBUG:
            if event == "order_intent":
                sample_rate = max(0.0, min(1.0, getattr(self.cfg, "log_order_intent_sample", 0.0)))
            elif event in {"order_submit_ack", "router_enqueue", "router_send"}:
                sample_rate = max(0.0, min(1.0, getattr(self.cfg, "log_submit_sample", 0.05)))
            elif event in {"order_state_created", "order_state_transition"}:
                sample_rate = 0.1  # 10% of state machine events
            elif event == "rest_fills_polled":
                sample_rate = 0.2  # 20% of REST polls
            else:
                sample_rate = 0.05  # Default 5% for other debug events
            
            if sample_rate < 1.0:
                if secrets.randbelow(10_000) >= int(sample_rate * 10_000):
                    return
        
        payload = {"event": event, "coin": self.coin}
        payload.update(data)
        log.log(level, json.dumps(payload))
