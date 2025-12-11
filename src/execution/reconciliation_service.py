"""
ReconciliationService: Position and order reconciliation extracted from bot.py.

This module handles periodic reconciliation between local state and exchange:
- Position reconciliation (detect drift)
- Order reconciliation (sync local registry with exchange)
- Shadow ledger updates
- Risk engine state updates

Architecture:
    ReconciliationService runs on configurable intervals and ensures
    local state stays synchronized with exchange reality. It's essential
    for handling fills that may have been missed or during recovery.

Thread Safety:
    Uses position_lock when updating position state.
    Order index operations are coordinated through OrderManager.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from src.execution.order_manager import OrderManager, ActiveOrder
    from src.state.shadow_ledger import ShadowLedger
    from src.risk.risk import RiskEngine
    from src.core.event_bus import EventBus
    from src.monitoring.metrics_rich import RichMetrics
    from src.strategy.strategy import GridLevel
    from src.state.state_manager import StateManager

import logging

log = logging.getLogger("gridbot")


@dataclass
class ReconciliationConfig:
    """Configuration for ReconciliationService."""
    # Reconciliation intervals
    position_reconcile_interval_sec: float = 60.0
    order_reconcile_interval_sec: float = 60.0
    
    # Drift thresholds
    position_drift_alert_pct: float = 0.01  # 1% drift alerts
    position_drift_log_threshold: float = 1e-6  # Minimum drift to log
    
    # DEX configuration
    dex: str = "hl"
    
    # Logging callback
    log_event_callback: Optional[Callable[..., None]] = None


@dataclass
class PositionReconcileResult:
    """Result of position reconciliation."""
    success: bool
    exchange_position: float
    local_position: float
    drift: float
    drift_pct: float
    equity: Optional[float] = None
    daily_pnl: Optional[float] = None
    funding: Optional[float] = None
    error: Optional[str] = None


@dataclass
class OrderReconcileResult:
    """Result of order reconciliation."""
    success: bool
    orders_added: int = 0
    orders_removed: int = 0
    remote_count: int = 0
    local_count: int = 0
    error: Optional[str] = None


class ReconciliationService:
    """
    Position and order reconciliation service.
    
    Ensures local state stays synchronized with exchange reality.
    Critical for:
    - Detecting missed fills
    - Recovering from disconnections
    - Maintaining accurate position tracking
    
    Usage:
        service = ReconciliationService(
            coin="BTC",
            async_info=async_info,
            account=account,
            order_manager=order_manager,
            state_manager=state_manager,
            config=config,
        )
        
        # In main loop
        if service.is_position_reconcile_due:
            result = await service.reconcile_position()
            
        if service.is_order_reconcile_due:
            result = await service.reconcile_orders()
    """
    
    def __init__(
        self,
        coin: str,
        async_info,  # AsyncInfo
        info,  # Info (sync fallback)
        account: str,
        order_manager: "OrderManager",
        state_manager: "StateManager",
        shadow_ledger: "ShadowLedger",
        risk_engine: "RiskEngine",
        rich_metrics: Optional["RichMetrics"] = None,
        event_bus: Optional["EventBus"] = None,
        config: Optional[ReconciliationConfig] = None,
    ) -> None:
        """
        Initialize ReconciliationService.
        
        Args:
            coin: Trading symbol
            async_info: Async API client
            info: Sync API client (fallback)
            account: User account address
            order_manager: Order index management
            state_manager: Centralized state
            shadow_ledger: Position shadow tracking
            risk_engine: Risk limits
            rich_metrics: Prometheus metrics
            event_bus: Event publication
            config: Optional configuration
        """
        self.coin = coin
        self.async_info = async_info
        self.info = info
        self.account = account
        self.order_manager = order_manager
        self.state_manager = state_manager
        self.shadow_ledger = shadow_ledger
        self.risk_engine = risk_engine
        self.rich_metrics = rich_metrics
        self.event_bus = event_bus
        self.config = config or ReconciliationConfig()
        
        # Timing state
        self._last_position_reconcile: float = 0.0
        self._last_order_reconcile: float = 0.0
        
        # HTTP timeout
        self._http_timeout: float = 5.0
        
        # Logging
        self._log_event = self.config.log_event_callback or self._default_log
    
    def _default_log(self, event: str, **kwargs: Any) -> None:
        """Default logging."""
        import json
        payload = {"event": event, "coin": self.coin, **kwargs}
        log.info(json.dumps(payload))
    
    @property
    def is_position_reconcile_due(self) -> bool:
        """Check if position reconciliation is due."""
        return (
            time.time() - self._last_position_reconcile
            >= self.config.position_reconcile_interval_sec
        )
    
    @property
    def is_order_reconcile_due(self) -> bool:
        """Check if order reconciliation is due."""
        return (
            time.time() - self._last_order_reconcile
            >= self.config.order_reconcile_interval_sec
        )
    
    async def reconcile_position(
        self,
        position_lock: Optional[asyncio.Lock] = None,
    ) -> PositionReconcileResult:
        """
        Reconcile local position with exchange.
        
        Args:
            position_lock: Lock to hold during position update
            
        Returns:
            PositionReconcileResult with outcome
        """
        self._log_event("reconcile_position_start")
        local_position = self.state_manager.position
        
        try:
            # Fetch exchange state
            state = await self._fetch_user_state()
            
            # Extract position
            pos = 0.0
            margin_summary = state.get("marginSummary", {})
            equity = float(margin_summary.get("accountValue", 0.0))
            
            for ap in state.get("assetPositions", []):
                p = ap.get("position", {})
                if p.get("coin") == self.coin:
                    pos = float(p.get("szi", 0.0))
                    break
            
            # Calculate drift
            drift = abs(pos - local_position)
            drift_pct = drift / max(abs(local_position), abs(pos), 0.001)
            
            # Log if significant drift
            if drift > self.config.position_drift_log_threshold:
                if drift_pct > self.config.position_drift_alert_pct:
                    self._log_event(
                        "position_drift_detected",
                        local_position=local_position,
                        exchange_position=pos,
                        drift_amount=drift,
                        drift_pct=drift_pct,
                    )
                    
                    # Update metrics
                    if self.rich_metrics:
                        try:
                            self.rich_metrics.position_drift_detected.labels(
                                coin=self.coin
                            ).inc()
                            self.rich_metrics.position_drift_amount.labels(
                                coin=self.coin
                            ).set(drift)
                        except Exception:
                            pass
            
            # Update state
            if position_lock:
                async with position_lock:
                    await self.state_manager.set_position(pos, source="reconcile")
            else:
                await self.state_manager.set_position(pos, source="reconcile")
            
            # Update risk engine with account state
            self.risk_engine.update_equity(equity)
            
            daily_pnl = margin_summary.get("sessionPnl") or margin_summary.get("dailyPnl")
            if daily_pnl is not None:
                try:
                    self.risk_engine.set_daily_pnl(float(daily_pnl))
                except Exception:
                    pass
            
            funding = margin_summary.get("fundingAccrued") or margin_summary.get("funding")
            if funding is not None:
                try:
                    self.risk_engine.set_funding(float(funding))
                except Exception:
                    pass
            
            # Reconcile shadow ledger
            try:
                result = await self.shadow_ledger.reconcile_with_exchange(pos)
                if result.drift_detected:
                    self._log_event(
                        "shadow_ledger_drift",
                        drift_amount=result.drift_amount,
                        drift_percent=result.drift_percent,
                    )
            except Exception:
                pass
            
            # Update metrics
            if self.rich_metrics:
                try:
                    self.rich_metrics.position.labels(coin=self.coin).set(pos)
                    self.rich_metrics.daily_pnl.labels(coin=self.coin).set(
                        float(daily_pnl) if daily_pnl else 0.0
                    )
                except Exception:
                    pass
            
            self._last_position_reconcile = time.time()
            
            self._log_event(
                "reconcile_position_complete",
                position=pos,
                equity=equity,
                daily_pnl=daily_pnl,
                drift=drift,
            )
            
            return PositionReconcileResult(
                success=True,
                exchange_position=pos,
                local_position=local_position,
                drift=drift,
                drift_pct=drift_pct,
                equity=equity,
                daily_pnl=float(daily_pnl) if daily_pnl else None,
                funding=float(funding) if funding else None,
            )
            
        except Exception as exc:
            self._log_event("reconcile_position_error", error=str(exc))
            return PositionReconcileResult(
                success=False,
                exchange_position=0.0,
                local_position=local_position,
                drift=0.0,
                drift_pct=0.0,
                error=str(exc),
            )
    
    async def reconcile_orders(self) -> OrderReconcileResult:
        """
        Reconcile local order registry with exchange.
        
        Returns:
            OrderReconcileResult with outcome
        """
        self._log_event("reconcile_orders_start")
        
        try:
            # Fetch exchange orders
            remote = await self._fetch_open_orders()
            
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
                remote_pairs.add((
                    str(cloid) if cloid else None,
                    int(oid) if oid is not None else None,
                ))
                
                # Build price key
                from src.core.utils import quantize
                price_key = f"{side}:{quantize(px, self.order_manager.tick_sz, side, self.order_manager.px_decimals):.{self.order_manager.px_decimals}f}"
                
                if price_key in self.order_manager.orders_by_price:
                    continue
                
                # Register missing order
                from src.strategy.strategy import GridLevel
                lvl = GridLevel(
                    side=side,
                    px=px,
                    sz=sz,
                    oid=int(oid) if oid is not None else None,
                    cloid=str(cloid) if cloid else None,
                )
                self.order_manager.register(lvl, lvl.cloid, lvl.oid)
                added += 1
            
            # Remove orders not on exchange
            removed = 0
            for key in list(self.order_manager.orders_by_price.keys()):
                rec = self.order_manager.orders_by_price.get(key)
                if rec is None:
                    continue
                id_pair = (rec.cloid, rec.oid)
                if id_pair not in remote_pairs:
                    self.order_manager.unindex(rec)
                    removed += 1
            
            self._last_order_reconcile = time.time()
            
            local_count = self.order_manager.open_count()
            
            self._log_event(
                "reconcile_orders_complete",
                added=added,
                removed=removed,
                remote=len(remote),
                local=local_count,
            )
            
            return OrderReconcileResult(
                success=True,
                orders_added=added,
                orders_removed=removed,
                remote_count=len(remote),
                local_count=local_count,
            )
            
        except Exception as exc:
            self._log_event("reconcile_orders_error", error=str(exc))
            return OrderReconcileResult(
                success=False,
                error=str(exc),
            )
    
    async def _fetch_user_state(self) -> Dict[str, Any]:
        """Fetch user state from exchange."""
        builder_asset = self.config.dex.lower() == "xyz" or self.coin.startswith("xyz:")
        
        if self.async_info and not builder_asset:
            return await self.async_info.user_state(self.account, self.config.dex)
        else:
            def _call():
                return self.info.user_state(self.account, dex=self.config.dex)
            return await asyncio.wait_for(
                asyncio.get_running_loop().run_in_executor(None, _call),
                timeout=self._http_timeout,
            )
    
    async def _fetch_open_orders(self) -> List[Dict[str, Any]]:
        """Fetch open orders from exchange."""
        builder_asset = self.config.dex.lower() == "xyz" or self.coin.startswith("xyz:")
        
        if self.async_info and not builder_asset:
            return await self.async_info.frontend_open_orders(self.account, self.config.dex)
        else:
            def _call():
                try:
                    return self.info.frontend_open_orders(self.account, dex=self.config.dex)
                except Exception:
                    return []
            return await asyncio.wait_for(
                asyncio.get_running_loop().run_in_executor(None, _call),
                timeout=self._http_timeout,
            )
    
    def reset_timers(self) -> None:
        """Reset reconciliation timers."""
        self._last_position_reconcile = 0.0
        self._last_order_reconcile = 0.0
