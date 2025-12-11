"""
GridBuilder: Grid construction and placement orchestration.

This module handles all grid-related operations:
- Full grid rebuilds
- Level replacement after fills
- Position drift detection
- Grid metrics emission

Architecture:
    GridBuilder coordinates between GridCalculator (computes levels),
    ExecutionGateway (places orders), and Strategy (generates prices).
    It doesn't own state - state comes from StateManager.

Thread Safety:
    Uses rebuild lock to serialize with fill processing.
    Position snapshots are taken atomically.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, TYPE_CHECKING

if TYPE_CHECKING:
    from src.strategy.grid_calculator import GridCalculator, GridBuildResult, GridDiff
    from src.strategy.strategy import GridStrategy, GridLevel
    from src.risk.risk import RiskEngine
    from src.execution.execution_gateway import ExecutionGateway
    from src.monitoring.metrics_rich import RichMetrics
    from src.execution.order_router import OrderRouter

import logging

log = logging.getLogger("gridbot")


@dataclass
class GridRebuildResult:
    """Result of a grid rebuild operation."""
    success: bool
    mid: float
    spacing: float = 0.0
    levels_placed: int = 0
    levels_cancelled: int = 0
    open_after: int = 0
    aborted_reason: Optional[str] = None
    position_drift: float = 0.0
    
    @property
    def was_aborted(self) -> bool:
        """Check if rebuild was aborted."""
        return self.aborted_reason is not None


@dataclass
class ReplacementResult:
    """Result of a level replacement after fill."""
    success: bool
    level: Optional[Any] = None  # GridLevel
    skipped_reason: Optional[str] = None


@dataclass
class GridBuilderConfig:
    """Configuration for GridBuilder."""
    # Position drift threshold for abort (10%)
    drift_abort_threshold: float = 0.10
    
    # Flatten mode
    flatten_mode: bool = False
    
    # Logging
    log_event_callback: Optional[Callable[..., None]] = None


class GridBuilder:
    """
    Grid construction and placement orchestration.
    
    Encapsulates the grid rebuild logic previously in bot.py:
    - _build_and_place_grid()
    - _build_and_place_grid_inner()
    - _replace_after_fill()
    
    Uses GridCalculator for computations, ExecutionGateway for placement.
    """
    
    def __init__(
        self,
        coin: str,
        grid_calculator: "GridCalculator",
        strategy: "GridStrategy",
        risk_engine: "RiskEngine",
        execution_gateway: "ExecutionGateway",
        rich_metrics: "RichMetrics",
        router: Optional["OrderRouter"] = None,
        config: Optional[GridBuilderConfig] = None,
    ) -> None:
        """
        Initialize GridBuilder with dependencies.
        
        Args:
            coin: Trading symbol
            grid_calculator: Grid level computation
            strategy: Price level generation
            risk_engine: Order risk filtering
            execution_gateway: Order submission
            rich_metrics: Prometheus metrics
            router: Order router (for volatility adjustment)
            config: Optional configuration
        """
        self.coin = coin
        self.grid_calculator = grid_calculator
        self.strategy = strategy
        self.risk_engine = risk_engine
        self.execution_gateway = execution_gateway
        self.rich_metrics = rich_metrics
        self.router = router
        self.config = config or GridBuilderConfig()
        
        # Rebuild serialization
        self._rebuild_lock = asyncio.Lock()
        self._rebuild_in_progress = False
        self._rebuild_epoch = 0
        
        # Flag for pending rebuild
        self.rebuild_needed = False
        
        # Logging
        self._log_event = self.config.log_event_callback or self._default_log
    
    def _default_log(self, event: str, **kwargs: Any) -> None:
        """Default logging."""
        import json
        payload = {"event": event, "coin": self.coin, **kwargs}
        log.info(json.dumps(payload))
    
    @property
    def is_rebuilding(self) -> bool:
        """Check if rebuild is in progress."""
        return self._rebuild_in_progress
    
    @property
    def flatten_mode(self) -> bool:
        """Whether flatten mode is active."""
        return self.config.flatten_mode
    
    @flatten_mode.setter
    def flatten_mode(self, value: bool) -> None:
        """Set flatten mode."""
        self.config.flatten_mode = value
        self.execution_gateway.flatten_mode = value
    
    async def build_and_place_grid(
        self,
        mid: float,
        position: float,
        position_lock: asyncio.Lock,
        get_current_position: Callable[[], float],
    ) -> GridRebuildResult:
        """
        Build and place a complete grid.
        
        Args:
            mid: Current mid price
            position: Current position snapshot
            position_lock: Lock for position access
            get_current_position: Callback to get current position
            
        Returns:
            GridRebuildResult with outcome
        """
        if not self.strategy:
            return GridRebuildResult(
                success=False,
                mid=mid,
                aborted_reason="no_strategy",
            )
        
        if mid <= 0:
            self._log_event("grid_build_skip_mid", mid=mid)
            return GridRebuildResult(
                success=False,
                mid=mid,
                aborted_reason="invalid_mid",
            )
        
        # Acquire rebuild lock to serialize with fill processing
        async with self._rebuild_lock:
            self._rebuild_in_progress = True
            self._rebuild_epoch += 1
            try:
                return await self._build_and_place_grid_inner(
                    mid=mid,
                    position_snapshot=position,
                    position_lock=position_lock,
                    get_current_position=get_current_position,
                )
            finally:
                self._rebuild_in_progress = False
    
    async def _build_and_place_grid_inner(
        self,
        mid: float,
        position_snapshot: float,
        position_lock: asyncio.Lock,
        get_current_position: Callable[[], float],
    ) -> GridRebuildResult:
        """Inner grid building logic, called while holding rebuild lock."""
        
        self._log_event(
            "grid_rebuild_start",
            mid=mid,
            position=position_snapshot,
            open_orders=self.execution_gateway.open_count(),
        )
        
        # Update strategy internal models
        self._update_strategy_models(mid)
        
        # Calculate order size
        base_size = self.grid_calculator.calculate_order_size(mid)
        
        # Build filtered levels
        build_result = self.grid_calculator.build_filtered_levels(
            strategy=self.strategy,
            risk=self.risk_engine,
            mid=mid,
            position=position_snapshot,
            base_size=base_size,
            flatten_mode=self.config.flatten_mode,
        )
        
        spacing = build_result.spacing
        levels = build_result.levels
        
        # Export grid metrics
        self._emit_grid_metrics(mid, spacing)
        
        # Compute diff with existing orders
        existing_keys = set(self.execution_gateway.order_manager.orders_by_price.keys())
        grid_diff = self.grid_calculator.compute_grid_diff(
            desired_levels=levels,
            existing_keys=existing_keys,
            orders_by_price=self.execution_gateway.order_manager.orders_by_price,
            reprice_tick_threshold=getattr(self.grid_calculator.cfg, 'reprice_tick_threshold', 2),
        )
        
        to_cancel_keys = grid_diff.to_cancel_keys
        to_place = grid_diff.to_place
        
        # Cancel orders that need removal
        cancelled_count = await self._cancel_diff_orders(to_cancel_keys)
        
        # Check for position drift before placing
        async with position_lock:
            current_pos = get_current_position()
        
        position_drift = abs(current_pos - position_snapshot)
        if position_drift > 1e-9:
            self._log_event(
                "grid_position_drift",
                snapshot=position_snapshot,
                current=current_pos,
                drift=position_drift,
            )
            
            # If significant drift, abort and request another rebuild
            drift_pct = position_drift / max(abs(position_snapshot), 1e-9)
            if drift_pct > self.config.drift_abort_threshold:
                self._log_event(
                    "grid_rebuild_aborted_drift",
                    drift_pct=drift_pct,
                )
                self.rebuild_needed = True
                return GridRebuildResult(
                    success=False,
                    mid=mid,
                    spacing=spacing,
                    levels_cancelled=cancelled_count,
                    aborted_reason="position_drift",
                    position_drift=position_drift,
                )
        
        # Place new orders
        results = await self.execution_gateway.submit_levels_batch(
            levels=to_place,
            mode="grid",
            position=position_snapshot,
        )
        placed_count = sum(1 for r in results if r.success)
        
        open_after = self.execution_gateway.open_count()
        
        self._log_event(
            "grid_rebuild_complete",
            mid=mid,
            spacing=spacing,
            placed=placed_count,
            canceled=cancelled_count,
            remaining_open=open_after,
        )
        
        return GridRebuildResult(
            success=True,
            mid=mid,
            spacing=spacing,
            levels_placed=placed_count,
            levels_cancelled=cancelled_count,
            open_after=open_after,
        )
    
    def _update_strategy_models(self, mid: float) -> None:
        """Update strategy internal models and export metrics."""
        try:
            self.strategy.on_price(mid)
            
            # Capture volatility/ATR for metrics
            try:
                self.rich_metrics.volatility_estimate.labels(coin=self.coin).set(
                    self.strategy.last_vol
                )
            except Exception:
                pass
            
            try:
                self.rich_metrics.atr_value.labels(coin=self.coin).set(
                    self.strategy.last_atr
                )
            except Exception:
                pass
            
            # Adjust router coalescing based on volatility
            if self.router and hasattr(self.router, 'adjust_for_volatility'):
                self.router.adjust_for_volatility(self.strategy.last_vol)
                
        except Exception as e:
            self._log_event("strategy_update_error", error=str(e))
    
    def _emit_grid_metrics(self, mid: float, spacing: float) -> None:
        """Emit grid-related Prometheus metrics."""
        try:
            self.rich_metrics.grid_width_pct.labels(coin=self.coin).set(spacing * 100.0)
        except Exception:
            pass
        
        try:
            if self.strategy.grid_center:
                self.rich_metrics.grid_center.labels(coin=self.coin).set(
                    self.strategy.grid_center
                )
        except Exception:
            pass
        
        try:
            bias = self.strategy.trend_bias(mid)
            self.rich_metrics.trend_bias.labels(coin=self.coin).set(bias)
        except Exception:
            pass
    
    async def _cancel_diff_orders(self, to_cancel_keys: Set[str]) -> int:
        """Cancel orders that are no longer needed."""
        cancelled = 0
        for key in to_cancel_keys:
            rec = self.execution_gateway.order_manager.orders_by_price.get(key)
            if not rec:
                continue
            try:
                await self.execution_gateway.cancel_order(
                    cloid=rec.cloid,
                    oid=rec.oid,
                    reason="grid_diff",
                )
                cancelled += 1
            except Exception as exc:
                self._log_event("cancel_diff_error", error=str(exc), key=key)
        return cancelled
    
    async def replace_after_fill(
        self,
        fill_side: str,
        fill_px: float,
        fill_sz: float,
        position: float,
    ) -> ReplacementResult:
        """
        Place replacement order after a fill.
        
        Args:
            fill_side: Side of the filled order
            fill_px: Price of the fill
            fill_sz: Size of the fill
            position: Current position
            
        Returns:
            ReplacementResult with outcome
        """
        if not self.strategy:
            return ReplacementResult(
                success=False,
                skipped_reason="no_strategy",
            )
        
        # Calculate replacement level
        lvl = self.grid_calculator.calculate_replacement_level(
            strategy=self.strategy,
            fill_side=fill_side,
            fill_px=fill_px,
            fill_sz=fill_sz,
            position=position,
            flatten_mode=self.config.flatten_mode,
        )
        
        if lvl is None:
            self._log_event(
                "flatten_skip_refill",
                side=fill_side,
                px=fill_px,
                sz=fill_sz,
            )
            return ReplacementResult(
                success=False,
                skipped_reason="flatten_mode",
            )
        
        # Check risk limits
        if not self.risk_engine.allow_order(lvl.side, lvl.sz, lvl.px):
            self._log_event(
                "skip_replacement_risk",
                side=lvl.side,
                px=lvl.px,
                sz=lvl.sz,
                position=position,
            )
            return ReplacementResult(
                success=False,
                level=lvl,
                skipped_reason="risk_blocked",
            )
        
        # Submit the replacement
        result = await self.execution_gateway.submit_level(
            level=lvl,
            mode="refill",
            position=position,
        )
        
        return ReplacementResult(
            success=result.success,
            level=lvl,
            skipped_reason=result.error if not result.success else None,
        )
    
    def get_rebuild_epoch(self) -> int:
        """Get current rebuild epoch for change detection."""
        return self._rebuild_epoch
    
    def should_rebuild(self) -> bool:
        """Check if a rebuild is pending."""
        return self.rebuild_needed
    
    def clear_rebuild_flag(self) -> None:
        """Clear the rebuild needed flag."""
        self.rebuild_needed = False
