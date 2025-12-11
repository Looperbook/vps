"""
Risk engine enforcing drawdown, exposure, and PnL limits.

Struct-3/Improvement-3: Modular Risk Engine with:
- Event-based architecture with callbacks
- Per-level exposure tracking
- Order velocity limits
- Position change rate limits
- Risk snapshots for auditing
- State serialization for recovery
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Tuple

from src.config.config import Settings

logger = logging.getLogger(__name__)


class RiskEventType(Enum):
    """Types of risk events that can be emitted."""
    ORDER_BLOCKED = auto()
    HALT_TRIGGERED = auto()
    HALT_CLEARED = auto()
    DRAWDOWN_WARNING = auto()
    POSITION_LIMIT_WARNING = auto()
    VELOCITY_LIMIT_HIT = auto()
    EXPOSURE_UPDATE = auto()


@dataclass
class RiskEvent:
    """A risk event with metadata for audit trail."""
    event_type: RiskEventType
    timestamp_ms: int
    reason: str
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RiskState:
    """Current risk state snapshot."""
    position: float = 0.0
    equity_hwm: float = 0.0
    daily_pnl: float = 0.0
    equity: float = 0.0
    funding: float = 0.0
    realized: float = 0.0
    unrealized: float = 0.0


@dataclass
class RiskSnapshot:
    """Point-in-time risk snapshot for auditing."""
    timestamp_ms: int
    state: RiskState
    halted: bool
    halt_reason: Optional[str]
    margin_utilization: float
    exposure_by_level: Dict[float, float]  # price -> exposure
    order_count_1m: int
    position_delta_1m: float


class RiskEngine:
    """
    Institutional-grade risk engine with comprehensive limits and audit trail.
    
    Features:
    - Drawdown protection (equity vs HWM)
    - Daily PnL stop/take limits
    - Position size limits (absolute and notional)
    - Skew ratio limits
    - Funding bleed protection
    - Unrealized loss limits
    - Order velocity limits (orders per minute)
    - Position velocity limits (delta per minute)
    - Per-level exposure tracking
    - Event callbacks for alerting
    - State serialization for recovery
    """

    def __init__(
        self,
        cfg: Settings,
        max_orders_per_minute: int = 60,
        max_position_delta_per_minute: float = 0.0,  # 0 = disabled
        drawdown_warning_pct: float = 0.08,  # Warn at 8%, halt at max_drawdown_pct
    ) -> None:
        self.cfg = cfg
        self.state = RiskState()
        self.halted: bool = False
        self.halt_reason: Optional[str] = None
        self.margin_utilization: float = 0.0
        
        # Velocity limits
        self.max_orders_per_minute = max_orders_per_minute
        self.max_position_delta_per_minute = max_position_delta_per_minute
        self.drawdown_warning_pct = drawdown_warning_pct
        
        # Velocity tracking (thread-safe)
        self._lock = threading.RLock()
        self._order_timestamps: List[int] = []  # Last N order timestamps in ms
        self._position_deltas: List[Tuple[int, float]] = []  # (timestamp_ms, delta)
        
        # Per-level exposure tracking
        self._exposure_by_level: Dict[float, float] = {}  # price -> exposure in base
        
        # Event system
        self._event_callbacks: List[Callable[[RiskEvent], None]] = []
        self._event_history: List[RiskEvent] = []
        self._max_event_history = 1000
        
        # Warning state (to avoid spam)
        self._drawdown_warning_sent = False
        self._position_warning_sent = False

    # ─────────────────────────────────────────────────────────────────────
    # Event System
    # ─────────────────────────────────────────────────────────────────────

    def register_callback(self, callback: Callable[[RiskEvent], None]) -> None:
        """Register a callback for risk events."""
        with self._lock:
            self._event_callbacks.append(callback)

    def unregister_callback(self, callback: Callable[[RiskEvent], None]) -> None:
        """Unregister a callback."""
        with self._lock:
            if callback in self._event_callbacks:
                self._event_callbacks.remove(callback)

    def _emit_event(self, event: RiskEvent) -> None:
        """Emit a risk event to all registered callbacks."""
        with self._lock:
            self._event_history.append(event)
            if len(self._event_history) > self._max_event_history:
                self._event_history = self._event_history[-self._max_event_history:]
            callbacks = list(self._event_callbacks)
        
        for cb in callbacks:
            try:
                cb(event)
            except Exception as e:
                logger.warning(f"Risk callback error: {e}")

    def get_recent_events(self, count: int = 100) -> List[RiskEvent]:
        """Get recent risk events for auditing."""
        with self._lock:
            return list(self._event_history[-count:])

    # ─────────────────────────────────────────────────────────────────────
    # State Updates
    # ─────────────────────────────────────────────────────────────────────

    def update_equity(self, equity: float) -> None:
        """Update current equity and high water mark."""
        with self._lock:
            self.state.equity = equity
            if equity > self.state.equity_hwm:
                self.state.equity_hwm = equity
            
            # Check for drawdown warning
            if self.state.equity_hwm > 0:
                dd_pct = (self.state.equity_hwm - equity) / self.state.equity_hwm
                if dd_pct >= self.drawdown_warning_pct and not self._drawdown_warning_sent:
                    self._drawdown_warning_sent = True
                    self._emit_event(RiskEvent(
                        event_type=RiskEventType.DRAWDOWN_WARNING,
                        timestamp_ms=int(time.time() * 1000),
                        reason=f"Drawdown at {dd_pct:.1%}, warning threshold {self.drawdown_warning_pct:.1%}",
                        details={"drawdown_pct": dd_pct, "equity": equity, "hwm": self.state.equity_hwm}
                    ))
                elif dd_pct < self.drawdown_warning_pct * 0.5:
                    # Reset warning if drawdown recovered significantly
                    self._drawdown_warning_sent = False

    def update_margin_utilization(self, util: float) -> None:
        """Update margin utilization percentage."""
        with self._lock:
            self.margin_utilization = util

    def set_position(self, pos: float) -> None:
        """Update current position and track velocity."""
        now_ms = int(time.time() * 1000)
        with self._lock:
            old_pos = self.state.position
            delta = abs(pos - old_pos)
            if delta > 0:
                self._position_deltas.append((now_ms, delta))
                # Prune old deltas (older than 1 minute)
                cutoff = now_ms - 60_000
                self._position_deltas = [(t, d) for t, d in self._position_deltas if t > cutoff]
            self.state.position = pos

    def set_daily_pnl(self, pnl: float) -> None:
        """Update daily P&L."""
        with self._lock:
            self.state.daily_pnl = pnl

    def set_realized(self, realized: float) -> None:
        """Update realized P&L."""
        with self._lock:
            self.state.realized = realized

    def set_unrealized(self, unrealized: float) -> None:
        """Update unrealized P&L."""
        with self._lock:
            self.state.unrealized = unrealized

    def set_funding(self, funding: float) -> None:
        """Update funding paid/received."""
        with self._lock:
            self.state.funding = funding

    # ─────────────────────────────────────────────────────────────────────
    # Exposure Tracking
    # ─────────────────────────────────────────────────────────────────────

    def update_level_exposure(self, price: float, size: float) -> None:
        """Update exposure at a specific price level."""
        with self._lock:
            if size == 0:
                self._exposure_by_level.pop(price, None)
            else:
                self._exposure_by_level[price] = size
            
            self._emit_event(RiskEvent(
                event_type=RiskEventType.EXPOSURE_UPDATE,
                timestamp_ms=int(time.time() * 1000),
                reason=f"Level {price} exposure updated to {size}",
                details={"price": price, "size": size}
            ))

    def clear_level_exposure(self, price: float) -> None:
        """Clear exposure at a specific price level."""
        with self._lock:
            self._exposure_by_level.pop(price, None)

    def get_total_exposure(self) -> float:
        """Get total exposure across all levels."""
        with self._lock:
            return sum(abs(sz) for sz in self._exposure_by_level.values())

    def get_exposure_by_level(self) -> Dict[float, float]:
        """Get a copy of exposure by level."""
        with self._lock:
            return dict(self._exposure_by_level)

    # ─────────────────────────────────────────────────────────────────────
    # Velocity Tracking
    # ─────────────────────────────────────────────────────────────────────

    def record_order(self) -> None:
        """Record an order for velocity tracking."""
        now_ms = int(time.time() * 1000)
        with self._lock:
            self._order_timestamps.append(now_ms)
            # Prune old timestamps (older than 1 minute)
            cutoff = now_ms - 60_000
            self._order_timestamps = [t for t in self._order_timestamps if t > cutoff]

    def get_orders_last_minute(self) -> int:
        """Get count of orders in the last minute."""
        now_ms = int(time.time() * 1000)
        cutoff = now_ms - 60_000
        with self._lock:
            return sum(1 for t in self._order_timestamps if t > cutoff)

    def get_position_delta_last_minute(self) -> float:
        """Get total position change in the last minute."""
        now_ms = int(time.time() * 1000)
        cutoff = now_ms - 60_000
        with self._lock:
            return sum(d for t, d in self._position_deltas if t > cutoff)

    def _check_order_velocity(self) -> Tuple[bool, str]:
        """Check if order velocity is within limits."""
        if self.max_orders_per_minute <= 0:
            return True, ""
        count = self.get_orders_last_minute()
        if count >= self.max_orders_per_minute:
            return False, f"Order velocity limit: {count}/{self.max_orders_per_minute} orders/min"
        return True, ""

    def _check_position_velocity(self, proposed_delta: float) -> Tuple[bool, str]:
        """Check if position velocity is within limits."""
        if self.max_position_delta_per_minute <= 0:
            return True, ""
        current_delta = self.get_position_delta_last_minute()
        if current_delta + proposed_delta > self.max_position_delta_per_minute:
            return False, f"Position velocity limit: {current_delta + proposed_delta:.4f}/{self.max_position_delta_per_minute:.4f}/min"
        return True, ""

    # ─────────────────────────────────────────────────────────────────────
    # Order Approval
    # ─────────────────────────────────────────────────────────────────────

    def allow_order(
        self,
        side: str,
        sz: float,
        px: float,
        position_override: Optional[float] = None,
        record_if_allowed: bool = True,
    ) -> bool:
        """
        Evaluate whether placing an order of size `sz` on `side` is allowed.
        
        Args:
            side: "buy" or "sell"
            sz: Order size
            px: Order price
            position_override: Override current position for simulation
            record_if_allowed: If True, record the order for velocity tracking
            
        Returns:
            True if order is allowed, False otherwise
        """
        if self.halted:
            self._emit_event(RiskEvent(
                event_type=RiskEventType.ORDER_BLOCKED,
                timestamp_ms=int(time.time() * 1000),
                reason=f"System halted: {self.halt_reason}",
                details={"side": side, "sz": sz, "px": px}
            ))
            return False
        
        with self._lock:
            base_pos = self.state.position if position_override is None else position_override
        
        delta = sz if side == "buy" else -sz
        prospective_pos = base_pos + delta
        
        # Check velocity limits
        ok, reason = self._check_order_velocity()
        if not ok:
            self._emit_event(RiskEvent(
                event_type=RiskEventType.VELOCITY_LIMIT_HIT,
                timestamp_ms=int(time.time() * 1000),
                reason=reason,
                details={"side": side, "sz": sz, "px": px}
            ))
            return False
        
        ok, reason = self._check_position_velocity(sz)
        if not ok:
            self._emit_event(RiskEvent(
                event_type=RiskEventType.VELOCITY_LIMIT_HIT,
                timestamp_ms=int(time.time() * 1000),
                reason=reason,
                details={"side": side, "sz": sz, "px": px, "proposed_delta": sz}
            ))
            return False
        
        # Check drawdown
        if not self._check_drawdown():
            self._halt("drawdown")
            return False
        
        # Check daily P&L limits
        if not self._check_daily_pnl():
            self._halt("daily_pnl")
            return False
        
        # Check unrealized loss limit
        if not self._check_unrealized():
            self._halt("unrealized_dd")
            return False
        
        # Check position caps
        if not self._check_position_caps(px, prospective_pos):
            self._emit_event(RiskEvent(
                event_type=RiskEventType.ORDER_BLOCKED,
                timestamp_ms=int(time.time() * 1000),
                reason="Position cap exceeded",
                details={"side": side, "sz": sz, "px": px, "prospective_pos": prospective_pos}
            ))
            return False
        
        # Check funding bleed
        if not self._check_funding():
            self._halt("funding_bleed")
            return False
        
        # Record order for velocity tracking if allowed
        if record_if_allowed:
            self.record_order()
        
        return True

    def _check_drawdown(self) -> bool:
        """Check if drawdown is within limits."""
        with self._lock:
            if self.state.equity_hwm == 0:
                return True
            dd = (self.state.equity_hwm - self.state.equity) / self.state.equity_hwm
        return dd < self.cfg.max_drawdown_pct

    def _check_daily_pnl(self) -> bool:
        """Check if daily P&L is within stop/take limits."""
        with self._lock:
            return self.cfg.pnl_daily_stop <= self.state.daily_pnl <= self.cfg.pnl_daily_take

    def _check_position_caps(self, px: float, prospective_pos: float) -> bool:
        """Check position size limits."""
        # Absolute position limit
        if self.cfg.max_position_abs > 0 and abs(prospective_pos) > self.cfg.max_position_abs:
            return False
        
        # Notional limit
        notional = abs(prospective_pos) * px
        if self.cfg.max_symbol_notional > 0 and notional > self.cfg.max_symbol_notional:
            return False
        
        # Skew guard relative to target notional
        if self.cfg.max_skew_ratio > 0:
            target_pos = (self.cfg.investment_usd * self.cfg.leverage) / max(px, 1e-9)
            if target_pos > 0 and abs(prospective_pos) / target_pos > self.cfg.max_skew_ratio:
                return False
        
        return True

    def _check_funding(self) -> bool:
        """Check funding bleed ratio."""
        with self._lock:
            equity = max(self.state.equity, 1e-9)
            bleed_ratio = abs(self.state.funding) / equity
        return bleed_ratio <= self.cfg.funding_bleed_pct

    def _check_unrealized(self) -> bool:
        """Check unrealized loss limit."""
        if self.cfg.max_unrealized_dd_pct <= 0:
            return True
        with self._lock:
            equity = max(self.state.equity_hwm or self.state.equity, 1e-9)
            if equity == 0:
                return True
            dd = abs(self.state.unrealized) / equity
        return dd <= self.cfg.max_unrealized_dd_pct

    # ─────────────────────────────────────────────────────────────────────
    # Halt Management
    # ─────────────────────────────────────────────────────────────────────

    def _halt(self, reason: str) -> None:
        """Halt trading with reason."""
        with self._lock:
            if not self.halted:  # Only emit event on state change
                self.halted = True
                self.halt_reason = reason
                self._emit_event(RiskEvent(
                    event_type=RiskEventType.HALT_TRIGGERED,
                    timestamp_ms=int(time.time() * 1000),
                    reason=reason,
                    details={
                        "equity": self.state.equity,
                        "hwm": self.state.equity_hwm,
                        "daily_pnl": self.state.daily_pnl,
                        "position": self.state.position,
                        "funding": self.state.funding,
                        "unrealized": self.state.unrealized,
                    }
                ))

    def is_halted(self) -> bool:
        """Check if trading is halted."""
        with self._lock:
            return self.halted

    def get_halt_reason(self) -> Optional[str]:
        """Get the halt reason if halted."""
        with self._lock:
            return self.halt_reason

    def reset_halt(self) -> None:
        """Clear halt state (use with caution)."""
        with self._lock:
            if self.halted:
                self.halted = False
                old_reason = self.halt_reason
                self.halt_reason = None
                self._emit_event(RiskEvent(
                    event_type=RiskEventType.HALT_CLEARED,
                    timestamp_ms=int(time.time() * 1000),
                    reason=f"Halt cleared (was: {old_reason})",
                    details={}
                ))

    # ─────────────────────────────────────────────────────────────────────
    # Snapshots & Serialization
    # ─────────────────────────────────────────────────────────────────────

    def snapshot(self) -> RiskSnapshot:
        """Create a point-in-time snapshot for auditing."""
        with self._lock:
            return RiskSnapshot(
                timestamp_ms=int(time.time() * 1000),
                state=RiskState(
                    position=self.state.position,
                    equity_hwm=self.state.equity_hwm,
                    daily_pnl=self.state.daily_pnl,
                    equity=self.state.equity,
                    funding=self.state.funding,
                    realized=self.state.realized,
                    unrealized=self.state.unrealized,
                ),
                halted=self.halted,
                halt_reason=self.halt_reason,
                margin_utilization=self.margin_utilization,
                exposure_by_level=dict(self._exposure_by_level),
                order_count_1m=self.get_orders_last_minute(),
                position_delta_1m=self.get_position_delta_last_minute(),
            )

    def to_dict(self) -> Dict[str, Any]:
        """Serialize risk state for persistence."""
        with self._lock:
            return {
                "state": {
                    "position": self.state.position,
                    "equity_hwm": self.state.equity_hwm,
                    "daily_pnl": self.state.daily_pnl,
                    "equity": self.state.equity,
                    "funding": self.state.funding,
                    "realized": self.state.realized,
                    "unrealized": self.state.unrealized,
                },
                "halted": self.halted,
                "halt_reason": self.halt_reason,
                "margin_utilization": self.margin_utilization,
                "exposure_by_level": dict(self._exposure_by_level),
            }

    def from_dict(self, data: Dict[str, Any]) -> None:
        """Restore risk state from persistence."""
        with self._lock:
            state_data = data.get("state", {})
            self.state.position = state_data.get("position", 0.0)
            self.state.equity_hwm = state_data.get("equity_hwm", 0.0)
            self.state.daily_pnl = state_data.get("daily_pnl", 0.0)
            self.state.equity = state_data.get("equity", 0.0)
            self.state.funding = state_data.get("funding", 0.0)
            self.state.realized = state_data.get("realized", 0.0)
            self.state.unrealized = state_data.get("unrealized", 0.0)
            
            self.halted = data.get("halted", False)
            self.halt_reason = data.get("halt_reason")
            self.margin_utilization = data.get("margin_utilization", 0.0)
            self._exposure_by_level = data.get("exposure_by_level", {})

    # ─────────────────────────────────────────────────────────────────────
    # Utility Methods
    # ─────────────────────────────────────────────────────────────────────

    def get_drawdown_pct(self) -> float:
        """Get current drawdown percentage."""
        with self._lock:
            if self.state.equity_hwm == 0:
                return 0.0
            return (self.state.equity_hwm - self.state.equity) / self.state.equity_hwm

    def get_position_utilization(self) -> float:
        """Get position utilization as percentage of max allowed."""
        with self._lock:
            if self.cfg.max_position_abs <= 0:
                return 0.0
            return abs(self.state.position) / self.cfg.max_position_abs

    def get_risk_summary(self) -> Dict[str, Any]:
        """Get a human-readable risk summary."""
        with self._lock:
            return {
                "halted": self.halted,
                "halt_reason": self.halt_reason,
                "position": self.state.position,
                "equity": self.state.equity,
                "hwm": self.state.equity_hwm,
                "drawdown_pct": self.get_drawdown_pct(),
                "daily_pnl": self.state.daily_pnl,
                "unrealized": self.state.unrealized,
                "funding": self.state.funding,
                "margin_util": self.margin_utilization,
                "orders_1m": self.get_orders_last_minute(),
                "pos_delta_1m": self.get_position_delta_last_minute(),
                "exposure_levels": len(self._exposure_by_level),
                "total_exposure": self.get_total_exposure(),
            }
