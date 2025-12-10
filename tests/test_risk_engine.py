"""
Comprehensive unit tests for the modular RiskEngine.

Tests cover:
- Basic state management
- Drawdown protection
- Daily P&L limits
- Position limits
- Velocity limits (orders/min, position delta/min)
- Per-level exposure tracking
- Event system and callbacks
- State serialization/deserialization
- Risk snapshots
"""

import time
from unittest.mock import MagicMock, patch

import pytest

from src.risk import RiskEngine, RiskEvent, RiskEventType, RiskSnapshot, RiskState


@pytest.fixture
def mock_cfg():
    """Create a mock config with risk settings."""
    cfg = MagicMock()
    cfg.max_drawdown_pct = 0.10  # 10% max drawdown
    cfg.pnl_daily_stop = -500.0
    cfg.pnl_daily_take = 1000.0
    cfg.max_position_abs = 100.0
    cfg.max_symbol_notional = 10000.0
    cfg.max_skew_ratio = 0.6
    cfg.max_unrealized_dd_pct = 0.15
    cfg.funding_bleed_pct = 0.02
    cfg.investment_usd = 1000.0
    cfg.leverage = 10.0
    return cfg


@pytest.fixture
def risk_engine(mock_cfg):
    """Create a RiskEngine with default settings."""
    return RiskEngine(
        cfg=mock_cfg,
        max_orders_per_minute=60,
        max_position_delta_per_minute=0.0,  # Disabled by default
        drawdown_warning_pct=0.08,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Basic State Management Tests
# ─────────────────────────────────────────────────────────────────────────────


class TestBasicStateManagement:
    """Tests for basic state updates."""

    def test_initial_state(self, risk_engine):
        """Test initial state is clean."""
        assert risk_engine.state.position == 0.0
        assert risk_engine.state.equity == 0.0
        assert risk_engine.state.equity_hwm == 0.0
        assert risk_engine.halted is False
        assert risk_engine.halt_reason is None

    def test_update_equity(self, risk_engine):
        """Test equity update and HWM tracking."""
        risk_engine.update_equity(1000.0)
        assert risk_engine.state.equity == 1000.0
        assert risk_engine.state.equity_hwm == 1000.0

        # Equity increases
        risk_engine.update_equity(1200.0)
        assert risk_engine.state.equity == 1200.0
        assert risk_engine.state.equity_hwm == 1200.0

        # Equity decreases - HWM stays
        risk_engine.update_equity(1100.0)
        assert risk_engine.state.equity == 1100.0
        assert risk_engine.state.equity_hwm == 1200.0

    def test_set_position(self, risk_engine):
        """Test position setting."""
        risk_engine.set_position(10.5)
        assert risk_engine.state.position == 10.5

    def test_set_daily_pnl(self, risk_engine):
        """Test daily P&L setting."""
        risk_engine.set_daily_pnl(50.0)
        assert risk_engine.state.daily_pnl == 50.0

    def test_set_realized(self, risk_engine):
        """Test realized P&L setting."""
        risk_engine.set_realized(100.0)
        assert risk_engine.state.realized == 100.0

    def test_set_unrealized(self, risk_engine):
        """Test unrealized P&L setting."""
        risk_engine.set_unrealized(-50.0)
        assert risk_engine.state.unrealized == -50.0

    def test_set_funding(self, risk_engine):
        """Test funding setting."""
        risk_engine.set_funding(-5.0)
        assert risk_engine.state.funding == -5.0

    def test_update_margin_utilization(self, risk_engine):
        """Test margin utilization update."""
        risk_engine.update_margin_utilization(0.75)
        assert risk_engine.margin_utilization == 0.75


# ─────────────────────────────────────────────────────────────────────────────
# Drawdown Protection Tests
# ─────────────────────────────────────────────────────────────────────────────


class TestDrawdownProtection:
    """Tests for drawdown protection."""

    def test_allow_order_within_drawdown(self, risk_engine):
        """Test orders allowed within drawdown limit."""
        risk_engine.update_equity(1000.0)
        risk_engine.update_equity(950.0)  # 5% drawdown
        
        assert risk_engine.allow_order("buy", 1.0, 100.0, record_if_allowed=False)

    def test_halt_on_drawdown_breach(self, risk_engine):
        """Test halt triggered when drawdown exceeds limit."""
        risk_engine.update_equity(1000.0)
        risk_engine.update_equity(850.0)  # 15% drawdown (>10% limit)
        
        assert risk_engine.allow_order("buy", 1.0, 100.0, record_if_allowed=False) is False
        assert risk_engine.halted is True
        assert risk_engine.halt_reason == "drawdown"

    def test_drawdown_warning_emitted(self, risk_engine):
        """Test drawdown warning event is emitted."""
        events = []
        risk_engine.register_callback(lambda e: events.append(e))
        
        risk_engine.update_equity(1000.0)
        risk_engine.update_equity(910.0)  # 9% drawdown (>8% warning threshold)
        
        warning_events = [e for e in events if e.event_type == RiskEventType.DRAWDOWN_WARNING]
        assert len(warning_events) == 1
        assert "9.0%" in warning_events[0].reason

    def test_drawdown_warning_not_spam(self, risk_engine):
        """Test drawdown warning doesn't spam."""
        events = []
        risk_engine.register_callback(lambda e: events.append(e))
        
        risk_engine.update_equity(1000.0)
        risk_engine.update_equity(910.0)  # Warning
        risk_engine.update_equity(905.0)  # No additional warning
        risk_engine.update_equity(900.0)  # No additional warning
        
        warning_events = [e for e in events if e.event_type == RiskEventType.DRAWDOWN_WARNING]
        assert len(warning_events) == 1

    def test_get_drawdown_pct(self, risk_engine):
        """Test drawdown percentage calculation."""
        risk_engine.update_equity(1000.0)
        risk_engine.update_equity(900.0)
        
        assert risk_engine.get_drawdown_pct() == pytest.approx(0.10, rel=1e-3)


# ─────────────────────────────────────────────────────────────────────────────
# Daily P&L Limit Tests
# ─────────────────────────────────────────────────────────────────────────────


class TestDailyPnLLimits:
    """Tests for daily P&L stop/take limits."""

    def test_allow_order_within_pnl_limits(self, risk_engine):
        """Test orders allowed within daily P&L limits."""
        risk_engine.update_equity(1000.0)
        risk_engine.set_daily_pnl(100.0)
        
        assert risk_engine.allow_order("buy", 1.0, 100.0, record_if_allowed=False)

    def test_halt_on_daily_stop(self, risk_engine):
        """Test halt when daily loss exceeds stop."""
        risk_engine.update_equity(1000.0)
        risk_engine.set_daily_pnl(-600.0)  # Exceeds -500 stop
        
        assert risk_engine.allow_order("buy", 1.0, 100.0, record_if_allowed=False) is False
        assert risk_engine.halted is True
        assert risk_engine.halt_reason == "daily_pnl"

    def test_halt_on_daily_take(self, risk_engine):
        """Test halt when daily profit exceeds take."""
        risk_engine.update_equity(1000.0)
        risk_engine.set_daily_pnl(1100.0)  # Exceeds 1000 take
        
        assert risk_engine.allow_order("buy", 1.0, 100.0, record_if_allowed=False) is False
        assert risk_engine.halted is True
        assert risk_engine.halt_reason == "daily_pnl"


# ─────────────────────────────────────────────────────────────────────────────
# Position Limit Tests
# ─────────────────────────────────────────────────────────────────────────────


class TestPositionLimits:
    """Tests for position size limits."""

    def test_allow_order_within_position_limit(self, risk_engine):
        """Test orders allowed within position limit."""
        risk_engine.update_equity(1000.0)
        risk_engine.set_position(50.0)
        
        assert risk_engine.allow_order("buy", 10.0, 100.0, record_if_allowed=False)

    def test_block_order_exceeding_position_limit(self, risk_engine):
        """Test order blocked when exceeding max position."""
        risk_engine.update_equity(1000.0)
        risk_engine.set_position(90.0)
        
        # 90 + 15 = 105 > 100 max
        assert risk_engine.allow_order("buy", 15.0, 100.0, record_if_allowed=False) is False
        # Should not halt, just block
        assert risk_engine.halted is False

    def test_allow_order_with_position_override(self, risk_engine):
        """Test position override for simulation."""
        risk_engine.update_equity(1000.0)
        risk_engine.set_position(95.0)  # Close to limit
        
        # With override, simulate from 0
        assert risk_engine.allow_order("buy", 50.0, 100.0, position_override=0.0, record_if_allowed=False)

    def test_block_order_exceeding_notional_limit(self, risk_engine):
        """Test order blocked when exceeding notional limit."""
        risk_engine.update_equity(1000.0)
        risk_engine.set_position(0.0)
        
        # 50 * 250 = 12500 > 10000 max notional
        assert risk_engine.allow_order("buy", 50.0, 250.0, record_if_allowed=False) is False

    def test_get_position_utilization(self, risk_engine):
        """Test position utilization calculation."""
        risk_engine.set_position(60.0)  # 60% of 100 max
        
        assert risk_engine.get_position_utilization() == pytest.approx(0.6, rel=1e-3)


# ─────────────────────────────────────────────────────────────────────────────
# Velocity Limit Tests
# ─────────────────────────────────────────────────────────────────────────────


class TestVelocityLimits:
    """Tests for order and position velocity limits."""

    def test_record_order_tracking(self, risk_engine):
        """Test order recording for velocity tracking."""
        assert risk_engine.get_orders_last_minute() == 0
        
        risk_engine.record_order()
        risk_engine.record_order()
        risk_engine.record_order()
        
        assert risk_engine.get_orders_last_minute() == 3

    def test_order_velocity_limit(self, mock_cfg):
        """Test order velocity limit enforcement."""
        risk_engine = RiskEngine(cfg=mock_cfg, max_orders_per_minute=3)
        risk_engine.update_equity(1000.0)
        
        # First 3 orders allowed (recording happens on allow)
        assert risk_engine.allow_order("buy", 1.0, 100.0) is True
        assert risk_engine.allow_order("buy", 1.0, 100.0) is True
        assert risk_engine.allow_order("buy", 1.0, 100.0) is True
        
        # 4th order blocked by velocity limit
        assert risk_engine.allow_order("buy", 1.0, 100.0) is False
        assert risk_engine.halted is False  # Velocity doesn't halt, just blocks

    def test_position_velocity_tracking(self, risk_engine):
        """Test position delta tracking."""
        assert risk_engine.get_position_delta_last_minute() == 0.0
        
        risk_engine.set_position(10.0)  # Delta: 10
        risk_engine.set_position(15.0)  # Delta: 5
        risk_engine.set_position(12.0)  # Delta: 3
        
        assert risk_engine.get_position_delta_last_minute() == 18.0

    def test_position_velocity_limit(self, mock_cfg):
        """Test position velocity limit enforcement."""
        risk_engine = RiskEngine(cfg=mock_cfg, max_position_delta_per_minute=5.0)
        risk_engine.update_equity(1000.0)
        
        # First order: 3.0 delta allowed
        assert risk_engine.allow_order("buy", 3.0, 100.0) is True
        
        # Second order: 3.0 more would exceed 5.0 limit
        # But we need to simulate position change too
        risk_engine.set_position(3.0)  # Records delta
        
        # Now 3 + 3 = 6 > 5 limit
        assert risk_engine.allow_order("buy", 3.0, 100.0) is False

    def test_velocity_event_emitted(self, mock_cfg):
        """Test velocity limit event is emitted."""
        risk_engine = RiskEngine(cfg=mock_cfg, max_orders_per_minute=2)
        risk_engine.update_equity(1000.0)
        
        events = []
        risk_engine.register_callback(lambda e: events.append(e))
        
        risk_engine.allow_order("buy", 1.0, 100.0)
        risk_engine.allow_order("buy", 1.0, 100.0)
        risk_engine.allow_order("buy", 1.0, 100.0)  # Should be blocked
        
        velocity_events = [e for e in events if e.event_type == RiskEventType.VELOCITY_LIMIT_HIT]
        assert len(velocity_events) == 1


# ─────────────────────────────────────────────────────────────────────────────
# Exposure Tracking Tests
# ─────────────────────────────────────────────────────────────────────────────


class TestExposureTracking:
    """Tests for per-level exposure tracking."""

    def test_update_level_exposure(self, risk_engine):
        """Test updating exposure at a level."""
        risk_engine.update_level_exposure(100.0, 5.0)
        
        exposure = risk_engine.get_exposure_by_level()
        assert exposure[100.0] == 5.0

    def test_clear_level_exposure(self, risk_engine):
        """Test clearing exposure at a level."""
        risk_engine.update_level_exposure(100.0, 5.0)
        risk_engine.update_level_exposure(105.0, 3.0)
        
        risk_engine.clear_level_exposure(100.0)
        
        exposure = risk_engine.get_exposure_by_level()
        assert 100.0 not in exposure
        assert 105.0 in exposure

    def test_zero_size_clears_exposure(self, risk_engine):
        """Test that setting size to 0 clears the level."""
        risk_engine.update_level_exposure(100.0, 5.0)
        risk_engine.update_level_exposure(100.0, 0.0)
        
        exposure = risk_engine.get_exposure_by_level()
        assert 100.0 not in exposure

    def test_get_total_exposure(self, risk_engine):
        """Test total exposure calculation."""
        risk_engine.update_level_exposure(100.0, 5.0)
        risk_engine.update_level_exposure(105.0, -3.0)
        risk_engine.update_level_exposure(110.0, 2.0)
        
        # Total = |5| + |-3| + |2| = 10
        assert risk_engine.get_total_exposure() == 10.0

    def test_exposure_event_emitted(self, risk_engine):
        """Test exposure update event is emitted."""
        events = []
        risk_engine.register_callback(lambda e: events.append(e))
        
        risk_engine.update_level_exposure(100.0, 5.0)
        
        exposure_events = [e for e in events if e.event_type == RiskEventType.EXPOSURE_UPDATE]
        assert len(exposure_events) == 1
        assert exposure_events[0].details["price"] == 100.0
        assert exposure_events[0].details["size"] == 5.0


# ─────────────────────────────────────────────────────────────────────────────
# Event System Tests
# ─────────────────────────────────────────────────────────────────────────────


class TestEventSystem:
    """Tests for the event callback system."""

    def test_register_callback(self, risk_engine):
        """Test registering a callback."""
        events = []
        risk_engine.register_callback(lambda e: events.append(e))
        
        risk_engine.update_level_exposure(100.0, 5.0)
        
        assert len(events) == 1

    def test_unregister_callback(self, risk_engine):
        """Test unregistering a callback."""
        events = []
        cb = lambda e: events.append(e)
        
        risk_engine.register_callback(cb)
        risk_engine.update_level_exposure(100.0, 5.0)
        
        risk_engine.unregister_callback(cb)
        risk_engine.update_level_exposure(105.0, 3.0)
        
        assert len(events) == 1  # Only first event recorded

    def test_callback_error_handled(self, risk_engine):
        """Test callback errors don't crash the engine."""
        def bad_callback(e):
            raise ValueError("Callback error")
        
        risk_engine.register_callback(bad_callback)
        
        # Should not raise
        risk_engine.update_level_exposure(100.0, 5.0)

    def test_get_recent_events(self, risk_engine):
        """Test retrieving recent events."""
        risk_engine.update_level_exposure(100.0, 5.0)
        risk_engine.update_level_exposure(105.0, 3.0)
        risk_engine.update_level_exposure(110.0, 2.0)
        
        events = risk_engine.get_recent_events(count=2)
        assert len(events) == 2

    def test_event_history_pruning(self, risk_engine):
        """Test event history is pruned."""
        risk_engine._max_event_history = 5
        
        for i in range(10):
            risk_engine.update_level_exposure(100.0 + i, float(i))
        
        events = risk_engine.get_recent_events(count=100)
        assert len(events) == 5

    def test_halt_event_emitted(self, risk_engine):
        """Test halt event is emitted."""
        events = []
        risk_engine.register_callback(lambda e: events.append(e))
        
        risk_engine.update_equity(1000.0)
        risk_engine.update_equity(850.0)  # Trigger drawdown halt
        risk_engine.allow_order("buy", 1.0, 100.0, record_if_allowed=False)
        
        halt_events = [e for e in events if e.event_type == RiskEventType.HALT_TRIGGERED]
        assert len(halt_events) == 1
        assert halt_events[0].reason == "drawdown"

    def test_halt_cleared_event(self, risk_engine):
        """Test halt cleared event is emitted."""
        events = []
        risk_engine.register_callback(lambda e: events.append(e))
        
        risk_engine.update_equity(1000.0)
        risk_engine.update_equity(850.0)
        risk_engine.allow_order("buy", 1.0, 100.0, record_if_allowed=False)  # Triggers halt
        risk_engine.reset_halt()
        
        cleared_events = [e for e in events if e.event_type == RiskEventType.HALT_CLEARED]
        assert len(cleared_events) == 1


# ─────────────────────────────────────────────────────────────────────────────
# Halt Management Tests
# ─────────────────────────────────────────────────────────────────────────────


class TestHaltManagement:
    """Tests for halt state management."""

    def test_is_halted(self, risk_engine):
        """Test halt state checking."""
        assert risk_engine.is_halted() is False
        
        risk_engine.halted = True
        assert risk_engine.is_halted() is True

    def test_get_halt_reason(self, risk_engine):
        """Test halt reason retrieval."""
        assert risk_engine.get_halt_reason() is None
        
        risk_engine.halted = True
        risk_engine.halt_reason = "test_reason"
        assert risk_engine.get_halt_reason() == "test_reason"

    def test_reset_halt(self, risk_engine):
        """Test halt reset."""
        risk_engine.halted = True
        risk_engine.halt_reason = "test"
        
        risk_engine.reset_halt()
        
        assert risk_engine.halted is False
        assert risk_engine.halt_reason is None

    def test_halted_blocks_all_orders(self, risk_engine):
        """Test halted state blocks all orders."""
        risk_engine.update_equity(1000.0)
        risk_engine.halted = True
        risk_engine.halt_reason = "manual"
        
        assert risk_engine.allow_order("buy", 1.0, 100.0, record_if_allowed=False) is False
        assert risk_engine.allow_order("sell", 1.0, 100.0, record_if_allowed=False) is False


# ─────────────────────────────────────────────────────────────────────────────
# Serialization Tests
# ─────────────────────────────────────────────────────────────────────────────


class TestSerialization:
    """Tests for state serialization and deserialization."""

    def test_to_dict(self, risk_engine):
        """Test serializing state to dict."""
        risk_engine.update_equity(1000.0)
        risk_engine.set_position(10.0)
        risk_engine.set_daily_pnl(50.0)
        risk_engine.set_funding(-5.0)
        risk_engine.update_level_exposure(100.0, 5.0)
        
        data = risk_engine.to_dict()
        
        assert data["state"]["equity"] == 1000.0
        assert data["state"]["position"] == 10.0
        assert data["state"]["daily_pnl"] == 50.0
        assert data["state"]["funding"] == -5.0
        assert data["exposure_by_level"][100.0] == 5.0
        assert data["halted"] is False

    def test_from_dict(self, risk_engine):
        """Test restoring state from dict."""
        data = {
            "state": {
                "position": 15.0,
                "equity_hwm": 2000.0,
                "daily_pnl": 75.0,
                "equity": 1800.0,
                "funding": -10.0,
                "realized": 100.0,
                "unrealized": -50.0,
            },
            "halted": True,
            "halt_reason": "test_halt",
            "margin_utilization": 0.65,
            "exposure_by_level": {105.0: 3.0},
        }
        
        risk_engine.from_dict(data)
        
        assert risk_engine.state.position == 15.0
        assert risk_engine.state.equity_hwm == 2000.0
        assert risk_engine.state.daily_pnl == 75.0
        assert risk_engine.state.equity == 1800.0
        assert risk_engine.halted is True
        assert risk_engine.halt_reason == "test_halt"
        assert risk_engine.margin_utilization == 0.65

    def test_round_trip_serialization(self, risk_engine):
        """Test serialization round-trip preserves state."""
        risk_engine.update_equity(1500.0)
        risk_engine.set_position(25.0)
        risk_engine.set_daily_pnl(80.0)
        risk_engine.update_level_exposure(110.0, 7.0)
        
        data = risk_engine.to_dict()
        
        # Create new engine and restore
        new_engine = RiskEngine(cfg=risk_engine.cfg)
        new_engine.from_dict(data)
        
        assert new_engine.state.equity == 1500.0
        assert new_engine.state.position == 25.0
        assert new_engine.state.daily_pnl == 80.0


# ─────────────────────────────────────────────────────────────────────────────
# Snapshot Tests
# ─────────────────────────────────────────────────────────────────────────────


class TestSnapshots:
    """Tests for risk snapshots."""

    def test_snapshot_creation(self, risk_engine):
        """Test creating a snapshot."""
        risk_engine.update_equity(1000.0)
        risk_engine.set_position(10.0)
        risk_engine.update_level_exposure(100.0, 5.0)
        
        snapshot = risk_engine.snapshot()
        
        assert isinstance(snapshot, RiskSnapshot)
        assert snapshot.state.equity == 1000.0
        assert snapshot.state.position == 10.0
        assert snapshot.halted is False
        assert 100.0 in snapshot.exposure_by_level

    def test_snapshot_is_independent(self, risk_engine):
        """Test snapshot is independent of engine state."""
        risk_engine.update_equity(1000.0)
        snapshot = risk_engine.snapshot()
        
        # Change engine state
        risk_engine.update_equity(2000.0)
        
        # Snapshot unchanged
        assert snapshot.state.equity == 1000.0


# ─────────────────────────────────────────────────────────────────────────────
# Risk Summary Tests
# ─────────────────────────────────────────────────────────────────────────────


class TestRiskSummary:
    """Tests for risk summary."""

    def test_get_risk_summary(self, risk_engine):
        """Test risk summary generation."""
        risk_engine.update_equity(1000.0)
        risk_engine.update_equity(950.0)
        risk_engine.set_position(50.0)
        risk_engine.set_daily_pnl(25.0)
        risk_engine.update_level_exposure(100.0, 5.0)
        risk_engine.update_level_exposure(105.0, 3.0)
        
        summary = risk_engine.get_risk_summary()
        
        assert summary["halted"] is False
        assert summary["position"] == 50.0
        assert summary["equity"] == 950.0
        assert summary["hwm"] == 1000.0
        assert summary["drawdown_pct"] == pytest.approx(0.05, rel=1e-3)
        assert summary["daily_pnl"] == 25.0
        assert summary["exposure_levels"] == 2
        assert summary["total_exposure"] == 8.0


# ─────────────────────────────────────────────────────────────────────────────
# Funding Bleed Tests
# ─────────────────────────────────────────────────────────────────────────────


class TestFundingBleed:
    """Tests for funding bleed protection."""

    def test_allow_order_within_funding_limit(self, risk_engine):
        """Test orders allowed within funding bleed limit."""
        risk_engine.update_equity(1000.0)
        risk_engine.set_funding(-10.0)  # 1% of equity
        
        assert risk_engine.allow_order("buy", 1.0, 100.0, record_if_allowed=False)

    def test_halt_on_funding_bleed(self, risk_engine):
        """Test halt when funding bleed exceeds limit."""
        risk_engine.update_equity(1000.0)
        risk_engine.set_funding(-30.0)  # 3% of equity (>2% limit)
        
        assert risk_engine.allow_order("buy", 1.0, 100.0, record_if_allowed=False) is False
        assert risk_engine.halted is True
        assert risk_engine.halt_reason == "funding_bleed"


# ─────────────────────────────────────────────────────────────────────────────
# Unrealized Loss Tests
# ─────────────────────────────────────────────────────────────────────────────


class TestUnrealizedLoss:
    """Tests for unrealized loss protection."""

    def test_allow_order_within_unrealized_limit(self, risk_engine):
        """Test orders allowed within unrealized loss limit."""
        risk_engine.update_equity(1000.0)
        risk_engine.set_unrealized(-100.0)  # 10% of HWM
        
        assert risk_engine.allow_order("buy", 1.0, 100.0, record_if_allowed=False)

    def test_halt_on_unrealized_loss(self, risk_engine):
        """Test halt when unrealized loss exceeds limit."""
        risk_engine.update_equity(1000.0)
        risk_engine.set_unrealized(-200.0)  # 20% of HWM (>15% limit)
        
        assert risk_engine.allow_order("buy", 1.0, 100.0, record_if_allowed=False) is False
        assert risk_engine.halted is True
        assert risk_engine.halt_reason == "unrealized_dd"


# ─────────────────────────────────────────────────────────────────────────────
# Thread Safety Tests
# ─────────────────────────────────────────────────────────────────────────────


class TestThreadSafety:
    """Tests for thread safety."""

    def test_concurrent_state_updates(self, risk_engine):
        """Test concurrent state updates don't corrupt state."""
        import threading
        
        def update_position():
            for i in range(100):
                risk_engine.set_position(float(i))
        
        def update_equity():
            for i in range(100):
                risk_engine.update_equity(1000.0 + i)
        
        t1 = threading.Thread(target=update_position)
        t2 = threading.Thread(target=update_equity)
        
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        
        # Should complete without errors
        assert risk_engine.state.position >= 0
        assert risk_engine.state.equity >= 1000.0

    def test_concurrent_callbacks(self, risk_engine):
        """Test concurrent callback invocations are handled."""
        import threading
        
        events = []
        lock = threading.Lock()
        
        def cb(e):
            with lock:
                events.append(e)
        
        risk_engine.register_callback(cb)
        
        def emit_events():
            for i in range(50):
                risk_engine.update_level_exposure(100.0 + i, float(i))
        
        threads = [threading.Thread(target=emit_events) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        # All events should be captured
        assert len(events) == 250
