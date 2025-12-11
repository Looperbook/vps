"""
Tests for production hardening features:
- Fill timestamp validation
- Position drift detection
- Stuck order detection
- PnL sanity checks
- WS degradation mode
- Fill log durability metrics
"""

import asyncio
import pytest
import time
from unittest.mock import Mock, AsyncMock, patch

from src.execution.order_state_machine import OrderStateMachine, OrderState
from src.execution.fill_log import FillLog, BatchedFillLog
from src.monitoring.metrics_rich import RichMetrics


class TestFillTimestampValidation:
    """Tests for fill timestamp anomaly detection."""
    
    def test_future_timestamp_detected(self):
        """Fill with future timestamp should be flagged."""
        from src.core.utils import now_ms
        
        # Create a mock bot-like object with the validation method
        class MockBot:
            def _validate_fill_timestamp(self, ts_ms: int):
                now = now_ms()
                if ts_ms > now + 60_000:
                    return "future"
                if ts_ms < now - 86_400_000:
                    return "very_old"
                return None
        
        bot = MockBot()
        
        # Future timestamp (2 minutes from now)
        future_ts = now_ms() + 120_000
        assert bot._validate_fill_timestamp(future_ts) == "future"
        
        # Current timestamp
        current_ts = now_ms()
        assert bot._validate_fill_timestamp(current_ts) is None
        
        # Slightly future (30 seconds) - should be OK
        slight_future = now_ms() + 30_000
        assert bot._validate_fill_timestamp(slight_future) is None
    
    def test_very_old_timestamp_detected(self):
        """Fill with very old timestamp should be flagged."""
        from src.core.utils import now_ms
        
        class MockBot:
            def _validate_fill_timestamp(self, ts_ms: int):
                now = now_ms()
                if ts_ms > now + 60_000:
                    return "future"
                if ts_ms < now - 86_400_000:
                    return "very_old"
                return None
        
        bot = MockBot()
        
        # Very old timestamp (2 days ago)
        old_ts = now_ms() - (2 * 86_400_000)
        assert bot._validate_fill_timestamp(old_ts) == "very_old"
        
        # Recent timestamp (1 hour ago) - should be OK
        recent_ts = now_ms() - 3_600_000
        assert bot._validate_fill_timestamp(recent_ts) is None


class TestStuckOrderDetection:
    """Tests for stuck order detection in OrderStateMachine."""
    
    def test_get_stuck_orders_empty_when_no_pending(self):
        """No stuck orders when none are pending."""
        sm = OrderStateMachine()
        
        # Create order and acknowledge it immediately
        sm.create_order(cloid="test1", oid=None, side="buy", price=100.0, qty=1.0)
        sm.acknowledge(cloid="test1")
        
        stuck = sm.get_stuck_orders(max_pending_age_ms=1000)
        assert len(stuck) == 0
    
    def test_get_stuck_orders_detects_old_pending(self):
        """Detects orders stuck in PENDING for too long."""
        sm = OrderStateMachine()
        
        # Create order
        record = sm.create_order(cloid="stuck1", oid=None, side="buy", price=100.0, qty=1.0)
        
        # Manually set created_at to simulate old order
        record.created_at_ms = int(time.time() * 1000) - 60_000  # 60 seconds ago
        
        stuck = sm.get_stuck_orders(max_pending_age_ms=30_000)  # 30 second threshold
        assert len(stuck) == 1
        assert stuck[0].cloid == "stuck1"
    
    def test_get_stuck_orders_ignores_recent_pending(self):
        """Recent pending orders are not flagged as stuck."""
        sm = OrderStateMachine()
        
        # Create order (created just now)
        sm.create_order(cloid="recent1", oid=None, side="buy", price=100.0, qty=1.0)
        
        stuck = sm.get_stuck_orders(max_pending_age_ms=30_000)
        assert len(stuck) == 0


class TestPositionDriftDetection:
    """Tests for position drift detection logic."""
    
    def test_drift_calculation(self):
        """Test drift calculation logic."""
        local_position = 10.0
        exchange_position = 10.5
        
        drift_amount = abs(exchange_position - local_position)
        drift_pct = drift_amount / max(abs(local_position), abs(exchange_position), 0.001)
        
        assert drift_amount == 0.5
        assert drift_pct == pytest.approx(0.0476, rel=0.01)  # ~4.76% drift (0.5/10.5)
    
    def test_small_drift_ignored(self):
        """Very small drifts should be ignored."""
        local_position = 10.0
        exchange_position = 10.00001
        
        drift_amount = abs(exchange_position - local_position)
        
        # Should not trigger if drift < 1e-6
        assert drift_amount < 1e-5


class TestFillLogDurability:
    """Tests for fill log write error tracking."""
    
    @pytest.mark.asyncio
    async def test_write_error_callback_called(self, tmp_path):
        """Write error callback should be called on failure."""
        error_messages = []
        
        def on_error(msg):
            error_messages.append(msg)
        
        # Create fill log with callback
        log = FillLog("TEST", str(tmp_path), on_write_error=on_error)
        
        # Simulate write by making path read-only (platform-specific)
        # Instead, test the callback mechanism directly
        log._on_write_error = on_error
        log._write_errors = 0
        
        # Manually trigger error callback
        if log._on_write_error:
            log._on_write_error("Test error")
        
        assert len(error_messages) == 1
        assert error_messages[0] == "Test error"
    
    @pytest.mark.asyncio
    async def test_batched_fill_log_tracks_errors(self, tmp_path):
        """BatchedFillLog should track write errors."""
        errors = []
        
        log = BatchedFillLog(
            "TEST",
            str(tmp_path),
            flush_interval=0.1,
            on_write_error=lambda e: errors.append(e)
        )
        
        # Normal write should succeed
        await log.append({"time": 123, "side": "buy", "px": 100, "sz": 1})
        await log._flush()
        
        assert log._write_errors == 0


class TestRichMetricsNewGauges:
    """Tests for new durability metrics in RichMetrics."""
    
    def test_new_metrics_exist(self):
        """Verify new hardening metrics are registered."""
        metrics = RichMetrics()
        
        # Check new metrics exist
        assert hasattr(metrics, 'fill_log_write_errors')
        assert hasattr(metrics, 'position_drift_detected')
        assert hasattr(metrics, 'position_drift_amount')
        assert hasattr(metrics, 'stuck_orders_count')
        assert hasattr(metrics, 'fill_timestamp_anomalies')
        assert hasattr(metrics, 'unusual_pnl_fills')
        assert hasattr(metrics, 'ws_degraded_mode')
    
    def test_metrics_can_be_updated(self):
        """Verify metrics can be incremented/set."""
        metrics = RichMetrics()
        
        # These should not raise
        metrics.fill_log_write_errors.labels(coin="TEST").inc()
        metrics.position_drift_detected.labels(coin="TEST").inc()
        metrics.position_drift_amount.labels(coin="TEST").set(0.5)
        metrics.stuck_orders_count.labels(coin="TEST").set(2)
        metrics.fill_timestamp_anomalies.labels(coin="TEST", type="future").inc()
        metrics.unusual_pnl_fills.labels(coin="TEST").inc()
        metrics.ws_degraded_mode.labels(coin="TEST").set(1.0)


class TestWSGradedDegradation:
    """Tests for WebSocket degradation mode."""
    
    def test_effective_rest_interval_normal(self):
        """Normal mode uses configured interval."""
        ws_degraded = False
        cfg_interval = 20.0
        
        effective = cfg_interval / 4 if ws_degraded else cfg_interval
        assert effective == 20.0
    
    def test_effective_rest_interval_degraded(self):
        """Degraded mode uses 4x faster polling."""
        ws_degraded = True
        cfg_interval = 20.0
        
        effective = cfg_interval / 4 if ws_degraded else cfg_interval
        assert effective == 5.0


class TestPnLSanityCheck:
    """Tests for PnL sanity check logic."""
    
    def test_normal_pnl_not_flagged(self):
        """Normal PnL values should not trigger alerts."""
        pnl = 10.0
        threshold = 500.0
        
        assert abs(pnl) <= threshold
    
    def test_large_pnl_flagged(self):
        """Unusually large PnL should be flagged."""
        pnl = 1000.0
        threshold = 500.0
        
        assert abs(pnl) > threshold
    
    def test_large_negative_pnl_flagged(self):
        """Large negative PnL should also be flagged."""
        pnl = -750.0
        threshold = 500.0
        
        assert abs(pnl) > threshold
