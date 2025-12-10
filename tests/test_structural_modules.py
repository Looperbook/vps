"""
Unit tests for extracted structural modules.

Tests for:
- OrderManager
- CircuitBreaker
- FillDeduplicator
- PositionTracker
- GridCalculator
"""

import pytest
import asyncio
import time
from decimal import Decimal
from unittest.mock import MagicMock

from src.order_manager import OrderManager, ActiveOrder
from src.circuit_breaker import CircuitBreaker, CircuitBreakerConfig
from src.fill_deduplicator import FillDeduplicator
from src.position_tracker import PositionTracker, PositionSnapshot, DriftResult
from src.strategy import GridLevel


class TestOrderManager:
    """Tests for OrderManager class."""
    
    def test_register_and_lookup(self):
        """Test basic order registration and lookup."""
        om = OrderManager(tick_sz=0.01, px_decimals=2)
        lvl = GridLevel(side="buy", px=100.50, sz=1.0)
        
        rec = om.register(lvl, cloid="cloid123", oid=456)
        
        assert rec is not None
        assert rec.cloid == "cloid123"
        assert rec.oid == 456
        assert rec.level.px == 100.50
        assert om.open_count() == 1
    
    def test_lookup_by_cloid(self):
        """Test lookup by client order ID."""
        om = OrderManager(tick_sz=0.01, px_decimals=2)
        lvl = GridLevel(side="buy", px=100.50, sz=1.0)
        om.register(lvl, cloid="cloid123", oid=456)
        
        rec = om.lookup(cloid="cloid123")
        
        assert rec is not None
        assert rec.oid == 456
    
    def test_lookup_by_oid(self):
        """Test lookup by exchange order ID."""
        om = OrderManager(tick_sz=0.01, px_decimals=2)
        lvl = GridLevel(side="sell", px=101.50, sz=0.5)
        om.register(lvl, cloid="cloid456", oid=789)
        
        rec = om.lookup(oid=789)
        
        assert rec is not None
        assert rec.cloid == "cloid456"
    
    def test_price_key_generation(self):
        """Test price key uses Decimal for precision."""
        om = OrderManager(tick_sz=0.01, px_decimals=2)
        lvl = GridLevel(side="buy", px=100.005, sz=1.0)
        
        key = om.price_key(lvl)
        
        assert "buy:" in key
        # Verify it's using Decimal-based string
        assert "100" in key
    
    def test_pop_by_ids_removes_order(self):
        """Test pop_by_ids removes order from all indices."""
        om = OrderManager(tick_sz=0.01, px_decimals=2)
        lvl = GridLevel(side="buy", px=100.50, sz=1.0)
        om.register(lvl, cloid="cloid123", oid=456)
        
        rec = om.pop_by_ids(cloid="cloid123", oid=None)
        
        assert rec is not None
        assert om.open_count() == 0
        assert om.lookup(cloid="cloid123") is None
        assert om.lookup(oid=456) is None
    
    def test_partial_fill_keeps_order(self):
        """Test partial fill updates qty but keeps order."""
        om = OrderManager(tick_sz=0.01, px_decimals=2)
        lvl = GridLevel(side="buy", px=100.50, sz=1.0)
        om.register(lvl, cloid="cloid123", oid=456)
        
        rec = om.handle_partial_fill("cloid123", 456, 0.3)
        
        assert rec is not None
        assert rec.filled_qty == 0.3
        assert om.open_count() == 1  # Still in registry
    
    def test_full_fill_removes_order(self):
        """Test full fill removes order from registry."""
        om = OrderManager(tick_sz=0.01, px_decimals=2)
        lvl = GridLevel(side="buy", px=100.50, sz=1.0)
        om.register(lvl, cloid="cloid123", oid=456)
        
        rec = om.handle_partial_fill("cloid123", 456, 1.0)
        
        assert rec is not None
        assert om.open_count() == 0  # Removed from registry
    
    def test_clear_removes_all(self):
        """Test clear removes all orders."""
        om = OrderManager(tick_sz=0.01, px_decimals=2)
        for i in range(5):
            lvl = GridLevel(side="buy", px=100.0 + i, sz=1.0)
            om.register(lvl, cloid=f"cloid{i}", oid=i)
        
        assert om.open_count() == 5
        om.clear()
        assert om.open_count() == 0


class TestCircuitBreaker:
    """Tests for CircuitBreaker class."""
    
    def test_initial_state_not_tripped(self):
        """Test circuit starts in non-tripped state."""
        cb = CircuitBreaker(CircuitBreakerConfig(error_threshold=3))
        
        assert not cb.is_tripped
        assert cb.error_streak == 0
    
    def test_errors_below_threshold_no_trip(self):
        """Test errors below threshold don't trip."""
        cb = CircuitBreaker(CircuitBreakerConfig(error_threshold=3))
        
        cb.record_error("test", Exception("error1"))
        cb.record_error("test", Exception("error2"))
        
        assert not cb.is_tripped
        assert cb.error_streak == 2
    
    def test_errors_at_threshold_trips(self):
        """Test errors at threshold trip the circuit."""
        cb = CircuitBreaker(CircuitBreakerConfig(error_threshold=3))
        
        cb.record_error("test", Exception("error1"))
        cb.record_error("test", Exception("error2"))
        result = cb.record_error("test", Exception("error3"))
        
        assert result is True  # Returned True indicating trip
        assert cb.is_tripped
    
    def test_success_resets_streak(self):
        """Test success resets error streak."""
        cb = CircuitBreaker(CircuitBreakerConfig(error_threshold=5))
        
        cb.record_error("test", Exception("error1"))
        cb.record_error("test", Exception("error2"))
        assert cb.error_streak == 2
        
        cb.record_success()
        
        assert cb.error_streak == 0
    
    def test_cooldown_auto_reset(self):
        """Test circuit auto-resets after cooldown."""
        cb = CircuitBreaker(CircuitBreakerConfig(error_threshold=1, cooldown_sec=0.1))
        
        cb.record_error("test", Exception("error"))
        assert cb._tripped  # Use internal state to check trip
        
        # Manually set cooldown to expire
        cb._cooldown_until = time.time() - 1
        
        assert not cb.is_tripped  # Should auto-reset
    
    def test_force_trip(self):
        """Test force trip bypasses error counting."""
        cb = CircuitBreaker(CircuitBreakerConfig(error_threshold=10))
        
        cb.force_trip(cooldown_sec=1.0)
        
        assert cb.is_tripped
    
    def test_force_reset(self):
        """Test force reset immediately clears trip."""
        cb = CircuitBreaker(CircuitBreakerConfig(error_threshold=1, cooldown_sec=100))
        cb.record_error("test", Exception("error"))
        assert cb.is_tripped
        
        cb.force_reset()
        
        assert not cb.is_tripped
    
    def test_callback_on_trip(self):
        """Test on_trip callback is called."""
        trips = []
        cb = CircuitBreaker(
            CircuitBreakerConfig(error_threshold=1),
            on_trip=lambda: trips.append(True)
        )
        
        cb.record_error("test", Exception("error"))
        
        assert len(trips) == 1
    
    def test_callback_on_reset(self):
        """Test on_reset callback is called."""
        resets = []
        cb = CircuitBreaker(
            CircuitBreakerConfig(error_threshold=1, cooldown_sec=0.05),
            on_reset=lambda: resets.append(True)
        )
        
        cb.record_error("test", Exception("error"))
        # Manually expire cooldown
        cb._cooldown_until = time.time() - 1
        _ = cb.is_tripped  # Triggers auto-reset check
        
        assert len(resets) == 1


class TestFillDeduplicator:
    """Tests for FillDeduplicator class."""
    
    def test_new_fill_returns_true(self):
        """Test new fill returns True."""
        fd = FillDeduplicator(max_fills=100)
        fill = {"oid": 123, "cloid": "abc", "time": 1000, "px": 100.0, "sz": 1.0}
        
        result = fd.check_and_add(fill)
        
        assert result is True
    
    def test_duplicate_fill_returns_false(self):
        """Test duplicate fill returns False."""
        fd = FillDeduplicator(max_fills=100)
        fill = {"oid": 123, "cloid": "abc", "time": 1000, "px": 100.0, "sz": 1.0}
        
        fd.check_and_add(fill)
        result = fd.check_and_add(fill)
        
        assert result is False
    
    def test_different_fills_both_new(self):
        """Test different fills are both detected as new."""
        fd = FillDeduplicator(max_fills=100)
        fill1 = {"oid": 123, "cloid": "abc", "time": 1000, "px": 100.0, "sz": 1.0}
        fill2 = {"oid": 124, "cloid": "def", "time": 1001, "px": 101.0, "sz": 0.5}
        
        result1 = fd.check_and_add(fill1)
        result2 = fd.check_and_add(fill2)
        
        assert result1 is True
        assert result2 is True
        assert fd.size() == 2
    
    def test_bounded_eviction(self):
        """Test old fills are evicted when capacity reached."""
        fd = FillDeduplicator(max_fills=3)
        
        for i in range(5):
            fill = {"oid": i, "cloid": f"c{i}", "time": 1000 + i, "px": 100.0, "sz": 1.0}
            fd.check_and_add(fill)
        
        assert fd.size() == 3
        # First two should have been evicted
        assert fd.get_stats()["evictions"] == 2
    
    def test_make_fill_key_consistency(self):
        """Test fill key is consistent for same fill."""
        fill = {"oid": 123, "cloid": "abc", "time": 1000, "px": 100.0, "sz": 1.0}
        
        key1 = FillDeduplicator.make_fill_key(fill)
        key2 = FillDeduplicator.make_fill_key(fill)
        
        assert key1 == key2
    
    def test_clear_removes_all(self):
        """Test clear removes all tracked fills."""
        fd = FillDeduplicator(max_fills=100)
        for i in range(10):
            fill = {"oid": i, "cloid": f"c{i}", "time": 1000 + i, "px": 100.0, "sz": 1.0}
            fd.check_and_add(fill)
        
        assert fd.size() == 10
        fd.clear()
        assert fd.size() == 0
    
    def test_stats_tracking(self):
        """Test stats are tracked correctly."""
        fd = FillDeduplicator(max_fills=100)
        fill = {"oid": 123, "cloid": "abc", "time": 1000, "px": 100.0, "sz": 1.0}
        
        fd.check_and_add(fill)
        fd.check_and_add(fill)  # Duplicate
        
        stats = fd.get_stats()
        assert stats["processed"] == 1
        assert stats["duplicates"] == 1
    
    def test_resize_evicts_old(self):
        """Test resize evicts oldest entries if needed."""
        fd = FillDeduplicator(max_fills=10)
        for i in range(10):
            fill = {"oid": i, "cloid": f"c{i}", "time": 1000 + i, "px": 100.0, "sz": 1.0}
            fd.check_and_add(fill)
        
        assert fd.size() == 10
        fd.resize(5)
        assert fd.size() == 5


class TestPositionTracker:
    """Tests for PositionTracker class."""
    
    def test_initial_state(self):
        """Test initial state is zero."""
        pt = PositionTracker()
        
        assert pt.position == 0.0
        assert pt.realized_pnl == 0.0
    
    def test_initial_values(self):
        """Test can set initial values."""
        pt = PositionTracker(initial_position=10.5, initial_realized_pnl=100.0)
        
        assert pt.position == 10.5
        assert pt.realized_pnl == 100.0
    
    def test_position_setter(self):
        """Test position setter."""
        pt = PositionTracker()
        pt.position = 5.0
        
        assert pt.position == 5.0
    
    @pytest.mark.asyncio
    async def test_get_snapshot(self):
        """Test atomic snapshot retrieval."""
        pt = PositionTracker(initial_position=10.0, initial_realized_pnl=50.0)
        
        snapshot = await pt.get_snapshot()
        
        assert isinstance(snapshot, PositionSnapshot)
        assert snapshot.position == 10.0
        assert snapshot.realized_pnl == 50.0
    
    @pytest.mark.asyncio
    async def test_update_from_buy_fill(self):
        """Test position update from buy fill."""
        pt = PositionTracker(initial_position=0.0)
        
        pnl = await pt.update_from_fill(
            side="buy",
            size=1.0,
            fill_px=100.0,
            order_px=99.0,
            timestamp_ms=1000
        )
        
        assert pt.position == 1.0
        assert pnl == 1.0  # (100 - 99) * 1.0
        assert pt.realized_pnl == 1.0
    
    @pytest.mark.asyncio
    async def test_update_from_sell_fill(self):
        """Test position update from sell fill."""
        pt = PositionTracker(initial_position=1.0)
        
        pnl = await pt.update_from_fill(
            side="sell",
            size=1.0,
            fill_px=101.0,
            order_px=100.0,
            timestamp_ms=1000
        )
        
        assert pt.position == 0.0
        assert pnl == -1.0  # (100 - 101) * 1.0
        assert pt.realized_pnl == -1.0
    
    @pytest.mark.asyncio
    async def test_set_position_atomic(self):
        """Test atomic position set."""
        pt = PositionTracker(initial_position=0.0)
        
        await pt.set_position(15.5)
        
        assert pt.position == 15.5
    
    @pytest.mark.asyncio
    async def test_check_drift_no_change(self):
        """Test drift check with no change."""
        pt = PositionTracker(initial_position=10.0)
        
        result = await pt.check_drift(10.0)
        
        assert isinstance(result, DriftResult)
        assert not result.has_drift
        assert result.drift_amount == 0.0
    
    @pytest.mark.asyncio
    async def test_check_drift_with_change(self):
        """Test drift check with position change."""
        pt = PositionTracker(initial_position=10.0)
        pt.position = 12.0  # Simulate position change
        
        result = await pt.check_drift(10.0)
        
        assert result.has_drift
        assert result.drift_amount == 2.0
        assert result.drift_pct == 0.2  # 2.0 / 10.0
        assert result.snapshot_position == 10.0
        assert result.current_position == 12.0
    
    @pytest.mark.asyncio
    async def test_drift_significant_threshold(self):
        """Test significant drift threshold (>10%)."""
        pt = PositionTracker(initial_position=10.0)
        pt.position = 12.0  # 20% drift
        
        result = await pt.check_drift(10.0)
        
        assert result.is_significant
        
        pt.position = 10.5  # 5% drift
        result2 = await pt.check_drift(10.0)
        
        assert not result2.is_significant
    
    def test_calculate_skew_ratio(self):
        """Test skew ratio calculation."""
        pt = PositionTracker(initial_position=5.0)
        
        ratio = pt.calculate_skew_ratio(target_position=10.0)
        
        assert ratio == 0.5
    
    def test_calculate_skew_ratio_zero_target(self):
        """Test skew ratio with zero target."""
        pt = PositionTracker(initial_position=5.0)
        
        ratio = pt.calculate_skew_ratio(target_position=0.0)
        
        assert ratio == 0.0
    
    def test_get_disallowed_side_long(self):
        """Test disallowed side when long."""
        pt = PositionTracker(initial_position=5.0)
        
        assert pt.get_disallowed_side() == "buy"
    
    def test_get_disallowed_side_short(self):
        """Test disallowed side when short."""
        pt = PositionTracker(initial_position=-5.0)
        
        assert pt.get_disallowed_side() == "sell"
    
    def test_get_disallowed_side_flat(self):
        """Test disallowed side when flat."""
        pt = PositionTracker(initial_position=0.0)
        
        assert pt.get_disallowed_side() is None
    
    def test_get_reduce_side_long(self):
        """Test reduce side when long."""
        pt = PositionTracker(initial_position=5.0)
        
        assert pt.get_reduce_side() == "sell"
    
    def test_get_reduce_side_short(self):
        """Test reduce side when short."""
        pt = PositionTracker(initial_position=-5.0)
        
        assert pt.get_reduce_side() == "buy"
    
    def test_get_state(self):
        """Test state retrieval."""
        pt = PositionTracker(initial_position=10.0, initial_realized_pnl=50.0)
        
        state = pt.get_state()
        
        assert state["position"] == 10.0
        assert state["realized_pnl"] == 50.0
    
    def test_load_state(self):
        """Test state loading."""
        pt = PositionTracker()
        state = {"position": 15.0, "realized_pnl": 75.0, "last_update_ms": 1000}
        
        pt.load_state(state)
        
        assert pt.position == 15.0
        assert pt.realized_pnl == 75.0


# ============================================================================
# Struct-5: GridCalculator Tests
# ============================================================================

class TestGridCalculator:
    """Test suite for GridCalculator module."""
    
    @pytest.fixture
    def mock_cfg(self):
        """Create a minimal mock config for GridCalculator."""
        cfg = MagicMock()
        cfg.grids = 5
        cfg.leverage = 2.0
        cfg.investment_usd = 1000.0
        cfg.random_size_jitter = 0.01
        cfg.reprice_tick_threshold = 2
        # Strategy-specific config
        cfg.atr_len = 14
        cfg.ewma_alpha = 0.1
        cfg.init_vol_multiplier = 1.0
        cfg.trend_fast_ema = 8
        cfg.trend_slow_ema = 21
        cfg.base_spacing_pct = 0.005
        cfg.max_spacing_pct = 0.02
        cfg.skew_soft = 0.5
        # Risk config
        cfg.max_position_usd = 10000.0
        cfg.max_order_usd = 1000.0
        cfg.max_drawdown_usd = 500.0
        cfg.max_drawdown_pct = 0.10  # 10% max drawdown
        return cfg
    
    @pytest.fixture
    def calculator(self, mock_cfg):
        """Create a GridCalculator instance."""
        from src.grid_calculator import GridCalculator
        return GridCalculator(
            cfg=mock_cfg,
            tick_sz=0.01,
            px_decimals=2,
            sz_decimals=3,
            effective_grids=5,
            effective_investment_usd=1000.0
        )
    
    def test_price_key_generation(self, calculator):
        """Test price_key generates consistent keys."""
        # Same price should produce same key
        key1 = calculator.price_key("buy", 100.12)
        key2 = calculator.price_key("buy", 100.12)
        assert key1 == key2
        
        # Different sides should produce different keys
        buy_key = calculator.price_key("buy", 100.12)
        sell_key = calculator.price_key("sell", 100.12)
        assert buy_key != sell_key
        assert "buy:" in buy_key
        assert "sell:" in sell_key
    
    def test_price_key_decimal_precision(self, calculator):
        """Test price_key handles decimal precision correctly."""
        # These should be the same key (to 2 decimal places)
        key1 = calculator.price_key("buy", 100.123456)
        key2 = calculator.price_key("buy", 100.12)
        assert key1 == key2
    
    def test_calculate_order_size(self, calculator):
        """Test base order size calculation."""
        # With 1000 USD investment, 2x leverage, 5 grids per side = 10 total
        # Notional per order = (1000 * 2) / 10 = 200 USD
        # At mid = 100, size = 200 / 100 = 2.0
        size = calculator.calculate_order_size(mid=100.0)
        assert size == 2.0
    
    def test_calculate_order_size_rounds_down(self, calculator):
        """Test order size rounds down to sz_decimals."""
        # At mid = 30, notional = 200, size = 6.666...
        # Should round down to 6.666 (3 decimals)
        size = calculator.calculate_order_size(mid=30.0)
        assert size == 6.666
    
    def test_build_filtered_levels_basic(self, calculator, mock_cfg):
        """Test build_filtered_levels produces levels."""
        from src.strategy import GridStrategy
        
        strategy = GridStrategy(mock_cfg, tick_sz=0.01, px_decimals=2, sz_decimals=3)
        
        # Use mock risk that always allows orders
        mock_risk = MagicMock()
        mock_risk.allow_order = MagicMock(return_value=True)
        
        result = calculator.build_filtered_levels(
            strategy=strategy,
            risk=mock_risk,
            mid=100.0,
            position=0.0,
            base_size=1.0,
            flatten_mode=False
        )
        
        assert result.levels is not None
        assert result.spacing > 0
        assert result.per_order_size == 1.0
    
    def test_build_filtered_levels_flatten_mode(self, calculator, mock_cfg):
        """Test flatten_mode filters to reduce-only orders."""
        from src.strategy import GridStrategy
        
        strategy = GridStrategy(mock_cfg, tick_sz=0.01, px_decimals=2, sz_decimals=3)
        
        # Use mock risk that always allows orders
        mock_risk = MagicMock()
        mock_risk.allow_order = MagicMock(return_value=True)
        
        # With positive position, flatten mode should only allow sells
        result = calculator.build_filtered_levels(
            strategy=strategy,
            risk=mock_risk,
            mid=100.0,
            position=5.0,  # Long position
            base_size=1.0,
            flatten_mode=True
        )
        
        for lvl in result.levels:
            assert lvl.side == "sell", "Flatten mode with long should only allow sells"
    
    def test_compute_grid_diff_new_levels(self, calculator):
        """Test grid diff with all new levels."""
        from src.strategy import GridLevel
        
        levels = [
            GridLevel("buy", 99.0, 1.0),
            GridLevel("sell", 101.0, 1.0)
        ]
        
        diff = calculator.compute_grid_diff(
            desired_levels=levels,
            existing_keys=set(),
            orders_by_price={},
            reprice_tick_threshold=2
        )
        
        assert len(diff.to_cancel_keys) == 0
        assert len(diff.to_place) == 2
    
    def test_compute_grid_diff_cancel_stale(self, calculator):
        """Test grid diff cancels stale orders."""
        from src.strategy import GridLevel
        from src.order_manager import ActiveOrder
        
        # Existing order at 98.0 not in desired
        existing_order = MagicMock()
        existing_order.level = GridLevel("buy", 98.0, 1.0)
        
        orders_by_price = {"buy:98.00": existing_order}
        existing_keys = {"buy:98.00"}
        
        # Desired at different price
        desired = [GridLevel("buy", 99.0, 1.0)]
        
        diff = calculator.compute_grid_diff(
            desired_levels=desired,
            existing_keys=existing_keys,
            orders_by_price=orders_by_price,
            reprice_tick_threshold=2
        )
        
        assert "buy:98.00" in diff.to_cancel_keys
    
    def test_compute_grid_diff_preserve_queue(self, calculator):
        """Test grid diff preserves orders within tick threshold."""
        from src.strategy import GridLevel
        
        # Existing order
        existing_order = MagicMock()
        existing_order.level = GridLevel("buy", 99.00, 1.0)
        
        orders_by_price = {"buy:99.00": existing_order}
        existing_keys = {"buy:99.00"}
        
        # Desired at same price (tick_move = 0, within threshold)
        desired = [GridLevel("buy", 99.00, 1.0)]
        
        diff = calculator.compute_grid_diff(
            desired_levels=desired,
            existing_keys=existing_keys,
            orders_by_price=orders_by_price,
            reprice_tick_threshold=2
        )
        
        # Should not cancel or place - preserve existing
        assert "buy:99.00" not in diff.to_cancel_keys
        assert len([l for l in diff.to_place if l.px == 99.00]) == 0
    
    def test_calculate_replacement_level_buy_fill(self, calculator, mock_cfg):
        """Test replacement level after buy fill."""
        from src.strategy import GridStrategy
        
        strategy = GridStrategy(mock_cfg, tick_sz=0.01, px_decimals=2, sz_decimals=3)
        strategy.on_price(100.0)  # Initialize
        
        lvl = calculator.calculate_replacement_level(
            strategy=strategy,
            fill_side="buy",
            fill_px=99.0,
            fill_sz=1.0,
            position=1.0,
            flatten_mode=False
        )
        
        assert lvl is not None
        assert lvl.side == "sell"  # Opposite of fill
        assert lvl.px > 99.0  # Above fill price
    
    def test_calculate_replacement_level_sell_fill(self, calculator, mock_cfg):
        """Test replacement level after sell fill."""
        from src.strategy import GridStrategy
        
        strategy = GridStrategy(mock_cfg, tick_sz=0.01, px_decimals=2, sz_decimals=3)
        strategy.on_price(100.0)
        
        lvl = calculator.calculate_replacement_level(
            strategy=strategy,
            fill_side="sell",
            fill_px=101.0,
            fill_sz=1.0,
            position=-1.0,
            flatten_mode=False
        )
        
        assert lvl is not None
        assert lvl.side == "buy"  # Opposite of fill
        assert lvl.px < 101.0  # Below fill price
    
    def test_calculate_replacement_level_flatten_blocks(self, calculator, mock_cfg):
        """Test replacement returns None when flatten mode blocks."""
        from src.strategy import GridStrategy
        
        strategy = GridStrategy(mock_cfg, tick_sz=0.01, px_decimals=2, sz_decimals=3)
        strategy.on_price(100.0)
        
        # Long position, buy fill would want to place sell
        # But if position is negative, flatten mode blocks buys
        lvl = calculator.calculate_replacement_level(
            strategy=strategy,
            fill_side="sell",  # Would want to place buy
            fill_px=101.0,
            fill_sz=1.0,
            position=5.0,  # Long position - buy blocked in flatten
            flatten_mode=True
        )
        
        assert lvl is None
    
    def test_quantize_level(self, calculator):
        """Test level price quantization."""
        from src.strategy import GridLevel
        
        lvl = GridLevel("buy", 99.123, 1.0)
        result = calculator.quantize_level(lvl)
        
        # Should be quantized to tick boundary
        assert result.px == 99.13  # ceil for buy
    
    def test_get_state(self, calculator):
        """Test state serialization."""
        state = calculator.get_state()
        
        assert "tick_sz" in state
        assert "px_decimals" in state
        assert "sz_decimals" in state
        assert "effective_grids" in state
        assert "effective_investment_usd" in state
        assert state["tick_sz"] == 0.01
        assert state["effective_grids"] == 5
    
    def test_update_config(self, calculator):
        """Test config update."""
        calculator.update_config(effective_grids=10, effective_investment_usd=2000.0)
        
        assert calculator.effective_grids == 10
        assert calculator.effective_investment_usd == 2000.0
    
    def test_update_config_partial(self, calculator):
        """Test partial config update."""
        original_investment = calculator.effective_investment_usd
        calculator.update_config(effective_grids=10)
        
        assert calculator.effective_grids == 10
        assert calculator.effective_investment_usd == original_investment


# ============================================================================
# Struct-6: OrderStateMachine Tests
# ============================================================================

class TestOrderStateMachine:
    """Test suite for OrderStateMachine module."""
    
    @pytest.fixture
    def state_machine(self):
        """Create an OrderStateMachine instance."""
        from src.order_state_machine import OrderStateMachine
        return OrderStateMachine()
    
    def test_create_order_pending_state(self, state_machine):
        """Test that new orders start in PENDING state."""
        from src.order_state_machine import OrderState
        
        record = state_machine.create_order(
            cloid="test-001",
            oid=None,
            side="buy",
            price=100.0,
            qty=1.0,
        )
        
        assert record.state == OrderState.PENDING
        assert record.cloid == "test-001"
        assert record.original_qty == 1.0
        assert record.filled_qty == 0.0
    
    def test_lookup_by_cloid(self, state_machine):
        """Test order lookup by client order ID."""
        state_machine.create_order("cloid-1", None, "buy", 100.0, 1.0)
        
        record = state_machine.get_order(cloid="cloid-1")
        assert record is not None
        assert record.cloid == "cloid-1"
    
    def test_lookup_by_oid(self, state_machine):
        """Test order lookup by exchange order ID."""
        state_machine.create_order("cloid-1", 12345, "sell", 105.0, 2.0)
        
        record = state_machine.get_order(oid=12345)
        assert record is not None
        assert record.oid == 12345
    
    def test_update_oid(self, state_machine):
        """Test updating exchange order ID after creation."""
        state_machine.create_order("cloid-1", None, "buy", 100.0, 1.0)
        
        success = state_machine.update_oid("cloid-1", 54321)
        assert success
        
        record = state_machine.get_order(oid=54321)
        assert record is not None
        assert record.cloid == "cloid-1"
    
    def test_acknowledge_transition(self, state_machine):
        """Test PENDING -> OPEN transition."""
        from src.order_state_machine import OrderState
        
        state_machine.create_order("cloid-1", 100, "buy", 100.0, 1.0)
        
        success = state_machine.acknowledge(cloid="cloid-1")
        assert success
        
        record = state_machine.get_order(cloid="cloid-1")
        assert record.state == OrderState.OPEN
    
    def test_partial_fill_transition(self, state_machine):
        """Test OPEN -> PARTIALLY_FILLED transition."""
        from src.order_state_machine import OrderState
        
        state_machine.create_order("cloid-1", 100, "buy", 100.0, 10.0)
        state_machine.acknowledge(cloid="cloid-1")
        
        success = state_machine.fill(cloid="cloid-1", fill_qty=3.0, is_complete=False)
        assert success
        
        record = state_machine.get_order(cloid="cloid-1")
        assert record.state == OrderState.PARTIALLY_FILLED
        assert record.filled_qty == 3.0
        assert record.remaining_qty == 7.0
    
    def test_full_fill_transition(self, state_machine):
        """Test OPEN -> FILLED transition."""
        from src.order_state_machine import OrderState
        
        state_machine.create_order("cloid-1", 100, "buy", 100.0, 10.0)
        state_machine.acknowledge(cloid="cloid-1")
        
        success = state_machine.fill(cloid="cloid-1", fill_qty=10.0, is_complete=True)
        assert success
        
        record = state_machine.get_order(cloid="cloid-1")
        assert record.state == OrderState.FILLED
        assert record.filled_qty == 10.0
        assert record.remaining_qty == 0.0
    
    def test_cancel_transition(self, state_machine):
        """Test OPEN -> CANCELLED transition."""
        from src.order_state_machine import OrderState
        
        state_machine.create_order("cloid-1", 100, "buy", 100.0, 1.0)
        state_machine.acknowledge(cloid="cloid-1")
        
        success = state_machine.cancel(cloid="cloid-1", reason="user_request")
        assert success
        
        record = state_machine.get_order(cloid="cloid-1")
        assert record.state == OrderState.CANCELLED
    
    def test_reject_transition(self, state_machine):
        """Test PENDING -> REJECTED transition."""
        from src.order_state_machine import OrderState
        
        state_machine.create_order("cloid-1", None, "buy", 100.0, 1.0)
        
        success = state_machine.reject(
            cloid="cloid-1",
            error_code="INSUFFICIENT_MARGIN",
            error_message="Not enough margin",
        )
        assert success
        
        record = state_machine.get_order(cloid="cloid-1")
        assert record.state == OrderState.REJECTED
        assert record.error_code == "INSUFFICIENT_MARGIN"
    
    def test_invalid_transition_blocked(self, state_machine):
        """Test that invalid transitions are blocked."""
        from src.order_state_machine import OrderState
        
        state_machine.create_order("cloid-1", 100, "buy", 100.0, 1.0)
        state_machine.acknowledge(cloid="cloid-1")
        state_machine.fill(cloid="cloid-1", fill_qty=1.0, is_complete=True)
        
        # FILLED -> CANCELLED should be invalid
        success = state_machine.cancel(cloid="cloid-1")
        assert not success
        
        record = state_machine.get_order(cloid="cloid-1")
        assert record.state == OrderState.FILLED  # Unchanged
    
    def test_terminal_state_check(self, state_machine):
        """Test is_terminal for various states."""
        state_machine.create_order("cloid-1", 100, "buy", 100.0, 1.0)
        
        assert not state_machine.is_terminal(cloid="cloid-1")  # PENDING
        
        state_machine.acknowledge(cloid="cloid-1")
        assert not state_machine.is_terminal(cloid="cloid-1")  # OPEN
        
        state_machine.fill(cloid="cloid-1", fill_qty=1.0, is_complete=True)
        assert state_machine.is_terminal(cloid="cloid-1")  # FILLED
    
    def test_active_orders(self, state_machine):
        """Test get_active_orders returns only non-terminal orders."""
        state_machine.create_order("cloid-1", 100, "buy", 100.0, 1.0)
        state_machine.create_order("cloid-2", 101, "sell", 105.0, 1.0)
        
        state_machine.acknowledge(cloid="cloid-1")
        state_machine.fill(cloid="cloid-2", fill_qty=1.0, is_complete=True)
        
        active = state_machine.get_active_orders()
        assert len(active) == 1
        assert active[0].cloid == "cloid-1"
    
    def test_transition_history(self, state_machine):
        """Test that transition history is recorded."""
        state_machine.create_order("cloid-1", 100, "buy", 100.0, 5.0)
        state_machine.acknowledge(cloid="cloid-1")
        state_machine.fill(cloid="cloid-1", fill_qty=2.0, is_complete=False)
        state_machine.fill(cloid="cloid-1", fill_qty=3.0, is_complete=True)
        
        record = state_machine.get_order(cloid="cloid-1")
        assert len(record.transitions) == 3
        assert record.transitions[0].to_state.name == "OPEN"
        assert record.transitions[1].to_state.name == "PARTIALLY_FILLED"
        assert record.transitions[2].to_state.name == "FILLED"
    
    def test_stats_tracking(self, state_machine):
        """Test statistics are tracked correctly."""
        state_machine.create_order("cloid-1", 100, "buy", 100.0, 1.0)
        state_machine.create_order("cloid-2", 101, "sell", 105.0, 1.0)
        state_machine.create_order("cloid-3", None, "buy", 98.0, 1.0)
        
        state_machine.acknowledge(cloid="cloid-1")
        state_machine.fill(cloid="cloid-1", fill_qty=1.0, is_complete=True)
        
        state_machine.acknowledge(cloid="cloid-2")
        state_machine.cancel(cloid="cloid-2")
        
        state_machine.reject(cloid="cloid-3")
        
        stats = state_machine.get_stats()
        assert stats["total_created"] == 3
        assert stats["total_filled"] == 1
        assert stats["total_cancelled"] == 1
        assert stats["total_rejected"] == 1
    
    def test_remove_order(self, state_machine):
        """Test order removal."""
        state_machine.create_order("cloid-1", 100, "buy", 100.0, 1.0)
        
        success = state_machine.remove_order(cloid="cloid-1")
        assert success
        
        record = state_machine.get_order(cloid="cloid-1")
        assert record is None
    
    def test_state_change_callback(self):
        """Test on_state_change callback is fired."""
        from src.order_state_machine import OrderStateMachine, OrderState
        
        callback_calls = []
        
        def on_change(record, from_state, to_state):
            callback_calls.append((record.cloid, from_state, to_state))
        
        sm = OrderStateMachine(on_state_change=on_change)
        sm.create_order("cloid-1", 100, "buy", 100.0, 1.0)
        sm.acknowledge(cloid="cloid-1")
        sm.fill(cloid="cloid-1", fill_qty=1.0, is_complete=True)
        
        assert len(callback_calls) == 2
        assert callback_calls[0] == ("cloid-1", OrderState.PENDING, OrderState.OPEN)
        assert callback_calls[1] == ("cloid-1", OrderState.OPEN, OrderState.FILLED)
    
    def test_immediate_fill_from_pending(self, state_machine):
        """Test PENDING -> FILLED for market orders."""
        from src.order_state_machine import OrderState
        
        state_machine.create_order("cloid-1", 100, "buy", 100.0, 1.0)
        
        # Market orders can fill immediately without going through OPEN
        success = state_machine.fill(cloid="cloid-1", fill_qty=1.0, is_complete=True)
        assert success
        
        record = state_machine.get_order(cloid="cloid-1")
        assert record.state == OrderState.FILLED
    
    def test_expire_transition(self, state_machine):
        """Test PENDING -> EXPIRED transition."""
        from src.order_state_machine import OrderState
        
        state_machine.create_order("cloid-1", None, "buy", 100.0, 1.0)
        
        success = state_machine.expire(cloid="cloid-1", reason="ack_timeout")
        assert success
        
        record = state_machine.get_order(cloid="cloid-1")
        assert record.state == OrderState.EXPIRED
    
    def test_multiple_partial_fills(self, state_machine):
        """Test multiple partial fills accumulate correctly."""
        from src.order_state_machine import OrderState
        
        state_machine.create_order("cloid-1", 100, "buy", 100.0, 10.0)
        state_machine.acknowledge(cloid="cloid-1")
        
        state_machine.fill(cloid="cloid-1", fill_qty=3.0, is_complete=False)
        state_machine.fill(cloid="cloid-1", fill_qty=4.0, is_complete=False)
        state_machine.fill(cloid="cloid-1", fill_qty=3.0, is_complete=True)
        
        record = state_machine.get_order(cloid="cloid-1")
        assert record.state == OrderState.FILLED
        assert record.filled_qty == 10.0
        assert record.remaining_qty == 0.0