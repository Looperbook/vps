"""
Tests for GridBuilder - grid construction and placement orchestration.

Tests cover:
- Full grid rebuild
- Level replacement after fills
- Position drift detection
- Flatten mode handling
- Metrics emission
"""

import asyncio
import pytest
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set
from unittest.mock import AsyncMock, MagicMock

from src.execution.grid_builder import (
    GridBuilder,
    GridBuilderConfig,
    GridRebuildResult,
    ReplacementResult,
)


@dataclass
class GridLevel:
    """Simple GridLevel for testing."""
    side: str
    px: float
    sz: float


@dataclass
class MockGridBuildResult:
    """Mock grid build result."""
    spacing: float = 0.01
    levels: List[GridLevel] = field(default_factory=list)
    buy_levels: List[GridLevel] = field(default_factory=list)
    sell_levels: List[GridLevel] = field(default_factory=list)


@dataclass
class MockGridDiff:
    """Mock grid diff result."""
    to_cancel_keys: Set[str] = field(default_factory=set)
    to_place: List[GridLevel] = field(default_factory=list)


class MockGridCalculator:
    """Mock grid calculator."""
    def __init__(self):
        self.cfg = MagicMock()
        self.cfg.reprice_tick_threshold = 2
        self._order_size = 1.0
        self._build_result = MockGridBuildResult()
        self._grid_diff = MockGridDiff()
        self._replacement_level = None
    
    def calculate_order_size(self, mid: float) -> float:
        return self._order_size
    
    def build_filtered_levels(self, strategy, risk, mid, position, base_size, flatten_mode) -> MockGridBuildResult:
        return self._build_result
    
    def compute_grid_diff(self, desired_levels, existing_keys, orders_by_price, reprice_tick_threshold) -> MockGridDiff:
        return self._grid_diff
    
    def calculate_replacement_level(self, strategy, fill_side, fill_px, fill_sz, position, flatten_mode) -> Optional[GridLevel]:
        return self._replacement_level


class MockStrategy:
    """Mock strategy."""
    def __init__(self):
        self.grid_center = 100.0
        self.last_vol = 0.02
        self.last_atr = 1.5
    
    def on_price(self, mid: float) -> None:
        self.grid_center = mid
    
    def trend_bias(self, mid: float) -> float:
        return 0.0


class MockRiskEngine:
    """Mock risk engine."""
    def __init__(self):
        self._allow = True
    
    def allow_order(self, side: str, sz: float, px: float) -> bool:
        return self._allow


@dataclass
class MockOrderManager:
    """Mock order manager."""
    orders_by_price: Dict[str, Any] = field(default_factory=dict)
    
    def open_count(self) -> int:
        return len(self.orders_by_price)


@dataclass
class MockSubmitResult:
    """Mock submit result."""
    success: bool = True
    error: Optional[str] = None


class MockExecutionGateway:
    """Mock execution gateway."""
    def __init__(self):
        self.order_manager = MockOrderManager()
        self.flatten_mode = False
        self.submit_results: List[MockSubmitResult] = []
        self.submitted_levels: List[GridLevel] = []
        self.cancelled_orders: List[Dict] = []
    
    def open_count(self) -> int:
        return self.order_manager.open_count()
    
    async def submit_level(self, level: GridLevel, mode: str = "", position: float = 0.0) -> MockSubmitResult:
        self.submitted_levels.append(level)
        if self.submit_results:
            return self.submit_results.pop(0)
        return MockSubmitResult(success=True)
    
    async def submit_levels_batch(self, levels: List[GridLevel], mode: str = "", position: float = 0.0) -> List[MockSubmitResult]:
        self.submitted_levels.extend(levels)
        results = []
        for lvl in levels:
            if self.submit_results:
                results.append(self.submit_results.pop(0))
            else:
                results.append(MockSubmitResult(success=True))
        return results
    
    async def cancel_order(self, cloid: str = None, oid: int = None, reason: str = "") -> None:
        self.cancelled_orders.append({"cloid": cloid, "oid": oid, "reason": reason})


class MockGauge:
    """Mock Prometheus gauge."""
    def __init__(self):
        self._value = 0.0
    
    def set(self, value: float) -> None:
        self._value = value
    
    def labels(self, **kwargs) -> "MockGauge":
        return self


@dataclass
class MockRichMetrics:
    """Mock rich metrics."""
    grid_width_pct: MockGauge = field(default_factory=MockGauge)
    grid_center: MockGauge = field(default_factory=MockGauge)
    trend_bias: MockGauge = field(default_factory=MockGauge)
    volatility_estimate: MockGauge = field(default_factory=MockGauge)
    atr_value: MockGauge = field(default_factory=MockGauge)


@pytest.fixture
def mock_deps():
    """Create mock dependencies."""
    return {
        "grid_calculator": MockGridCalculator(),
        "strategy": MockStrategy(),
        "risk_engine": MockRiskEngine(),
        "execution_gateway": MockExecutionGateway(),
        "rich_metrics": MockRichMetrics(),
    }


@pytest.fixture
def grid_builder(mock_deps):
    """Create GridBuilder with mocks."""
    config = GridBuilderConfig(drift_abort_threshold=0.10)
    return GridBuilder(
        coin="TEST",
        grid_calculator=mock_deps["grid_calculator"],
        strategy=mock_deps["strategy"],
        risk_engine=mock_deps["risk_engine"],
        execution_gateway=mock_deps["execution_gateway"],
        rich_metrics=mock_deps["rich_metrics"],
        config=config,
    ), mock_deps


class TestGridBuilderRebuild:
    """Grid rebuild tests."""
    
    @pytest.mark.asyncio
    async def test_build_and_place_grid_success(self, grid_builder):
        """Test successful grid rebuild."""
        builder, deps = grid_builder
        
        # Set up levels to place
        levels = [
            GridLevel(side="buy", px=99.0, sz=1.0),
            GridLevel(side="sell", px=101.0, sz=1.0),
        ]
        deps["grid_calculator"]._build_result = MockGridBuildResult(
            spacing=0.01,
            levels=levels,
        )
        deps["grid_calculator"]._grid_diff = MockGridDiff(
            to_place=levels,
        )
        
        position_lock = asyncio.Lock()
        current_position = 0.0
        
        result = await builder.build_and_place_grid(
            mid=100.0,
            position=current_position,
            position_lock=position_lock,
            get_current_position=lambda: current_position,
        )
        
        assert result.success is True
        assert result.mid == 100.0
        assert result.levels_placed == 2
        assert result.spacing == 0.01
        
    @pytest.mark.asyncio
    async def test_build_grid_invalid_mid(self, grid_builder):
        """Test grid rebuild with invalid mid price."""
        builder, deps = grid_builder
        
        position_lock = asyncio.Lock()
        
        result = await builder.build_and_place_grid(
            mid=0.0,
            position=0.0,
            position_lock=position_lock,
            get_current_position=lambda: 0.0,
        )
        
        assert result.success is False
        assert result.aborted_reason == "invalid_mid"
        
    @pytest.mark.asyncio
    async def test_build_grid_position_drift_abort(self, grid_builder):
        """Test grid rebuild aborted on position drift."""
        builder, deps = grid_builder
        
        position_lock = asyncio.Lock()
        
        # Initial position is 10.0, but position changes to 15.0 during rebuild
        # That's a 50% drift (5.0 / 10.0)
        initial_position = 10.0
        drifted_position = 15.0  # 50% drift
        
        result = await builder.build_and_place_grid(
            mid=100.0,
            position=initial_position,
            position_lock=position_lock,
            get_current_position=lambda: drifted_position,  # Position changed
        )
        
        assert result.success is False
        assert result.aborted_reason == "position_drift"
        assert builder.rebuild_needed is True
        
    @pytest.mark.asyncio
    async def test_build_grid_cancels_stale_orders(self, grid_builder):
        """Test grid rebuild cancels orders no longer needed."""
        builder, deps = grid_builder
        
        # Set up existing orders
        deps["execution_gateway"].order_manager.orders_by_price = {
            "buy:98.0": MagicMock(cloid="c1", oid=1001),
        }
        
        # Grid diff says to cancel the old order
        deps["grid_calculator"]._grid_diff = MockGridDiff(
            to_cancel_keys={"buy:98.0"},
            to_place=[GridLevel(side="buy", px=99.0, sz=1.0)],
        )
        
        position_lock = asyncio.Lock()
        
        result = await builder.build_and_place_grid(
            mid=100.0,
            position=0.0,
            position_lock=position_lock,
            get_current_position=lambda: 0.0,
        )
        
        assert result.levels_cancelled == 1
        assert len(deps["execution_gateway"].cancelled_orders) == 1


class TestGridBuilderReplacement:
    """Level replacement tests."""
    
    @pytest.mark.asyncio
    async def test_replace_after_fill_success(self, grid_builder):
        """Test successful level replacement."""
        builder, deps = grid_builder
        
        replacement = GridLevel(side="sell", px=102.0, sz=1.0)
        deps["grid_calculator"]._replacement_level = replacement
        
        result = await builder.replace_after_fill(
            fill_side="buy",
            fill_px=100.0,
            fill_sz=1.0,
            position=1.0,
        )
        
        assert result.success is True
        assert result.level == replacement
        assert len(deps["execution_gateway"].submitted_levels) == 1
        
    @pytest.mark.asyncio
    async def test_replace_after_fill_flatten_blocks(self, grid_builder):
        """Test replacement blocked in flatten mode."""
        builder, deps = grid_builder
        builder.config.flatten_mode = True
        
        # Calculator returns None in flatten mode
        deps["grid_calculator"]._replacement_level = None
        
        result = await builder.replace_after_fill(
            fill_side="buy",
            fill_px=100.0,
            fill_sz=1.0,
            position=1.0,
        )
        
        assert result.success is False
        assert result.skipped_reason == "flatten_mode"
        
    @pytest.mark.asyncio
    async def test_replace_after_fill_risk_blocks(self, grid_builder):
        """Test replacement blocked by risk engine."""
        builder, deps = grid_builder
        
        replacement = GridLevel(side="buy", px=99.0, sz=1.0)
        deps["grid_calculator"]._replacement_level = replacement
        deps["risk_engine"]._allow = False
        
        result = await builder.replace_after_fill(
            fill_side="sell",
            fill_px=100.0,
            fill_sz=1.0,
            position=-1.0,
        )
        
        assert result.success is False
        assert result.skipped_reason == "risk_blocked"
        assert result.level == replacement


class TestGridBuilderState:
    """State management tests."""
    
    @pytest.mark.asyncio
    async def test_rebuild_lock_serializes(self, grid_builder):
        """Test rebuild lock prevents concurrent rebuilds."""
        builder, deps = grid_builder
        
        # Start a rebuild
        async with builder._rebuild_lock:
            assert builder._rebuild_in_progress is False  # Not set until inner
            
            # Try to start another - should wait
            lock_acquired = asyncio.Event()
            
            async def try_rebuild():
                async with builder._rebuild_lock:
                    lock_acquired.set()
            
            task = asyncio.create_task(try_rebuild())
            
            # Give it a moment
            await asyncio.sleep(0.01)
            assert not lock_acquired.is_set()
            
        # Now it should acquire
        await asyncio.wait_for(task, timeout=1.0)
        assert lock_acquired.is_set()
        
    @pytest.mark.asyncio
    async def test_rebuild_epoch_increments(self, grid_builder):
        """Test rebuild epoch increments on each rebuild."""
        builder, deps = grid_builder
        
        position_lock = asyncio.Lock()
        initial_epoch = builder.get_rebuild_epoch()
        
        await builder.build_and_place_grid(
            mid=100.0,
            position=0.0,
            position_lock=position_lock,
            get_current_position=lambda: 0.0,
        )
        
        assert builder.get_rebuild_epoch() == initial_epoch + 1
        
    @pytest.mark.asyncio
    async def test_rebuild_needed_flag(self, grid_builder):
        """Test rebuild needed flag management."""
        builder, deps = grid_builder
        
        assert builder.should_rebuild() is False
        
        builder.rebuild_needed = True
        assert builder.should_rebuild() is True
        
        builder.clear_rebuild_flag()
        assert builder.should_rebuild() is False


class TestGridBuilderFlattenMode:
    """Flatten mode tests."""
    
    @pytest.mark.asyncio
    async def test_flatten_mode_property(self, grid_builder):
        """Test flatten mode property."""
        builder, deps = grid_builder
        
        assert builder.flatten_mode is False
        
        builder.flatten_mode = True
        assert builder.flatten_mode is True
        assert deps["execution_gateway"].flatten_mode is True
        
    @pytest.mark.asyncio
    async def test_flatten_mode_passed_to_calculator(self, grid_builder):
        """Test flatten mode is passed to grid calculator."""
        builder, deps = grid_builder
        builder.flatten_mode = True
        
        # Track what's passed to build_filtered_levels
        original_build = deps["grid_calculator"].build_filtered_levels
        called_with = {}
        
        def track_build(*args, **kwargs):
            called_with.update(kwargs)
            return original_build(*args, **kwargs)
        
        deps["grid_calculator"].build_filtered_levels = track_build
        
        position_lock = asyncio.Lock()
        await builder.build_and_place_grid(
            mid=100.0,
            position=0.0,
            position_lock=position_lock,
            get_current_position=lambda: 0.0,
        )
        
        assert called_with.get("flatten_mode") is True


class TestGridBuilderMetrics:
    """Metrics emission tests."""
    
    @pytest.mark.asyncio
    async def test_metrics_emitted_on_rebuild(self, grid_builder):
        """Test metrics are emitted during rebuild."""
        builder, deps = grid_builder
        
        deps["grid_calculator"]._build_result = MockGridBuildResult(
            spacing=0.02,
            levels=[],
        )
        
        position_lock = asyncio.Lock()
        await builder.build_and_place_grid(
            mid=100.0,
            position=0.0,
            position_lock=position_lock,
            get_current_position=lambda: 0.0,
        )
        
        # Check metrics were set
        assert deps["rich_metrics"].grid_width_pct._value == 2.0  # 0.02 * 100
        assert deps["rich_metrics"].volatility_estimate._value == 0.02
        assert deps["rich_metrics"].atr_value._value == 1.5


class TestGridBuilderNoStrategy:
    """Tests with missing strategy."""
    
    @pytest.mark.asyncio
    async def test_rebuild_without_strategy(self, mock_deps):
        """Test rebuild fails gracefully without strategy."""
        builder = GridBuilder(
            coin="TEST",
            grid_calculator=mock_deps["grid_calculator"],
            strategy=None,  # No strategy
            risk_engine=mock_deps["risk_engine"],
            execution_gateway=mock_deps["execution_gateway"],
            rich_metrics=mock_deps["rich_metrics"],
        )
        
        position_lock = asyncio.Lock()
        result = await builder.build_and_place_grid(
            mid=100.0,
            position=0.0,
            position_lock=position_lock,
            get_current_position=lambda: 0.0,
        )
        
        assert result.success is False
        assert result.aborted_reason == "no_strategy"
        
    @pytest.mark.asyncio
    async def test_replace_without_strategy(self, mock_deps):
        """Test replacement fails gracefully without strategy."""
        builder = GridBuilder(
            coin="TEST",
            grid_calculator=mock_deps["grid_calculator"],
            strategy=None,  # No strategy
            risk_engine=mock_deps["risk_engine"],
            execution_gateway=mock_deps["execution_gateway"],
            rich_metrics=mock_deps["rich_metrics"],
        )
        
        result = await builder.replace_after_fill(
            fill_side="buy",
            fill_px=100.0,
            fill_sz=1.0,
            position=1.0,
        )
        
        assert result.success is False
        assert result.skipped_reason == "no_strategy"
