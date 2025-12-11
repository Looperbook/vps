"""
Tests for FillProcessor - the extracted fill handling module.

Tests cover:
- Basic fill processing (buy/sell)
- Replay fill handling
- PnL calculation
- WAL integration
- Unmatched fill handling
- Out-of-order fills
- Metrics and event emission
"""

import asyncio
import pytest
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, Mock, patch

from src.execution.fill_processor import FillProcessor, FillProcessorConfig, FillResult
from src.execution.order_manager import OrderManager, ActiveOrder
from src.strategy.strategy import GridLevel
from src.state.state_manager import StateManager, StateManagerConfig
from src.state.position_tracker import PositionTracker


@dataclass
class MockOrderStateMachine:
    """Mock order state machine for testing."""
    fills: List[Dict[str, Any]] = field(default_factory=list)
    
    def fill(self, cloid: Optional[str], oid: Optional[int], fill_qty: float, is_complete: bool) -> None:
        self.fills.append({
            "cloid": cloid,
            "oid": oid,
            "fill_qty": fill_qty,
            "is_complete": is_complete,
        })


@dataclass
class MockShadowLedger:
    """Mock shadow ledger for testing."""
    fills: List[Dict[str, Any]] = field(default_factory=list)
    local_position: float = 0.0
    confirmed_position: float = 0.0
    
    async def apply_fill(self, side: str, size: float, cloid: Optional[str], 
                         oid: Optional[int], timestamp_ms: int) -> None:
        self.fills.append({
            "side": side,
            "size": size,
            "cloid": cloid,
            "oid": oid,
            "timestamp_ms": timestamp_ms,
        })


@dataclass 
class MockRiskEngine:
    """Mock risk engine for testing."""
    position: float = 0.0
    
    def set_position(self, pos: float) -> None:
        self.position = pos


@dataclass
class MockEventBus:
    """Mock event bus for testing."""
    events: List[Dict[str, Any]] = field(default_factory=list)
    
    async def emit(self, event_type: Any, **kwargs) -> None:
        self.events.append({"type": event_type, **kwargs})


class MockCounter:
    """Mock Prometheus counter."""
    def __init__(self):
        self._value = 0
        
    def inc(self, amount: int = 1) -> None:
        self._value += amount
        
    def labels(self, **kwargs) -> "MockCounter":
        return self


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
    """Mock rich metrics for testing."""
    fills_total: MockCounter = field(default_factory=MockCounter)
    position: MockGauge = field(default_factory=MockGauge)
    realized_pnl: MockGauge = field(default_factory=MockGauge)
    unusual_pnl_fills: MockCounter = field(default_factory=MockCounter)


@dataclass
class MockWAL:
    """Mock write-ahead log for testing."""
    entries: List[Dict[str, Any]] = field(default_factory=list)
    committed: List[int] = field(default_factory=list)
    _next_id: int = 1
    
    def append_sync(self, **kwargs) -> int:
        entry_id = self._next_id
        self._next_id += 1
        self.entries.append({"id": entry_id, **kwargs})
        return entry_id
    
    def mark_committed(self, entry_id: int) -> None:
        self.committed.append(entry_id)


class MockFillLog:
    """Mock fill log for testing."""
    def __init__(self):
        self.fills: List[Dict[str, Any]] = []
        
    async def append(self, fill: Dict[str, Any]) -> None:
        self.fills.append(fill)


@pytest.fixture
def order_manager():
    """Create order manager with test configuration."""
    return OrderManager(tick_sz=0.01, px_decimals=2)


@pytest.fixture
def mock_dependencies():
    """Create all mock dependencies for FillProcessor."""
    return {
        "order_state_machine": MockOrderStateMachine(),
        "shadow_ledger": MockShadowLedger(),
        "risk_engine": MockRiskEngine(),
        "event_bus": MockEventBus(),
        "rich_metrics": MockRichMetrics(),
        "wal": MockWAL(),
        "fill_log": MockFillLog(),
    }


class DummyStateStore:
    """In-memory state store for tests (async API)."""
    def __init__(self) -> None:
        self.data: Dict[str, Any] = {}
    async def save(self, data: Dict[str, Any]) -> None:
        self.data = data
    async def load(self) -> Dict[str, Any]:
        return self.data


@pytest.fixture
def state_manager(mock_dependencies):
    """Real StateManager wired to mock dependencies for single-source state."""
    store = DummyStateStore()
    tracker = PositionTracker(log_event=lambda *args, **kwargs: None)
    return StateManager(
        coin="TEST",
        state_store=store,
        position_tracker=tracker,
        shadow_ledger=mock_dependencies["shadow_ledger"],
        risk_engine=mock_dependencies["risk_engine"],
        config=StateManagerConfig(log_event_callback=lambda *args, **kwargs: None),
    )


@pytest.fixture
def fill_processor(order_manager, mock_dependencies, state_manager):
    """Create FillProcessor with all dependencies."""
    config = FillProcessorConfig(wal_enabled=True)
    processor = FillProcessor(
        coin="TEST",
        order_manager=order_manager,
        order_state_machine=mock_dependencies["order_state_machine"],
        shadow_ledger=mock_dependencies["shadow_ledger"],
        risk_engine=mock_dependencies["risk_engine"],
        event_bus=mock_dependencies["event_bus"],
        rich_metrics=mock_dependencies["rich_metrics"],
        wal=mock_dependencies["wal"],
        fill_log=mock_dependencies["fill_log"],
        config=config,
        state_manager=state_manager,
    )
    return processor, {**mock_dependencies, "state_manager": state_manager}


class TestFillProcessorBasic:
    """Basic fill processing tests."""
    
    @pytest.mark.asyncio
    async def test_process_buy_fill_matched(self, fill_processor, order_manager):
        """Test processing a matched buy fill."""
        processor, deps = fill_processor
        
        # Register a buy order
        level = GridLevel(side="buy", px=100.0, sz=1.0)
        order_manager.register(level, cloid="buy-1", oid=1001)
        
        # Process fill
        result = await processor.process_fill(
            side="buy",
            px=100.0,
            sz=1.0,
            oid=1001,
            cloid="buy-1",
            ts_ms=1000000,
        )
        
        # Verify result
        assert result.matched is True
        assert result.position_delta == 1.0
        assert result.new_position == 1.0
        assert result.order_was_fully_filled is True
        assert result.success is True
        
        # Verify position updated
        assert processor.position == 1.0
        
        # Verify risk engine updated
        assert deps["risk_engine"].position == 1.0
        
        # Verify shadow ledger updated
        assert len(deps["shadow_ledger"].fills) == 1
        assert deps["shadow_ledger"].fills[0]["side"] == "buy"
        
    @pytest.mark.asyncio
    async def test_process_sell_fill_matched(self, fill_processor, order_manager):
        """Test processing a matched sell fill."""
        processor, deps = fill_processor
        processor.position = 2.0  # Start with position
        
        # Register a sell order
        level = GridLevel(side="sell", px=110.0, sz=1.0)
        order_manager.register(level, cloid="sell-1", oid=2001)
        
        # Process fill
        result = await processor.process_fill(
            side="sell",
            px=110.0,
            sz=1.0,
            oid=2001,
            cloid="sell-1",
            ts_ms=1000000,
        )
        
        # Verify result
        assert result.matched is True
        assert result.position_delta == -1.0
        assert result.new_position == 1.0
        assert result.success is True
        
        # Verify position updated
        assert processor.position == 1.0
        
    @pytest.mark.asyncio
    async def test_unmatched_fill_signals_reconciliation(self, fill_processor):
        """Test that unmatched fills signal need for reconciliation."""
        processor, deps = fill_processor
        
        # Process fill with no matching order
        result = await processor.process_fill(
            side="buy",
            px=100.0,
            sz=1.0,
            oid=9999,  # Unknown order
            cloid="unknown-1",
            ts_ms=1000000,
        )
        
        # Verify result
        assert result.matched is False
        assert result.needs_reconciliation is True
        assert result.success is False
        
        # Position should not change
        assert processor.position == 0.0


class TestFillProcessorPnL:
    """PnL calculation tests."""
    
    @pytest.mark.asyncio
    async def test_pnl_buy_then_sell_profit(self, fill_processor, order_manager):
        """Test PnL when buying low and selling high."""
        processor, deps = fill_processor
        
        # Register and fill a buy at 100
        buy_level = GridLevel(side="buy", px=100.0, sz=1.0)
        order_manager.register(buy_level, cloid="buy-1", oid=1001)
        
        result = await processor.process_fill(
            side="buy", px=100.0, sz=1.0, oid=1001, cloid="buy-1", ts_ms=1000
        )
        
        # PnL from buy (closing notional short position in grid)
        # For a buy fill, PnL = (fill_price - level_price) * size = 0
        assert result.realized_pnl == 0.0  # Same price
        
        # Register and fill a sell at 110
        sell_level = GridLevel(side="sell", px=110.0, sz=1.0)
        order_manager.register(sell_level, cloid="sell-1", oid=2001)
        
        result2 = await processor.process_fill(
            side="sell", px=110.0, sz=1.0, oid=2001, cloid="sell-1", ts_ms=2000
        )
        
        # For a sell fill, PnL = (level_price - fill_price) * size = 0
        assert result2.realized_pnl == 0.0  # Same price
        
    @pytest.mark.asyncio
    async def test_pnl_buy_slippage(self, fill_processor, order_manager):
        """Test PnL with slippage on buy fill."""
        processor, deps = fill_processor
        
        # Register buy at 100, fill at 99 (positive slippage)
        buy_level = GridLevel(side="buy", px=100.0, sz=1.0)
        order_manager.register(buy_level, cloid="buy-1", oid=1001)
        
        result = await processor.process_fill(
            side="buy", px=99.0, sz=1.0, oid=1001, cloid="buy-1", ts_ms=1000
        )
        
        # PnL = (fill_price - level_price) * size = (99 - 100) * 1 = -1
        assert result.realized_pnl == -1.0
        
    @pytest.mark.asyncio
    async def test_pnl_unusual_fill_alert(self, fill_processor, order_manager):
        """Test that unusual PnL triggers alert."""
        processor, deps = fill_processor
        processor.config.max_single_fill_pnl = 10.0  # Low threshold
        
        # Register order with big slippage
        level = GridLevel(side="buy", px=100.0, sz=100.0)
        order_manager.register(level, cloid="buy-1", oid=1001)
        
        result = await processor.process_fill(
            side="buy", px=80.0, sz=100.0, oid=1001, cloid="buy-1", ts_ms=1000
        )
        
        # PnL = (80 - 100) * 100 = -2000 (very unusual)
        assert abs(result.realized_pnl) > processor.config.max_single_fill_pnl


class TestFillProcessorWAL:
    """WAL integration tests."""
    
    @pytest.mark.asyncio
    async def test_wal_write_before_position_update(self, fill_processor, order_manager):
        """Test that WAL entry is written before position update."""
        processor, deps = fill_processor
        wal = deps["wal"]
        
        # Register order
        level = GridLevel(side="buy", px=100.0, sz=1.0)
        order_manager.register(level, cloid="buy-1", oid=1001)
        
        # Process fill
        result = await processor.process_fill(
            side="buy", px=100.0, sz=1.0, oid=1001, cloid="buy-1", ts_ms=1000
        )
        
        # Verify WAL entry
        assert len(wal.entries) == 1
        entry = wal.entries[0]
        assert entry["op"] == "fill"
        assert entry["side"] == "buy"
        assert entry["old_position"] == 0.0
        assert entry["new_position"] == 1.0
        
        # Verify committed
        assert len(wal.committed) == 1
        assert wal.committed[0] == entry["id"]
        
    @pytest.mark.asyncio
    async def test_wal_disabled_no_writes(self, order_manager, mock_dependencies, state_manager):
        """Test that WAL is not used when disabled."""
        config = FillProcessorConfig(wal_enabled=False)
        processor = FillProcessor(
            coin="TEST",
            order_manager=order_manager,
            config=config,
            state_manager=state_manager,
            **mock_dependencies,
        )
        
        wal = mock_dependencies["wal"]
        
        # Register and fill
        level = GridLevel(side="buy", px=100.0, sz=1.0)
        order_manager.register(level, cloid="buy-1", oid=1001)
        
        await processor.process_fill(
            side="buy", px=100.0, sz=1.0, oid=1001, cloid="buy-1", ts_ms=1000
        )
        
        # No WAL writes
        assert len(wal.entries) == 0


class TestFillProcessorReplay:
    """Replay fill handling tests."""
    
    @pytest.mark.asyncio
    async def test_replay_fill_no_position_change(self, fill_processor, order_manager):
        """Test that replay fills don't change position."""
        processor, deps = fill_processor
        processor.position = 5.0  # Existing position from state
        
        # Register order
        level = GridLevel(side="buy", px=100.0, sz=1.0)
        order_manager.register(level, cloid="buy-1", oid=1001)
        
        # Process replay fill
        result = await processor.process_fill(
            side="buy", px=100.0, sz=1.0, oid=1001, cloid="buy-1", ts_ms=1000,
            replay=True,
        )
        
        # Verify no position change
        assert result.is_replay is True
        assert processor.position == 5.0  # Unchanged
        
        # Order should be removed from indices
        assert order_manager.lookup(cloid="buy-1") is None
        
    @pytest.mark.asyncio
    async def test_replay_updates_last_fill_time(self, fill_processor, order_manager):
        """Test that replay updates last fill timestamp."""
        processor, deps = fill_processor
        processor.last_fill_time_ms = 500
        
        # Register order
        level = GridLevel(side="buy", px=100.0, sz=1.0)
        order_manager.register(level, cloid="buy-1", oid=1001)
        
        # Process replay with later timestamp
        await processor.process_fill(
            side="buy", px=100.0, sz=1.0, oid=1001, cloid="buy-1", ts_ms=1000,
            replay=True,
        )
        
        assert processor.last_fill_time_ms == 1000


class TestFillProcessorPartialFills:
    """Partial fill handling tests."""
    
    @pytest.mark.asyncio
    async def test_partial_fill_keeps_order(self, fill_processor, order_manager):
        """Test that partial fills keep order in registry."""
        processor, deps = fill_processor
        
        # Register large order
        level = GridLevel(side="buy", px=100.0, sz=10.0)
        order_manager.register(level, cloid="buy-1", oid=1001)
        
        # Partial fill (5 of 10)
        result = await processor.process_fill(
            side="buy", px=100.0, sz=5.0, oid=1001, cloid="buy-1", ts_ms=1000
        )
        
        assert result.matched is True
        assert result.order_was_fully_filled is False
        assert processor.position == 5.0
        
        # Order still in registry
        rec = order_manager.lookup(cloid="buy-1")
        assert rec is not None
        assert rec.filled_qty == 5.0
        
    @pytest.mark.asyncio
    async def test_multiple_partial_fills(self, fill_processor, order_manager):
        """Test multiple partial fills completing an order."""
        processor, deps = fill_processor
        
        # Register order for 10 units
        level = GridLevel(side="buy", px=100.0, sz=10.0)
        order_manager.register(level, cloid="buy-1", oid=1001)
        
        # First partial fill (3)
        result1 = await processor.process_fill(
            side="buy", px=100.0, sz=3.0, oid=1001, cloid="buy-1", ts_ms=1000
        )
        assert result1.order_was_fully_filled is False
        
        # Second partial fill (4)
        result2 = await processor.process_fill(
            side="buy", px=100.0, sz=4.0, oid=1001, cloid="buy-1", ts_ms=2000
        )
        assert result2.order_was_fully_filled is False
        
        # Final partial fill (3)
        result3 = await processor.process_fill(
            side="buy", px=100.0, sz=3.0, oid=1001, cloid="buy-1", ts_ms=3000
        )
        assert result3.order_was_fully_filled is True
        
        # Total position
        assert processor.position == 10.0


class TestFillProcessorEvents:
    """Event emission tests."""
    
    @pytest.mark.asyncio
    async def test_position_event_emitted(self, fill_processor, order_manager):
        """Test that position changed event is emitted."""
        processor, deps = fill_processor
        event_bus = deps["event_bus"]
        
        # Register and fill
        level = GridLevel(side="buy", px=100.0, sz=1.0)
        order_manager.register(level, cloid="buy-1", oid=1001)
        
        await processor.process_fill(
            side="buy", px=100.0, sz=1.0, oid=1001, cloid="buy-1", ts_ms=1000
        )
        
        # Find position event
        position_events = [e for e in event_bus.events if "position" in e]
        assert len(position_events) >= 1
        
    @pytest.mark.asyncio
    async def test_fill_event_emitted(self, fill_processor, order_manager):
        """Test that fill event is emitted."""
        processor, deps = fill_processor
        event_bus = deps["event_bus"]
        
        # Register and fill
        level = GridLevel(side="buy", px=100.0, sz=1.0)
        order_manager.register(level, cloid="buy-1", oid=1001)
        
        await processor.process_fill(
            side="buy", px=100.0, sz=1.0, oid=1001, cloid="buy-1", ts_ms=1000
        )
        
        # Should have fill event
        fill_events = [e for e in event_bus.events if e.get("matched") is not None]
        assert len(fill_events) >= 1


class TestFillProcessorState:
    """State management tests."""
    
    @pytest.mark.asyncio
    async def test_get_state_snapshot(self, fill_processor, order_manager):
        """Test state snapshot captures all state."""
        processor, deps = fill_processor
        processor.position = 5.0
        processor.state_manager.sync_from_fill_processor(
            position=5.0,
            session_pnl=100.0,
            alltime_pnl=500.0,
            last_fill_time_ms=12345,
        )
        
        snapshot = processor.get_state_snapshot()
        
        assert snapshot["position"] == 5.0
        assert snapshot["session_pnl"] == 100.0
        assert snapshot["alltime_pnl"] == 500.0
        assert snapshot["last_fill_time_ms"] == 12345
        
    @pytest.mark.asyncio
    async def test_restore_from_snapshot(self, fill_processor):
        """Test state can be restored from snapshot."""
        processor, deps = fill_processor
        
        snapshot = {
            "position": 10.0,
            "session_pnl": 200.0,
            "alltime_pnl": 1000.0,
            "last_fill_time_ms": 99999,
        }
        
        processor.restore_from_snapshot(snapshot)
        
        assert processor.position == 10.0
        assert processor.session_realized_pnl == 200.0
        assert processor.alltime_realized_pnl == 1000.0
        assert processor.last_fill_time_ms == 99999


class TestFillProcessorTimestamp:
    """Timestamp validation tests."""
    
    def test_validate_normal_timestamp(self, fill_processor):
        """Test that normal timestamps pass validation."""
        processor, deps = fill_processor
        
        current_ms = 1000000
        fill_ms = current_ms - 1000  # 1 second ago
        
        assert processor.validate_fill_timestamp(fill_ms, current_ms) is True
        
    def test_validate_old_timestamp_fails(self, fill_processor):
        """Test that very old timestamps fail validation."""
        processor, deps = fill_processor
        processor.config.max_fill_age_ms = 60000  # 1 minute
        
        current_ms = 1000000
        fill_ms = current_ms - 120000  # 2 minutes ago
        
        assert processor.validate_fill_timestamp(fill_ms, current_ms) is False
        
    def test_validate_future_timestamp_fails(self, fill_processor):
        """Test that future timestamps fail validation."""
        processor, deps = fill_processor
        processor.config.max_future_fill_ms = 5000  # 5 seconds
        
        current_ms = 1000000
        fill_ms = current_ms + 10000  # 10 seconds in future
        
        assert processor.validate_fill_timestamp(fill_ms, current_ms) is False


class TestFillProcessorRebuildFlag:
    """Rebuild during fill tests."""
    
    @pytest.mark.asyncio
    async def test_fill_during_rebuild_sets_flag(self, fill_processor, order_manager):
        """Test that fill during rebuild sets needs_grid_rebuild flag."""
        processor, deps = fill_processor
        
        # Register order
        level = GridLevel(side="buy", px=100.0, sz=1.0)
        order_manager.register(level, cloid="buy-1", oid=1001)
        
        # Process fill with rebuild_in_progress=True
        result = await processor.process_fill(
            side="buy", px=100.0, sz=1.0, oid=1001, cloid="buy-1", ts_ms=1000,
            rebuild_in_progress=True,
        )
        
        assert result.needs_grid_rebuild is True
        assert result.matched is True  # Still processes normally
