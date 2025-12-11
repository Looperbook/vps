"""
Tests for ExecutionGateway - unified order execution interface.

Tests cover:
- Order submission (single and batch)
- Order cancellation
- Flatten mode
- State machine integration
- Shadow ledger tracking
- Event emission
"""

import asyncio
import pytest
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock

from src.execution.execution_gateway import (
    ExecutionGateway, 
    ExecutionGatewayConfig,
    SubmitResult,
    CancelResult,
)
from src.execution.order_manager import OrderManager, ActiveOrder
from src.strategy.strategy import GridLevel


@dataclass
class MockOrderStateMachine:
    """Mock order state machine."""
    orders_created: List[Dict] = field(default_factory=list)
    orders_acknowledged: List[str] = field(default_factory=list)
    orders_rejected: List[Dict] = field(default_factory=list)
    orders_cancelled: List[Dict] = field(default_factory=list)
    oid_updates: List[Dict] = field(default_factory=list)
    
    def create_order(self, cloid, oid, side, price, qty) -> None:
        self.orders_created.append({
            "cloid": cloid, "oid": oid, "side": side, "price": price, "qty": qty
        })
    
    def acknowledge(self, cloid: str = None, oid: int = None) -> None:
        self.orders_acknowledged.append(cloid or str(oid))
    
    def update_oid(self, cloid: str, oid: int) -> None:
        self.oid_updates.append({"cloid": cloid, "oid": oid})
    
    def reject(self, cloid: str = None, oid: int = None, error_code: str = "", error_message: str = "") -> None:
        self.orders_rejected.append({
            "cloid": cloid, "oid": oid, "error_code": error_code, "message": error_message
        })
    
    def cancel(self, cloid: str = None, oid: int = None) -> None:
        self.orders_cancelled.append({"cloid": cloid, "oid": oid})


@dataclass
class MockShadowLedger:
    """Mock shadow ledger."""
    pending_orders: Dict[str, Dict] = field(default_factory=dict)
    
    def add_pending_order(self, cloid: str, side: str, qty: float) -> None:
        self.pending_orders[cloid] = {"side": side, "qty": qty}
    
    def remove_pending_order(self, order_id: str) -> None:
        self.pending_orders.pop(order_id, None)


@dataclass
class MockEventBus:
    """Mock event bus."""
    events: List[Dict] = field(default_factory=list)
    
    async def emit(self, event_type, **kwargs) -> None:
        self.events.append({"type": event_type, **kwargs})


class MockCounter:
    """Mock Prometheus counter."""
    def __init__(self):
        self._value = 0
    
    def inc(self) -> None:
        self._value += 1
    
    def labels(self, **kwargs) -> "MockCounter":
        return self


@dataclass
class MockRichMetrics:
    """Mock rich metrics."""
    orders_submitted: MockCounter = field(default_factory=MockCounter)
    orders_rejected: MockCounter = field(default_factory=MockCounter)


@dataclass
class MockCircuitBreaker:
    """Mock circuit breaker."""
    errors: List[Dict] = field(default_factory=list)
    
    def record_error(self, category: str, exc: Exception) -> None:
        self.errors.append({"category": category, "error": str(exc)})


class MockRouter:
    """Mock order router."""
    def __init__(self):
        self.submit_results: List[Dict] = []
        self.submit_index = 0
        self.cancel_all_called = False
        self.safe_cancel_calls: List[Dict] = []
        self.should_fail = False
        self.fail_message = "mock_error"
    
    def set_submit_results(self, results: List[Dict]) -> None:
        """Set results to return from submit calls."""
        self.submit_results = results
        self.submit_index = 0
    
    async def submit(self, req) -> Dict:
        if self.should_fail:
            raise Exception(self.fail_message)
        
        if self.submit_index < len(self.submit_results):
            result = self.submit_results[self.submit_index]
            self.submit_index += 1
            return result
        return {"oid": 1000 + self.submit_index, "cloid": req.cloid}
    
    async def cancel_all(self) -> None:
        if self.should_fail:
            raise Exception(self.fail_message)
        self.cancel_all_called = True
    
    async def safe_cancel(self, cloid: str = None, oid: int = None) -> None:
        self.safe_cancel_calls.append({"cloid": cloid, "oid": oid})


@pytest.fixture
def order_manager():
    """Create order manager."""
    return OrderManager(tick_sz=0.01, px_decimals=2)


@pytest.fixture
def mock_deps():
    """Create mock dependencies."""
    return {
        "order_state_machine": MockOrderStateMachine(),
        "shadow_ledger": MockShadowLedger(),
        "event_bus": MockEventBus(),
        "rich_metrics": MockRichMetrics(),
        "circuit_breaker": MockCircuitBreaker(),
        "router": MockRouter(),
    }


@pytest.fixture
def gateway(order_manager, mock_deps):
    """Create ExecutionGateway with mocks."""
    config = ExecutionGatewayConfig(use_cloid=True)
    return ExecutionGateway(
        coin="TEST",
        router=mock_deps["router"],
        order_manager=order_manager,
        order_state_machine=mock_deps["order_state_machine"],
        shadow_ledger=mock_deps["shadow_ledger"],
        event_bus=mock_deps["event_bus"],
        rich_metrics=mock_deps["rich_metrics"],
        circuit_breaker=mock_deps["circuit_breaker"],
        config=config,
    ), mock_deps


class TestExecutionGatewaySubmit:
    """Order submission tests."""
    
    @pytest.mark.asyncio
    async def test_submit_level_success(self, gateway, order_manager):
        """Test successful single order submission."""
        gw, deps = gateway
        deps["router"].set_submit_results([{"oid": 1001, "cloid": "test-cloid"}])
        
        level = GridLevel(side="buy", px=100.0, sz=1.0)
        result = await gw.submit_level(level, mode="test")
        
        assert result.success is True
        assert result.oid == 1001
        assert result.cloid is not None
        
        # Check state machine was updated
        assert len(deps["order_state_machine"].orders_created) == 1
        assert len(deps["order_state_machine"].orders_acknowledged) == 1
        
        # Check order manager
        assert order_manager.open_count() == 1
        
    @pytest.mark.asyncio
    async def test_submit_level_no_oid(self, gateway, order_manager):
        """Test submission with cloid only (no oid)."""
        gw, deps = gateway
        deps["router"].set_submit_results([{"cloid": "test-cloid"}])
        
        level = GridLevel(side="buy", px=100.0, sz=1.0)
        result = await gw.submit_level(level, mode="test")
        
        assert result.success is True
        assert result.oid is None
        assert result.cloid is not None
        
    @pytest.mark.asyncio
    async def test_submit_level_error(self, gateway, order_manager):
        """Test submission error handling."""
        gw, deps = gateway
        deps["router"].should_fail = True
        deps["router"].fail_message = "network_error"
        
        level = GridLevel(side="buy", px=100.0, sz=1.0)
        result = await gw.submit_level(level, mode="test")
        
        assert result.success is False
        assert "network_error" in result.error
        
        # State machine should have rejection
        assert len(deps["order_state_machine"].orders_rejected) == 1
        
        # Circuit breaker should record error
        assert len(deps["circuit_breaker"].errors) == 1
        
    @pytest.mark.asyncio
    async def test_submit_level_flatten_blocks_buy(self, gateway):
        """Test flatten mode blocks buy orders when long."""
        gw, deps = gateway
        gw.flatten_mode = True
        
        level = GridLevel(side="buy", px=100.0, sz=1.0)
        result = await gw.submit_level(level, mode="test", position=5.0)  # Long
        
        assert result.success is False
        assert result.error == "flatten_blocked"
        
    @pytest.mark.asyncio
    async def test_submit_level_flatten_allows_sell(self, gateway, order_manager):
        """Test flatten mode allows sell orders when long."""
        gw, deps = gateway
        gw.flatten_mode = True
        
        level = GridLevel(side="sell", px=110.0, sz=1.0)
        result = await gw.submit_level(level, mode="test", position=5.0)  # Long
        
        assert result.success is True


class TestExecutionGatewayBatchSubmit:
    """Batch submission tests."""
    
    @pytest.mark.asyncio
    async def test_submit_batch_success(self, gateway, order_manager):
        """Test successful batch submission."""
        gw, deps = gateway
        deps["router"].set_submit_results([
            {"oid": 1001, "cloid": "c1"},
            {"oid": 1002, "cloid": "c2"},
            {"oid": 1003, "cloid": "c3"},
        ])
        
        levels = [
            GridLevel(side="buy", px=99.0, sz=1.0),
            GridLevel(side="buy", px=98.0, sz=1.0),
            GridLevel(side="sell", px=101.0, sz=1.0),
        ]
        
        results = await gw.submit_levels_batch(levels, mode="grid")
        
        assert len(results) == 3
        assert all(r.success for r in results)
        assert order_manager.open_count() == 3
        
    @pytest.mark.asyncio
    async def test_submit_batch_partial_failure(self, gateway, order_manager):
        """Test batch with some failures."""
        gw, deps = gateway
        
        # Make router fail on second call
        class PartialFailRouter(MockRouter):
            call_count = 0
            async def submit(self, req):
                self.call_count += 1
                if self.call_count == 2:
                    raise Exception("submit_error")
                return {"oid": 1000 + self.call_count, "cloid": req.cloid}
        
        deps["router"] = PartialFailRouter()
        gw.router = deps["router"]
        
        levels = [
            GridLevel(side="buy", px=99.0, sz=1.0),
            GridLevel(side="buy", px=98.0, sz=1.0),
            GridLevel(side="sell", px=101.0, sz=1.0),
        ]
        
        results = await gw.submit_levels_batch(levels, mode="grid")
        
        assert len(results) == 3
        successes = [r for r in results if r.success]
        failures = [r for r in results if not r.success]
        assert len(successes) == 2
        assert len(failures) == 1


class TestExecutionGatewayCancellation:
    """Order cancellation tests."""
    
    @pytest.mark.asyncio
    async def test_cancel_all_success(self, gateway, order_manager):
        """Test cancel all orders."""
        gw, deps = gateway
        
        # Register some orders
        order_manager.register(GridLevel(side="buy", px=100.0, sz=1.0), "c1", 1001)
        order_manager.register(GridLevel(side="sell", px=110.0, sz=1.0), "c2", 1002)
        assert order_manager.open_count() == 2
        
        result = await gw.cancel_all(reason="test")
        
        assert result.success is True
        assert result.cancelled_count == 2
        assert order_manager.open_count() == 0
        assert deps["router"].cancel_all_called is True
        
    @pytest.mark.asyncio
    async def test_cancel_all_with_fallback(self, gateway, order_manager):
        """Test cancel all falls back to individual cancels on error."""
        gw, deps = gateway
        deps["router"].should_fail = True
        
        # Register orders
        order_manager.register(GridLevel(side="buy", px=100.0, sz=1.0), "c1", 1001)
        order_manager.register(GridLevel(side="sell", px=110.0, sz=1.0), "c2", 1002)
        
        # Reset should_fail for individual cancels
        async def safe_cancel_success(cloid=None, oid=None):
            deps["router"].safe_cancel_calls.append({"cloid": cloid, "oid": oid})
        deps["router"].safe_cancel = safe_cancel_success
        
        result = await gw.cancel_all(reason="test")
        
        # Should have tried individual cancels
        assert len(deps["router"].safe_cancel_calls) == 2
        assert order_manager.open_count() == 0
        
    @pytest.mark.asyncio
    async def test_cancel_side(self, gateway, order_manager):
        """Test cancelling orders on one side."""
        gw, deps = gateway
        
        # Register orders on both sides
        order_manager.register(GridLevel(side="buy", px=100.0, sz=1.0), "c1", 1001)
        order_manager.register(GridLevel(side="buy", px=99.0, sz=1.0), "c2", 1002)
        order_manager.register(GridLevel(side="sell", px=110.0, sz=1.0), "c3", 1003)
        assert order_manager.open_count() == 3
        
        result = await gw.cancel_side("buy", reason="flatten")
        
        assert result.success is True
        assert result.cancelled_count == 2
        assert order_manager.open_count() == 1
        
        # Check state machine cancellations
        assert len(deps["order_state_machine"].orders_cancelled) == 2
        
    @pytest.mark.asyncio
    async def test_cancel_order_by_cloid(self, gateway, order_manager):
        """Test cancelling single order by cloid."""
        gw, deps = gateway
        
        order_manager.register(GridLevel(side="buy", px=100.0, sz=1.0), "c1", 1001)
        
        result = await gw.cancel_order(cloid="c1", reason="test")
        
        assert result.success is True
        assert result.cancelled_count == 1
        assert order_manager.open_count() == 0
        
    @pytest.mark.asyncio
    async def test_cancel_order_not_found(self, gateway):
        """Test cancelling non-existent order."""
        gw, deps = gateway
        
        result = await gw.cancel_order(cloid="unknown", reason="test")
        
        assert result.success is False
        assert "order_not_found" in result.errors


class TestExecutionGatewayStateTracking:
    """State tracking and event tests."""
    
    @pytest.mark.asyncio
    async def test_shadow_ledger_tracking(self, gateway):
        """Test shadow ledger is updated on submit."""
        gw, deps = gateway
        
        level = GridLevel(side="buy", px=100.0, sz=1.0)
        await gw.submit_level(level, mode="test")
        
        assert len(deps["shadow_ledger"].pending_orders) == 1
        
    @pytest.mark.asyncio
    async def test_shadow_ledger_removed_on_cancel(self, gateway, order_manager):
        """Test shadow ledger updated on cancel."""
        gw, deps = gateway
        
        # Submit order
        level = GridLevel(side="buy", px=100.0, sz=1.0)
        result = await gw.submit_level(level, mode="test")
        assert len(deps["shadow_ledger"].pending_orders) == 1
        
        # Cancel
        await gw.cancel_order(cloid=result.cloid)
        
        assert len(deps["shadow_ledger"].pending_orders) == 0
        
    @pytest.mark.asyncio
    async def test_events_emitted_on_submit(self, gateway):
        """Test events are emitted on order submit."""
        gw, deps = gateway
        
        level = GridLevel(side="buy", px=100.0, sz=1.0)
        await gw.submit_level(level, mode="test")
        
        # Should have ORDER_SUBMITTED event
        submit_events = [e for e in deps["event_bus"].events if "ORDER_SUBMITTED" in str(e.get("type", ""))]
        assert len(submit_events) >= 1


class TestExecutionGatewayQueries:
    """Order query tests."""
    
    @pytest.mark.asyncio
    async def test_open_count(self, gateway, order_manager):
        """Test open_count returns correct count."""
        gw, deps = gateway
        
        assert gw.open_count() == 0
        
        order_manager.register(GridLevel(side="buy", px=100.0, sz=1.0), "c1", 1001)
        order_manager.register(GridLevel(side="sell", px=110.0, sz=1.0), "c2", 1002)
        
        assert gw.open_count() == 2
        
    @pytest.mark.asyncio
    async def test_get_order(self, gateway, order_manager):
        """Test get_order lookup."""
        gw, deps = gateway
        
        level = GridLevel(side="buy", px=100.0, sz=1.0)
        order_manager.register(level, "c1", 1001)
        
        rec = gw.get_order(cloid="c1")
        assert rec is not None
        assert rec.cloid == "c1"
        assert rec.oid == 1001
        
    @pytest.mark.asyncio
    async def test_all_orders(self, gateway, order_manager):
        """Test all_orders returns all."""
        gw, deps = gateway
        
        order_manager.register(GridLevel(side="buy", px=100.0, sz=1.0), "c1", 1001)
        order_manager.register(GridLevel(side="sell", px=110.0, sz=1.0), "c2", 1002)
        
        orders = gw.all_orders()
        assert len(orders) == 2


class TestExecutionGatewayFlattenMode:
    """Flatten mode tests."""
    
    @pytest.mark.asyncio
    async def test_flatten_mode_property(self, gateway):
        """Test flatten mode property."""
        gw, deps = gateway
        
        assert gw.flatten_mode is False
        
        gw.flatten_mode = True
        assert gw.flatten_mode is True
        
    @pytest.mark.asyncio
    async def test_flatten_blocks_exposure_increase(self, gateway):
        """Test flatten blocks orders that increase exposure."""
        gw, deps = gateway
        gw.flatten_mode = True
        
        # Long position - should block buys
        buy = GridLevel(side="buy", px=100.0, sz=1.0)
        result = await gw.submit_level(buy, mode="test", position=10.0)
        assert result.success is False
        
        # Should allow sells (reduce exposure)
        sell = GridLevel(side="sell", px=110.0, sz=1.0)
        result = await gw.submit_level(sell, mode="test", position=10.0)
        assert result.success is True
        
    @pytest.mark.asyncio
    async def test_flatten_batch_filters(self, gateway):
        """Test flatten mode filters batch orders."""
        gw, deps = gateway
        gw.flatten_mode = True
        
        levels = [
            GridLevel(side="buy", px=99.0, sz=1.0),  # Blocked
            GridLevel(side="buy", px=98.0, sz=1.0),  # Blocked
            GridLevel(side="sell", px=101.0, sz=1.0),  # Allowed
        ]
        
        results = await gw.submit_levels_batch(levels, mode="grid", position=5.0)
        
        # Only sell should be submitted
        assert len(results) == 1
        assert results[0].success is True
