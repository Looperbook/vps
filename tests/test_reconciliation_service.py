"""
Tests for ReconciliationService.
"""
import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from src.execution.reconciliation_service import (
    ReconciliationService,
    ReconciliationConfig,
    PositionReconcileResult,
    OrderReconcileResult,
)


class MockStateManager:
    """Mock StateManager for testing."""
    def __init__(self):
        self.position = 0.0
        self._set_position_calls = []
    
    async def set_position(self, value, source="unknown"):
        self._set_position_calls.append((value, source))
        self.position = value


class MockOrderManager:
    """Mock OrderManager for testing."""
    def __init__(self):
        self.orders_by_price = {}
        self.tick_sz = 0.01
        self.px_decimals = 2
        self._register_calls = []
        self._unindex_calls = []
    
    def register(self, lvl, cloid, oid):
        rec = MagicMock()
        rec.cloid = cloid
        rec.oid = oid
        self.orders_by_price[f"{lvl.side}:{lvl.px}"] = rec
        self._register_calls.append((lvl, cloid, oid))
        return rec
    
    def unindex(self, rec):
        self._unindex_calls.append(rec)
    
    def open_count(self):
        return len(self.orders_by_price)


class TestPositionReconciliation:
    """Position reconciliation tests."""
    
    @pytest.fixture
    def mock_async_info(self):
        info = MagicMock()
        info.user_state = AsyncMock(return_value={
            "marginSummary": {
                "accountValue": 10000.0,
                "sessionPnl": 100.0,
                "fundingAccrued": 5.0,
            },
            "assetPositions": [
                {"position": {"coin": "BTC", "szi": 1.5}},
            ],
        })
        return info
    
    @pytest.fixture
    def mock_state_manager(self):
        return MockStateManager()
    
    @pytest.fixture
    def mock_shadow_ledger(self):
        ledger = MagicMock()
        ledger.reconcile_with_exchange = AsyncMock(return_value=MagicMock(
            drift_detected=False,
            drift_amount=0.0,
            drift_percent=0.0,
        ))
        return ledger
    
    @pytest.fixture
    def mock_risk_engine(self):
        risk = MagicMock()
        risk.update_equity = MagicMock()
        risk.set_daily_pnl = MagicMock()
        risk.set_funding = MagicMock()
        return risk
    
    @pytest.fixture
    def service(self, mock_async_info, mock_state_manager, mock_shadow_ledger, mock_risk_engine):
        return ReconciliationService(
            coin="BTC",
            async_info=mock_async_info,
            info=None,
            account="0x123",
            order_manager=MockOrderManager(),
            state_manager=mock_state_manager,
            shadow_ledger=mock_shadow_ledger,
            risk_engine=mock_risk_engine,
        )
    
    @pytest.mark.asyncio
    async def test_reconcile_position_success(self, service):
        """Position reconciliation succeeds."""
        result = await service.reconcile_position()
        
        assert isinstance(result, PositionReconcileResult)
        assert result.success
        assert result.exchange_position == 1.5
    
    @pytest.mark.asyncio
    async def test_reconcile_position_updates_state(self, service, mock_state_manager):
        """Position reconciliation updates state manager."""
        await service.reconcile_position()
        
        assert len(mock_state_manager._set_position_calls) == 1
        value, source = mock_state_manager._set_position_calls[0]
        assert value == 1.5
        assert source == "reconcile"
    
    @pytest.mark.asyncio
    async def test_reconcile_position_detects_drift(self, service, mock_state_manager):
        """Position reconciliation detects drift."""
        mock_state_manager.position = 0.5  # Different from exchange 1.5
        
        result = await service.reconcile_position()
        
        assert result.drift == 1.0  # abs(1.5 - 0.5)
        assert result.drift_pct > 0
    
    @pytest.mark.asyncio
    async def test_reconcile_position_updates_risk(self, service, mock_risk_engine):
        """Position reconciliation updates risk engine."""
        await service.reconcile_position()
        
        mock_risk_engine.update_equity.assert_called_once_with(10000.0)
        mock_risk_engine.set_daily_pnl.assert_called_once()
        mock_risk_engine.set_funding.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_reconcile_position_with_lock(self, service, mock_state_manager):
        """Position reconciliation respects lock."""
        lock = asyncio.Lock()
        
        # Pre-acquire lock
        async with lock:
            # Start reconciliation (should wait for lock)
            task = asyncio.create_task(service.reconcile_position(position_lock=lock))
            await asyncio.sleep(0.01)
            # Task should be waiting
            assert not task.done()
        
        # Lock released, should complete
        result = await task
        assert result.success


class TestOrderReconciliation:
    """Order reconciliation tests."""
    
    @pytest.fixture
    def mock_async_info(self):
        info = MagicMock()
        info.frontend_open_orders = AsyncMock(return_value=[
            {"coin": "BTC", "side": "buy", "limitPx": 50000, "sz": 0.1, "oid": 1, "cloid": "c1"},
            {"coin": "BTC", "side": "sell", "limitPx": 51000, "sz": 0.1, "oid": 2, "cloid": "c2"},
        ])
        return info
    
    @pytest.fixture
    def mock_order_manager(self):
        return MockOrderManager()
    
    @pytest.fixture
    def mock_state_manager(self):
        return MockStateManager()
    
    @pytest.fixture
    def service(self, mock_async_info, mock_order_manager, mock_state_manager):
        return ReconciliationService(
            coin="BTC",
            async_info=mock_async_info,
            info=None,
            account="0x123",
            order_manager=mock_order_manager,
            state_manager=mock_state_manager,
            shadow_ledger=MagicMock(),
            risk_engine=MagicMock(),
        )
    
    @pytest.mark.asyncio
    async def test_reconcile_orders_success(self, service):
        """Order reconciliation succeeds."""
        result = await service.reconcile_orders()
        
        assert isinstance(result, OrderReconcileResult)
        assert result.success
    
    @pytest.mark.asyncio
    async def test_reconcile_orders_adds_missing(self, service, mock_order_manager):
        """Order reconciliation adds missing orders."""
        result = await service.reconcile_orders()
        
        assert result.orders_added == 2
        assert len(mock_order_manager._register_calls) == 2
    
    @pytest.mark.asyncio
    async def test_reconcile_orders_removes_stale(self, service, mock_order_manager, mock_async_info):
        """Order reconciliation removes stale orders."""
        # Pre-register an order not on exchange
        mock_rec = MagicMock()
        mock_rec.cloid = "stale"
        mock_rec.oid = 99
        mock_order_manager.orders_by_price["buy:49000.00"] = mock_rec
        
        # Exchange returns different orders
        mock_async_info.frontend_open_orders.return_value = []
        
        result = await service.reconcile_orders()
        
        assert result.orders_removed == 1
        assert len(mock_order_manager._unindex_calls) == 1
    
    @pytest.mark.asyncio
    async def test_reconcile_orders_filters_other_coins(self, service, mock_async_info):
        """Order reconciliation filters other coins."""
        mock_async_info.frontend_open_orders.return_value = [
            {"coin": "ETH", "side": "buy", "limitPx": 3000, "sz": 1.0, "oid": 3, "cloid": "c3"},
        ]
        
        result = await service.reconcile_orders()
        
        assert result.orders_added == 0


class TestReconciliationTiming:
    """Timing and interval tests."""
    
    @pytest.fixture
    def service(self):
        return ReconciliationService(
            coin="BTC",
            async_info=MagicMock(),
            info=None,
            account="0x123",
            order_manager=MockOrderManager(),
            state_manager=MockStateManager(),
            shadow_ledger=MagicMock(),
            risk_engine=MagicMock(),
            config=ReconciliationConfig(
                position_reconcile_interval_sec=60.0,
                order_reconcile_interval_sec=60.0,
            ),
        )
    
    def test_is_position_reconcile_due_initially(self, service):
        """Initially, position reconcile is due."""
        assert service.is_position_reconcile_due
    
    def test_is_order_reconcile_due_initially(self, service):
        """Initially, order reconcile is due."""
        assert service.is_order_reconcile_due
    
    def test_reset_timers(self, service):
        """Reset clears timing state."""
        import time
        service._last_position_reconcile = time.time()
        service._last_order_reconcile = time.time()
        
        service.reset_timers()
        
        assert service._last_position_reconcile == 0.0
        assert service._last_order_reconcile == 0.0


class TestReconciliationErrorHandling:
    """Error handling tests."""
    
    @pytest.fixture
    def mock_async_info(self):
        info = MagicMock()
        info.user_state = AsyncMock(side_effect=Exception("API Error"))
        info.frontend_open_orders = AsyncMock(side_effect=Exception("API Error"))
        return info
    
    @pytest.fixture
    def service(self, mock_async_info):
        return ReconciliationService(
            coin="BTC",
            async_info=mock_async_info,
            info=None,
            account="0x123",
            order_manager=MockOrderManager(),
            state_manager=MockStateManager(),
            shadow_ledger=MagicMock(),
            risk_engine=MagicMock(),
        )
    
    @pytest.mark.asyncio
    async def test_position_reconcile_handles_error(self, service):
        """Position reconcile handles API errors gracefully."""
        result = await service.reconcile_position()
        
        assert not result.success
        assert result.error is not None
        assert "API Error" in result.error
    
    @pytest.mark.asyncio
    async def test_order_reconcile_handles_error(self, service):
        """Order reconcile handles API errors gracefully."""
        result = await service.reconcile_orders()
        
        assert not result.success
        assert result.error is not None
        assert "API Error" in result.error
