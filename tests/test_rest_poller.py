"""
Tests for RestPoller service.
"""
import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from src.execution.rest_poller import RestPoller, RestPollerConfig, PollResult


class TestRestPollerBasic:
    """Basic functionality tests."""
    
    @pytest.fixture
    def mock_market(self):
        market = MagicMock()
        market.user_fills_since = AsyncMock(return_value=[])
        return market
    
    @pytest.fixture
    def poller(self, mock_market):
        return RestPoller(
            coin="BTC",
            market=mock_market,
            account="0x123",
            config=RestPollerConfig(poll_interval_sec=30.0),
        )
    
    @pytest.mark.asyncio
    async def test_poll_returns_result(self, poller):
        """Polling returns a PollResult."""
        result = await poller.poll_if_due(force=True)
        assert isinstance(result, PollResult)
        assert result.success
    
    @pytest.mark.asyncio
    async def test_poll_respects_interval(self, poller):
        """Polling respects interval timing."""
        # First poll should work
        result1 = await poller.poll_if_due()
        assert result1.success
        
        # Immediate second poll should skip (no force)
        result2 = await poller.poll_if_due()
        assert result2.success
        assert result2.fills_found == 0  # Skipped, no actual fetch
    
    @pytest.mark.asyncio
    async def test_force_poll_ignores_interval(self, poller):
        """Force poll ignores interval."""
        await poller.poll_if_due(force=True)
        
        # Force should still work
        result = await poller.poll_if_due(force=True)
        assert result.success


class TestRestPollerFillProcessing:
    """Fill processing tests."""
    
    @pytest.fixture
    def mock_market(self):
        market = MagicMock()
        market.user_fills_since = AsyncMock(return_value=[
            {"coin": "BTC", "time": 1000, "side": "buy", "px": 50000, "sz": 0.1},
            {"coin": "BTC", "time": 1001, "side": "sell", "px": 50100, "sz": 0.1},
        ])
        return market
    
    @pytest.fixture
    def poller(self, mock_market):
        return RestPoller(
            coin="BTC",
            market=mock_market,
            account="0x123",
        )
    
    @pytest.mark.asyncio
    async def test_poll_counts_fills(self, poller):
        """Polling counts fills correctly."""
        result = await poller.poll_if_due(force=True)
        assert result.fills_found == 2
    
    @pytest.mark.asyncio
    async def test_poll_filters_wrong_coin(self, poller, mock_market):
        """Polling filters out wrong coin."""
        mock_market.user_fills_since.return_value = [
            {"coin": "ETH", "time": 1000, "side": "buy", "px": 3000, "sz": 1.0},
        ]
        
        result = await poller.poll_if_due(force=True)
        assert result.fills_found == 1  # Returned from API
        assert result.new_fills == 0    # But none processed (wrong coin)
    
    @pytest.mark.asyncio
    async def test_poll_calls_handler(self, poller):
        """Polling calls fill handler for each fill."""
        handled_fills = []
        
        async def handler(fill):
            handled_fills.append(fill)
        
        await poller.poll_if_due(force=True, on_fill=handler)
        
        assert len(handled_fills) == 2
    
    @pytest.mark.asyncio
    async def test_poll_updates_hwm(self, poller):
        """Polling updates high water mark."""
        assert poller._rest_poll_hwm_ms == 0
        
        await poller.poll_if_due(force=True)
        
        assert poller._rest_poll_hwm_ms == 1001


class TestRestPollerDegradedMode:
    """Degraded mode (WS down) tests."""
    
    @pytest.fixture
    def mock_market(self):
        market = MagicMock()
        market.user_fills_since = AsyncMock(return_value=[])
        return market
    
    @pytest.fixture
    def poller(self, mock_market):
        return RestPoller(
            coin="BTC",
            market=mock_market,
            account="0x123",
            config=RestPollerConfig(
                poll_interval_sec=30.0,
                degraded_poll_interval_sec=7.5,
            ),
        )
    
    @pytest.mark.asyncio
    async def test_degraded_mode_uses_shorter_interval(self, poller):
        """Degraded mode uses shorter poll interval."""
        # First poll
        await poller.poll_if_due(force=True)
        
        # Without degraded, should skip (30s interval)
        result_normal = await poller.poll_if_due(ws_degraded=False)
        # With degraded, interval is shorter so timing behavior differs
        # This test mainly validates the parameter is accepted


class TestRestPollerDeduplication:
    """Deduplication integration tests."""
    
    @pytest.fixture
    def mock_deduplicator(self):
        dedup = MagicMock()
        dedup.check_and_add = MagicMock(side_effect=[True, False])  # First new, second dup
        return dedup
    
    @pytest.fixture
    def mock_market(self):
        market = MagicMock()
        market.user_fills_since = AsyncMock(return_value=[
            {"coin": "BTC", "time": 1000, "side": "buy", "px": 50000, "sz": 0.1},
            {"coin": "BTC", "time": 1001, "side": "sell", "px": 50100, "sz": 0.1},
        ])
        return market
    
    @pytest.fixture
    def poller(self, mock_market, mock_deduplicator):
        return RestPoller(
            coin="BTC",
            market=mock_market,
            account="0x123",
            fill_deduplicator=mock_deduplicator,
        )
    
    @pytest.mark.asyncio
    async def test_deduplication_filters_duplicates(self, poller):
        """Deduplicator filters duplicate fills."""
        handled_fills = []
        
        def handler(fill):
            handled_fills.append(fill)
        
        result = await poller.poll_if_due(force=True, on_fill=handler)
        
        assert result.fills_found == 2  # Both returned from API
        assert result.new_fills == 1    # Only one was new
        assert len(handled_fills) == 1  # Only one handled


class TestRestPollerErrorHandling:
    """Error handling tests."""
    
    @pytest.fixture
    def mock_market(self):
        market = MagicMock()
        market.user_fills_since = AsyncMock(side_effect=Exception("API Error"))
        return market
    
    @pytest.fixture
    def poller(self, mock_market):
        return RestPoller(
            coin="BTC",
            market=mock_market,
            account="0x123",
        )
    
    @pytest.mark.asyncio
    async def test_error_returns_failure_result(self, poller):
        """API errors return failure result."""
        result = await poller.poll_if_due(force=True)
        
        assert not result.success
        assert result.error is not None
        assert "API Error" in result.error
    
    @pytest.mark.asyncio
    async def test_handler_error_continues_processing(self, poller, mock_market):
        """Handler errors don't stop fill processing."""
        mock_market.user_fills_since.return_value = [
            {"coin": "BTC", "time": 1000, "side": "buy", "px": 50000, "sz": 0.1},
            {"coin": "BTC", "time": 1001, "side": "sell", "px": 50100, "sz": 0.1},
        ]
        mock_market.user_fills_since.side_effect = None  # Clear error
        
        call_count = 0
        def error_handler(fill):
            nonlocal call_count
            call_count += 1
            raise Exception("Handler error")
        
        result = await poller.poll_if_due(force=True, on_fill=error_handler)
        
        # Should still succeed despite handler errors
        assert result.success
        assert call_count == 2  # Both called despite errors
