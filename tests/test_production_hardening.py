"""
Tests for Step C: Production Hardening modules.

Tests cover:
- HealthChecker and health endpoints
- ConfigValidator schema validation
- AlertManager and webhook formatting
"""

import asyncio
import json
import time
from unittest.mock import MagicMock, AsyncMock, patch

import pytest

from src.monitoring.metrics import HealthChecker, HealthStatus, Metrics, start_metrics_server
from src.config.config_validator import (
    ConfigValidator,
    ValidationResult,
    ValidationIssue,
    ValidationSeverity,
    validate_config,
    validate_and_log,
)
from src.monitoring.alerting import (
    Alert,
    AlertConfig,
    AlertManager,
    AlertSeverity,
    AlertType,
    WebhookFormatter,
    configure_alerts,
    get_alert_manager,
)


# ─────────────────────────────────────────────────────────────────────────────
# HealthChecker Tests
# ─────────────────────────────────────────────────────────────────────────────


class TestHealthChecker:
    """Tests for HealthChecker."""

    def test_initial_state(self):
        """Test initial health state."""
        hc = HealthChecker()
        assert hc.is_healthy() is True
        assert hc.is_ready() is False  # Not ready until explicitly set

    def test_set_component_health(self):
        """Test setting component health."""
        hc = HealthChecker()
        hc.set_component_health("database", True)
        hc.set_component_health("api", True)
        
        assert hc.is_healthy() is True
        assert hc._components["database"] is True
        assert hc._components["api"] is True

    def test_unhealthy_component(self):
        """Test unhealthy component makes system unhealthy."""
        hc = HealthChecker()
        hc.set_component_health("database", True)
        hc.set_component_health("api", False)
        
        assert hc.is_healthy() is False

    def test_set_ready(self):
        """Test setting ready state."""
        hc = HealthChecker()
        hc.set_component_health("all", True)
        
        assert hc.is_ready() is False
        
        hc.set_ready(True)
        assert hc.is_ready() is True

    def test_ready_requires_healthy(self):
        """Test ready requires healthy."""
        hc = HealthChecker()
        hc.set_ready(True)
        hc.set_component_health("api", False)
        
        assert hc.is_ready() is False

    def test_heartbeat(self):
        """Test heartbeat updates timestamp."""
        hc = HealthChecker()
        before = hc._last_heartbeat
        time.sleep(0.01)
        hc.heartbeat()
        
        assert hc._last_heartbeat > before

    def test_get_status(self):
        """Test getting full status."""
        hc = HealthChecker()
        hc.set_component_health("ws", True, "Connected")
        hc.set_component_health("rest", True, "Available")
        hc.set_ready(True)
        
        status = hc.get_status()
        
        assert isinstance(status, HealthStatus)
        assert status.healthy is True
        assert status.ready is True
        assert status.components["ws"] is True
        assert status.details["ws"] == "Connected"

    def test_to_dict(self):
        """Test serialization to dict."""
        hc = HealthChecker()
        hc.set_component_health("test", True)
        hc.set_ready(True)
        
        data = hc.to_dict()
        
        assert "healthy" in data
        assert "ready" in data
        assert "components" in data
        assert "last_heartbeat_ms" in data

    def test_callback_registration(self):
        """Test health change callbacks."""
        hc = HealthChecker()
        events = []
        
        hc.register_callback(lambda name, healthy: events.append((name, healthy)))
        hc.set_component_health("test", True)
        hc.set_component_health("test", False)
        
        assert len(events) == 2
        assert events[0] == ("test", True)
        assert events[1] == ("test", False)


# ─────────────────────────────────────────────────────────────────────────────
# ConfigValidator Tests
# ─────────────────────────────────────────────────────────────────────────────


class TestConfigValidator:
    """Tests for ConfigValidator."""

    @pytest.fixture
    def mock_cfg(self):
        """Create a mock valid config."""
        cfg = MagicMock()
        cfg.base_url = "https://api.hyperliquid.xyz"
        cfg.dex = "xyz"
        cfg.coins = ["xyz:TSLA", "xyz:NVDA"]
        cfg.agent_key = "test_key"
        cfg.user_address = "0x123"
        cfg.private_key = None
        cfg.investment_usd = 1000.0
        cfg.leverage = 10.0
        cfg.grids = 30
        cfg.base_spacing_pct = 0.0006
        cfg.max_spacing_pct = 0.005
        cfg.trailing_pct = 0.001
        cfg.loop_interval = 2.0
        cfg.rest_audit_interval = 300.0
        cfg.max_drawdown_pct = 0.12
        cfg.max_unrealized_dd_pct = 0.15
        cfg.funding_bleed_pct = 0.02
        cfg.pnl_daily_stop = -500.0
        cfg.pnl_daily_take = 1000.0
        cfg.ws_stale_after = 20.0
        cfg.http_timeout = 30.0
        cfg.coalesce_ms = 500
        cfg.api_error_threshold = 5
        return cfg

    def test_valid_config(self, mock_cfg):
        """Test validation of valid config."""
        result = validate_config(mock_cfg)
        
        assert result.valid is True
        assert len(result.get_errors()) == 0

    def test_missing_required_string(self, mock_cfg):
        """Test error for missing required string."""
        mock_cfg.base_url = ""
        
        result = validate_config(mock_cfg)
        
        assert result.valid is False
        errors = result.get_errors()
        assert any(e.field == "base_url" for e in errors)

    def test_numeric_below_minimum(self, mock_cfg):
        """Test error for value below minimum."""
        mock_cfg.investment_usd = 1.0  # Below 10.0 minimum
        
        result = validate_config(mock_cfg)
        
        assert result.valid is False
        errors = result.get_errors()
        assert any(e.field == "investment_usd" for e in errors)

    def test_numeric_above_maximum(self, mock_cfg):
        """Test error for value above maximum."""
        mock_cfg.leverage = 100.0  # Above 50.0 maximum
        
        result = validate_config(mock_cfg)
        
        assert result.valid is False
        errors = result.get_errors()
        assert any(e.field == "leverage" for e in errors)

    def test_conditional_requirement(self, mock_cfg):
        """Test conditional requirement validation."""
        mock_cfg.agent_key = "test_key"
        mock_cfg.user_address = None  # Missing when agent_key is set
        
        result = validate_config(mock_cfg)
        
        assert result.valid is False
        errors = result.get_errors()
        assert any("user_address" in e.field for e in errors)

    def test_invalid_coin_format(self, mock_cfg):
        """Test error for invalid coin format."""
        mock_cfg.coins = ["TSLA"]  # Missing dex prefix
        
        result = validate_config(mock_cfg)
        
        assert result.valid is False
        errors = result.get_errors()
        assert any(e.field == "coins" for e in errors)

    def test_no_coins(self, mock_cfg):
        """Test error for no coins configured."""
        mock_cfg.coins = []
        
        result = validate_config(mock_cfg)
        
        assert result.valid is False
        errors = result.get_errors()
        assert any(e.field == "coins" for e in errors)

    def test_high_leverage_warning(self, mock_cfg):
        """Test warning for high leverage."""
        mock_cfg.leverage = 25.0  # Above 20 warning threshold
        
        result = validate_config(mock_cfg)
        
        warnings = result.get_warnings()
        assert any(e.field == "leverage" for e in warnings)

    def test_low_grid_count_warning(self, mock_cfg):
        """Test warning for low grid count."""
        mock_cfg.grids = 5  # Below 10 warning threshold
        
        result = validate_config(mock_cfg)
        
        warnings = result.get_warnings()
        assert any(e.field == "grids" for e in warnings)

    def test_no_auth_error(self, mock_cfg):
        """Test error when no authentication configured."""
        mock_cfg.agent_key = None
        mock_cfg.private_key = None
        
        result = validate_config(mock_cfg)
        
        assert result.valid is False
        errors = result.get_errors()
        assert any(e.field == "agent_key" for e in errors)

    def test_many_coins_warning(self, mock_cfg):
        """Test warning for many coins."""
        mock_cfg.coins = [f"xyz:COIN{i}" for i in range(15)]
        
        result = validate_config(mock_cfg)
        
        warnings = result.get_warnings()
        assert any(e.field == "coins" for e in warnings)

    def test_custom_validator(self, mock_cfg):
        """Test custom validator registration."""
        validator = ConfigValidator()
        
        def custom_check(cfg):
            if cfg.grids > 100:
                return [ValidationIssue(
                    field="grids",
                    message="Too many grids",
                    severity=ValidationSeverity.ERROR,
                )]
            return []
        
        validator.register_validator(custom_check)
        mock_cfg.grids = 150
        
        result = validator.validate(mock_cfg)
        
        assert result.valid is False

    def test_validate_and_log(self, mock_cfg, caplog):
        """Test validate_and_log function."""
        mock_cfg.investment_usd = 1.0  # Invalid
        
        import logging
        with caplog.at_level(logging.ERROR):
            is_valid = validate_and_log(mock_cfg)
        
        assert is_valid is False
        assert "CONFIG ERROR" in caplog.text


# ─────────────────────────────────────────────────────────────────────────────
# AlertManager Tests
# ─────────────────────────────────────────────────────────────────────────────


class TestAlertManager:
    """Tests for AlertManager."""

    @pytest.fixture
    def alert_config(self):
        """Create alert config."""
        return AlertConfig(
            webhook_url="https://hooks.example.com/test",
            webhook_type="generic",
            min_severity=AlertSeverity.WARNING,
            rate_limit_seconds=1,
            batch_window_ms=100,
            enabled=True,
        )

    @pytest.fixture
    def alert_manager(self, alert_config):
        """Create AlertManager."""
        return AlertManager(alert_config)

    def test_alert_creation(self):
        """Test creating an alert."""
        alert = Alert(
            alert_type=AlertType.RISK_HALT,
            severity=AlertSeverity.CRITICAL,
            title="Test Alert",
            message="Test message",
            coin="TSLA",
            details={"key": "value"},
        )
        
        assert alert.alert_type == AlertType.RISK_HALT
        assert alert.severity == AlertSeverity.CRITICAL
        assert alert.coin == "TSLA"
        assert alert.timestamp_ms > 0

    def test_alert_to_dict(self):
        """Test alert serialization."""
        alert = Alert(
            alert_type=AlertType.DRAWDOWN_WARNING,
            severity=AlertSeverity.WARNING,
            title="Drawdown",
            message="High drawdown",
        )
        
        data = alert.to_dict()
        
        assert data["type"] == "DRAWDOWN_WARNING"
        assert data["severity"] == "WARNING"
        assert "timestamp_iso" in data

    @pytest.mark.asyncio
    async def test_alert_disabled(self, alert_config):
        """Test alerts not sent when disabled."""
        alert_config.enabled = False
        manager = AlertManager(alert_config)
        
        result = await manager.send_alert(Alert(
            alert_type=AlertType.RISK_HALT,
            severity=AlertSeverity.CRITICAL,
            title="Test",
            message="Test",
        ))
        
        assert result is False

    @pytest.mark.asyncio
    async def test_alert_no_webhook(self):
        """Test alerts not sent without webhook URL."""
        config = AlertConfig(webhook_url=None, enabled=True)
        manager = AlertManager(config)
        
        result = await manager.send_alert(Alert(
            alert_type=AlertType.RISK_HALT,
            severity=AlertSeverity.CRITICAL,
            title="Test",
            message="Test",
        ))
        
        assert result is False

    @pytest.mark.asyncio
    async def test_severity_filtering(self, alert_config):
        """Test alerts filtered by severity."""
        alert_config.min_severity = AlertSeverity.WARNING
        manager = AlertManager(alert_config)
        
        # INFO severity should be filtered
        result = await manager.send_alert(Alert(
            alert_type=AlertType.STARTUP,
            severity=AlertSeverity.INFO,
            title="Info",
            message="Info message",
        ))
        
        assert result is False

    @pytest.mark.asyncio
    async def test_rate_limiting(self, alert_config):
        """Test rate limiting of same alert type."""
        alert_config.rate_limit_seconds = 10
        manager = AlertManager(alert_config)
        
        # First alert should be queued
        with patch.object(manager, '_http_post', new_callable=AsyncMock) as mock_post:
            mock_post.return_value = True
            
            result1 = await manager.send_alert(Alert(
                alert_type=AlertType.RISK_HALT,
                severity=AlertSeverity.CRITICAL,
                title="Test1",
                message="Test1",
            ))
            
            # Same type within rate limit window
            result2 = await manager.send_alert(Alert(
                alert_type=AlertType.RISK_HALT,
                severity=AlertSeverity.CRITICAL,
                title="Test2",
                message="Test2",
            ))
        
        assert result1 is True
        assert result2 is False  # Rate limited


class TestWebhookFormatter:
    """Tests for WebhookFormatter."""

    @pytest.fixture
    def sample_alert(self):
        """Create sample alert."""
        return Alert(
            alert_type=AlertType.RISK_HALT,
            severity=AlertSeverity.CRITICAL,
            title="Risk Halt",
            message="Trading halted due to drawdown",
            coin="TSLA",
            details={"drawdown_pct": 0.15, "equity": 850.0},
        )

    @pytest.fixture
    def config(self):
        """Create alert config."""
        return AlertConfig(bot_name="TestBot", include_details=True)

    def test_format_generic(self, sample_alert, config):
        """Test generic webhook formatting."""
        payload = WebhookFormatter.format_generic(sample_alert, config)
        
        assert payload["type"] == "RISK_HALT"
        assert payload["severity"] == "CRITICAL"
        assert payload["title"] == "Risk Halt"
        assert payload["coin"] == "TSLA"

    def test_format_slack(self, sample_alert, config):
        """Test Slack webhook formatting."""
        payload = WebhookFormatter.format_slack(sample_alert, config)
        
        assert "attachments" in payload
        assert len(payload["attachments"]) == 1
        attachment = payload["attachments"][0]
        assert "🚨" in attachment["title"]  # Critical emoji
        assert attachment["color"] == "#FF0000"  # Red for critical

    def test_format_discord(self, sample_alert, config):
        """Test Discord webhook formatting."""
        payload = WebhookFormatter.format_discord(sample_alert, config)
        
        assert "embeds" in payload
        assert len(payload["embeds"]) == 1
        embed = payload["embeds"][0]
        assert embed["color"] == 0xFF0000  # Red for critical
        assert any(f["name"] == "Coin" for f in embed["fields"])

    def test_format_pagerduty(self, sample_alert, config):
        """Test PagerDuty webhook formatting."""
        config.webhook_url = "test_routing_key"
        payload = WebhookFormatter.format_pagerduty(sample_alert, config)
        
        assert payload["event_action"] == "trigger"
        assert payload["payload"]["severity"] == "critical"
        assert "drawdown_pct" in payload["payload"]["custom_details"]


class TestAlertConvenienceMethods:
    """Tests for AlertManager convenience methods."""

    @pytest.fixture
    def manager(self):
        """Create AlertManager with mock HTTP."""
        config = AlertConfig(
            webhook_url="https://test.com/hook",
            enabled=True,
            min_severity=AlertSeverity.INFO,
        )
        return AlertManager(config)

    @pytest.mark.asyncio
    async def test_alert_risk_halt(self, manager):
        """Test risk halt alert."""
        with patch.object(manager, '_http_post', new_callable=AsyncMock) as mock:
            mock.return_value = True
            result = await manager.alert_risk_halt("drawdown", coin="TSLA", equity=850.0)
        
        assert result is True

    @pytest.mark.asyncio
    async def test_alert_circuit_breaker(self, manager):
        """Test circuit breaker alert."""
        with patch.object(manager, '_http_post', new_callable=AsyncMock) as mock:
            mock.return_value = True
            result = await manager.alert_circuit_breaker(True, "API errors exceeded threshold")
        
        assert result is True

    @pytest.mark.asyncio
    async def test_alert_drawdown_warning(self, manager):
        """Test drawdown warning alert."""
        with patch.object(manager, '_http_post', new_callable=AsyncMock) as mock:
            mock.return_value = True
            result = await manager.alert_drawdown_warning(0.08, coin="TSLA")
        
        assert result is True

    @pytest.mark.asyncio
    async def test_alert_startup(self, manager):
        """Test startup alert."""
        with patch.object(manager, '_http_post', new_callable=AsyncMock) as mock:
            mock.return_value = True
            result = await manager.alert_startup(["TSLA", "NVDA"], investment_usd=1000)
        
        assert result is True

    @pytest.mark.asyncio
    async def test_alert_shutdown(self, manager):
        """Test shutdown alert."""
        with patch.object(manager, '_http_post', new_callable=AsyncMock) as mock:
            mock.return_value = True
            result = await manager.alert_shutdown("normal")
        
        assert result is True


class TestGlobalAlertManager:
    """Tests for global alert manager functions."""

    def test_configure_alerts(self):
        """Test configuring global alert manager."""
        manager = configure_alerts(
            webhook_url="https://test.com",
            webhook_type="slack",
            enabled=True,
        )
        
        assert manager.config.webhook_url == "https://test.com"
        assert manager.config.webhook_type == "slack"

    def test_get_alert_manager(self):
        """Test getting global alert manager."""
        manager1 = get_alert_manager()
        manager2 = get_alert_manager()
        
        assert manager1 is manager2  # Same instance


# ─────────────────────────────────────────────────────────────────────────────
# Integration Tests
# ─────────────────────────────────────────────────────────────────────────────


class TestHealthEndpointIntegration:
    """Integration tests for health endpoints."""

    @pytest.mark.asyncio
    async def test_health_endpoint_healthy(self):
        """Test /health endpoint returns 200 when healthy."""
        metrics = Metrics()
        health_checker = HealthChecker()
        health_checker.set_component_health("test", True)
        
        # Start server on random port
        srv = await start_metrics_server(
            metrics, 0, status_board=None,
            auth_token=None, health_checker=health_checker
        )
        port = srv.sockets[0].getsockname()[1]
        
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.write(b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n")
            await writer.drain()
            response = await reader.read(1024)
            writer.close()
            await writer.wait_closed()
            
            assert b"200 OK" in response
            assert b'"healthy": true' in response
        finally:
            srv.close()
            await srv.wait_closed()

    @pytest.mark.asyncio
    async def test_health_endpoint_unhealthy(self):
        """Test /health endpoint returns 503 when unhealthy."""
        metrics = Metrics()
        health_checker = HealthChecker()
        health_checker.set_component_health("test", False)
        
        srv = await start_metrics_server(
            metrics, 0, status_board=None,
            auth_token=None, health_checker=health_checker
        )
        port = srv.sockets[0].getsockname()[1]
        
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.write(b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n")
            await writer.drain()
            response = await reader.read(1024)
            writer.close()
            await writer.wait_closed()
            
            assert b"503 Service Unavailable" in response
            assert b'"healthy": false' in response
        finally:
            srv.close()
            await srv.wait_closed()

    @pytest.mark.asyncio
    async def test_ready_endpoint(self):
        """Test /ready endpoint."""
        metrics = Metrics()
        health_checker = HealthChecker()
        health_checker.set_component_health("all", True)
        health_checker.set_ready(True)
        
        srv = await start_metrics_server(
            metrics, 0, status_board=None,
            auth_token=None, health_checker=health_checker
        )
        port = srv.sockets[0].getsockname()[1]
        
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.write(b"GET /ready HTTP/1.1\r\nHost: localhost\r\n\r\n")
            await writer.drain()
            response = await reader.read(1024)
            writer.close()
            await writer.wait_closed()
            
            assert b"200 OK" in response
            assert b'"ready": true' in response
        finally:
            srv.close()
            await srv.wait_closed()
