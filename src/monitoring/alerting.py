"""
Webhook alerting module for critical events.

Step C: Production Hardening
- Send alerts to webhooks (Slack, Discord, PagerDuty, generic HTTP)
- Rate limiting to prevent alert storms
- Alert batching for related events
- Async non-blocking delivery
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


class AlertSeverity(Enum):
    """Alert severity levels."""
    CRITICAL = auto()  # Immediate attention required
    WARNING = auto()   # Potential issue
    INFO = auto()      # Informational


class AlertType(Enum):
    """Types of alerts."""
    RISK_HALT = auto()
    CIRCUIT_BREAKER_OPEN = auto()
    CIRCUIT_BREAKER_CLOSED = auto()
    DRAWDOWN_WARNING = auto()
    POSITION_LIMIT = auto()
    API_ERROR = auto()
    WEBSOCKET_DISCONNECT = auto()
    ORDER_REJECTED = auto()
    FILL_ANOMALY = auto()
    STARTUP = auto()
    SHUTDOWN = auto()
    CUSTOM = auto()


@dataclass
class Alert:
    """An alert to be sent."""
    alert_type: AlertType
    severity: AlertSeverity
    title: str
    message: str
    timestamp_ms: int = field(default_factory=lambda: int(time.time() * 1000))
    details: Dict[str, Any] = field(default_factory=dict)
    coin: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "type": self.alert_type.name,
            "severity": self.severity.name,
            "title": self.title,
            "message": self.message,
            "timestamp_ms": self.timestamp_ms,
            "timestamp_iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(self.timestamp_ms / 1000)),
            "details": self.details,
            "coin": self.coin,
        }


@dataclass
class AlertConfig:
    """Configuration for alerting."""
    webhook_url: Optional[str] = None
    webhook_type: str = "generic"  # generic, slack, discord, pagerduty
    min_severity: AlertSeverity = AlertSeverity.WARNING
    rate_limit_seconds: int = 60  # Min seconds between same alert type
    batch_window_ms: int = 5000  # Batch alerts within this window
    enabled: bool = True
    include_details: bool = True
    bot_name: str = "GridBot"


class WebhookFormatter:
    """Formats alerts for different webhook types."""

    @staticmethod
    def format_generic(alert: Alert, config: AlertConfig) -> Dict[str, Any]:
        """Format for generic HTTP webhook."""
        return alert.to_dict()

    @staticmethod
    def format_slack(alert: Alert, config: AlertConfig) -> Dict[str, Any]:
        """Format for Slack webhook."""
        severity_emoji = {
            AlertSeverity.CRITICAL: "🚨",
            AlertSeverity.WARNING: "⚠️",
            AlertSeverity.INFO: "ℹ️",
        }
        emoji = severity_emoji.get(alert.severity, "📢")
        
        color = {
            AlertSeverity.CRITICAL: "#FF0000",
            AlertSeverity.WARNING: "#FFA500",
            AlertSeverity.INFO: "#0000FF",
        }.get(alert.severity, "#808080")
        
        fields = []
        if alert.coin:
            fields.append({"title": "Coin", "value": alert.coin, "short": True})
        fields.append({"title": "Type", "value": alert.alert_type.name, "short": True})
        
        if config.include_details and alert.details:
            for key, value in list(alert.details.items())[:5]:  # Limit to 5 fields
                fields.append({"title": key, "value": str(value), "short": True})
        
        return {
            "username": config.bot_name,
            "icon_emoji": ":robot_face:",
            "attachments": [{
                "color": color,
                "title": f"{emoji} {alert.title}",
                "text": alert.message,
                "fields": fields,
                "footer": f"{config.bot_name} | {alert.severity.name}",
                "ts": alert.timestamp_ms // 1000,
            }]
        }

    @staticmethod
    def format_discord(alert: Alert, config: AlertConfig) -> Dict[str, Any]:
        """Format for Discord webhook."""
        color = {
            AlertSeverity.CRITICAL: 0xFF0000,
            AlertSeverity.WARNING: 0xFFA500,
            AlertSeverity.INFO: 0x0000FF,
        }.get(alert.severity, 0x808080)
        
        fields = []
        if alert.coin:
            fields.append({"name": "Coin", "value": alert.coin, "inline": True})
        fields.append({"name": "Type", "value": alert.alert_type.name, "inline": True})
        
        if config.include_details and alert.details:
            for key, value in list(alert.details.items())[:5]:
                fields.append({"name": key, "value": str(value), "inline": True})
        
        return {
            "username": config.bot_name,
            "embeds": [{
                "title": alert.title,
                "description": alert.message,
                "color": color,
                "fields": fields,
                "footer": {"text": f"{config.bot_name} | {alert.severity.name}"},
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(alert.timestamp_ms / 1000)),
            }]
        }

    @staticmethod
    def format_pagerduty(alert: Alert, config: AlertConfig) -> Dict[str, Any]:
        """Format for PagerDuty Events API v2."""
        severity_map = {
            AlertSeverity.CRITICAL: "critical",
            AlertSeverity.WARNING: "warning",
            AlertSeverity.INFO: "info",
        }
        
        return {
            "routing_key": config.webhook_url,  # PagerDuty uses routing key
            "event_action": "trigger",
            "dedup_key": f"{config.bot_name}-{alert.alert_type.name}-{alert.coin or 'global'}",
            "payload": {
                "summary": f"{alert.title}: {alert.message}",
                "severity": severity_map.get(alert.severity, "warning"),
                "source": config.bot_name,
                "component": alert.coin or "global",
                "custom_details": alert.details,
            }
        }


class AlertManager:
    """
    Manages alert delivery with rate limiting and batching.
    
    Features:
    - Rate limiting per alert type
    - Alert batching within time window
    - Async non-blocking delivery
    - Multiple webhook support
    - Automatic retry on failure
    """

    def __init__(self, config: Optional[AlertConfig] = None) -> None:
        self.config = config or AlertConfig()
        self._last_alert_times: Dict[AlertType, int] = {}
        self._pending_alerts: List[Alert] = []
        self._batch_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()
        self._session: Optional[Any] = None  # aiohttp session

    async def send_alert(self, alert: Alert) -> bool:
        """
        Queue an alert for delivery.
        
        Args:
            alert: Alert to send
            
        Returns:
            True if alert was queued, False if rate limited or disabled
        """
        if not self.config.enabled:
            return False
        
        if not self.config.webhook_url:
            logger.debug(f"Alert not sent (no webhook): {alert.title}")
            return False
        
        # Check severity threshold
        if alert.severity.value > self.config.min_severity.value:
            return False
        
        # Check rate limit
        now_ms = int(time.time() * 1000)
        last_time = self._last_alert_times.get(alert.alert_type, 0)
        if now_ms - last_time < self.config.rate_limit_seconds * 1000:
            logger.debug(f"Alert rate limited: {alert.alert_type.name}")
            return False
        
        async with self._lock:
            self._pending_alerts.append(alert)
            self._last_alert_times[alert.alert_type] = now_ms
            
            # Start batch task if not running
            if self._batch_task is None or self._batch_task.done():
                self._batch_task = asyncio.create_task(self._batch_deliver())
        
        return True

    async def _batch_deliver(self) -> None:
        """Deliver batched alerts after window expires."""
        await asyncio.sleep(self.config.batch_window_ms / 1000)
        
        async with self._lock:
            alerts = self._pending_alerts.copy()
            self._pending_alerts.clear()
        
        if not alerts:
            return
        
        # If single alert, send directly; otherwise batch
        if len(alerts) == 1:
            await self._deliver_single(alerts[0])
        else:
            await self._deliver_batch(alerts)

    async def _deliver_single(self, alert: Alert) -> bool:
        """Deliver a single alert."""
        payload = self._format_alert(alert)
        return await self._http_post(payload)

    async def _deliver_batch(self, alerts: List[Alert]) -> bool:
        """Deliver multiple alerts as a batch."""
        if self.config.webhook_type == "slack":
            # Slack: Send multiple attachments
            payload = self._format_alert(alerts[0])
            for alert in alerts[1:]:
                single = self._format_alert(alert)
                if "attachments" in single:
                    payload["attachments"].extend(single["attachments"])
        elif self.config.webhook_type == "discord":
            # Discord: Send multiple embeds
            payload = self._format_alert(alerts[0])
            for alert in alerts[1:]:
                single = self._format_alert(alert)
                if "embeds" in single:
                    payload["embeds"].extend(single["embeds"])
        else:
            # Generic: Send array of alerts
            payload = {"alerts": [alert.to_dict() for alert in alerts]}
        
        return await self._http_post(payload)

    def _format_alert(self, alert: Alert) -> Dict[str, Any]:
        """Format alert based on webhook type."""
        formatters = {
            "generic": WebhookFormatter.format_generic,
            "slack": WebhookFormatter.format_slack,
            "discord": WebhookFormatter.format_discord,
            "pagerduty": WebhookFormatter.format_pagerduty,
        }
        formatter = formatters.get(self.config.webhook_type, WebhookFormatter.format_generic)
        return formatter(alert, self.config)

    async def _http_post(self, payload: Dict[str, Any], retries: int = 2) -> bool:
        """Send HTTP POST to webhook URL."""
        if not self.config.webhook_url:
            return False
        
        try:
            import aiohttp
            
            async with aiohttp.ClientSession() as session:
                for attempt in range(retries + 1):
                    try:
                        async with session.post(
                            self.config.webhook_url,
                            json=payload,
                            timeout=aiohttp.ClientTimeout(total=10),
                        ) as resp:
                            if resp.status < 300:
                                logger.debug(f"Alert delivered successfully")
                                return True
                            else:
                                logger.warning(f"Alert delivery failed: HTTP {resp.status}")
                    except asyncio.TimeoutError:
                        logger.warning(f"Alert delivery timeout (attempt {attempt + 1})")
                    except Exception as e:
                        logger.warning(f"Alert delivery error: {e}")
                    
                    if attempt < retries:
                        await asyncio.sleep(1 * (attempt + 1))  # Backoff
            
            return False
            
        except ImportError:
            # Fallback to urllib if aiohttp not available
            logger.debug("aiohttp not available, using sync fallback")
            return self._sync_post(payload)

    def _sync_post(self, payload: Dict[str, Any]) -> bool:
        """Synchronous fallback for HTTP POST."""
        import urllib.request
        
        try:
            req = urllib.request.Request(
                self.config.webhook_url,
                data=json.dumps(payload).encode("utf-8"),
                headers={"Content-Type": "application/json"},
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                return resp.status < 300
        except Exception as e:
            logger.warning(f"Sync alert delivery error: {e}")
            return False

    # ─────────────────────────────────────────────────────────────────────
    # Convenience Methods for Common Alerts
    # ─────────────────────────────────────────────────────────────────────

    async def alert_risk_halt(self, reason: str, coin: Optional[str] = None, **details) -> bool:
        """Send risk halt alert."""
        return await self.send_alert(Alert(
            alert_type=AlertType.RISK_HALT,
            severity=AlertSeverity.CRITICAL,
            title="Risk Halt Triggered",
            message=f"Trading halted: {reason}",
            coin=coin,
            details=details,
        ))

    async def alert_circuit_breaker(self, is_open: bool, reason: str, coin: Optional[str] = None, **details) -> bool:
        """Send circuit breaker alert."""
        alert_type = AlertType.CIRCUIT_BREAKER_OPEN if is_open else AlertType.CIRCUIT_BREAKER_CLOSED
        severity = AlertSeverity.WARNING if is_open else AlertSeverity.INFO
        title = "Circuit Breaker Opened" if is_open else "Circuit Breaker Closed"
        
        return await self.send_alert(Alert(
            alert_type=alert_type,
            severity=severity,
            title=title,
            message=reason,
            coin=coin,
            details=details,
        ))

    async def alert_drawdown_warning(self, drawdown_pct: float, coin: Optional[str] = None, **details) -> bool:
        """Send drawdown warning alert."""
        return await self.send_alert(Alert(
            alert_type=AlertType.DRAWDOWN_WARNING,
            severity=AlertSeverity.WARNING,
            title="Drawdown Warning",
            message=f"Drawdown reached {drawdown_pct:.1%}",
            coin=coin,
            details={"drawdown_pct": drawdown_pct, **details},
        ))

    async def alert_api_error(self, error: str, coin: Optional[str] = None, **details) -> bool:
        """Send API error alert."""
        return await self.send_alert(Alert(
            alert_type=AlertType.API_ERROR,
            severity=AlertSeverity.WARNING,
            title="API Error",
            message=error,
            coin=coin,
            details=details,
        ))

    async def alert_startup(self, coins: List[str], **details) -> bool:
        """Send startup alert."""
        return await self.send_alert(Alert(
            alert_type=AlertType.STARTUP,
            severity=AlertSeverity.INFO,
            title="Bot Started",
            message=f"GridBot started trading {', '.join(coins)}",
            details={"coins": coins, **details},
        ))

    async def alert_shutdown(self, reason: str = "normal", **details) -> bool:
        """Send shutdown alert."""
        severity = AlertSeverity.INFO if reason == "normal" else AlertSeverity.WARNING
        return await self.send_alert(Alert(
            alert_type=AlertType.SHUTDOWN,
            severity=severity,
            title="Bot Shutdown",
            message=f"GridBot shutting down: {reason}",
            details=details,
        ))


# Global alert manager instance
_alert_manager: Optional[AlertManager] = None


def get_alert_manager() -> AlertManager:
    """Get the global alert manager instance."""
    global _alert_manager
    if _alert_manager is None:
        _alert_manager = AlertManager()
    return _alert_manager


def configure_alerts(
    webhook_url: Optional[str] = None,
    webhook_type: str = "generic",
    min_severity: AlertSeverity = AlertSeverity.WARNING,
    enabled: bool = True,
    bot_name: str = "GridBot",
) -> AlertManager:
    """
    Configure the global alert manager.
    
    Args:
        webhook_url: URL to send alerts to
        webhook_type: Type of webhook (generic, slack, discord, pagerduty)
        min_severity: Minimum severity to send
        enabled: Whether alerting is enabled
        bot_name: Name to use in alerts
        
    Returns:
        Configured AlertManager
    """
    global _alert_manager
    _alert_manager = AlertManager(AlertConfig(
        webhook_url=webhook_url,
        webhook_type=webhook_type,
        min_severity=min_severity,
        enabled=enabled,
        bot_name=bot_name,
    ))
    return _alert_manager
