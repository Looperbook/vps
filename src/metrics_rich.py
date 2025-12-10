"""
Rich Prometheus metrics for production observability.

Organized into: execution, fills, strategy, risk, operational.
"""

from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry
from typing import Optional


class RichMetrics:
    """Comprehensive metrics for grid bot observability."""

    def __init__(self, registry: Optional[CollectorRegistry] = None):
        reg = registry or CollectorRegistry()

        # === Execution Metrics ===
        self.orders_submitted = Counter(
            'orders_submitted_total',
            'Orders submitted to exchange',
            labelnames=['coin', 'side'],
            registry=reg
        )
        self.orders_rejected = Counter(
            'orders_rejected_total',
            'Orders rejected by exchange',
            labelnames=['coin', 'reason'],
            registry=reg
        )
        self.orders_cancelled = Counter(
            'orders_cancelled_total',
            'Orders cancelled',
            labelnames=['coin', 'reason'],
            registry=reg
        )
        self.order_latency_ms = Histogram(
            'order_latency_ms',
            'Time from submit to ACK (milliseconds)',
            labelnames=['coin'],
            buckets=[5, 10, 20, 50, 100, 200, 500, 1000],
            registry=reg
        )

        # === Fill Metrics ===
        self.fills_total = Counter(
            'fills_total',
            'Fills executed',
            labelnames=['coin', 'side'],
            registry=reg
        )
        self.fill_pnl = Histogram(
            'fill_pnl',
            'PnL per fill (USD)',
            labelnames=['coin'],
            buckets=[-100, -50, -10, 0, 10, 50, 100, 500],
            registry=reg
        )
        self.position = Gauge(
            'position',
            'Current position (coins)',
            labelnames=['coin'],
            registry=reg
        )
        self.realized_pnl = Gauge(
            'realized_pnl',
            'Realized PnL (USD)',
            labelnames=['coin'],
            registry=reg
        )
        self.fill_reconciliation_lag = Gauge(
            'fill_reconciliation_lag_sec',
            'Time since last fill reconciliation (seconds)',
            labelnames=['coin'],
            registry=reg
        )

        # === Strategy Metrics ===
        self.grid_width_pct = Gauge(
            'grid_width_pct',
            'Current grid spacing (%)',
            labelnames=['coin'],
            registry=reg
        )
        self.grid_center = Gauge(
            'grid_center',
            'Grid center price',
            labelnames=['coin'],
            registry=reg
        )
        self.skew_ratio = Gauge(
            'skew_ratio',
            'Position / target position',
            labelnames=['coin'],
            registry=reg
        )
        self.volatility_estimate = Gauge(
            'volatility_estimate',
            'EWMA volatility estimate',
            labelnames=['coin'],
            registry=reg
        )
        self.atr_value = Gauge(
            'atr_value',
            'ATR value',
            labelnames=['coin'],
            registry=reg
        )
        self.trend_bias = Gauge(
            'trend_bias',
            'Trend bias multiplier',
            labelnames=['coin'],
            registry=reg
        )

        # === Risk Metrics ===
        self.max_drawdown_pct = Gauge(
            'max_drawdown_pct',
            'Max drawdown from high water mark (%)',
            labelnames=['coin'],
            registry=reg
        )
        self.margin_utilization_pct = Gauge(
            'margin_utilization_pct',
            'Used margin / available margin (%)',
            labelnames=['coin'],
            registry=reg
        )
        self.daily_pnl = Gauge(
            'daily_pnl',
            'Daily realized PnL (USD)',
            labelnames=['coin'],
            registry=reg
        )
        self.funding_paid = Counter(
            'funding_paid_total',
            'Cumulative funding paid (USD)',
            labelnames=['coin'],
            registry=reg
        )
        self.risk_halted = Gauge(
            'risk_halted',
            'Risk engine halt status (1=halted, 0=ok)',
            labelnames=['coin'],
            registry=reg
        )

        # === Operational Metrics ===
        self.ws_gaps_total = Counter(
            'ws_gaps_total',
            'WebSocket feed gaps (fills missed)',
            labelnames=['coin'],
            registry=reg
        )
        # C-3 FIX: Track WS reconnect REST polls for observability
        self.ws_reconnect_rest_polls = Counter(
            'ws_reconnect_rest_polls_total',
            'REST fill polls triggered by WS reconnect (fill gap recovery)',
            labelnames=['coin'],
            registry=reg
        )
        self.state_reconciliation_ms = Histogram(
            'state_reconciliation_ms',
            'REST audit duration (milliseconds)',
            labelnames=['coin'],
            buckets=[100, 500, 1000, 2000, 5000, 10000],
            registry=reg
        )
        self.api_errors_total = Counter(
            'api_errors_total',
            'API errors encountered',
            labelnames=['coin', 'error_type'],
            registry=reg
        )
        self.api_error_streak = Gauge(
            'api_error_streak',
            'Current API error streak (count)',
            labelnames=['coin'],
            registry=reg
        )
        self.fill_log_size_bytes = Gauge(
            'fill_log_size_bytes',
            'Fill log file size (bytes)',
            labelnames=['coin'],
            registry=reg
        )
        self.fill_log_entries = Gauge(
            'fill_log_entries',
            'Number of entries in fill log',
            labelnames=['coin'],
            registry=reg
        )
        self.state_save_duration_ms = Histogram(
            'state_save_duration_ms',
            'Time to save state (milliseconds)',
            labelnames=['coin'],
            buckets=[1, 5, 10, 50, 100, 500],
            registry=reg
        )
        
        # === Durability Metrics ===
        self.fill_log_write_errors = Counter(
            'fill_log_write_errors_total',
            'Fill log write failures (potential data loss)',
            labelnames=['coin'],
            registry=reg
        )
        self.position_drift_detected = Counter(
            'position_drift_detected_total',
            'Position drift events detected during reconciliation',
            labelnames=['coin'],
            registry=reg
        )
        self.position_drift_amount = Gauge(
            'position_drift_amount',
            'Last detected position drift amount (coins)',
            labelnames=['coin'],
            registry=reg
        )
        # S-1: Shadow ledger drift alerts (significant drift > 10%)
        self.position_drift_alerts = Counter(
            'position_drift_alerts_total',
            'Significant position drift alerts from shadow ledger',
            labelnames=['coin'],
            registry=reg
        )
        self.stuck_orders_count = Gauge(
            'stuck_orders_count',
            'Orders stuck in PENDING state > threshold',
            labelnames=['coin'],
            registry=reg
        )
        self.fill_timestamp_anomalies = Counter(
            'fill_timestamp_anomalies_total',
            'Fills with suspicious timestamps (future/very old)',
            labelnames=['coin', 'type'],
            registry=reg
        )
        self.unusual_pnl_fills = Counter(
            'unusual_pnl_fills_total',
            'Fills with unusually large PnL',
            labelnames=['coin'],
            registry=reg
        )
        self.ws_degraded_mode = Gauge(
            'ws_degraded_mode',
            'WebSocket degraded mode (1=degraded/faster REST polling)',
            labelnames=['coin'],
            registry=reg
        )

        # === Bot Lifecycle ===
        self.bot_started = Counter(
            'bot_started_total',
            'Bot instances started',
            labelnames=['coin'],
            registry=reg
        )
        self.bot_stopped = Counter(
            'bot_stopped_total',
            'Bot instances stopped',
            labelnames=['coin'],
            registry=reg
        )
        self.bot_cycles = Counter(
            'bot_cycles_total',
            'Main loop cycles executed',
            labelnames=['coin'],
            registry=reg
        )

        self.registry = reg

    def get_registry(self):
        """Return the Prometheus registry for export."""
        return self.registry
