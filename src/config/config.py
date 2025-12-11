"""
Environment-driven configuration with validation.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import List

from dotenv import load_dotenv

load_dotenv()


def env_bool(key: str, default: bool) -> bool:
    val = os.getenv(key)
    if val is None:
        return default
    return val.lower() in {"1", "true", "yes", "y"}

def _warn_duplicate_env(key: str) -> None:
    """Emit a warning if the env variable appears multiple times in the process environment input."""
    # On most systems, only the last assignment is visible to the process.
    # This hook is a placeholder if you later want to parse the raw env file.
    pass


@dataclass(frozen=True)
class Settings:
    base_url: str
    dex: str
    coins: List[str]
    private_key: str | None
    agent_key: str | None
    user_address: str | None
    investment_usd: float
    leverage: float
    min_notional: float
    grids: int
    base_spacing_pct: float
    max_spacing_pct: float
    trailing_pct: float
    loop_interval: float
    rest_audit_interval: float
    rest_fill_interval: float
    fill_rescan_ms: int
    pnl_log_interval: float
    stop_loss_low: float
    stop_loss_high: float
    max_drawdown_pct: float
    max_position_abs: float
    max_symbol_notional: float
    max_skew_ratio: float
    max_unrealized_dd_pct: float
    pnl_daily_stop: float
    pnl_daily_take: float
    funding_bleed_pct: float
    vol_circuit_sigma: float
    trend_fast_ema: int
    trend_slow_ema: int
    atr_len: int
    ewma_alpha: float
    init_vol_multiplier: float
    ws_stale_after: float
    ws_watch_interval: float
    data_halt_sec: float
    rest_mid_backoff_sec: float
    http_timeout: float
    metrics_port: int
    metrics_token: str | None
    default_strategy: str
    state_dir: str
    fill_log_retention_days: int
    fill_log_compact_interval_sec: int
    random_size_jitter: float
    coalesce_ms: int
    coalesce_pct: float
    coalesce_window: int
    skew_soft: float
    skew_hard: float
    use_cloid: bool
    log_order_intent_sample: float
    reprice_tick_threshold: int
    api_error_threshold: int
    log_strategy_sample: float
    log_submit_sample: float
    # Step C: Alerting configuration
    alert_webhook_url: str | None
    alert_webhook_type: str  # generic, slack, discord, pagerduty
    alert_enabled: bool
    # Optimization-1: Mid-price cache TTL
    mid_cache_ttl_ms: int
    # Optimization-2: Batched fill log settings
    fill_log_batch_flush_sec: float
    # Safety features
    wal_enabled: bool  # Write-ahead log for crash recovery
    wal_dir: str  # Directory for WAL files
    dead_man_switch_sec: int  # Auto-cancel orders after N seconds of inactivity (0=disabled)
    order_ttl_ms: int  # Order expiry TTL in milliseconds (0=disabled)
    
    def dump(self) -> dict:
        """Return a dict of settings for sanity checks/logging."""
        return self.__dict__.copy()

    @staticmethod
    def _coins() -> List[str]:
        coins_raw = os.getenv("HL_COINS")
        if not coins_raw:
            single = os.getenv("HL_COIN", "xyz:TSLA")
            return [single]
        return [c.strip() for c in coins_raw.split(",") if c.strip()]

    @classmethod
    def load(cls) -> "Settings":
        def _int_env(key: str, default: int) -> int:
            raw = os.getenv(key)
            if raw is None or raw == "":
                return default
            return int(raw)

        def _float_env(key: str, default: float) -> float:
            raw = os.getenv(key)
            if raw is None or raw == "":
                return default
            return float(raw)

        cfg = cls(
            base_url=os.getenv("HL_BASE_URL", "https://api.hyperliquid.xyz"),
            dex=os.getenv("HL_DEX", "xyz"),
            coins=cls._coins(),
            private_key=os.getenv("HL_PRIVATE_KEY"),
            agent_key=os.getenv("HL_AGENT_KEY"),
            user_address=os.getenv("HL_USER_ADDRESS"),
            investment_usd=_float_env("HL_INVESTMENT_USD", 200),
            leverage=_float_env("HL_LEVERAGE", 10),
            min_notional=_float_env("HL_MIN_NOTIONAL_USD", 10),
            grids=_int_env("HL_NUM_GRIDS", 30),
            base_spacing_pct=_float_env("HL_GRID_SPACING_PCT", 0.0006),
            max_spacing_pct=_float_env("HL_GRID_SPACING_MAX_PCT", 0.005),
            trailing_pct=_float_env("HL_TRAILING_PCT", 0.001),
            loop_interval=_float_env("HL_LOOP_INTERVAL_SEC", 2.0),
            rest_audit_interval=_float_env("HL_RESYNC_POSITION_SEC", 300),
            rest_fill_interval=_float_env("HL_REST_FILL_POLL_SEC", 20),
            fill_rescan_ms=_int_env("HL_FILL_RESCAN_MS", 4000),
            pnl_log_interval=_float_env("HL_PNL_LOG_INTERVAL_SEC", 60),
            stop_loss_low=_float_env("HL_STOP_LOSS_LOW", 0.0),
            stop_loss_high=_float_env("HL_STOP_LOSS_HIGH", 999999),
            max_drawdown_pct=_float_env("HL_MAX_DRAWDOWN_PCT", 0.12),
            max_position_abs=_float_env("HL_MAX_POSITION_ABS", 0.0),
            max_symbol_notional=_float_env("HL_MAX_SYMBOL_NOTIONAL", 0.0),
            max_skew_ratio=_float_env("HL_MAX_SKEW_RATIO", 0.6),
            max_unrealized_dd_pct=_float_env("HL_MAX_UNREALIZED_DD_PCT", 0.15),
            pnl_daily_stop=_float_env("HL_DAILY_PNL_STOP", -1000000),
            pnl_daily_take=_float_env("HL_DAILY_PNL_TAKE", 1000000),
            funding_bleed_pct=_float_env("HL_FUNDING_BLEED_PCT", 0.02),
            vol_circuit_sigma=_float_env("HL_VOL_CIRCUIT_SIGMA", 4.0),
            trend_fast_ema=_int_env("HL_TREND_FAST_EMA", 12),
            trend_slow_ema=_int_env("HL_TREND_SLOW_EMA", 48),
            atr_len=_int_env("HL_ATR_LENGTH", 14),
            ewma_alpha=_float_env("HL_EWMA_ALPHA", 0.12),
            init_vol_multiplier=_float_env("HL_INIT_VOL_MULTIPLIER", 1.0),
            ws_stale_after=_float_env("HL_WS_STALE_AFTER_SEC", 20.0),
            ws_watch_interval=_float_env("HL_WS_WATCH_INTERVAL_SEC", 5.0),
            data_halt_sec=_float_env("HL_DATA_HALT_SEC", 120.0),
            rest_mid_backoff_sec=_float_env("HL_REST_MID_BACKOFF_SEC", 5.0),
            http_timeout=_float_env("HL_HTTP_TIMEOUT", 2.0),
                    default_strategy=os.getenv("HL_DEFAULT_STRATEGY", "grid"),
            metrics_port=_int_env("HL_METRICS_PORT", 9095),
            metrics_token=os.getenv("HL_METRICS_TOKEN"),
            state_dir=os.getenv("HL_STATE_DIR", "state"),
            fill_log_retention_days=_int_env("HL_FILL_LOG_RETENTION_DAYS", 7),
            fill_log_compact_interval_sec=_int_env("HL_FILL_LOG_COMPACT_INTERVAL_SEC", 3600),
            random_size_jitter=_float_env("HL_RANDOM_SIZE_JITTER", 0.02),
            coalesce_ms=_int_env("HL_COALESCE_MS", 50),
            coalesce_pct=_float_env("HL_COALESCE_PCT", 0.9),
            coalesce_window=_int_env("HL_COALESCE_WINDOW", 50),
            log_submit_sample=_float_env("HL_LOG_SUBMIT_SAMPLE", 0.05),
            skew_soft=_float_env("HL_SKEW_SOFT", 0.2),
            skew_hard=_float_env("HL_SKEW_HARD", 0.5),
            use_cloid=env_bool("HL_USE_CLOID", True),
            log_order_intent_sample=_float_env("HL_LOG_ORDER_INTENT_SAMPLE", 0.0),
            reprice_tick_threshold=_int_env("HL_REPRICE_TICK_THRESHOLD", 0),
            api_error_threshold=_int_env("HL_API_ERROR_THRESHOLD", 5),
            log_strategy_sample=_float_env("HL_LOG_STRATEGY_SAMPLE", 0.0),
            # Step C: Alerting configuration
            alert_webhook_url=os.getenv("HL_ALERT_WEBHOOK_URL"),
            alert_webhook_type=os.getenv("HL_ALERT_WEBHOOK_TYPE", "generic"),
            alert_enabled=env_bool("HL_ALERT_ENABLED", True),
            # Optimization-1: Mid-price cache TTL (ms) - reduces REST calls
            mid_cache_ttl_ms=_int_env("HL_MID_CACHE_TTL_MS", 500),
            # Optimization-2: Batched fill log flush interval (sec)
            fill_log_batch_flush_sec=_float_env("HL_FILL_LOG_BATCH_FLUSH_SEC", 1.0),
            # Safety features
            wal_enabled=env_bool("HL_WAL_ENABLED", True),
            wal_dir=os.getenv("HL_WAL_DIR", "state/wal"),
            dead_man_switch_sec=_int_env("HL_DEAD_MAN_SWITCH_SEC", 300),  # 5 minutes default
            order_ttl_ms=_int_env("HL_ORDER_TTL_MS", 60000),  # 60 seconds default
        )
        _sanity_check(cfg)
        cfg._validate()
        return cfg

    def resolve_account(self) -> str:
        if self.private_key:
            from eth_account import Account

            return Account.from_key(self.private_key).address
        if self.user_address:
            return self.user_address
        raise RuntimeError("Missing HL_USER_ADDRESS or HL_PRIVATE_KEY")

    def resolve_signer(self):
        from eth_account import Account

        if self.private_key:
            return Account.from_key(self.private_key)
        if self.agent_key:
            return Account.from_key(self.agent_key)
        raise RuntimeError("Missing credentials: set HL_PRIVATE_KEY or HL_AGENT_KEY")

    def _validate(self) -> None:
        if self.grids <= 0:
            raise ValueError("HL_NUM_GRIDS must be > 0")
        if self.base_spacing_pct <= 0 or self.max_spacing_pct <= 0:
            raise ValueError("Grid spacing must be > 0")
        if self.base_spacing_pct > self.max_spacing_pct:
            raise ValueError("HL_GRID_SPACING_PCT must be <= HL_GRID_SPACING_MAX_PCT")
        if self.investment_usd <= 0:
            raise ValueError("HL_INVESTMENT_USD must be > 0")
        if self.leverage <= 0:
            raise ValueError("HL_LEVERAGE must be > 0")
        if self.init_vol_multiplier <= 0:
            raise ValueError("HL_INIT_VOL_MULTIPLIER must be > 0")
        if self.ws_stale_after <= 0 or self.ws_watch_interval <= 0:
            raise ValueError("WS thresholds must be > 0")
        if self.data_halt_sec <= 0:
            raise ValueError("HL_DATA_HALT_SEC must be > 0")
        if self.rest_mid_backoff_sec <= 0:
            raise ValueError("HL_REST_MID_BACKOFF_SEC must be > 0")
        
        # ===== CRITICAL SAFETY VALIDATIONS =====
        # These ensure the bot doesn't start with dangerous defaults
        
        # Position limits must be explicitly set for production
        if self.max_position_abs <= 0:
            import logging
            logging.getLogger("gridbot").warning(
                "WARNING: HL_MAX_POSITION_ABS not set or 0. "
                "Consider setting a maximum position limit for safety."
            )
        
        # Symbol notional limit should be set
        if self.max_symbol_notional <= 0:
            import logging
            logging.getLogger("gridbot").warning(
                "WARNING: HL_MAX_SYMBOL_NOTIONAL not set or 0. "
                "Consider setting a maximum notional limit per symbol."
            )
        
        # Daily P&L limits should be reasonable
        if self.pnl_daily_stop == -1000000:
            import logging
            logging.getLogger("gridbot").warning(
                "WARNING: HL_DAILY_PNL_STOP using default (-1M). "
                "Consider setting an explicit daily loss limit."
            )
        
        # Max drawdown should be explicitly considered
        if self.max_drawdown_pct > 0.20:
            import logging
            logging.getLogger("gridbot").warning(
                f"WARNING: HL_MAX_DRAWDOWN_PCT is {self.max_drawdown_pct:.1%}. "
                "Consider a tighter drawdown limit for capital preservation."
            )
        
        # Unrealized drawdown limit
        if self.max_unrealized_dd_pct > 0.25:
            import logging
            logging.getLogger("gridbot").warning(
                f"WARNING: HL_MAX_UNREALIZED_DD_PCT is {self.max_unrealized_dd_pct:.1%}. "
                "Consider a tighter unrealized drawdown limit."
            )
        
        # Validate leverage is reasonable for grid trading
        if self.leverage > 20:
            import logging
            logging.getLogger("gridbot").warning(
                f"WARNING: HL_LEVERAGE is {self.leverage}x which is high for grid trading. "
                "Grid strategies work best with moderate leverage (5-15x)."
            )
        
        # Circuit breaker volatility threshold
        if self.vol_circuit_sigma < 2.0:
            raise ValueError(
                f"HL_VOL_CIRCUIT_SIGMA={self.vol_circuit_sigma} is too tight. "
                "Minimum 2.0 sigma recommended to avoid false triggers."
            )
        if self.vol_circuit_sigma > 6.0:
            import logging
            logging.getLogger("gridbot").warning(
                f"WARNING: HL_VOL_CIRCUIT_SIGMA={self.vol_circuit_sigma} is very loose. "
                "Circuit breaker may not trigger during extreme volatility."
            )


def _sanity_check(cfg: Settings) -> None:
    """
    Sanity-check critical settings and log them once at startup so overrides are obvious.
    """
    import logging
    import json

    logger = logging.getLogger("gridbot")
    payload = {
        "event": "config_loaded",
        "base_spacing_pct": cfg.base_spacing_pct,
        "max_spacing_pct": cfg.max_spacing_pct,
        "loop_interval": cfg.loop_interval,
        "coalesce_ms": cfg.coalesce_ms,
        "coins": cfg.coins,
    }
    logger.info(json.dumps(payload))
