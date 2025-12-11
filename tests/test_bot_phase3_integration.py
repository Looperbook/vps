"""Integration tests for Phase 3 metric wiring in GridBot.

These tests create a minimal GridBot with stubbed Info/Router/StatusBoard
and verify that strategy/risk-related RichMetrics are updated when
calling internal workflows like _build_and_place_grid and _reconcile_position.
"""

import asyncio
import types
import time
from typing import Any, Dict

from src.bot import GridBot
from src.strategy.strategy import GridStrategy
from src.monitoring.metrics_rich import RichMetrics
from src.risk.risk import RiskEngine


class DummyInfo:
    def __init__(self, mid: float = 100.0):
        self._mid = mid

    def all_mids(self, dex: str = None):
        return {"xyz:TSLA": self._mid}

    # sync user_state used by _reconcile_position
    def user_state(self, account: str, dex: str = None):
        return {
            "marginSummary": {"accountValue": 10000.0, "sessionPnl": 5.0, "fundingAccrued": 0.0},
            "assetPositions": [{"position": {"coin": "xyz:TSLA", "szi": 0.0, "entryPx": 0.0}}],
        }

    def frontend_open_orders(self, account: str, dex: str = None):
        return []

    def subscribe(self, sub, cb):
        return None

    def unsubscribe(self, sub, id):
        return None


class DummyRouter:
    def start(self):
        pass

    def stop(self):
        pass

    async def submit(self, req):
        # return a simple ack with oid
        return {"oid": 1, "cloid": "c1"}

    async def safe_cancel(self, cloid=None, oid=None):
        return None

    async def cancel_all(self):
        return None


class DummyMetrics:
    async def set_gauge(self, *a, **k):
        return None

    async def add_counter(self, *a, **k):
        return None


class DummyStatusBoard:
    async def update(self, coin: str, payload: Dict[str, Any]):
        return None


def make_cfg():
    # minimal settings object with attributes used by GridBot and GridStrategy
    d = dict(
        dex="xyz",
        http_timeout=2.0,
        investment_usd=200.0,
        leverage=10.0,
        grids=6,
        base_spacing_pct=0.0006,
        max_spacing_pct=0.005,
        trailing_pct=0.001,
        loop_interval=0.1,
        rest_audit_interval=300,
        rest_fill_interval=20,
        fill_rescan_ms=4000,
        pnl_log_interval=60,
        stop_loss_low=0.0,
        stop_loss_high=999999,
        max_drawdown_pct=0.12,
        max_position_abs=0.0,
        max_symbol_notional=0.0,
        max_skew_ratio=0.6,
        max_unrealized_dd_pct=0.15,
        pnl_daily_stop=-1000000,
        pnl_daily_take=1000000,
        funding_bleed_pct=0.02,
        vol_circuit_sigma=4.0,
        trend_fast_ema=12,
        trend_slow_ema=48,
        atr_len=14,
        ewma_alpha=0.12,
        init_vol_multiplier=1.0,
        ws_stale_after=20.0,
        ws_watch_interval=5.0,
        data_halt_sec=120.0,
        rest_mid_backoff_sec=5.0,
        default_strategy="grid",
        metrics_port=9095,
        metrics_token=None,
        state_dir="state",
        fill_log_retention_days=7,
        fill_log_compact_interval_sec=3600,
        random_size_jitter=0.02,
        coalesce_ms=50,
        coalesce_pct=0.9,
        coalesce_window=50,
        log_submit_sample=0.05,
        skew_soft=0.2,
        skew_hard=0.5,
        use_cloid=True,
        log_order_intent_sample=0.0,
        reprice_tick_threshold=0,
        api_error_threshold=5,
        log_strategy_sample=0.0,
        # Optimization-1 & Optimization-2: New config fields
        mid_cache_ttl_ms=500,
        fill_log_batch_flush_sec=1.0,
    )
    obj = types.SimpleNamespace(**d)
    # provide minimal resolve_account used by GridBot
    obj.resolve_account = lambda: "test_account"
    return obj


async def run_integration():
    cfg = make_cfg()
    info = DummyInfo(mid=123.45)
    async_info = None
    exchange = None
    metrics = DummyMetrics()
    nonce_lock = asyncio.Lock()
    status_board = DummyStatusBoard()

    bot = GridBot("xyz:TSLA", cfg, info, async_info, exchange, metrics, nonce_lock, status_board)
    # attach lightweight strategy and router
    bot.strategy = GridStrategy(cfg, bot.tick_sz, bot.px_decimals, bot.sz_decimals)
    bot.router = DummyRouter()
    # replace rich_metrics with a fresh one for inspection
    bot.rich_metrics = RichMetrics()
    
    # Set up services to use the new architecture path (not fallbacks)
    from src.bot_factory import setup_test_services
    setup_test_services(bot)

    # ensure no exceptions when building grid
    await bot._build_and_place_grid(100.0)

    # After building, strategy.grid_center should be set and metric updated
    assert bot.strategy.grid_center == 100.0

    # collect metric samples to verify grid_center and grid_width_pct present
    found_grid_center = False
    found_grid_width = False
    for metric in bot.rich_metrics.registry.collect():
        if metric.name == "grid_center":
            for s in metric.samples:
                if s.labels.get("coin") == "xyz:TSLA":
                    found_grid_center = True
        if metric.name == "grid_width_pct":
            for s in metric.samples:
                if s.labels.get("coin") == "xyz:TSLA":
                    found_grid_width = True

    assert found_grid_center, "grid_center metric not exported"
    assert found_grid_width, "grid_width_pct metric not exported"

    # Test reconcile_position updates risk metrics
    # mutate info.user_state to return a position
    info2 = DummyInfo(mid=200.0)
    info2.user_state = lambda account, dex=None: {
        "marginSummary": {"accountValue": 20000.0, "sessionPnl": 10.0, "fundingAccrued": 1.0},
        "assetPositions": [{"position": {"coin": "xyz:TSLA", "szi": 2.0, "entryPx": 150.0}}],
    }
    bot.info = info2
    await bot._reconcile_position()

    # After reconcile, position and daily_pnl should be reflected in metrics
    found_daily_pnl = False
    for metric in bot.rich_metrics.registry.collect():
        if metric.name == "daily_pnl":
            for s in metric.samples:
                if s.labels.get("coin") == "xyz:TSLA":
                    found_daily_pnl = True

    assert found_daily_pnl, "daily_pnl metric not exported"


def test_phase3_integration():
    asyncio.get_event_loop().run_until_complete(run_integration())

