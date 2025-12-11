"""
BotFactory: Factory for creating fully-initialized GridBot instances.

This module extracts the complex initialization logic from GridBot.__init__()
and provides a clean factory interface for creating bots with all services
properly wired together.

Usage:
    from src.bot_factory import create_bot, BotDependencies
    
    deps = BotDependencies(
        coin="BTC",
        cfg=settings,
        info=info,
        exchange=exchange,
        ...
    )
    bot = await create_bot(deps)
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Dict, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from hyperliquid.exchange import Exchange
    from hyperliquid.info import Info
    from src.config.config import Settings
    from src.monitoring.metrics import Metrics
    from src.monitoring.status import StatusBoard
    from src.market_data.market_data import MarketData
    from src.monitoring.metrics_rich import RichMetrics
    from src.bot import GridBot

import logging

log = logging.getLogger("gridbot")


@dataclass
class BotDependencies:
    """All dependencies needed to create a GridBot."""
    coin: str
    cfg: "Settings"
    info: "Info"
    async_info: Optional[Any]  # AsyncInfo
    exchange: Optional["Exchange"]
    metrics: "Metrics"
    nonce_lock: asyncio.Lock
    status_board: "StatusBoard"
    per_coin_cfg: Optional[Dict[str, Any]] = None
    
    # Optional overrides for testing
    market: Optional["MarketData"] = None
    rich_metrics: Optional["RichMetrics"] = None


async def create_bot(deps: BotDependencies) -> "GridBot":
    """
    Create a fully-initialized GridBot with all services.
    
    This is the recommended way to create a GridBot instance.
    It ensures all services are properly wired together.
    
    Args:
        deps: All required dependencies
        
    Returns:
        Fully initialized GridBot ready to run
    """
    from src.bot import GridBot
    
    # Create bot instance
    bot = GridBot(
        coin=deps.coin,
        cfg=deps.cfg,
        info=deps.info,
        async_info=deps.async_info,
        exchange=deps.exchange,
        metrics=deps.metrics,
        nonce_lock=deps.nonce_lock,
        status_board=deps.status_board,
        per_coin_cfg=deps.per_coin_cfg,
    )
    
    # Override components if provided (for testing)
    if deps.market:
        bot.market = deps.market
    if deps.rich_metrics:
        bot.rich_metrics = deps.rich_metrics
    
    # Initialize bot (sets up services)
    await bot.initialize()
    
    return bot


def create_bot_sync(deps: BotDependencies) -> "GridBot":
    """
    Create a GridBot without async initialization.
    
    Use this for testing scenarios where you need more control
    over initialization, or when services will be manually set up.
    
    Args:
        deps: All required dependencies
        
    Returns:
        GridBot instance (not fully initialized)
    """
    from src.bot import GridBot
    
    bot = GridBot(
        coin=deps.coin,
        cfg=deps.cfg,
        info=deps.info,
        async_info=deps.async_info,
        exchange=deps.exchange,
        metrics=deps.metrics,
        nonce_lock=deps.nonce_lock,
        status_board=deps.status_board,
        per_coin_cfg=deps.per_coin_cfg,
    )
    
    # Override components if provided
    if deps.market:
        bot.market = deps.market
    if deps.rich_metrics:
        bot.rich_metrics = deps.rich_metrics
    
    return bot


def setup_test_services(bot: "GridBot") -> None:
    """
    Set up minimal services for testing without full initialization.
    
    This creates lightweight versions of services that don't require
    network access or external dependencies.
    
    Args:
        bot: GridBot instance to configure
    """
    from src.execution.rest_poller import RestPoller, RestPollerConfig
    from src.execution.reconciliation_service import ReconciliationService, ReconciliationConfig
    from src.execution.grid_builder import GridBuilder, GridBuilderConfig
    from src.execution.execution_gateway import ExecutionGateway, ExecutionGatewayConfig
    
    # Create RestPoller if market is available
    if bot.market and not bot.rest_poller:
        rest_config = RestPollerConfig(
            poll_interval_sec=30.0,
            log_event_callback=bot._log_event,
        )
        bot.rest_poller = RestPoller(
            coin=bot.coin,
            market=bot.market,
            account=bot.account,
            fill_deduplicator=bot.fill_deduplicator,
            config=rest_config,
        )
    
    # Create ReconciliationService
    if not bot.reconciliation_service:
        reconcile_config = ReconciliationConfig(
            dex=bot.cfg.dex if hasattr(bot.cfg, 'dex') else "hl",
            log_event_callback=bot._log_event,
        )
        bot.reconciliation_service = ReconciliationService(
            coin=bot.coin,
            async_info=bot.async_info,
            info=bot.info,
            account=bot.account,
            order_manager=bot.order_manager,
            state_manager=bot.state_manager,
            shadow_ledger=bot.shadow_ledger,
            risk_engine=bot.risk,
            rich_metrics=bot.rich_metrics,
            event_bus=bot.event_bus,
            config=reconcile_config,
        )
    
    # Create ExecutionGateway if router is available
    if bot.router and not bot.execution_gateway:
        gateway_config = ExecutionGatewayConfig(
            use_cloid=bot.cfg.use_cloid if hasattr(bot.cfg, 'use_cloid') else True,
            log_event_callback=bot._log_event,
        )
        bot.execution_gateway = ExecutionGateway(
            coin=bot.coin,
            router=bot.router,
            order_manager=bot.order_manager,
            order_state_machine=bot.order_state_machine,
            shadow_ledger=bot.shadow_ledger,
            event_bus=bot.event_bus,
            rich_metrics=bot.rich_metrics,
            circuit_breaker=bot.circuit_breaker,
            config=gateway_config,
        )
    
    # Create GridBuilder if strategy and execution_gateway are available
    if bot.strategy and bot.execution_gateway and not bot.grid_builder:
        builder_config = GridBuilderConfig(
            log_event_callback=bot._log_event,
        )
        bot.grid_builder = GridBuilder(
            coin=bot.coin,
            grid_calculator=bot.grid_calculator,
            strategy=bot.strategy,
            risk_engine=bot.risk,
            execution_gateway=bot.execution_gateway,
            rich_metrics=bot.rich_metrics,
            router=bot.router,
            config=builder_config,
        )
