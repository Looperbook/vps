# Upgrade Summary:
# - Modularized bot into async components (config, logging, metrics, state, market data, strategy, risk, order router).
# - Wrapped blocking SDK calls via asyncio.to_thread, added nonce-safe order queue, bounded fill dedup, and risk gates.
# - Added tick-safe quantization, better spacing sizing, and graceful startup/shutdown with metrics server.
# - Step C: Added config validation, health endpoints, and webhook alerting.
"""
Entry point wiring all components.
"""

from __future__ import annotations

import asyncio
import json
import signal
import sys
from typing import List

from hyperliquid.exchange import Exchange
from hyperliquid.info import Info

from src.config.config import Settings
from src.infra.logging_cfg import build_logger
from src.monitoring.metrics import Metrics, HealthChecker, start_metrics_server
from src.monitoring.status import StatusBoard
from src.infra.async_info import AsyncInfo
import httpx
from src.infra.async_execution import AsyncExchange
from src.app import run_all
from src.execution.fill_log_manager import start_compactor
from src.config.config_validator import validate_and_log
from src.monitoring.alerting import configure_alerts, get_alert_manager, AlertSeverity

log = build_logger("gridbot")


async def main() -> None:
    cfg = Settings.load()
    
    # Step C: Validate configuration before starting
    if not validate_and_log(cfg, log):
        log.error("Configuration validation failed, exiting")
        sys.exit(1)
    
    # Step C: Configure alerting
    alert_manager = configure_alerts(
        webhook_url=cfg.alert_webhook_url,
        webhook_type=cfg.alert_webhook_type,
        min_severity=AlertSeverity.WARNING,
        enabled=cfg.alert_enabled,
        bot_name="GridBot",
    )
    
    # Step C: Create health checker
    health_checker = HealthChecker()
    health_checker.set_component_health("config", True, "Configuration validated")
    
    wallet = cfg.resolve_signer()
    info = Info(cfg.base_url, skip_ws=False, perp_dexs=[cfg.dex])
    # Create a single shared HTTP/2 client for Info endpoints to avoid multiple HTTP/2 connections.
    shared_info_client = httpx.AsyncClient(base_url=cfg.base_url.rstrip("/"), http2=True, timeout=cfg.http_timeout)
    async_info = AsyncInfo(cfg.base_url, timeout=cfg.http_timeout, client=shared_info_client)
    base_exchange = Exchange(wallet, cfg.base_url, account_address=cfg.resolve_account(), perp_dexs=[cfg.dex])
    async_exchange = AsyncExchange(base_exchange, timeout=cfg.http_timeout)
    coins = [c.strip() for c in cfg.coins if c.strip()]
    metrics = Metrics()
    status_board = StatusBoard()
    
    # Step C: Pass health_checker to metrics server for /health and /ready endpoints
    srv = await start_metrics_server(metrics, cfg.metrics_port, status_board, auth_token=cfg.metrics_token, health_checker=health_checker)
    
    log.info(json.dumps({"event": "startup", "coins": coins}))
    
    # Step C: Send startup alert
    await alert_manager.alert_startup(coins, investment_usd=cfg.investment_usd, leverage=cfg.leverage)
    
    loop = asyncio.get_running_loop()
    # Start background compactor for fill logs before launching bots
    compactor = start_compactor(cfg)
    run_task = asyncio.create_task(run_all(coins, cfg, info, async_info, async_exchange, metrics, status_board))

    def stop_all() -> None:
        # Trigger graceful cancellation so bots run their shutdown (cancel_all/persist).
        if not run_task.done():
            run_task.cancel()
        srv.close()

    # Windows doesn't support add_signal_handler, so rely on KeyboardInterrupt handling
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_all)
        except NotImplementedError:
            pass
    
    try:
        await run_task
    except (asyncio.CancelledError, KeyboardInterrupt):
        log.info("Shutdown signal received, cleaning up...")
        # Step C: Send shutdown alert
        await alert_manager.alert_shutdown("signal_received")
        if not run_task.done():
            run_task.cancel()
            try:
                await run_task
            except asyncio.CancelledError:
                pass
    finally:
        log.info("Closing servers and connections...")
        srv.close()
        await srv.wait_closed()
        await async_exchange.close()
        # stop background compactor
        try:
            await compactor.stop()
        except Exception:
            pass
        # Close AsyncInfo resources we own and the shared client
        await async_info.close()
        try:
            await shared_info_client.aclose()
        except Exception:
            pass
        # Disconnect hyperliquid SDK websocket to stop its background thread
        try:
            info.disconnect_websocket()
        except Exception:
            pass
        log.info("Shutdown complete")


if __name__ == "__main__":
    # Windows compatible asyncio runner with proper cleanup
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nBot stopped by user")
    sys.exit(0)
