"""
Lightweight Prometheus-style metrics and HTTP server with health endpoint.

Step C: Production Hardening
- /metrics - Prometheus metrics endpoint
- /status - Bot status JSON (existing)
- /health - Load balancer health check
- /ready - Readiness probe (trading active)
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import parse_qs, urlparse


@dataclass
class HealthStatus:
    """Health status for the bot."""
    healthy: bool = True
    ready: bool = False  # Ready to trade
    last_heartbeat_ms: int = 0
    components: Dict[str, bool] = field(default_factory=dict)
    details: Dict[str, Any] = field(default_factory=dict)


class HealthChecker:
    """
    Centralized health checker for production readiness.
    
    Tracks component health and provides endpoints for:
    - /health - Basic liveness (is the process running?)
    - /ready - Readiness (is the bot ready to trade?)
    """

    def __init__(self) -> None:
        self._components: Dict[str, bool] = {}
        self._details: Dict[str, Any] = {}
        self._ready = False
        self._last_heartbeat = int(time.time() * 1000)
        self._callbacks: List[Callable[[str, bool], None]] = []

    def set_component_health(self, name: str, healthy: bool, detail: Optional[str] = None) -> None:
        """Set health status for a component."""
        self._components[name] = healthy
        if detail:
            self._details[name] = detail
        self._last_heartbeat = int(time.time() * 1000)
        
        # Notify callbacks
        for cb in self._callbacks:
            try:
                cb(name, healthy)
            except Exception:
                pass

    def set_ready(self, ready: bool) -> None:
        """Set overall readiness status."""
        self._ready = ready
        self._last_heartbeat = int(time.time() * 1000)

    def heartbeat(self) -> None:
        """Update heartbeat timestamp."""
        self._last_heartbeat = int(time.time() * 1000)

    def register_callback(self, callback: Callable[[str, bool], None]) -> None:
        """Register callback for health changes."""
        self._callbacks.append(callback)

    def is_healthy(self) -> bool:
        """Check if all components are healthy."""
        if not self._components:
            return True
        return all(self._components.values())

    def is_ready(self) -> bool:
        """Check if bot is ready to trade."""
        return self._ready and self.is_healthy()

    def get_status(self) -> HealthStatus:
        """Get full health status."""
        return HealthStatus(
            healthy=self.is_healthy(),
            ready=self.is_ready(),
            last_heartbeat_ms=self._last_heartbeat,
            components=dict(self._components),
            details=dict(self._details),
        )

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dict for JSON response."""
        status = self.get_status()
        return {
            "healthy": status.healthy,
            "ready": status.ready,
            "last_heartbeat_ms": status.last_heartbeat_ms,
            "components": status.components,
            "details": status.details,
        }


class Metrics:
    def __init__(self) -> None:
        self._gauges: Dict[str, float] = {}
        self._counters: Dict[str, float] = {}
        self._lock = asyncio.Lock()
        self._log = logging.getLogger("gridbot")

    async def set_gauge(self, key: str, value: float) -> None:
        async with self._lock:
            self._gauges[key] = value

    async def add_counter(self, key: str, delta: float = 1.0) -> None:
        async with self._lock:
            self._counters[key] = self._counters.get(key, 0.0) + delta

    async def render(self) -> str:
        async with self._lock:
            lines = []
            for k, v in self._gauges.items():
                lines.append(f"# TYPE {k} gauge")
                lines.append(f"{k} {v}")
            for k, v in self._counters.items():
                lines.append(f"# TYPE {k} counter")
                lines.append(f"{k} {v}")
            return "\n".join(lines) + "\n"


async def start_metrics_server(
    metrics: Metrics,
    port: int,
    status_board=None,
    auth_token: Optional[str] = None,
    health_checker: Optional[HealthChecker] = None,
) -> asyncio.AbstractServer:
    """
    Start HTTP server for metrics, status, and health endpoints.
    
    Endpoints:
    - GET /metrics - Prometheus metrics (auth required if token set)
    - GET /status - Bot status JSON (auth required if token set)
    - GET /health - Liveness probe (no auth, returns 200 if alive)
    - GET /ready - Readiness probe (no auth, returns 200 if ready to trade)
    """
    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        req = await reader.read(2048)
        path_raw = b"/"
        header_lines = []
        if b"\r\n" in req:
            header_lines = req.split(b"\r\n")
        if header_lines and b" " in header_lines[0]:
            try:
                path_raw = header_lines[0].split(b" ")[1]
            except Exception:
                path_raw = b"/"
        headers = {}
        for line in header_lines[1:]:
            if b":" in line:
                k, v = line.split(b":", 1)
                headers[k.strip().lower()] = v.strip()

        parsed = urlparse(path_raw.decode("utf-8", errors="ignore"))
        query = parse_qs(parsed.query)
        
        # Health endpoints - no auth required for load balancers
        if parsed.path == "/health":
            if health_checker:
                is_healthy = health_checker.is_healthy()
                status_code = b"200 OK" if is_healthy else b"503 Service Unavailable"
                body = json.dumps(health_checker.to_dict())
            else:
                status_code = b"200 OK"
                body = json.dumps({"healthy": True, "ready": True})
            
            resp = (
                b"HTTP/1.1 " + status_code + b"\r\n"
                b"Content-Type: application/json\r\n"
                b"Connection: close\r\n\r\n"
                + body.encode()
            )
            writer.write(resp)
            await writer.drain()
            writer.close()
            return

        if parsed.path == "/ready":
            if health_checker:
                is_ready = health_checker.is_ready()
                status_code = b"200 OK" if is_ready else b"503 Service Unavailable"
                body = json.dumps({"ready": is_ready})
            else:
                status_code = b"200 OK"
                body = json.dumps({"ready": True})
            
            resp = (
                b"HTTP/1.1 " + status_code + b"\r\n"
                b"Content-Type: application/json\r\n"
                b"Connection: close\r\n\r\n"
                + body.encode()
            )
            writer.write(resp)
            await writer.drain()
            writer.close()
            return

        # Auth-protected endpoints
        def _unauthorized() -> None:
            writer.write(b"HTTP/1.1 401 Unauthorized\r\nConnection: close\r\n\r\n")

        token_ok = False
        if auth_token:
            header_auth = headers.get(b"authorization", b"").decode("utf-8", errors="ignore")
            if header_auth == f"Bearer {auth_token}":
                token_ok = True
            elif query.get("token", [""])[0] == auth_token:
                token_ok = True
            if not token_ok:
                _unauthorized()
                await writer.drain()
                writer.close()
                return

        if parsed.path.startswith("/status") and status_board is not None:
            body = ""
            try:
                snap = await status_board.snapshot()
                body = json.dumps(snap)
                resp = (
                    b"HTTP/1.1 200 OK\r\n"
                    b"Content-Type: application/json\r\n"
                    b"Connection: close\r\n\r\n"
                    + body.encode()
                )
            except Exception:
                resp = b"HTTP/1.1 500 Internal Server Error\r\nConnection: close\r\n\r\n"
            writer.write(resp)
            await writer.drain()
            writer.close()
            return

        # default Prometheus text
        body = await metrics.render()
        resp = (
            b"HTTP/1.1 200 OK\r\n"
            b"Content-Type: text/plain; version=0.0.4\r\n"
            b"Connection: close\r\n\r\n"
            + body.encode()
        )
        writer.write(resp)
        await writer.drain()
        writer.close()

    return await asyncio.start_server(handle, "0.0.0.0", port)
