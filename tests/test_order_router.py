import asyncio
import time

from src.execution.order_router import OrderRouter, OrderRequest


class FakeExchange:
    def __init__(self, record):
        self._record = record
        self._oid = 1000
        self._expires_after = None

    def set_expires_after(self, expires_after):
        """Mock method for order expiry TTL."""
        self._expires_after = expires_after

    async def order(self, coin, is_buy, sz, px, order_type, reduce_only, cloid=None):
        # record start, simulate network latency, record end
        s = time.time()
        self._record.append(("start", s))
        await asyncio.sleep(0.01)
        e = time.time()
        self._record.append(("end", e))
        resp = {"status": "ok", "response": {"data": {"resting": {"oid": self._oid, "cloid": cloid}}}}
        self._oid += 1
        return resp

    async def bulk_orders(self, reqs):
        await asyncio.sleep(0.01)
        return {"response": {"data": {"statuses": [{"resting": {"oid": 1}} for _ in reqs]}}}


async def _run_router_test(loop, router, requests):
    # submit requests concurrently
    futs = [asyncio.create_task(router.submit(r)) for r in requests]
    res = await asyncio.gather(*futs)
    return res


def test_order_router_serializes_with_shared_lock():
    async def inner():
        record = []
        fake = FakeExchange(record)
        # shared lock
        lock = asyncio.Lock()
        # minimal config object required by OrderRouter
        class Cfg:
            http_timeout = 2.0
            coalesce_ms = 1
            use_cloid = False
            min_notional = 0.0
            order_ttl_ms = 0  # Disable for test

        cfg = Cfg()
        r1 = OrderRouter("COIN", fake, None, cfg, 2, 0.01, 3, lock)
        r2 = OrderRouter("COIN", fake, None, cfg, 2, 0.01, 3, lock)
        r1.start()
        r2.start()

        reqs1 = [OrderRequest(True, 1.0, 100.0) for _ in range(5)]
        reqs2 = [OrderRequest(False, 1.0, 101.0) for _ in range(5)]

        t1 = asyncio.create_task(_run_router_test(asyncio.get_running_loop(), r1, reqs1))
        t2 = asyncio.create_task(_run_router_test(asyncio.get_running_loop(), r2, reqs2))
        await asyncio.gather(t1, t2)

        # stop routers
        await r1.stop()
        await r2.stop()

        # examine record to ensure no two 'start' intervals overlap 'end' of others
        intervals = []
        it = iter(record)
        # transform start/end pairs into windows
        starts = [t for typ, t in record if typ == "start"]
        ends = [t for typ, t in record if typ == "end"]
        # ensure sequencing: every start happens after previous end when using same lock
        for i in range(1, len(starts)):
            assert starts[i] >= ends[i-1]

    asyncio.run(inner())
