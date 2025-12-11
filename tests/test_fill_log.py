import asyncio
import os
import json

from src.execution.fill_log import FillLog


def test_fill_log_append_and_read(tmp_path):
    coin = "TEST:USD"
    state_dir = str(tmp_path)
    fl = FillLog(coin, state_dir)

    async def inner():
        await fl.append({"side": "buy", "px": 100.0, "sz": 1.0, "time": 1000})
        await fl.append({"side": "sell", "px": 101.0, "sz": 0.5, "time": 2000})
        ev = await fl.read_since(1500)
        assert isinstance(ev, list)
        assert len(ev) == 1
        assert ev[0]["px"] == 101.0

    asyncio.run(inner())


def test_compact_keeps_recent(tmp_path):
    coin = "TEST:USD"
    state_dir = str(tmp_path)
    fl = FillLog(coin, state_dir)

    async def inner():
        await fl.append({"side": "buy", "px": 100.0, "sz": 1.0, "time": 1000})
        await fl.append({"side": "sell", "px": 101.0, "sz": 0.5, "time": 2000})
        await fl.compact(1500)
        ev = await fl.read_since(0)
        assert len(ev) == 1
        assert ev[0]["time"] == 2000

    asyncio.run(inner())
