import asyncio
from src.execution.fill_log import FillLog
from src.execution.fill_log_manager import FillLogManager
from src.core.utils import now_ms


def test_fill_log_manager_compacts_old_events(tmp_path):
    coin = "TST:USD"
    state_dir = str(tmp_path)
    fl = FillLog(coin, state_dir)

    # create one old event (2 days ago) and one recent event
    now = now_ms()
    old_ts = now - 2 * 24 * 60 * 60 * 1000
    recent_ts = now

    asyncio.run(fl.append({"side": "buy", "px": 100.0, "sz": 1.0, "time": old_ts}))
    asyncio.run(fl.append({"side": "sell", "px": 101.0, "sz": 0.5, "time": recent_ts}))

    class Cfg:
        pass

    Cfg.coins = [coin]
    Cfg.state_dir = state_dir
    Cfg.fill_log_retention_days = 1  # keep 1 day
    Cfg.fill_log_compact_interval_sec = 60

    mgr = FillLogManager(Cfg)
    # run a single compaction pass
    asyncio.run(mgr._compact_once())

    ev = asyncio.run(fl.read_since(0))
    # only recent event should remain
    assert len(ev) == 1
    assert ev[0]["time"] == recent_ts
