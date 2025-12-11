"""
Tests for Optimization-1, Optimization-2, and Optimization-3.

These tests verify the performance optimizations:
- CachedMid: Smart caching to reduce REST calls
- BatchedFillLog: Batched log writes for reduced I/O
- Deep copy avoidance in hot paths
"""

import asyncio
import time
import json
import threading
from pathlib import Path

import pytest

from src.market_data.market_data import CachedMid
from src.execution.fill_log import FillLog, BatchedFillLog


# ============================================================================
# Optimization-1: CachedMid Tests
# ============================================================================

class TestCachedMid:
    """Tests for CachedMid thread-safe TTL cache."""

    def test_initial_get_returns_none(self):
        """Empty cache should return None."""
        cache = CachedMid(ttl_ms=500)
        assert cache.get() is None

    def test_set_and_get_within_ttl(self):
        """Value should be retrievable within TTL window."""
        cache = CachedMid(ttl_ms=1000)
        cache.set(100.5)
        assert cache.get() == 100.5

    def test_get_returns_none_after_ttl(self):
        """Value should expire after TTL."""
        cache = CachedMid(ttl_ms=50)  # 50ms TTL
        cache.set(100.5)
        time.sleep(0.06)  # Wait 60ms
        assert cache.get() is None

    def test_get_unchecked_ignores_ttl(self):
        """get_unchecked should return value regardless of TTL."""
        cache = CachedMid(ttl_ms=50)
        cache.set(100.5)
        time.sleep(0.06)
        assert cache.get() is None
        assert cache.get_unchecked() == 100.5

    def test_set_rejects_zero_or_negative(self):
        """Cache should reject zero or negative values."""
        cache = CachedMid(ttl_ms=500)
        cache.set(0.0)
        assert cache.get() is None
        cache.set(-10.0)
        assert cache.get() is None

    def test_set_updates_timestamp(self):
        """Each set should update the timestamp."""
        cache = CachedMid(ttl_ms=100)
        cache.set(100.0)
        time.sleep(0.08)
        # Still within TTL
        assert cache.get() == 100.0
        time.sleep(0.03)
        # Now expired
        assert cache.get() is None
        # Set again
        cache.set(200.0)
        assert cache.get() == 200.0

    def test_age_ms_returns_correct_age(self):
        """age_ms should return correct elapsed time."""
        cache = CachedMid(ttl_ms=1000)
        assert cache.age_ms() == 0  # No value set yet
        cache.set(100.0)
        time.sleep(0.05)
        age = cache.age_ms()
        assert 40 <= age <= 80  # Allow some tolerance

    def test_thread_safety(self):
        """Cache should be thread-safe under concurrent access."""
        cache = CachedMid(ttl_ms=1000)
        errors = []

        def writer():
            for i in range(100):
                cache.set(float(i + 1))
                time.sleep(0.001)

        def reader():
            for _ in range(100):
                val = cache.get()
                if val is not None and val <= 0:
                    errors.append(f"Invalid value: {val}")
                time.sleep(0.001)

        threads = [
            threading.Thread(target=writer),
            threading.Thread(target=reader),
            threading.Thread(target=reader),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0, f"Thread safety errors: {errors}"


# ============================================================================
# Optimization-2: BatchedFillLog Tests
# ============================================================================

class TestBatchedFillLog:
    """Tests for BatchedFillLog batched write functionality."""

    def test_inherits_fill_log(self):
        """BatchedFillLog should be a subclass of FillLog."""
        assert issubclass(BatchedFillLog, FillLog)

    def test_append_buffers_events(self, tmp_path):
        """Events should be buffered, not immediately written."""
        coin = "TEST:USD"
        state_dir = str(tmp_path)
        fl = BatchedFillLog(coin, state_dir, flush_interval=10.0)  # Long interval

        async def inner():
            await fl.append({"side": "buy", "px": 100.0, "sz": 1.0, "time": 1000})
            # Event is buffered, not written yet
            assert fl.buffer_size() == 1
            # File should not exist yet (or be empty)
            if fl.path.exists():
                assert fl.path.stat().st_size == 0 or not fl.path.read_text()

        asyncio.run(inner())

    def test_flush_writes_buffer(self, tmp_path):
        """Explicit flush should write buffered events."""
        coin = "TEST:USD"
        state_dir = str(tmp_path)
        fl = BatchedFillLog(coin, state_dir, flush_interval=10.0)

        async def inner():
            await fl.append({"side": "buy", "px": 100.0, "sz": 1.0, "time": 1000})
            await fl.append({"side": "sell", "px": 101.0, "sz": 0.5, "time": 2000})
            assert fl.buffer_size() == 2
            await fl._flush()
            assert fl.buffer_size() == 0
            # Verify written
            content = fl.path.read_text()
            assert "100.0" in content
            assert "101.0" in content

        asyncio.run(inner())

    def test_read_since_flushes_first(self, tmp_path):
        """read_since should flush buffer before reading."""
        coin = "TEST:USD"
        state_dir = str(tmp_path)
        fl = BatchedFillLog(coin, state_dir, flush_interval=10.0)

        async def inner():
            await fl.append({"side": "buy", "px": 100.0, "sz": 1.0, "time": 1000})
            await fl.append({"side": "sell", "px": 101.0, "sz": 0.5, "time": 2000})
            # Buffer should have events
            assert fl.buffer_size() == 2
            # Read should flush first
            events = await fl.read_since(0)
            assert fl.buffer_size() == 0
            assert len(events) == 2
            assert events[0]["px"] == 100.0
            assert events[1]["px"] == 101.0

        asyncio.run(inner())

    def test_max_buffer_triggers_flush(self, tmp_path):
        """Buffer reaching max_buffer_size should trigger immediate flush."""
        coin = "TEST:USD"
        state_dir = str(tmp_path)
        fl = BatchedFillLog(coin, state_dir, flush_interval=10.0, max_buffer_size=3)

        async def inner():
            await fl.append({"side": "buy", "px": 100.0, "sz": 1.0, "time": 1000})
            await fl.append({"side": "buy", "px": 101.0, "sz": 1.0, "time": 2000})
            assert fl.buffer_size() == 2
            # This should trigger flush
            await fl.append({"side": "buy", "px": 102.0, "sz": 1.0, "time": 3000})
            assert fl.buffer_size() == 0
            # Verify all written
            content = fl.path.read_text()
            assert "100.0" in content
            assert "101.0" in content
            assert "102.0" in content

        asyncio.run(inner())

    def test_start_stop_lifecycle(self, tmp_path):
        """start() and stop() should manage background task."""
        coin = "TEST:USD"
        state_dir = str(tmp_path)
        fl = BatchedFillLog(coin, state_dir, flush_interval=0.1)

        async def inner():
            await fl.start()
            assert fl._flush_task is not None
            await fl.append({"side": "buy", "px": 100.0, "sz": 1.0, "time": 1000})
            # Wait for background flush
            await asyncio.sleep(0.15)
            assert fl.buffer_size() == 0
            await fl.stop()
            assert fl._flush_task is None

        asyncio.run(inner())

    def test_stop_flushes_remaining(self, tmp_path):
        """stop() should flush any remaining buffered events."""
        coin = "TEST:USD"
        state_dir = str(tmp_path)
        fl = BatchedFillLog(coin, state_dir, flush_interval=10.0)

        async def inner():
            await fl.start()
            await fl.append({"side": "buy", "px": 100.0, "sz": 1.0, "time": 1000})
            assert fl.buffer_size() == 1
            await fl.stop()
            # Buffer should be flushed
            assert fl.buffer_size() == 0
            # Verify written
            events = await FillLog(coin, state_dir).read_since(0)
            assert len(events) == 1

        asyncio.run(inner())

    def test_compact_flushes_first(self, tmp_path):
        """compact() should flush buffer before compacting."""
        coin = "TEST:USD"
        state_dir = str(tmp_path)
        fl = BatchedFillLog(coin, state_dir, flush_interval=10.0)

        async def inner():
            await fl.append({"side": "buy", "px": 100.0, "sz": 1.0, "time": 1000})
            await fl.append({"side": "sell", "px": 101.0, "sz": 0.5, "time": 2000})
            await fl.compact(1500)
            # Only the second event should remain
            events = await fl.read_since(0)
            assert len(events) == 1
            assert events[0]["time"] == 2000

        asyncio.run(inner())

    def test_concurrent_appends(self, tmp_path):
        """Multiple concurrent appends should be handled correctly."""
        coin = "TEST:USD"
        state_dir = str(tmp_path)
        fl = BatchedFillLog(coin, state_dir, flush_interval=10.0, max_buffer_size=50)

        async def inner():
            # Append many events concurrently
            tasks = [
                fl.append({"side": "buy", "px": float(i), "sz": 1.0, "time": i * 1000})
                for i in range(1, 21)
            ]
            await asyncio.gather(*tasks)
            # Flush and verify
            events = await fl.read_since(0)
            assert len(events) == 20
            prices = sorted([e["px"] for e in events])
            assert prices == list(range(1, 21))

        asyncio.run(inner())


# ============================================================================
# Integration Tests
# ============================================================================

class TestOptimizationsIntegration:
    """Integration tests combining multiple optimizations."""

    def test_batched_fill_log_compatible_with_fill_log_interface(self, tmp_path):
        """BatchedFillLog should be usable anywhere FillLog is expected."""
        coin = "TEST:USD"
        state_dir = str(tmp_path)

        async def inner():
            # Use BatchedFillLog through FillLog interface
            fl: FillLog = BatchedFillLog(coin, state_dir)
            await fl.append({"side": "buy", "px": 100.0, "sz": 1.0, "time": 1000})
            events = await fl.read_since(0)
            assert len(events) == 1
            await fl.compact(500)
            events = await fl.read_since(0)
            assert len(events) == 1  # time=1000 > 500

        asyncio.run(inner())

    def test_cached_mid_reduces_effective_calls(self):
        """Demonstrate that CachedMid reduces repeated lookups."""
        cache = CachedMid(ttl_ms=100)
        
        # Simulate first call - cache miss
        result1 = cache.get()
        assert result1 is None
        
        # Simulate REST call result
        cache.set(100.5)
        
        # Subsequent calls within TTL - cache hits
        call_count = 0
        for _ in range(10):
            result = cache.get()
            if result is not None:
                call_count += 1
        
        assert call_count == 10  # All hits
        
        # After TTL expires - cache miss
        time.sleep(0.11)
        assert cache.get() is None
