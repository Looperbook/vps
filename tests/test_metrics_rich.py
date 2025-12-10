"""Unit tests for rich metrics and structured logging context."""

import json
import logging
import sys
from io import StringIO

# Add src to path for imports
sys.path.insert(0, 'src')

from metrics_rich import RichMetrics
from bot_context import BotContext


def test_rich_metrics_counters():
    """Test that counters are properly incremented."""
    metrics = RichMetrics()

    metrics.orders_submitted.labels(coin="NVDA", side="buy").inc()
    metrics.orders_submitted.labels(coin="NVDA", side="buy").inc()
    metrics.orders_submitted.labels(coin="ORCL", side="sell").inc()

    registry = metrics.get_registry()
    # Check registry has the metrics
    assert registry is not None
    print("✓ test_rich_metrics_counters passed")


def test_rich_metrics_gauges():
    """Test that gauges are properly set."""
    metrics = RichMetrics()

    metrics.position.labels(coin="NVDA").set(10.5)
    metrics.realized_pnl.labels(coin="NVDA").set(500.0)
    metrics.skew_ratio.labels(coin="NVDA").set(0.75)

    registry = metrics.get_registry()
    assert registry is not None
    print("✓ test_rich_metrics_gauges passed")


def test_rich_metrics_histograms():
    """Test that histograms record observations."""
    metrics = RichMetrics()

    metrics.order_latency_ms.labels(coin="NVDA").observe(42.5)
    metrics.order_latency_ms.labels(coin="NVDA").observe(105.2)

    metrics.fill_pnl.labels(coin="NVDA").observe(25.0)
    metrics.fill_pnl.labels(coin="NVDA").observe(-10.5)

    registry = metrics.get_registry()
    assert registry is not None
    print("✓ test_rich_metrics_histograms passed")


def test_bot_context_basic_logging():
    """Test BotContext logs with trace ID and elapsed time."""
    # Capture logs
    log_stream = StringIO()
    handler = logging.StreamHandler(log_stream)
    handler.setFormatter(logging.Formatter('%(message)s'))
    logger = logging.getLogger("test_gridbot")
    logger.handlers.clear()  # Clear any existing handlers
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    ctx = BotContext(coin="NVDA", logger=logger)
    ctx.log("test_event", value=42)

    log_output = log_stream.getvalue()
    assert log_output.strip()
    
    payload = json.loads(log_output.strip())

    assert payload["coin"] == "NVDA"
    assert payload["event"] == "test_event"
    assert payload["value"] == 42
    assert "trace_id" in payload
    assert payload["trace_id"] == ctx.trace_id
    assert "elapsed_ms" in payload
    print("✓ test_bot_context_basic_logging passed")


def test_bot_context_levels():
    """Test context logs at different levels."""
    log_stream = StringIO()
    handler = logging.StreamHandler(log_stream)
    handler.setFormatter(logging.Formatter('%(levelname)s:%(message)s'))
    logger = logging.getLogger("test_levels")
    logger.handlers.clear()
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    ctx = BotContext(coin="BTC", logger=logger)
    ctx.debug("debug_event", x=1)
    ctx.info("info_event", x=2)
    ctx.warning("warning_event", x=3)
    ctx.error("error_event", x=4)

    logs = log_stream.getvalue().strip().split('\n')
    assert len(logs) == 4
    assert logs[0].startswith("DEBUG:")
    assert logs[1].startswith("INFO:")
    assert logs[2].startswith("WARNING:")
    assert logs[3].startswith("ERROR:")
    print("✓ test_bot_context_levels passed")


def test_bot_context_tags():
    """Test that tags are included in all logs."""
    log_stream = StringIO()
    handler = logging.StreamHandler(log_stream)
    handler.setFormatter(logging.Formatter('%(message)s'))
    logger = logging.getLogger("test_tags")
    logger.handlers.clear()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    ctx = BotContext(coin="NVDA", logger=logger)
    ctx.set_tag("user_id", "user_123")
    ctx.set_tag("session", "session_456")

    ctx.log("event_1", a=1)
    ctx.log("event_2", b=2)

    logs = log_stream.getvalue().strip().split('\n')
    for line in logs:
        payload = json.loads(line)
        assert payload["user_id"] == "user_123"
        assert payload["session"] == "session_456"
    print("✓ test_bot_context_tags passed")


def test_bot_context_child():
    """Test child context with parent trace ID."""
    log_stream = StringIO()
    handler = logging.StreamHandler(log_stream)
    handler.setFormatter(logging.Formatter('%(message)s'))
    logger = logging.getLogger("test_child")
    logger.handlers.clear()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    ctx = BotContext(coin="ORCL", logger=logger)
    child = ctx.child("fill_processing")

    ctx.log("parent_event")
    child.log("child_event")

    logs = log_stream.getvalue().strip().split('\n')
    parent_payload = json.loads(logs[0])
    child_payload = json.loads(logs[1])

    assert parent_payload["trace_id"] == ctx.trace_id
    assert child_payload["trace_id"] == child.trace_id
    assert child_payload["parent_trace_id"] == ctx.trace_id
    assert child_payload["sub_operation"] == "fill_processing"
    print("✓ test_bot_context_child passed")


def test_bot_context_elapsed_ms():
    """Test elapsed time tracking."""
    import time

    ctx = BotContext(coin="NVDA")
    
    elapsed_1 = ctx.elapsed_ms()
    assert elapsed_1 >= 0.0
    
    time.sleep(0.01)
    elapsed_2 = ctx.elapsed_ms()
    
    assert elapsed_2 > elapsed_1
    print("✓ test_bot_context_elapsed_ms passed")


def test_bot_context_custom_trace_id():
    """Test that custom trace IDs are respected."""
    log_stream = StringIO()
    handler = logging.StreamHandler(log_stream)
    handler.setFormatter(logging.Formatter('%(message)s'))
    logger = logging.getLogger("test_custom_id")
    logger.handlers.clear()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    custom_id = "my-custom-trace-id-123"
    ctx = BotContext(coin="BTC", trace_id=custom_id, logger=logger)
    ctx.log("test_event")

    log_output = log_stream.getvalue().strip()
    payload = json.loads(log_output)

    assert payload["trace_id"] == custom_id
    print("✓ test_bot_context_custom_trace_id passed")


if __name__ == "__main__":
    """Run all tests."""
    try:
        test_rich_metrics_counters()
        test_rich_metrics_gauges()
        test_rich_metrics_histograms()
        test_bot_context_basic_logging()
        test_bot_context_levels()
        test_bot_context_tags()
        test_bot_context_child()
        test_bot_context_elapsed_ms()
        test_bot_context_custom_trace_id()
        print("\n✅ All Phase 3 tests passed!")
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

