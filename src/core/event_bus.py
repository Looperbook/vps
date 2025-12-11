"""
Event Bus: Central event distribution for decoupled component communication.

S-2 Structural Improvement: Implements pub/sub pattern for loose coupling
between bot components. Events flow through a central bus, allowing handlers
to subscribe to specific event types without direct dependencies.

Features:
- Type-safe events with dataclasses
- Async event handlers
- Priority-based handler execution
- Error isolation (one handler failure doesn't stop others)
- Event history for debugging
- Metrics tracking
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Coroutine, Dict, List, Optional, Union

log = logging.getLogger("gridbot")


class EventType(Enum):
    """
    Event types supported by the event bus.
    
    Naming convention: NOUN_VERB for state changes.
    """
    # Fill events
    FILL_RECEIVED = auto()        # New fill from WS or REST
    FILL_PROCESSED = auto()       # Fill fully processed
    FILL_DUPLICATE = auto()       # Duplicate fill detected
    
    # Order events
    ORDER_SUBMITTED = auto()      # Order sent to exchange
    ORDER_ACKNOWLEDGED = auto()   # Exchange acknowledged order
    ORDER_REJECTED = auto()       # Exchange rejected order
    ORDER_CANCELLED = auto()      # Order cancelled
    ORDER_FILLED = auto()         # Order fully filled
    ORDER_PARTIAL = auto()        # Order partially filled
    
    # Position events
    POSITION_CHANGED = auto()     # Position updated
    POSITION_RECONCILED = auto()  # Position reconciled with exchange
    POSITION_DRIFT = auto()       # Position drift detected
    
    # Grid events
    GRID_BUILT = auto()           # Grid rebuilt
    GRID_DRIFT = auto()           # Grid anchor drifted
    GRID_LEVEL_FILLED = auto()    # Grid level filled
    
    # Market data events
    PRICE_UPDATE = auto()         # New mid price
    TICKER_UPDATE = auto()        # Full ticker update
    
    # Connection events
    WS_CONNECTED = auto()         # WebSocket connected
    WS_DISCONNECTED = auto()      # WebSocket disconnected
    WS_RECONNECTED = auto()       # WebSocket reconnected after disconnect
    
    # Risk events
    RISK_ALERT = auto()           # Risk threshold exceeded
    RISK_HALT = auto()            # Trading halted due to risk
    RISK_RESUME = auto()          # Trading resumed
    
    # Circuit breaker events
    CIRCUIT_OPENED = auto()       # Circuit breaker tripped
    CIRCUIT_CLOSED = auto()       # Circuit breaker reset
    
    # System events
    BOT_STARTED = auto()          # Bot started
    BOT_STOPPED = auto()          # Bot stopped
    BOT_PAUSED = auto()           # Bot paused
    BOT_RESUMED = auto()          # Bot resumed


@dataclass
class Event:
    """
    Base event container.
    
    All events have:
    - type: EventType enum value
    - data: Dict with event-specific payload
    - timestamp_ms: When event was created
    - source: Where event originated (optional)
    - correlation_id: For tracing related events (optional)
    """
    type: EventType
    data: Dict[str, Any]
    timestamp_ms: int = field(default_factory=lambda: int(time.time() * 1000))
    source: Optional[str] = None
    correlation_id: Optional[str] = None
    
    def __str__(self) -> str:
        return f"Event({self.type.name}, ts={self.timestamp_ms}, source={self.source})"


# Handler type: async function or sync function taking Event
Handler = Union[
    Callable[[Event], Coroutine[Any, Any, None]],
    Callable[[Event], None],
]


@dataclass
class Subscription:
    """Internal subscription record."""
    handler: Handler
    priority: int = 0  # Higher = called first
    filter_fn: Optional[Callable[[Event], bool]] = None
    name: Optional[str] = None


class EventBus:
    """
    Central event bus for decoupled component communication.
    
    Usage:
        bus = EventBus()
        
        # Subscribe to events
        bus.subscribe(EventType.FILL_RECEIVED, handle_fill)
        bus.subscribe(EventType.POSITION_CHANGED, handle_position, priority=10)
        
        # Publish events
        await bus.publish(Event(
            type=EventType.FILL_RECEIVED,
            data={"side": "buy", "size": 1.0, "price": 100.0},
            source="websocket",
        ))
        
        # Start processing (in background task)
        asyncio.create_task(bus.start())
        
        # Stop when done
        bus.stop()
    
    Thread-safety: All operations are async-safe via queue.
    """
    
    # Default max history size
    DEFAULT_HISTORY_SIZE = 1000
    # Default queue size (0 = unlimited)
    DEFAULT_QUEUE_SIZE = 0
    
    def __init__(
        self,
        history_size: int = DEFAULT_HISTORY_SIZE,
        queue_size: int = DEFAULT_QUEUE_SIZE,
        log_event: Optional[Callable[..., None]] = None,
    ) -> None:
        """
        Initialize EventBus.
        
        Args:
            history_size: Max events to keep in history (0 = disabled)
            queue_size: Max queue size (0 = unlimited)
            log_event: Callback for structured logging
        """
        self._log = log_event or self._default_log
        
        # Subscribers by event type
        self._subscribers: Dict[EventType, List[Subscription]] = {}
        # Global subscribers (receive all events)
        self._global_subscribers: List[Subscription] = []
        
        # Event queue for async processing
        maxsize = queue_size if queue_size > 0 else 0
        self._queue: asyncio.Queue[Event] = asyncio.Queue(maxsize=maxsize)
        
        # Control flags
        self._running = False
        self._process_task: Optional[asyncio.Task] = None
        
        # Event history (ring buffer)
        self._history_size = history_size
        self._history: List[Event] = []
        
        # Statistics
        self._stats = {
            "events_published": 0,
            "events_processed": 0,
            "events_dropped": 0,
            "handler_errors": 0,
            "queue_high_water": 0,
        }
    
    def _default_log(self, event: str, **kwargs: Any) -> None:
        """Default logging when no callback provided."""
        log.debug(f'{{"event":"{event}",{",".join(f"{k}:{v}" for k,v in kwargs.items())}}}')
    
    # -------------------------------------------------------------------------
    # Subscription Management
    # -------------------------------------------------------------------------
    
    def subscribe(
        self,
        event_type: EventType,
        handler: Handler,
        priority: int = 0,
        filter_fn: Optional[Callable[[Event], bool]] = None,
        name: Optional[str] = None,
    ) -> Subscription:
        """
        Subscribe to a specific event type.
        
        Args:
            event_type: Type of events to receive
            handler: Async or sync function to handle events
            priority: Higher priority handlers called first (default 0)
            filter_fn: Optional filter function (receives event, returns bool)
            name: Optional name for debugging
            
        Returns:
            Subscription object (for unsubscribing)
        """
        sub = Subscription(
            handler=handler,
            priority=priority,
            filter_fn=filter_fn,
            name=name,
        )
        
        if event_type not in self._subscribers:
            self._subscribers[event_type] = []
        
        # Insert sorted by priority (descending)
        subs = self._subscribers[event_type]
        insert_idx = 0
        for i, existing in enumerate(subs):
            if existing.priority < priority:
                insert_idx = i
                break
            insert_idx = i + 1
        subs.insert(insert_idx, sub)
        
        self._log(
            "event_bus_subscribe",
            event_type=event_type.name,
            handler_name=name or handler.__name__,
            priority=priority,
            total_subscribers=len(subs),
        )
        
        return sub
    
    def subscribe_all(
        self,
        handler: Handler,
        priority: int = 0,
        filter_fn: Optional[Callable[[Event], bool]] = None,
        name: Optional[str] = None,
    ) -> Subscription:
        """
        Subscribe to all event types (global subscriber).
        
        Global subscribers receive every event and are called before
        type-specific subscribers.
        
        Args:
            handler: Handler function
            priority: Priority within global subscribers
            filter_fn: Optional filter
            name: Optional name
            
        Returns:
            Subscription object
        """
        sub = Subscription(
            handler=handler,
            priority=priority,
            filter_fn=filter_fn,
            name=name,
        )
        
        # Insert sorted by priority
        insert_idx = 0
        for i, existing in enumerate(self._global_subscribers):
            if existing.priority < priority:
                insert_idx = i
                break
            insert_idx = i + 1
        self._global_subscribers.insert(insert_idx, sub)
        
        self._log(
            "event_bus_subscribe_all",
            handler_name=name or handler.__name__,
            priority=priority,
        )
        
        return sub
    
    def unsubscribe(
        self,
        event_type: Optional[EventType],
        subscription: Subscription,
    ) -> bool:
        """
        Remove a subscription.
        
        Args:
            event_type: Event type (None for global)
            subscription: Subscription to remove
            
        Returns:
            True if removed, False if not found
        """
        if event_type is None:
            # Global subscriber
            if subscription in self._global_subscribers:
                self._global_subscribers.remove(subscription)
                return True
        else:
            # Type-specific subscriber
            if event_type in self._subscribers:
                subs = self._subscribers[event_type]
                if subscription in subs:
                    subs.remove(subscription)
                    return True
        return False
    
    # -------------------------------------------------------------------------
    # Event Publishing
    # -------------------------------------------------------------------------
    
    async def publish(self, event: Event) -> bool:
        """
        Publish an event to the bus.
        
        Event is queued for async processing by subscribers.
        
        Args:
            event: Event to publish
            
        Returns:
            True if queued, False if queue full
        """
        try:
            self._queue.put_nowait(event)
            self._stats["events_published"] += 1
            
            # Track queue high water mark
            qsize = self._queue.qsize()
            if qsize > self._stats["queue_high_water"]:
                self._stats["queue_high_water"] = qsize
            
            return True
        except asyncio.QueueFull:
            self._stats["events_dropped"] += 1
            self._log(
                "event_bus_queue_full",
                event_type=event.type.name,
                dropped=True,
            )
            return False
    
    def publish_sync(self, event: Event) -> bool:
        """
        Publish event from sync context (non-blocking).
        
        Convenience wrapper for code that can't await.
        """
        try:
            self._queue.put_nowait(event)
            self._stats["events_published"] += 1
            return True
        except asyncio.QueueFull:
            self._stats["events_dropped"] += 1
            return False
    
    async def publish_and_wait(self, event: Event, timeout: float = 5.0) -> bool:
        """
        Publish event and wait for it to be processed.
        
        Args:
            event: Event to publish
            timeout: Max time to wait
            
        Returns:
            True if processed, False if timeout
        """
        processed = asyncio.Event()
        original_data = event.data.copy()
        event.data["_wait_event"] = processed
        
        await self.publish(event)
        
        try:
            await asyncio.wait_for(processed.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False
        finally:
            event.data = original_data
    
    # -------------------------------------------------------------------------
    # Event Processing
    # -------------------------------------------------------------------------
    
    async def start(self) -> None:
        """
        Start processing events.
        
        This method runs until stop() is called.
        Should be run as a background task.
        """
        self._running = True
        self._log("event_bus_started")
        
        while self._running:
            try:
                # Wait for event with timeout (allows periodic stop check)
                try:
                    event = await asyncio.wait_for(
                        self._queue.get(),
                        timeout=1.0,
                    )
                except asyncio.TimeoutError:
                    continue
                
                await self._process_event(event)
                
            except asyncio.CancelledError:
                self._log("event_bus_cancelled")
                break
            except Exception as e:
                self._log(
                    "event_bus_error",
                    error=str(e),
                    error_type=type(e).__name__,
                )
        
        self._log("event_bus_stopped")
    
    async def _process_event(self, event: Event) -> None:
        """Process a single event by calling all subscribers."""
        # Add to history
        if self._history_size > 0:
            self._history.append(event)
            if len(self._history) > self._history_size:
                self._history.pop(0)
        
        # Collect handlers: global first, then type-specific
        handlers: List[Subscription] = []
        handlers.extend(self._global_subscribers)
        handlers.extend(self._subscribers.get(event.type, []))
        
        # Call handlers
        for sub in handlers:
            # Apply filter
            if sub.filter_fn and not sub.filter_fn(event):
                continue
            
            try:
                if asyncio.iscoroutinefunction(sub.handler):
                    await sub.handler(event)
                else:
                    sub.handler(event)
            except Exception as e:
                self._stats["handler_errors"] += 1
                self._log(
                    "event_bus_handler_error",
                    event_type=event.type.name,
                    handler_name=sub.name or "unknown",
                    error=str(e),
                    error_type=type(e).__name__,
                )
        
        # Signal completion if waiting
        wait_event = event.data.get("_wait_event")
        if isinstance(wait_event, asyncio.Event):
            wait_event.set()
        
        self._stats["events_processed"] += 1
    
    def stop(self) -> None:
        """Stop processing events."""
        self._running = False
    
    async def drain(self, timeout: float = 5.0) -> int:
        """
        Process all queued events.
        
        Args:
            timeout: Max time to wait
            
        Returns:
            Number of events processed
        """
        count = 0
        deadline = time.time() + timeout
        
        while not self._queue.empty() and time.time() < deadline:
            try:
                event = self._queue.get_nowait()
                await self._process_event(event)
                count += 1
            except asyncio.QueueEmpty:
                break
        
        return count
    
    # -------------------------------------------------------------------------
    # Query Methods
    # -------------------------------------------------------------------------
    
    def get_history(
        self,
        event_type: Optional[EventType] = None,
        limit: int = 100,
    ) -> List[Event]:
        """
        Get recent events from history.
        
        Args:
            event_type: Filter by type (None = all)
            limit: Max events to return
            
        Returns:
            List of events (most recent last)
        """
        events = self._history
        if event_type:
            events = [e for e in events if e.type == event_type]
        return events[-limit:]
    
    def get_stats(self) -> Dict[str, Any]:
        """Get event bus statistics."""
        return {
            **self._stats,
            "queue_size": self._queue.qsize(),
            "history_size": len(self._history),
            "subscriber_count": sum(len(s) for s in self._subscribers.values()),
            "global_subscriber_count": len(self._global_subscribers),
            "running": self._running,
        }
    
    def get_subscriber_count(self, event_type: EventType) -> int:
        """Get number of subscribers for an event type."""
        return len(self._subscribers.get(event_type, []))
    
    # -------------------------------------------------------------------------
    # Convenience Methods
    # -------------------------------------------------------------------------
    
    def create_event(
        self,
        event_type: EventType,
        source: Optional[str] = None,
        correlation_id: Optional[str] = None,
        **data: Any,
    ) -> Event:
        """
        Create an event with keyword arguments as data.
        
        Convenience for creating events with less boilerplate.
        
        Args:
            event_type: Event type
            source: Event source
            correlation_id: Correlation ID
            **data: Event data fields
            
        Returns:
            New Event object
        """
        return Event(
            type=event_type,
            data=data,
            source=source,
            correlation_id=correlation_id,
        )
    
    async def emit(
        self,
        event_type: EventType,
        source: Optional[str] = None,
        **data: Any,
    ) -> bool:
        """
        Create and publish an event in one call.
        
        Args:
            event_type: Event type
            source: Event source
            **data: Event data fields
            
        Returns:
            True if published
        """
        event = self.create_event(event_type, source=source, **data)
        return await self.publish(event)
    
    def clear(self) -> None:
        """Clear all state (for testing)."""
        self._subscribers.clear()
        self._global_subscribers.clear()
        self._history.clear()
        # Clear queue
        while not self._queue.empty():
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break
