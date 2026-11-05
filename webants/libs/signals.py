"""Signals Module

This module provides an event system with:
- Asynchronous signal handling
- Priority-based receivers
- Exception handling
- Signal categories
"""

import asyncio
import itertools
from collections import defaultdict
from contextlib import contextmanager
from typing import Any, Awaitable, Callable, Dict, List, Set, Union

from webants.libs.exceptions import exception_tracker
from webants.utils.logger import get_logger

# Signal sender types
Sender = Union[str, object, None]
# Signal handler types
Handler = Callable[..., Awaitable[Any]]


class Signal:
    """Asynchronous signal for event handling."""
    
    def __init__(self, name: str, doc: str = ""):
        """Initialize signal.
        
        Args:
            name: Signal name
            doc: Signal documentation
        """
        self.name = name
        self.__doc__ = doc
        self.logger = get_logger(f"Signal[{name}]")
        
        # Receivers stored by sender as (priority, sequence, handler)
        self._receivers: Dict[Sender, List[tuple[int, int, Handler]]] = defaultdict(list)
        # Monotonic connection sequence, used as a stable tie-breaker
        self._seq = itertools.count()
        # Set to track temporary disconnections
        self._disabled: Set[Handler] = set()
        
    async def send(
        self,
        sender: Sender,
        **kwargs: Any
    ) -> List[tuple[Handler, Any]]:
        """Send signal to all receivers.
        
        Args:
            sender: Signal sender
            **kwargs: Signal parameters
            
        Returns:
            List of (receiver, result) pairs
        """
        responses = []

        # Get all matching receivers
        receivers = self._get_receivers(sender)
        if not receivers:
            return responses

        # Higher priority first; equal priorities keep connection order
        for priority, seq, receiver in sorted(receivers, key=lambda item: (-item[0], item[1])):
            if receiver in self._disabled:
                continue
                
            try:
                if asyncio.iscoroutinefunction(receiver):
                    response = await receiver(sender, **kwargs)
                else:
                    response = receiver(sender, **kwargs)
                responses.append((receiver, response))
                
            except Exception as e:
                self.logger.error(
                    f"Error in signal handler {receiver.__name__}: {str(e)}"
                )
                exception_tracker.track(e, handled=False)
                
        return responses
        
    def connect(
        self,
        receiver: Handler,
        sender: Sender = None,
        priority: int = 0
    ) -> None:
        """Connect a receiver to this signal.
        
        Args:
            receiver: Signal handler
            sender: Optional sender to filter by
            priority: Handler priority (higher = earlier)
        """
        if not asyncio.iscoroutinefunction(receiver):
            raise ValueError(
                f"Signal receiver {receiver.__name__} must be a coroutine function"
            )
        
        self._receivers[sender].append((priority, next(self._seq), receiver))
        
    def disconnect(
        self,
        receiver: Handler,
        sender: Sender = None
    ) -> None:
        """Disconnect a receiver from this signal.
        
        Args:
            receiver: Signal handler to disconnect
            sender: Optional sender to disconnect from
        """
        if sender is None:
            for s in list(self._receivers.keys()):
                self._receivers[s] = [
                    (p, seq, r) for p, seq, r in self._receivers[s]
                    if r != receiver
                ]
        else:
            self._receivers[sender] = [
                (p, seq, r) for p, seq, r in self._receivers[sender]
                if r != receiver
            ]
            
    def _get_receivers(
        self,
        sender: Sender
    ) -> List[tuple[int, int, Handler]]:
        """Get all receivers for a sender.
        
        Args:
            sender: Signal sender
            
        Returns:
            List of (priority, sequence, handler) tuples
        """
        # Include receivers for the specific sender and None (any sender).
        # When sender is None, the list must not be concatenated with itself.
        if sender is None:
            return list(self._receivers.get(None, ()))
        return list(self._receivers.get(sender, ())) + list(
            self._receivers.get(None, ())
        )
        
    @contextmanager
    def disable(self, receiver: Handler) -> None:
        """Temporarily disable a receiver.
        
        Args:
            receiver: Signal handler to disable
        """
        self._disabled.add(receiver)
        try:
            yield
        finally:
            self._disabled.remove(receiver)
            
    def __str__(self) -> str:
        return f"<Signal: {self.name}>"


class SignalManager:
    """Manages a collection of signals."""
    
    def __init__(self):
        """Initialize signal manager."""
        self.logger = get_logger(self.__class__.__name__)
        self._signals: Dict[str, Signal] = {}
        
    def register(self, signal: Signal) -> None:
        """Register a new signal.
        
        Args:
            signal: Signal to register
        """
        if signal.name in self._signals:
            raise ValueError(f"Signal {signal.name} already registered")
        self._signals[signal.name] = signal
        
    def unregister(self, name: str) -> None:
        """Unregister a signal.
        
        Args:
            name: Name of signal to unregister
        """
        if name in self._signals:
            del self._signals[name]
            
    def get_signal(self, name: str) -> Signal:
        """Get a registered signal.
        
        Args:
            name: Signal name
            
        Returns:
            Signal instance
            
        Raises:
            KeyError: If signal not found
        """
        if name not in self._signals:
            raise KeyError(f"Signal {name} not registered")
        return self._signals[name]
        
    def connect(
        self,
        signal: str,
        receiver: Handler,
        sender: Sender = None,
        priority: int = 0
    ) -> None:
        """Connect a receiver to a signal.
        
        Args:
            signal: Signal name
            receiver: Signal handler
            sender: Optional sender to filter by
            priority: Handler priority
        """
        self.get_signal(signal).connect(receiver, sender, priority)
        
    def disconnect(
        self,
        signal: str,
        receiver: Handler,
        sender: Sender = None
    ) -> None:
        """Disconnect a receiver from a signal.
        
        Args:
            signal: Signal name
            receiver: Signal handler
            sender: Optional sender
        """
        self.get_signal(signal).disconnect(receiver, sender)
        
    async def send(
        self,
        signal: str,
        sender: Sender,
        **kwargs: Any
    ) -> List[tuple[Handler, Any]]:
        """Send a signal.
        
        Args:
            signal: Signal name
            sender: Signal sender
            **kwargs: Signal parameters
            
        Returns:
            List of (receiver, result) pairs
        """
        return await self.get_signal(signal).send(sender, **kwargs)


# Global signal manager
signal_manager = SignalManager()

# Define standard signals
spider_opened = Signal("spider_opened", "Spider is starting")
spider_closed = Signal("spider_closed", "Spider has finished")
spider_idle = Signal("spider_idle", "Spider has no more requests")
request_scheduled = Signal("request_scheduled", "Request added to scheduler")
request_dropped = Signal("request_dropped", "Request filtered out")
request_reached = Signal("request_reached", "Request limit reached")
response_received = Signal("response_received", "Response received from server")
response_downloaded = Signal("response_downloaded", "Response body downloaded")
item_scraped = Signal("item_scraped", "Item extracted from response")
item_dropped = Signal("item_dropped", "Item filtered out")
item_error = Signal("item_error", "Error processing item")

# Register standard signals
for name, signal in list(globals().items()):
    if isinstance(signal, Signal):
        signal_manager.register(signal)