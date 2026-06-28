"""
RedVER Pub/Sub Manager

Implements Redis-compatible channel and pattern subscriptions using
asyncio.Queue per subscriber so published messages are delivered
asynchronously without blocking the publisher or other connections.

Architecture:
  - Each subscribed TCP connection is represented by an asyncio.Queue.
  - PUBLISH puts a message list onto every matching queue.
  - The connection's push-task drains its queue and writes to the socket.
  - Pattern matching uses fnmatch (already used in KEYS).
"""

import asyncio
import fnmatch


class PubSubManager:
    """
    Central registry for Pub/Sub channels and pattern subscriptions.

    Thread-safety note: all operations run in the single asyncio event loop;
    put_nowait is used (never blocking), so no locking is needed.
    """

    def __init__(self):
        # channel name  → set of asyncio.Queue  (exact subscriptions)
        self._channels: dict[str, set] = {}
        # glob pattern  → set of asyncio.Queue  (pattern subscriptions)
        self._patterns: dict[str, set] = {}

    # ── Exact-channel subscriptions ──────────────────────────────────────

    def subscribe(self, channel: str, queue: asyncio.Queue) -> None:
        """Register queue for exact channel."""
        if channel not in self._channels:
            self._channels[channel] = set()
        self._channels[channel].add(queue)

    def unsubscribe(self, channel: str, queue: asyncio.Queue) -> None:
        """Deregister queue from channel; prune empty sets."""
        qs = self._channels.get(channel)
        if qs:
            qs.discard(queue)
            if not qs:
                del self._channels[channel]

    # ── Pattern subscriptions ────────────────────────────────────────────

    def psubscribe(self, pattern: str, queue: asyncio.Queue) -> None:
        """Register queue for glob pattern."""
        if pattern not in self._patterns:
            self._patterns[pattern] = set()
        self._patterns[pattern].add(queue)

    def punsubscribe(self, pattern: str, queue: asyncio.Queue) -> None:
        """Deregister queue from pattern; prune empty sets."""
        qs = self._patterns.get(pattern)
        if qs:
            qs.discard(queue)
            if not qs:
                del self._patterns[pattern]

    # ── Publish ──────────────────────────────────────────────────────────

    def publish(self, channel: str, message: str) -> int:
        """
        Deliver message to all subscribers of channel (exact + patterns).
        Returns total number of message deliveries.
        """
        count = 0

        # Exact-channel subscribers receive: ["message", channel, payload]
        for q in list(self._channels.get(channel, set())):
            q.put_nowait(["message", channel, message])
            count += 1

        # Pattern subscribers receive: ["pmessage", pattern, channel, payload]
        for pattern, queues in list(self._patterns.items()):
            if fnmatch.fnmatch(channel, pattern):
                for q in list(queues):
                    q.put_nowait(["pmessage", pattern, channel, message])
                    count += 1

        return count

    # ── Cleanup ──────────────────────────────────────────────────────────

    def remove_subscriber(self, queue: asyncio.Queue) -> None:
        """
        Remove a queue from every channel and pattern it was registered with.
        Called when a client connection closes.
        """
        for qs in self._channels.values():
            qs.discard(queue)
        for qs in self._patterns.values():
            qs.discard(queue)
        # Prune empty sets
        self._channels = {k: v for k, v in self._channels.items() if v}
        self._patterns = {k: v for k, v in self._patterns.items() if v}

    # ── Introspection ────────────────────────────────────────────────────

    @property
    def active_channel_count(self) -> int:
        """Number of channels with at least one subscriber."""
        return len(self._channels)

    @property
    def active_pattern_count(self) -> int:
        """Number of patterns with at least one subscriber."""
        return len(self._patterns)

    def subscriber_count(self, channel: str) -> int:
        """Number of subscribers on a specific channel."""
        return len(self._channels.get(channel, set()))
