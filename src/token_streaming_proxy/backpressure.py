"""Backpressure controller for managing flow between upstream and client.

When the upstream LLM generates tokens faster than the client can
consume them (slow network, overwhelmed browser), we need flow control.
This module implements a bounded buffer with pause/resume signaling.
"""

from __future__ import annotations

import asyncio
import logging
from collections import deque

from token_streaming_proxy.models import SSEEvent

logger = logging.getLogger(__name__)

DEFAULT_HIGH_WATERMARK = 64 * 1024  # 64KB
DEFAULT_LOW_WATERMARK = 16 * 1024   # 16KB


class BackpressureController:
    """Manages backpressure between upstream producer and client consumer.

    Uses high/low watermarks to signal when the producer should pause
    (buffer too full) and resume (buffer drained enough). This prevents
    unbounded memory growth when clients are slow.

    Attributes:
        high_watermark: Pause upstream when buffer exceeds this (bytes).
        low_watermark: Resume upstream when buffer drops below this.
        max_buffer_size: Hard limit -- drop connection if exceeded.
    """

    def __init__(
        self,
        high_watermark: int = DEFAULT_HIGH_WATERMARK,
        low_watermark: int = DEFAULT_LOW_WATERMARK,
        max_buffer_size: int = 1024 * 1024,
    ) -> None:
        """Initialize the backpressure controller.

        Args:
            high_watermark: Bytes threshold to trigger backpressure.
            low_watermark: Bytes threshold to release backpressure.
            max_buffer_size: Absolute max buffer before error.
        """
        self._high_watermark = high_watermark
        self._low_watermark = low_watermark
        self._max_buffer_size = max_buffer_size
        self._buffer: deque[SSEEvent] = deque()
        self._buffer_size = 0
        self._paused = False
        self._event_ready = asyncio.Event()
        self._drain_event = asyncio.Event()
        self._drain_event.set()
        self._closed = False
        self._backpressure_count = 0

    @property
    def buffer_size(self) -> int:
        """Current buffer size in bytes."""
        return self._buffer_size

    @property
    def is_paused(self) -> bool:
        """Whether upstream should pause producing."""
        return self._paused

    @property
    def backpressure_count(self) -> int:
        """Number of times backpressure was triggered."""
        return self._backpressure_count

    @property
    def pending_events(self) -> int:
        """Number of events waiting in the buffer."""
        return len(self._buffer)

    async def push(self, event: SSEEvent) -> bool:
        """Push an event into the buffer from upstream.

        If the buffer exceeds max_buffer_size, returns False to signal
        the caller should abort the stream.

        Args:
            event: SSE event to buffer.

        Returns:
            True if accepted, False if buffer overflow.
        """
        if self._closed:
            return False

        event_size = len(event.raw) if event.raw else len(event.encode())
        new_size = self._buffer_size + event_size

        if new_size > self._max_buffer_size:
            logger.warning(
                "Buffer overflow: %d + %d > %d",
                self._buffer_size, event_size, self._max_buffer_size,
            )
            return False

        self._buffer.append(event)
        self._buffer_size = new_size
        self._event_ready.set()

        # Check high watermark
        if not self._paused and self._buffer_size >= self._high_watermark:
            self._paused = True
            self._drain_event.clear()
            self._backpressure_count += 1
            logger.debug(
                "Backpressure ON: buffer=%d >= high=%d",
                self._buffer_size, self._high_watermark,
            )

        return True

    async def pull(self, timeout: float | None = None) -> SSEEvent | None:
        """Pull the next event from the buffer for the client.

        Blocks until an event is available or timeout expires.

        Args:
            timeout: Max seconds to wait (None = wait forever).

        Returns:
            Next SSE event, or None if closed/timeout.
        """
        while not self._closed:
            if self._buffer:
                event = self._buffer.popleft()
                event_size = len(event.raw) if event.raw else len(event.encode())
                self._buffer_size -= event_size

                # Check low watermark for releasing backpressure
                if self._paused and self._buffer_size <= self._low_watermark:
                    self._paused = False
                    self._drain_event.set()
                    logger.debug(
                        "Backpressure OFF: buffer=%d <= low=%d",
                        self._buffer_size, self._low_watermark,
                    )

                if not self._buffer:
                    self._event_ready.clear()

                return event

            try:
                await asyncio.wait_for(
                    self._event_ready.wait(),
                    timeout=timeout,
                )
            except asyncio.TimeoutError:
                return None

        return None

    async def wait_for_drain(self) -> None:
        """Wait until backpressure is released.

        Called by the upstream reader when backpressure is active.
        Returns immediately if not paused.
        """
        await self._drain_event.wait()

    def close(self) -> None:
        """Close the controller, releasing any waiters."""
        self._closed = True
        self._event_ready.set()
        self._drain_event.set()
