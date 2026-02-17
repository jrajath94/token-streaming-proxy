"""Data models for proxy configuration and metrics."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

DEFAULT_PORT = 8080
DEFAULT_UPSTREAM_TIMEOUT = 120.0
DEFAULT_CONNECT_TIMEOUT = 10.0
DEFAULT_HEARTBEAT_INTERVAL = 15.0
DEFAULT_MAX_BUFFER_SIZE = 1024 * 1024  # 1MB
DEFAULT_MAX_CONNECTIONS = 100


class ProxyState(str, Enum):
    """Current state of the proxy server."""

    STARTING = "starting"
    RUNNING = "running"
    DRAINING = "draining"
    STOPPED = "stopped"


class StreamState(str, Enum):
    """State of an individual SSE stream."""

    CONNECTING = "connecting"
    STREAMING = "streaming"
    BACKPRESSURED = "backpressured"
    COMPLETED = "completed"
    ERRORED = "errored"


@dataclass
class ProxyConfig:
    """Configuration for the streaming proxy.

    Attributes:
        host: Bind address for the proxy server.
        port: Bind port.
        upstream_base_url: Base URL of the upstream LLM API.
        upstream_timeout: Max seconds to wait for upstream response.
        connect_timeout: Max seconds for initial connection.
        heartbeat_interval: Seconds between SSE keepalive comments.
        max_buffer_size: Max bytes to buffer per stream before backpressure.
        max_connections: Max concurrent proxy connections.
        strip_headers: Headers to remove from upstream response.
        add_headers: Headers to add to client response.
    """

    host: str = "0.0.0.0"
    port: int = DEFAULT_PORT
    upstream_base_url: str = "https://api.openai.com"
    upstream_timeout: float = DEFAULT_UPSTREAM_TIMEOUT
    connect_timeout: float = DEFAULT_CONNECT_TIMEOUT
    heartbeat_interval: float = DEFAULT_HEARTBEAT_INTERVAL
    max_buffer_size: int = DEFAULT_MAX_BUFFER_SIZE
    max_connections: int = DEFAULT_MAX_CONNECTIONS
    strip_headers: List[str] = field(
        default_factory=lambda: ["server", "x-request-id"]
    )
    add_headers: Dict[str, str] = field(
        default_factory=lambda: {
            "x-accel-buffering": "no",
            "cache-control": "no-cache, no-transform",
        }
    )


@dataclass
class SSEEvent:
    """Parsed Server-Sent Event.

    Attributes:
        event: Event type (default: "message").
        data: Event data payload.
        id: Event ID for reconnection.
        retry: Reconnection delay in milliseconds.
        raw: Original raw bytes.
    """

    data: str
    event: str = "message"
    id: Optional[str] = None
    retry: Optional[int] = None
    raw: bytes = b""

    @property
    def is_done(self) -> bool:
        """Check if this is a terminal [DONE] event."""
        return self.data.strip() == "[DONE]"

    def encode(self) -> bytes:
        """Encode this event as SSE wire format.

        Returns:
            Bytes in SSE format: "event: ...\ndata: ...\n\n"
        """
        lines = []
        if self.event != "message":
            lines.append(f"event: {self.event}")
        if self.id is not None:
            lines.append(f"id: {self.id}")
        if self.retry is not None:
            lines.append(f"retry: {self.retry}")
        for data_line in self.data.split("\n"):
            lines.append(f"data: {data_line}")
        lines.append("")
        lines.append("")
        return "\n".join(lines).encode("utf-8")


@dataclass
class StreamMetrics:
    """Metrics for a single proxied stream.

    Attributes:
        stream_id: Unique identifier for this stream.
        start_time: When the stream started (monotonic).
        first_byte_time: When the first SSE event arrived.
        end_time: When the stream completed.
        events_received: Number of SSE events from upstream.
        events_sent: Number of SSE events forwarded to client.
        bytes_received: Total bytes from upstream.
        bytes_sent: Total bytes to client.
        backpressure_count: Times backpressure was applied.
        heartbeats_sent: Number of keepalive comments sent.
        state: Current stream state.
        error: Error message if stream failed.
    """

    stream_id: str
    start_time: float = field(default_factory=time.monotonic)
    first_byte_time: float = 0.0
    end_time: float = 0.0
    events_received: int = 0
    events_sent: int = 0
    bytes_received: int = 0
    bytes_sent: int = 0
    backpressure_count: int = 0
    heartbeats_sent: int = 0
    state: StreamState = StreamState.CONNECTING
    error: Optional[str] = None

    @property
    def ttfb_ms(self) -> float:
        """Time to first byte in milliseconds."""
        if self.first_byte_time <= 0 or self.start_time <= 0:
            return 0.0
        return (self.first_byte_time - self.start_time) * 1000

    @property
    def duration_ms(self) -> float:
        """Total stream duration in milliseconds."""
        end = self.end_time if self.end_time > 0 else time.monotonic()
        return (end - self.start_time) * 1000

    @property
    def throughput_bytes_per_sec(self) -> float:
        """Effective throughput in bytes per second."""
        duration_s = self.duration_ms / 1000
        if duration_s <= 0:
            return 0.0
        return self.bytes_sent / duration_s

    def complete(self, error: Optional[str] = None) -> None:
        """Mark this stream as completed.

        Args:
            error: Error message if the stream failed.
        """
        self.end_time = time.monotonic()
        if error:
            self.state = StreamState.ERRORED
            self.error = error
        else:
            self.state = StreamState.COMPLETED


@dataclass
class ProxyStats:
    """Aggregate statistics for the proxy server.

    Attributes:
        total_streams: Total streams handled since start.
        active_streams: Currently active streams.
        total_events: Total SSE events forwarded.
        total_bytes: Total bytes forwarded.
        total_errors: Total stream errors.
        avg_ttfb_ms: Average time to first byte.
        avg_duration_ms: Average stream duration.
    """

    total_streams: int = 0
    active_streams: int = 0
    total_events: int = 0
    total_bytes: int = 0
    total_errors: int = 0
    avg_ttfb_ms: float = 0.0
    avg_duration_ms: float = 0.0

    def record_stream(self, metrics: StreamMetrics) -> None:
        """Update aggregate stats with a completed stream's metrics.

        Args:
            metrics: Completed stream metrics.
        """
        self.total_streams += 1
        self.total_events += metrics.events_sent
        self.total_bytes += metrics.bytes_sent
        if metrics.error:
            self.total_errors += 1
        # Running average
        n = self.total_streams
        self.avg_ttfb_ms = (
            self.avg_ttfb_ms * (n - 1) + metrics.ttfb_ms
        ) / n
        self.avg_duration_ms = (
            self.avg_duration_ms * (n - 1) + metrics.duration_ms
        ) / n
