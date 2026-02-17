"""Tests for data models."""

from __future__ import annotations

import time

import pytest

from token_streaming_proxy.models import (
    ProxyConfig,
    ProxyStats,
    SSEEvent,
    StreamMetrics,
    StreamState,
)


class TestSSEEvent:
    """Tests for SSEEvent dataclass."""

    def test_encode_simple(self) -> None:
        """Test encoding a simple message event."""
        event = SSEEvent(data="hello")
        encoded = event.encode()
        assert b"data: hello\n\n" in encoded

    def test_encode_with_event_type(self) -> None:
        """Test encoding with custom event type."""
        event = SSEEvent(data="payload", event="custom")
        encoded = event.encode()
        assert b"event: custom\n" in encoded
        assert b"data: payload\n" in encoded

    def test_encode_with_id(self) -> None:
        """Test encoding with event ID."""
        event = SSEEvent(data="payload", id="42")
        encoded = event.encode()
        assert b"id: 42\n" in encoded

    def test_encode_multiline_data(self) -> None:
        """Test encoding with multiline data."""
        event = SSEEvent(data="line1\nline2\nline3")
        encoded = event.encode()
        assert b"data: line1\n" in encoded
        assert b"data: line2\n" in encoded
        assert b"data: line3\n" in encoded

    def test_is_done_true(self) -> None:
        """Test [DONE] sentinel detection."""
        event = SSEEvent(data="[DONE]")
        assert event.is_done is True

    def test_is_done_false(self) -> None:
        """Test non-DONE events."""
        event = SSEEvent(data='{"choices": []}')
        assert event.is_done is False

    @pytest.mark.parametrize(
        "data,expected",
        [("[DONE]", True), (" [DONE] ", True), ("hello", False), ("", False)],
    )
    def test_is_done_variants(self, data: str, expected: bool) -> None:
        """Test DONE detection with whitespace variants."""
        assert SSEEvent(data=data).is_done == expected


class TestStreamMetrics:
    """Tests for StreamMetrics dataclass."""

    def test_ttfb_calculation(self) -> None:
        """Test time-to-first-byte calculation."""
        metrics = StreamMetrics(stream_id="test")
        metrics.start_time = 1000.0
        metrics.first_byte_time = 1000.05
        assert abs(metrics.ttfb_ms - 50.0) < 0.1

    def test_ttfb_zero_when_no_first_byte(self) -> None:
        """Test TTFB is 0 when first byte hasn't arrived."""
        metrics = StreamMetrics(stream_id="test")
        assert metrics.ttfb_ms == 0.0

    def test_complete_success(self) -> None:
        """Test marking stream as completed."""
        metrics = StreamMetrics(stream_id="test")
        metrics.complete()
        assert metrics.state == StreamState.COMPLETED
        assert metrics.error is None
        assert metrics.end_time > 0

    def test_complete_error(self) -> None:
        """Test marking stream as errored."""
        metrics = StreamMetrics(stream_id="test")
        metrics.complete(error="connection reset")
        assert metrics.state == StreamState.ERRORED
        assert metrics.error == "connection reset"

    def test_throughput(self) -> None:
        """Test throughput calculation."""
        metrics = StreamMetrics(stream_id="test")
        metrics.start_time = time.monotonic()
        metrics.bytes_sent = 10000
        metrics.end_time = metrics.start_time + 1.0  # 1 second
        assert abs(metrics.throughput_bytes_per_sec - 10000.0) < 100


class TestProxyStats:
    """Tests for ProxyStats aggregate metrics."""

    def test_record_stream(self) -> None:
        """Test recording a completed stream."""
        stats = ProxyStats()
        metrics = StreamMetrics(stream_id="test")
        metrics.start_time = time.monotonic() - 0.1
        metrics.first_byte_time = metrics.start_time + 0.05
        metrics.events_sent = 10
        metrics.bytes_sent = 1000
        metrics.complete()

        stats.record_stream(metrics)
        assert stats.total_streams == 1
        assert stats.total_events == 10
        assert stats.total_bytes == 1000
        assert stats.total_errors == 0

    def test_record_error_stream(self) -> None:
        """Test recording a failed stream."""
        stats = ProxyStats()
        metrics = StreamMetrics(stream_id="test")
        metrics.complete(error="timeout")
        stats.record_stream(metrics)
        assert stats.total_errors == 1

    def test_running_average(self) -> None:
        """Test running average calculation."""
        stats = ProxyStats()
        for i in range(5):
            metrics = StreamMetrics(stream_id=f"test-{i}")
            metrics.start_time = time.monotonic() - 0.1
            metrics.first_byte_time = metrics.start_time + 0.01
            metrics.complete()
            stats.record_stream(metrics)
        assert stats.avg_ttfb_ms > 0


class TestProxyConfig:
    """Tests for ProxyConfig."""

    def test_defaults(self) -> None:
        """Test default configuration values."""
        config = ProxyConfig()
        assert config.port == 8080
        assert config.upstream_timeout == 120.0
        assert config.max_buffer_size == 1024 * 1024

    def test_custom_config(self) -> None:
        """Test custom configuration."""
        config = ProxyConfig(
            port=9090,
            upstream_base_url="http://my-llm:8000",
            max_connections=50,
        )
        assert config.port == 9090
        assert config.upstream_base_url == "http://my-llm:8000"
        assert config.max_connections == 50

    def test_add_headers_default(self) -> None:
        """Test default headers include anti-buffering."""
        config = ProxyConfig()
        assert "x-accel-buffering" in config.add_headers
        assert config.add_headers["x-accel-buffering"] == "no"
