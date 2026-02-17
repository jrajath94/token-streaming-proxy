"""Tests for custom exceptions."""

from __future__ import annotations

from token_streaming_proxy.exceptions import (
    BackpressureExceededError,
    InvalidSSEError,
    ProxyError,
    UpstreamConnectionError,
    UpstreamTimeoutError,
)


class TestExceptions:
    """Tests for exception classes."""

    def test_upstream_timeout_error(self) -> None:
        """UpstreamTimeoutError captures url and timeout."""
        exc = UpstreamTimeoutError("http://api.example.com", 30.0)
        assert exc.url == "http://api.example.com"
        assert exc.timeout == 30.0
        assert "30.0s" in str(exc)

    def test_upstream_connection_error(self) -> None:
        """UpstreamConnectionError captures url and reason."""
        exc = UpstreamConnectionError("http://api.example.com", "refused")
        assert exc.url == "http://api.example.com"
        assert exc.reason == "refused"
        assert "refused" in str(exc)

    def test_backpressure_exceeded_error(self) -> None:
        """BackpressureExceededError captures buffer sizes."""
        exc = BackpressureExceededError(2048, 1024)
        assert exc.buffer_size == 2048
        assert exc.max_size == 1024
        assert "2048" in str(exc)

    def test_invalid_sse_error(self) -> None:
        """InvalidSSEError captures raw data."""
        exc = InvalidSSEError("garbled data here")
        assert exc.raw == "garbled data here"
        assert "garbled" in str(exc)

    def test_all_inherit_proxy_error(self) -> None:
        """All exceptions inherit from ProxyError."""
        assert issubclass(UpstreamTimeoutError, ProxyError)
        assert issubclass(UpstreamConnectionError, ProxyError)
        assert issubclass(BackpressureExceededError, ProxyError)
        assert issubclass(InvalidSSEError, ProxyError)
