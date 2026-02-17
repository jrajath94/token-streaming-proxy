"""Tests for core proxy functionality: endpoints, client lifecycle, streaming pipeline."""

from __future__ import annotations

import asyncio
import json
from typing import AsyncIterator
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from token_streaming_proxy.core import StreamingProxy
from token_streaming_proxy.exceptions import (
    UpstreamConnectionError,
    UpstreamTimeoutError,
)
from token_streaming_proxy.models import (
    ProxyConfig,
    ProxyState,
    SSEEvent,
    StreamMetrics,
    StreamState,
)


class TestBuildResponseHeaders:
    """Tests for response header construction."""

    def test_strips_hop_by_hop_headers(self) -> None:
        """Hop-by-hop headers are removed from upstream response."""
        proxy = StreamingProxy()
        upstream = httpx.Headers({
            "content-type": "application/json",
            "connection": "keep-alive",
            "transfer-encoding": "chunked",
        })
        result = proxy._build_response_headers(upstream)
        assert "content-type" in result
        assert "connection" not in result
        assert "transfer-encoding" not in result

    def test_strips_content_length(self) -> None:
        """Content-length is removed (streaming has no fixed length)."""
        proxy = StreamingProxy()
        upstream = httpx.Headers({
            "content-type": "text/event-stream",
            "content-length": "1234",
        })
        result = proxy._build_response_headers(upstream)
        assert "content-length" not in result

    def test_strips_configured_headers(self) -> None:
        """Custom strip_headers from config are removed."""
        config = ProxyConfig(strip_headers=["x-request-id", "server"])
        proxy = StreamingProxy(config)
        upstream = httpx.Headers({
            "content-type": "text/event-stream",
            "x-request-id": "abc-123",
            "server": "nginx",
        })
        result = proxy._build_response_headers(upstream)
        assert "x-request-id" not in result
        assert "server" not in result

    def test_adds_configured_headers(self) -> None:
        """Custom add_headers from config are included."""
        config = ProxyConfig(add_headers={"x-proxy": "streaming"})
        proxy = StreamingProxy(config)
        upstream = httpx.Headers({"content-type": "text/plain"})
        result = proxy._build_response_headers(upstream)
        assert result["x-proxy"] == "streaming"

    def test_default_adds_buffering_headers(self) -> None:
        """Default config adds x-accel-buffering and cache-control."""
        proxy = StreamingProxy()
        upstream = httpx.Headers({})
        result = proxy._build_response_headers(upstream)
        assert result["x-accel-buffering"] == "no"
        assert "no-cache" in result["cache-control"]


class TestGetClient:
    """Tests for HTTP client lifecycle."""

    @pytest.mark.asyncio
    async def test_creates_client_on_first_call(self) -> None:
        """Client is lazily created on first access."""
        proxy = StreamingProxy()
        assert proxy._client is None
        client = await proxy._get_client()
        assert client is not None
        assert isinstance(client, httpx.AsyncClient)
        await proxy.close()

    @pytest.mark.asyncio
    async def test_reuses_existing_client(self) -> None:
        """Subsequent calls return the same client instance."""
        proxy = StreamingProxy()
        client1 = await proxy._get_client()
        client2 = await proxy._get_client()
        assert client1 is client2
        await proxy.close()

    @pytest.mark.asyncio
    async def test_recreates_closed_client(self) -> None:
        """A new client is created if the previous one was closed."""
        proxy = StreamingProxy()
        client1 = await proxy._get_client()
        await client1.aclose()
        client2 = await proxy._get_client()
        assert client2 is not client1
        await proxy.close()


class TestClose:
    """Tests for proxy shutdown."""

    @pytest.mark.asyncio
    async def test_close_transitions_state(self) -> None:
        """Close transitions state through DRAINING to STOPPED."""
        proxy = StreamingProxy()
        proxy.state = ProxyState.RUNNING
        await proxy.close()
        assert proxy.state == ProxyState.STOPPED

    @pytest.mark.asyncio
    async def test_close_closes_client(self) -> None:
        """Close closes the underlying httpx client."""
        proxy = StreamingProxy()
        client = await proxy._get_client()
        assert not client.is_closed
        await proxy.close()
        assert client.is_closed

    @pytest.mark.asyncio
    async def test_close_idempotent(self) -> None:
        """Closing twice does not raise."""
        proxy = StreamingProxy()
        await proxy.close()
        await proxy.close()
        assert proxy.state == ProxyState.STOPPED


class TestHealthCheck:
    """Tests for the /health endpoint."""

    @pytest.mark.asyncio
    async def test_returns_json_status(self) -> None:
        """Health check returns JSON with proxy state."""
        proxy = StreamingProxy()
        proxy.state = ProxyState.RUNNING

        # Create a mock request
        request = MagicMock()
        response = await proxy.health_check(request)

        assert response.media_type == "application/json"
        body = json.loads(response.body.decode())
        assert body["status"] == "running"
        assert body["active_streams"] == 0
        assert body["total_streams"] == 0

    @pytest.mark.asyncio
    async def test_reflects_active_streams(self) -> None:
        """Health check shows current active stream count."""
        proxy = StreamingProxy()
        proxy.stats.active_streams = 3
        proxy.stats.total_streams = 10
        proxy.stats.total_errors = 2

        request = MagicMock()
        response = await proxy.health_check(request)
        body = json.loads(response.body.decode())
        assert body["active_streams"] == 3
        assert body["total_streams"] == 10
        assert body["total_errors"] == 2


class TestMetricsEndpoint:
    """Tests for the /metrics endpoint."""

    @pytest.mark.asyncio
    async def test_returns_aggregate_stats(self) -> None:
        """Metrics endpoint returns aggregate statistics."""
        proxy = StreamingProxy()
        proxy.stats.total_streams = 5
        proxy.stats.total_events = 100
        proxy.stats.total_bytes = 50000
        proxy.stats.total_errors = 1

        request = MagicMock()
        response = await proxy.metrics_endpoint(request)
        body = json.loads(response.body.decode())

        assert body["stats"]["total_streams"] == 5
        assert body["stats"]["total_events"] == 100
        assert body["stats"]["total_bytes"] == 50000
        assert body["stats"]["total_errors"] == 1

    @pytest.mark.asyncio
    async def test_includes_active_streams(self) -> None:
        """Metrics shows details of active streams."""
        proxy = StreamingProxy()
        metrics = StreamMetrics(stream_id="test-1")
        metrics.state = StreamState.STREAMING
        metrics.events_sent = 10
        proxy._active_streams["test-1"] = metrics

        request = MagicMock()
        response = await proxy.metrics_endpoint(request)
        body = json.loads(response.body.decode())

        assert len(body["active"]) == 1
        assert body["active"][0]["stream_id"] == "test-1"
        assert body["active"][0]["state"] == "streaming"
        assert body["active"][0]["events"] == 10


class TestProxyStreamErrorHandling:
    """Tests for proxy_stream error paths."""

    @pytest.mark.asyncio
    async def test_proxy_stream_catches_exceptions(self) -> None:
        """proxy_stream returns 502 on unhandled exceptions."""
        proxy = StreamingProxy()

        # Mock request
        request = MagicMock()
        request.url = MagicMock()
        request.url.path = "/v1/chat/completions"
        request.method = "POST"
        request.headers = {"content-type": "application/json"}
        request.query_params = {}
        request.body = AsyncMock(return_value=b'{"test": true}')

        # Mock _handle_stream to raise
        with patch.object(
            proxy, "_handle_stream",
            side_effect=Exception("upstream failure"),
        ):
            response = await proxy.proxy_stream(request)

        assert response.status_code == 502
        assert b"Proxy error" in response.body
        assert proxy.stats.total_errors == 1
        assert proxy.stats.total_streams == 1

    @pytest.mark.asyncio
    async def test_proxy_stream_records_metrics(self) -> None:
        """proxy_stream records metrics even on success."""
        proxy = StreamingProxy()

        request = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200

        with patch.object(
            proxy, "_handle_stream",
            return_value=mock_response,
        ):
            response = await proxy.proxy_stream(request)

        assert response.status_code == 200
        assert proxy.stats.total_streams == 1
        assert proxy.stats.active_streams == 0

    @pytest.mark.asyncio
    async def test_proxy_stream_cleans_active_streams(self) -> None:
        """Active streams are cleaned up after completion."""
        proxy = StreamingProxy()

        request = MagicMock()
        with patch.object(
            proxy, "_handle_stream",
            side_effect=Exception("boom"),
        ):
            await proxy.proxy_stream(request)

        assert len(proxy._active_streams) == 0


class TestHandleStreamNonSSE:
    """Tests for _handle_stream with non-SSE responses."""

    @pytest.mark.asyncio
    async def test_non_sse_response_forwarded(self) -> None:
        """Non-SSE responses are forwarded directly."""
        config = ProxyConfig(upstream_base_url="http://mock-api.local")
        proxy = StreamingProxy(config)

        # Mock upstream response
        mock_resp = MagicMock()
        mock_resp.headers = httpx.Headers({
            "content-type": "application/json",
        })
        mock_resp.status_code = 200
        mock_resp.aread = AsyncMock(
            return_value=b'{"id": "chatcmpl-xxx", "choices": []}'
        )

        # Mock request
        request = MagicMock()
        request.url = MagicMock()
        request.url.path = "/v1/chat/completions"
        request.method = "POST"
        request.headers = {"content-type": "application/json"}
        request.query_params = {}
        request.body = AsyncMock(return_value=b'{}')

        # Mock httpx client
        mock_client = AsyncMock()
        mock_built_req = MagicMock()
        mock_client.build_request.return_value = mock_built_req
        mock_client.send = AsyncMock(return_value=mock_resp)
        mock_client.is_closed = False
        proxy._client = mock_client

        metrics = StreamMetrics(stream_id="test")
        response = await proxy._handle_stream(request, metrics)

        assert response.status_code == 200
        assert metrics.bytes_received == len(b'{"id": "chatcmpl-xxx", "choices": []}')
        assert metrics.state == StreamState.COMPLETED

    @pytest.mark.asyncio
    async def test_connect_timeout_raises(self) -> None:
        """ConnectTimeout is wrapped in UpstreamTimeoutError."""
        config = ProxyConfig(upstream_base_url="http://mock-api.local")
        proxy = StreamingProxy(config)

        request = MagicMock()
        request.url = MagicMock()
        request.url.path = "/v1/chat/completions"
        request.method = "POST"
        request.headers = {}
        request.query_params = {}
        request.body = AsyncMock(return_value=b'{}')

        mock_client = AsyncMock()
        mock_built_req = MagicMock()
        mock_client.build_request.return_value = mock_built_req
        mock_client.send = AsyncMock(side_effect=httpx.ConnectTimeout("timeout"))
        mock_client.is_closed = False
        proxy._client = mock_client

        metrics = StreamMetrics(stream_id="test")
        with pytest.raises(UpstreamTimeoutError):
            await proxy._handle_stream(request, metrics)

    @pytest.mark.asyncio
    async def test_connect_error_raises(self) -> None:
        """ConnectError is wrapped in UpstreamConnectionError."""
        config = ProxyConfig(upstream_base_url="http://mock-api.local")
        proxy = StreamingProxy(config)

        request = MagicMock()
        request.url = MagicMock()
        request.url.path = "/v1/chat/completions"
        request.method = "POST"
        request.headers = {}
        request.query_params = {}
        request.body = AsyncMock(return_value=b'{}')

        mock_client = AsyncMock()
        mock_built_req = MagicMock()
        mock_client.build_request.return_value = mock_built_req
        mock_client.send = AsyncMock(
            side_effect=httpx.ConnectError("connection refused")
        )
        mock_client.is_closed = False
        proxy._client = mock_client

        metrics = StreamMetrics(stream_id="test")
        with pytest.raises(UpstreamConnectionError):
            await proxy._handle_stream(request, metrics)


class TestHandleStreamSSE:
    """Tests for _handle_stream with SSE streaming responses."""

    @pytest.mark.asyncio
    async def test_sse_returns_streaming_response(self) -> None:
        """SSE response produces a StreamingResponse."""
        from starlette.responses import StreamingResponse

        config = ProxyConfig(upstream_base_url="http://mock-api.local")
        proxy = StreamingProxy(config)

        # Build SSE chunks
        sse_data = (
            b'data: {"choices":[{"delta":{"content":"Hi"}}]}\n\n'
            b"data: [DONE]\n\n"
        )

        async def mock_aiter_bytes():
            yield sse_data

        mock_resp = MagicMock()
        mock_resp.headers = httpx.Headers({
            "content-type": "text/event-stream",
        })
        mock_resp.status_code = 200
        mock_resp.aiter_bytes = mock_aiter_bytes
        mock_resp.aclose = AsyncMock()

        request = MagicMock()
        request.url = MagicMock()
        request.url.path = "/v1/chat/completions"
        request.method = "POST"
        request.headers = {"content-type": "application/json"}
        request.query_params = {}
        request.body = AsyncMock(return_value=b'{}')

        mock_client = AsyncMock()
        mock_built_req = MagicMock()
        mock_client.build_request.return_value = mock_built_req
        mock_client.send = AsyncMock(return_value=mock_resp)
        mock_client.is_closed = False
        proxy._client = mock_client

        metrics = StreamMetrics(stream_id="test")
        response = await proxy._handle_stream(request, metrics)

        assert isinstance(response, StreamingResponse)
        assert metrics.state == StreamState.STREAMING


class TestStreamWithBackpressure:
    """Tests for the _stream_with_backpressure method."""

    @pytest.mark.asyncio
    async def test_reads_events_from_upstream(self) -> None:
        """Events are parsed from upstream and pushed to controller."""
        from token_streaming_proxy.backpressure import BackpressureController

        proxy = StreamingProxy()
        controller = BackpressureController()
        metrics = StreamMetrics(stream_id="test")

        sse_chunks = [
            b'data: {"choices":[{"delta":{"content":"Hello"}}]}\n\n',
            b"data: [DONE]\n\n",
        ]

        async def mock_aiter_bytes():
            for chunk in sse_chunks:
                yield chunk

        mock_resp = MagicMock()
        mock_resp.aiter_bytes = mock_aiter_bytes

        await proxy._stream_with_backpressure(mock_resp, controller, metrics)

        assert metrics.events_received == 2
        assert metrics.state == StreamState.COMPLETED
        assert metrics.first_byte_time > 0

    @pytest.mark.asyncio
    async def test_handles_empty_events(self) -> None:
        """Empty SSE blocks are skipped."""
        from token_streaming_proxy.backpressure import BackpressureController

        proxy = StreamingProxy()
        controller = BackpressureController()
        metrics = StreamMetrics(stream_id="test")

        sse_chunks = [
            b"\n\n",  # empty
            b'data: {"choices":[{"delta":{"content":"ok"}}]}\n\n',
        ]

        async def mock_aiter_bytes():
            for chunk in sse_chunks:
                yield chunk

        mock_resp = MagicMock()
        mock_resp.aiter_bytes = mock_aiter_bytes

        await proxy._stream_with_backpressure(mock_resp, controller, metrics)

        assert metrics.events_received == 1

    @pytest.mark.asyncio
    async def test_handles_remaining_buffer(self) -> None:
        """Data remaining after last chunk boundary is still parsed."""
        from token_streaming_proxy.backpressure import BackpressureController

        proxy = StreamingProxy()
        controller = BackpressureController()
        metrics = StreamMetrics(stream_id="test")

        # No trailing \n\n -- data left in buffer
        sse_chunks = [
            b'data: {"choices":[{"delta":{"content":"tail"}}]}',
        ]

        async def mock_aiter_bytes():
            for chunk in sse_chunks:
                yield chunk

        mock_resp = MagicMock()
        mock_resp.aiter_bytes = mock_aiter_bytes

        await proxy._stream_with_backpressure(mock_resp, controller, metrics)

        assert metrics.events_received == 1

    @pytest.mark.asyncio
    async def test_buffer_overflow_aborts(self) -> None:
        """Buffer overflow sets error and returns."""
        from token_streaming_proxy.backpressure import BackpressureController

        proxy = StreamingProxy()
        # Tiny buffer to force overflow
        controller = BackpressureController(max_buffer_size=10)
        metrics = StreamMetrics(stream_id="test")

        big_data = "x" * 100
        sse_chunks = [
            f'data: {{"choices":[{{"delta":{{"content":"{big_data}"}}}}]}}\n\n'.encode(),
        ]

        async def mock_aiter_bytes():
            for chunk in sse_chunks:
                yield chunk

        mock_resp = MagicMock()
        mock_resp.aiter_bytes = mock_aiter_bytes

        await proxy._stream_with_backpressure(mock_resp, controller, metrics)

        assert metrics.error == "Buffer overflow"

    @pytest.mark.asyncio
    async def test_closed_controller_stops_reading(self) -> None:
        """A closed controller causes reading to stop."""
        from token_streaming_proxy.backpressure import BackpressureController

        proxy = StreamingProxy()
        controller = BackpressureController()
        controller.close()  # Pre-close

        metrics = StreamMetrics(stream_id="test")

        sse_chunks = [
            b'data: {"choices":[{"delta":{"content":"ignored"}}]}\n\n',
        ]

        async def mock_aiter_bytes():
            for chunk in sse_chunks:
                yield chunk

        mock_resp = MagicMock()
        mock_resp.aiter_bytes = mock_aiter_bytes

        await proxy._stream_with_backpressure(mock_resp, controller, metrics)
        # No events should be received since controller is closed
        assert metrics.events_received == 0
