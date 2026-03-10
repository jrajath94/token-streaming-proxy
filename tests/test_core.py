"""Tests for core proxy and SSE parsing."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import pytest

from token_streaming_proxy.backpressure import BackpressureController
from token_streaming_proxy.core import StreamingProxy
from token_streaming_proxy.models import (
    ProxyConfig,
    SSEEvent,
)
from token_streaming_proxy.sse import (
    encode_heartbeat_comment,
    iter_sse_events,
    parse_sse_event,
)
from token_streaming_proxy.utils import (
    create_mock_sse_stream,
    extract_token_from_sse,
)


class TestParseSSEEvent:
    """Tests for SSE event parsing."""

    def test_simple_data(self) -> None:
        """Test parsing a simple data event."""
        raw = b"data: hello world"
        event = parse_sse_event(raw)
        assert event is not None
        assert event.data == "hello world"
        assert event.event == "message"

    def test_multiline_data(self) -> None:
        """Test parsing multiline data fields."""
        raw = b"data: line1\ndata: line2\ndata: line3"
        event = parse_sse_event(raw)
        assert event is not None
        assert event.data == "line1\nline2\nline3"

    def test_event_type(self) -> None:
        """Test parsing custom event type."""
        raw = b"event: error\ndata: something went wrong"
        event = parse_sse_event(raw)
        assert event is not None
        assert event.event == "error"
        assert event.data == "something went wrong"

    def test_event_id(self) -> None:
        """Test parsing event ID."""
        raw = b"id: 42\ndata: payload"
        event = parse_sse_event(raw)
        assert event is not None
        assert event.id == "42"

    def test_retry(self) -> None:
        """Test parsing retry field."""
        raw = b"retry: 3000\ndata: reconnect"
        event = parse_sse_event(raw)
        assert event is not None
        assert event.retry == 3000

    def test_comment_ignored(self) -> None:
        """Test that comments (lines starting with :) are ignored."""
        raw = b": this is a comment\ndata: actual data"
        event = parse_sse_event(raw)
        assert event is not None
        assert event.data == "actual data"

    def test_empty_block(self) -> None:
        """Test parsing empty block returns None."""
        assert parse_sse_event(b"") is None
        assert parse_sse_event(b"\n\n") is None

    def test_comment_only(self) -> None:
        """Test comment-only block returns None."""
        assert parse_sse_event(b": keepalive") is None

    @pytest.mark.parametrize(
        "raw,expected_data",
        [
            (b"data: [DONE]", "[DONE]"),
            (b'data: {"choices":[]}', '{"choices":[]}'),
            (b"data:no-space", "no-space"),
        ],
    )
    def test_data_formats(self, raw: bytes, expected_data: str) -> None:
        """Test various data format edge cases."""
        event = parse_sse_event(raw)
        assert event is not None
        assert event.data == expected_data

    def test_done_event(self) -> None:
        """Test [DONE] sentinel parsing."""
        raw = b"data: [DONE]"
        event = parse_sse_event(raw)
        assert event is not None
        assert event.is_done

    def test_openai_format(self) -> None:
        """Test parsing OpenAI chat completion chunk format."""
        raw = b'data: {"id":"chatcmpl-xxx","choices":[{"delta":{"content":"Hi"}}]}'
        event = parse_sse_event(raw)
        assert event is not None
        token = extract_token_from_sse(event)
        assert token == "Hi"


class TestIterSSEEvents:
    """Tests for async SSE event iteration."""

    @pytest.mark.asyncio
    async def test_parse_stream(self) -> None:
        """Test parsing a complete SSE byte stream."""
        chunks = [
            b"data: event1\n\n",
            b"data: event2\n\ndata: event3\n\n",
        ]

        async def make_stream() -> AsyncIterator[bytes]:
            for chunk in chunks:
                yield chunk

        events = []
        async for event in iter_sse_events(make_stream()):
            events.append(event)

        assert len(events) == 3
        assert events[0].data == "event1"
        assert events[1].data == "event2"
        assert events[2].data == "event3"

    @pytest.mark.asyncio
    async def test_split_across_chunks(self) -> None:
        """Test events split across multiple chunks."""
        chunks = [
            b"data: hel",
            b"lo\n\ndata: world\n\n",
        ]

        async def make_stream() -> AsyncIterator[bytes]:
            for chunk in chunks:
                yield chunk

        events = []
        async for event in iter_sse_events(make_stream()):
            events.append(event)

        assert len(events) == 2
        assert events[0].data == "hello"
        assert events[1].data == "world"

    @pytest.mark.asyncio
    async def test_mock_llm_stream(self) -> None:
        """Test parsing a mock LLM SSE stream."""
        mock = create_mock_sse_stream(["Hello", " world", "!"])

        async def make_stream() -> AsyncIterator[bytes]:
            for event in mock:
                yield event.encode()

        events = []
        async for event in iter_sse_events(make_stream()):
            events.append(event)

        assert len(events) == 4  # 3 tokens + [DONE]
        assert events[-1].is_done


class TestBackpressureController:
    """Tests for the backpressure controller."""

    @pytest.mark.asyncio
    async def test_push_pull_basic(self) -> None:
        """Test basic push/pull flow."""
        ctrl = BackpressureController(
            high_watermark=1024,
            low_watermark=256,
            max_buffer_size=4096,
        )

        event = SSEEvent(data="hello", raw=b"data: hello\n\n")
        assert await ctrl.push(event) is True
        assert ctrl.pending_events == 1

        pulled = await ctrl.pull(timeout=1.0)
        assert pulled is not None
        assert pulled.data == "hello"
        assert ctrl.pending_events == 0

    @pytest.mark.asyncio
    async def test_backpressure_triggers(self) -> None:
        """Test that backpressure triggers at high watermark."""
        ctrl = BackpressureController(
            high_watermark=50,
            low_watermark=10,
            max_buffer_size=1000,
        )

        # Push enough to trigger backpressure
        big_data = "x" * 60
        event = SSEEvent(data=big_data, raw=f"data: {big_data}\n\n".encode())
        await ctrl.push(event)
        assert ctrl.is_paused is True
        assert ctrl.backpressure_count == 1

    @pytest.mark.asyncio
    async def test_backpressure_releases(self) -> None:
        """Test that backpressure releases at low watermark."""
        ctrl = BackpressureController(
            high_watermark=50,
            low_watermark=10,
            max_buffer_size=1000,
        )

        big_data = "x" * 60
        event = SSEEvent(data=big_data, raw=f"data: {big_data}\n\n".encode())
        await ctrl.push(event)
        assert ctrl.is_paused is True

        # Pull to drain below low watermark
        await ctrl.pull(timeout=1.0)
        assert ctrl.is_paused is False

    @pytest.mark.asyncio
    async def test_buffer_overflow(self) -> None:
        """Test buffer overflow returns False."""
        ctrl = BackpressureController(
            high_watermark=50,
            low_watermark=10,
            max_buffer_size=100,
        )

        big_data = "x" * 200
        event = SSEEvent(data=big_data, raw=f"data: {big_data}\n\n".encode())
        result = await ctrl.push(event)
        assert result is False

    @pytest.mark.asyncio
    async def test_pull_timeout(self) -> None:
        """Test pull returns None on timeout."""
        ctrl = BackpressureController()
        result = await ctrl.pull(timeout=0.01)
        assert result is None

    @pytest.mark.asyncio
    async def test_close_releases_waiters(self) -> None:
        """Test closing releases blocked pull."""
        ctrl = BackpressureController()

        async def delayed_close() -> None:
            await asyncio.sleep(0.05)
            ctrl.close()

        task = asyncio.create_task(delayed_close())
        result = await ctrl.pull(timeout=1.0)
        assert result is None
        await task


class TestHeartbeat:
    """Tests for heartbeat encoding."""

    def test_heartbeat_format(self) -> None:
        """Test heartbeat is a valid SSE comment."""
        hb = encode_heartbeat_comment()
        assert hb.startswith(b":")
        assert hb.endswith(b"\n\n")

    def test_heartbeat_is_comment(self) -> None:
        """Test heartbeat doesn't parse as an event."""
        hb = encode_heartbeat_comment()
        event = parse_sse_event(hb)
        assert event is None  # Comments don't produce events


class TestExtractToken:
    """Tests for token extraction from SSE events."""

    def test_chat_completion_format(self) -> None:
        """Test extracting from chat completions delta."""
        event = SSEEvent(
            data='{"choices": [{"delta": {"content": "Hello"}}]}',
        )
        assert extract_token_from_sse(event) == "Hello"

    def test_completions_format(self) -> None:
        """Test extracting from legacy completions format."""
        event = SSEEvent(
            data='{"choices": [{"text": "world"}]}',
        )
        assert extract_token_from_sse(event) == "world"

    def test_done_event(self) -> None:
        """Test DONE event returns None."""
        event = SSEEvent(data="[DONE]")
        assert extract_token_from_sse(event) is None

    def test_invalid_json(self) -> None:
        """Test invalid JSON returns None."""
        event = SSEEvent(data="not json")
        assert extract_token_from_sse(event) is None

    def test_no_choices(self) -> None:
        """Test missing choices returns None."""
        event = SSEEvent(data='{"id": "123"}')
        assert extract_token_from_sse(event) is None


class TestStreamingProxy:
    """Tests for the StreamingProxy class."""

    def test_create_proxy(self) -> None:
        """Test proxy creation with default config."""
        proxy = StreamingProxy()
        assert proxy.config.port == 8080
        assert proxy.stats.total_streams == 0

    def test_create_proxy_custom_config(self) -> None:
        """Test proxy creation with custom config."""
        config = ProxyConfig(port=9090, upstream_base_url="http://local:8000")
        proxy = StreamingProxy(config)
        assert proxy.config.port == 9090

    def test_build_upstream_url(self) -> None:
        """Test URL construction."""
        config = ProxyConfig(upstream_base_url="http://api.example.com")
        proxy = StreamingProxy(config)
        url = proxy._build_upstream_url("/v1/chat/completions")
        assert url == "http://api.example.com/v1/chat/completions"

    def test_build_upstream_url_trailing_slash(self) -> None:
        """Test URL construction with trailing slash on base."""
        config = ProxyConfig(upstream_base_url="http://api.example.com/")
        proxy = StreamingProxy(config)
        url = proxy._build_upstream_url("/v1/chat")
        assert url == "http://api.example.com/v1/chat"

    def test_filter_request_headers(self) -> None:
        """Test hop-by-hop headers are removed."""
        proxy = StreamingProxy()
        headers = {
            "authorization": "Bearer sk-xxx",
            "content-type": "application/json",
            "connection": "keep-alive",
            "host": "example.com",
        }
        filtered = proxy._filter_request_headers(headers)
        assert "authorization" in filtered
        assert "content-type" in filtered
        assert "connection" not in filtered
        assert "host" not in filtered

    def test_create_app(self) -> None:
        """Test ASGI app creation."""
        proxy = StreamingProxy()
        app = proxy.create_app()
        assert app is not None
        assert len(app.routes) == 3  # health, metrics, catch-all
