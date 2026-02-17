"""Tests for SSE parser edge cases not covered by test_core.py."""

from __future__ import annotations

from typing import AsyncIterator

import pytest

from token_streaming_proxy.sse import iter_sse_events, parse_sse_event


class TestParseSSEEdgeCases:
    """Edge cases for SSE event parsing."""

    def test_field_without_colon(self) -> None:
        """Field line without colon sets empty value."""
        raw = b"data\ndata: actual"
        event = parse_sse_event(raw)
        assert event is not None
        # "data" without colon -> value ""
        # "data: actual" -> value "actual"
        assert event.data == "\nactual"

    def test_invalid_retry_value(self) -> None:
        """Non-integer retry field is ignored."""
        raw = b"retry: not-a-number\ndata: test"
        event = parse_sse_event(raw)
        assert event is not None
        assert event.retry is None
        assert event.data == "test"

    def test_whitespace_only_lines_skipped(self) -> None:
        """Lines with only whitespace are skipped."""
        raw = b"data: hello\n  \n\t\ndata: world"
        event = parse_sse_event(raw)
        assert event is not None
        assert event.data == "hello\nworld"

    def test_empty_id_field(self) -> None:
        """Empty id field (just 'id:') sets id to None."""
        raw = b"id:\ndata: test"
        event = parse_sse_event(raw)
        assert event is not None
        assert event.id is None


class TestIterSSEEdgeCases:
    """Edge cases for async SSE iteration."""

    @pytest.mark.asyncio
    async def test_remaining_buffer_after_stream(self) -> None:
        """Data left in buffer after stream ends is still parsed."""
        async def make_stream() -> AsyncIterator[bytes]:
            # No trailing \n\n -- data remains in buffer
            yield b"data: final"

        events = []
        async for event in iter_sse_events(make_stream()):
            events.append(event)

        assert len(events) == 1
        assert events[0].data == "final"

    @pytest.mark.asyncio
    async def test_empty_remaining_buffer_ignored(self) -> None:
        """Empty remaining buffer produces no extra events."""
        async def make_stream() -> AsyncIterator[bytes]:
            yield b"data: hello\n\n"

        events = []
        async for event in iter_sse_events(make_stream()):
            events.append(event)

        assert len(events) == 1

    @pytest.mark.asyncio
    async def test_comment_only_blocks_skipped(self) -> None:
        """Blocks containing only comments produce no events."""
        async def make_stream() -> AsyncIterator[bytes]:
            yield b": keepalive\n\ndata: real\n\n"

        events = []
        async for event in iter_sse_events(make_stream()):
            events.append(event)

        assert len(events) == 1
        assert events[0].data == "real"
