"""Tests for utility functions."""

from __future__ import annotations

import pytest

from token_streaming_proxy.models import SSEEvent
from token_streaming_proxy.utils import (
    create_mock_sse_stream,
    extract_token_from_sse,
    format_stats_table,
    simulate_sse_stream,
)


class TestCreateMockStream:
    """Tests for mock SSE stream creation."""

    def test_creates_events_plus_done(self) -> None:
        """Creates N token events plus [DONE]."""
        events = create_mock_sse_stream(["a", "b", "c"])
        assert len(events) == 4
        assert events[-1].is_done

    def test_event_data_is_valid_json(self) -> None:
        """Token events contain valid OpenAI-format JSON."""
        import json

        events = create_mock_sse_stream(["Hello"])
        data = json.loads(events[0].data)
        assert data["choices"][0]["delta"]["content"] == "Hello"

    def test_custom_model_name(self) -> None:
        """Custom model name appears in events."""
        import json

        events = create_mock_sse_stream(["x"], model="gpt-5")
        data = json.loads(events[0].data)
        assert data["model"] == "gpt-5"

    def test_events_have_raw_bytes(self) -> None:
        """Each event has raw bytes set."""
        events = create_mock_sse_stream(["test"])
        assert events[0].raw.startswith(b"data: ")


class TestSimulateSSEStream:
    """Tests for async SSE stream simulation."""

    @pytest.mark.asyncio
    async def test_yields_encoded_events(self) -> None:
        """Yields encoded bytes for each event."""
        events = create_mock_sse_stream(["Hi"])
        chunks = []
        async for chunk in simulate_sse_stream(events, delay_ms=0):
            chunks.append(chunk)
        assert len(chunks) == 2  # "Hi" + [DONE]
        assert all(isinstance(c, bytes) for c in chunks)


class TestFormatStatsTable:
    """Tests for stats table formatting."""

    def test_formats_table(self) -> None:
        """Table contains all keys and values."""
        stats = {"Requests": 100, "Errors": 5, "Latency": "10ms"}
        table = format_stats_table(stats, title="Test Stats")
        assert "Test Stats" in table
        assert "Requests" in table
        assert "100" in table
        assert "10ms" in table

    def test_empty_stats(self) -> None:
        """Empty stats produces minimal table."""
        table = format_stats_table({})
        assert "=" in table


class TestExtractToken:
    """Additional token extraction edge cases."""

    def test_delta_without_content(self) -> None:
        """Delta without content key returns None."""
        event = SSEEvent(data='{"choices": [{"delta": {"role": "assistant"}}]}')
        assert extract_token_from_sse(event) is None

    def test_empty_content(self) -> None:
        """Empty content string is still returned."""
        event = SSEEvent(data='{"choices": [{"delta": {"content": ""}}]}')
        assert extract_token_from_sse(event) == ""
