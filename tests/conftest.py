"""Shared fixtures for streaming proxy tests."""

from __future__ import annotations

import pytest

from token_streaming_proxy.models import ProxyConfig, SSEEvent
from token_streaming_proxy.utils import create_mock_sse_stream


@pytest.fixture
def proxy_config() -> ProxyConfig:
    """Default proxy config for testing."""
    return ProxyConfig(
        port=0,  # Random port
        upstream_base_url="http://localhost:9999",
        upstream_timeout=5.0,
        heartbeat_interval=1.0,
        max_buffer_size=4096,
    )


@pytest.fixture
def mock_events() -> list:
    """Mock SSE events simulating an LLM response."""
    return create_mock_sse_stream(
        tokens=["Hello", " ", "world", "!", " How", " are", " you", "?"],
        model="test-model",
    )


@pytest.fixture
def single_event() -> SSEEvent:
    """Single mock SSE event."""
    return SSEEvent(
        data='{"choices": [{"delta": {"content": "Hi"}}]}',
        event="message",
    )
