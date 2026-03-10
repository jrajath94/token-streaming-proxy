"""Utility functions for the streaming proxy."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from typing import Any

from token_streaming_proxy.models import SSEEvent

logger = logging.getLogger(__name__)


def create_mock_sse_stream(
    tokens: list[str],
    delay_ms: float = 50.0,
    model: str = "mock-model",
) -> list[SSEEvent]:
    """Create a list of mock SSE events simulating an LLM response.

    Useful for testing without an actual LLM API.

    Args:
        tokens: List of token strings to stream.
        delay_ms: Simulated delay between tokens (for benchmarking).
        model: Model name to include in the response.

    Returns:
        List of SSE events in OpenAI chat completions format.
    """
    events = []
    for i, token in enumerate(tokens):
        chunk = {
            "id": f"chatcmpl-mock-{i}",
            "object": "chat.completion.chunk",
            "model": model,
            "choices": [{
                "index": 0,
                "delta": {"content": token},
                "finish_reason": None,
            }],
        }
        events.append(SSEEvent(
            data=json.dumps(chunk),
            raw=f"data: {json.dumps(chunk)}\n\n".encode(),
        ))

    # Add [DONE] sentinel
    events.append(SSEEvent(
        data="[DONE]",
        raw=b"data: [DONE]\n\n",
    ))
    return events


async def simulate_sse_stream(
    events: list[SSEEvent],
    delay_ms: float = 50.0,
) -> AsyncIterator[bytes]:
    """Simulate an upstream SSE byte stream with delays.

    Args:
        events: List of SSE events to stream.
        delay_ms: Milliseconds between events.

    Yields:
        Raw bytes of each SSE event.
    """
    for event in events:
        await asyncio.sleep(delay_ms / 1000)
        yield event.encode()


def format_stats_table(
    stats: dict[str, Any],
    title: str = "Proxy Statistics",
) -> str:
    """Format statistics as an ASCII table.

    Args:
        stats: Dictionary of metric name -> value.
        title: Table title.

    Returns:
        Formatted ASCII table string.
    """
    max_key = max(len(k) for k in stats) if stats else 0
    max_val = max(len(str(v)) for v in stats.values()) if stats else 0
    width = max(max_key + max_val + 7, len(title) + 4)

    lines = [
        "=" * width,
        f"  {title}",
        "=" * width,
    ]
    for key, value in stats.items():
        lines.append(f"  {key:<{max_key}}  {value}")
    lines.append("=" * width)
    return "\n".join(lines)


def extract_token_from_sse(event: SSEEvent) -> str | None:
    """Extract the text token from an OpenAI-format SSE event.

    Handles both chat completions (delta.content) and completions
    (choices[0].text) formats.

    Args:
        event: Parsed SSE event.

    Returns:
        Extracted token string, or None if not a content event.
    """
    if event.is_done:
        return None

    try:
        data = json.loads(event.data)
    except (json.JSONDecodeError, ValueError):
        return None

    choices = data.get("choices", [])
    if not choices:
        return None

    choice = choices[0]
    # Chat completions format
    delta = choice.get("delta", {})
    if "content" in delta:
        return delta["content"]

    # Completions format
    if "text" in choice:
        return choice["text"]

    return None
