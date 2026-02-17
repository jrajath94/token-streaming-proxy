"""SSE (Server-Sent Events) parser and encoder.

Implements the SSE specification (W3C) for parsing streaming responses
from LLM APIs. Handles multi-line data fields, event types, and the
[DONE] sentinel that OpenAI/Anthropic APIs use.
"""

from __future__ import annotations

import logging
from typing import AsyncIterator, List, Optional

from token_streaming_proxy.models import SSEEvent

logger = logging.getLogger(__name__)

# SSE line endings per spec
SSE_LINE_SEP = b"\n"
SSE_EVENT_SEP = b"\n\n"


def parse_sse_event(raw: bytes) -> Optional[SSEEvent]:
    """Parse a single SSE event from raw bytes.

    Handles the SSE wire format:
        event: <type>
        id: <id>
        retry: <ms>
        data: <payload>
        data: <continuation>

        (blank line = event boundary)

    Args:
        raw: Raw bytes of a single SSE event block.

    Returns:
        Parsed SSEEvent, or None if the block is empty/comment-only.
    """
    lines = raw.decode("utf-8", errors="replace").split("\n")

    event_type = "message"
    data_lines: List[str] = []
    event_id: Optional[str] = None
    retry: Optional[int] = None

    for line in lines:
        if not line or line.isspace():
            continue

        # Comments start with ':'
        if line.startswith(":"):
            continue

        if ":" in line:
            field, _, value = line.partition(":")
            # Strip leading space from value per SSE spec
            if value.startswith(" "):
                value = value[1:]
        else:
            field = line
            value = ""

        if field == "event":
            event_type = value
        elif field == "data":
            data_lines.append(value)
        elif field == "id":
            event_id = value if value else None
        elif field == "retry":
            try:
                retry = int(value)
            except ValueError:
                pass

    if not data_lines:
        return None

    return SSEEvent(
        event=event_type,
        data="\n".join(data_lines),
        id=event_id,
        retry=retry,
        raw=raw,
    )


async def iter_sse_events(
    byte_stream: AsyncIterator[bytes],
) -> AsyncIterator[SSEEvent]:
    """Parse an async byte stream into SSE events.

    Buffers incoming bytes and yields complete events as they arrive.
    Handles partial reads and multi-chunk events correctly.

    Args:
        byte_stream: Async iterator of raw bytes from upstream.

    Yields:
        Parsed SSE events.
    """
    buffer = b""

    async for chunk in byte_stream:
        buffer += chunk

        # Split on double newline (event boundary)
        while SSE_EVENT_SEP in buffer:
            event_raw, buffer = buffer.split(SSE_EVENT_SEP, 1)
            if not event_raw.strip():
                continue

            event = parse_sse_event(event_raw)
            if event is not None:
                yield event

    # Handle any remaining data in buffer
    if buffer.strip():
        event = parse_sse_event(buffer)
        if event is not None:
            yield event


def encode_heartbeat_comment() -> bytes:
    """Encode an SSE comment for keepalive.

    SSE comments (lines starting with ':') keep the connection alive
    without triggering event handlers on the client.

    Returns:
        Encoded keepalive comment bytes.
    """
    return b": heartbeat\n\n"
