"""Quickstart example for token-streaming-proxy.

Demonstrates the proxy's core capabilities:
1. SSE parsing from mock LLM responses
2. Backpressure control with slow consumers
3. Token extraction and metrics collection
"""

from __future__ import annotations

import asyncio
import logging
import time

from token_streaming_proxy.backpressure import BackpressureController
from token_streaming_proxy.models import StreamMetrics
from token_streaming_proxy.utils import (
    create_mock_sse_stream,
    extract_token_from_sse,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


async def demo_sse_parsing() -> None:
    """Demo: Parse an SSE stream and extract tokens."""
    logger.info("=== SSE Parsing Demo ===")

    tokens = ["The", " quick", " brown", " fox", " jumps", " over", " the", " lazy", " dog", "."]
    events = create_mock_sse_stream(tokens, model="gpt-4")

    logger.info("Created %d SSE events from %d tokens", len(events), len(tokens))

    reconstructed = []
    for event in events:
        token = extract_token_from_sse(event)
        if token:
            reconstructed.append(token)

    result = "".join(reconstructed)
    logger.info("Reconstructed: '%s'", result)
    logger.info("Matches original: %s", result == "".join(tokens))


async def demo_backpressure() -> None:
    """Demo: Backpressure with fast producer and slow consumer."""
    logger.info("\n=== Backpressure Demo ===")

    controller = BackpressureController(
        high_watermark=200,
        low_watermark=50,
        max_buffer_size=1000,
    )

    # Simulate fast producer
    tokens = [f"token_{i}" for i in range(20)]
    events = create_mock_sse_stream(tokens)

    producer_done = False

    async def producer() -> None:
        nonlocal producer_done
        for event in events:
            if controller._closed:
                break
            accepted = await controller.push(event)
            if not accepted:
                logger.info("  Producer: buffer overflow, stopping")
                break
            if controller.is_paused:
                logger.info(
                    "  Producer: backpressured at %d bytes, waiting...",
                    controller.buffer_size,
                )
                await controller.wait_for_drain()
                logger.info("  Producer: resumed")
        producer_done = True

    async def consumer() -> None:
        consumed = 0
        while True:
            event = await controller.pull(timeout=0.5)
            if event is None:
                if producer_done and controller.pending_events == 0:
                    break
                continue
            consumed += 1
            # Simulate slow consumer
            await asyncio.sleep(0.02)

            token = extract_token_from_sse(event)
            if token:
                logger.info("  Consumer: received '%s'", token[:20])
            if event.is_done:
                break

        logger.info("  Consumer: finished, consumed %d events", consumed)

    await asyncio.gather(
        producer(),
        consumer(),
    )

    logger.info("  Backpressure triggered %d times", controller.backpressure_count)


async def demo_streaming_metrics() -> None:
    """Demo: Collect metrics during streaming."""
    logger.info("\n=== Metrics Demo ===")

    metrics = StreamMetrics(stream_id="demo-1")
    tokens = ["Hello", " ", "world", "!"]
    events = create_mock_sse_stream(tokens)

    time.monotonic()
    for i, event in enumerate(events):
        if i == 0:
            metrics.first_byte_time = time.monotonic()
        metrics.events_received += 1
        metrics.events_sent += 1
        encoded = event.encode()
        metrics.bytes_received += len(encoded)
        metrics.bytes_sent += len(encoded)
        await asyncio.sleep(0.01)

    metrics.complete()

    logger.info("  Stream ID: %s", metrics.stream_id)
    logger.info("  TTFB: %.1f ms", metrics.ttfb_ms)
    logger.info("  Duration: %.1f ms", metrics.duration_ms)
    logger.info("  Events: %d sent", metrics.events_sent)
    logger.info("  Bytes: %d sent", metrics.bytes_sent)
    logger.info("  Throughput: %.0f B/s", metrics.throughput_bytes_per_sec)
    logger.info("  State: %s", metrics.state.value)


async def main() -> None:
    """Run all demos."""
    await demo_sse_parsing()
    await demo_backpressure()
    await demo_streaming_metrics()
    logger.info("\nAll demos completed successfully!")


if __name__ == "__main__":
    asyncio.run(main())
