"""Benchmarks for SSE parsing and backpressure throughput."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import List

from token_streaming_proxy.backpressure import BackpressureController
from token_streaming_proxy.models import SSEEvent
from token_streaming_proxy.sse import iter_sse_events, parse_sse_event
from token_streaming_proxy.utils import create_mock_sse_stream, extract_token_from_sse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

NUM_WARMUP = 3
NUM_ITERS = 100


def bench_sse_parsing() -> None:
    """Benchmark SSE event parsing throughput."""
    logger.info("=== SSE Parsing Benchmark ===")

    # Generate raw SSE data
    tokens = [f"token_{i}" for i in range(100)]
    events = create_mock_sse_stream(tokens)
    raw_data = [event.encode() for event in events]

    # Warmup
    for _ in range(NUM_WARMUP):
        for raw in raw_data:
            parse_sse_event(raw)

    # Benchmark
    start = time.perf_counter()
    total_parsed = 0
    total_bytes = 0
    for _ in range(NUM_ITERS):
        for raw in raw_data:
            event = parse_sse_event(raw)
            if event is not None:
                total_parsed += 1
                total_bytes += len(raw)
    elapsed = time.perf_counter() - start

    events_per_sec = total_parsed / elapsed
    mb_per_sec = (total_bytes / 1e6) / elapsed

    logger.info(
        "  Parsed %d events in %.3fs: %.0f events/s, %.2f MB/s",
        total_parsed, elapsed, events_per_sec, mb_per_sec,
    )


def bench_token_extraction() -> None:
    """Benchmark token extraction from parsed events."""
    logger.info("\n=== Token Extraction Benchmark ===")

    tokens = [f"word_{i}" for i in range(100)]
    events = create_mock_sse_stream(tokens)

    # Warmup
    for _ in range(NUM_WARMUP):
        for event in events:
            extract_token_from_sse(event)

    start = time.perf_counter()
    total_extracted = 0
    for _ in range(NUM_ITERS):
        for event in events:
            token = extract_token_from_sse(event)
            if token is not None:
                total_extracted += 1
    elapsed = time.perf_counter() - start

    logger.info(
        "  Extracted %d tokens in %.3fs: %.0f tokens/s",
        total_extracted, elapsed, total_extracted / elapsed,
    )


async def bench_backpressure_throughput() -> None:
    """Benchmark backpressure controller throughput."""
    logger.info("\n=== Backpressure Throughput Benchmark ===")

    num_events = 10000
    events = []
    for i in range(num_events):
        data = f"token_{i}"
        events.append(SSEEvent(
            data=data,
            raw=f"data: {data}\n\n".encode(),
        ))

    ctrl = BackpressureController(
        high_watermark=1024 * 1024,
        low_watermark=512 * 1024,
        max_buffer_size=10 * 1024 * 1024,
    )

    # Push all events
    start = time.perf_counter()
    for event in events:
        await ctrl.push(event)
    push_elapsed = time.perf_counter() - start

    # Pull all events
    start = time.perf_counter()
    pulled = 0
    while ctrl.pending_events > 0:
        event = await ctrl.pull(timeout=0.001)
        if event is not None:
            pulled += 1
    pull_elapsed = time.perf_counter() - start

    logger.info(
        "  Push: %d events in %.3fs = %.0f events/s",
        num_events, push_elapsed, num_events / push_elapsed,
    )
    logger.info(
        "  Pull: %d events in %.3fs = %.0f events/s",
        pulled, pull_elapsed, pulled / max(pull_elapsed, 1e-9),
    )


async def bench_concurrent_streams() -> None:
    """Benchmark concurrent stream handling."""
    logger.info("\n=== Concurrent Streams Benchmark ===")

    num_streams = 50
    events_per_stream = 100

    async def simulate_stream(stream_id: int) -> float:
        ctrl = BackpressureController(
            max_buffer_size=1024 * 1024,
        )
        events = create_mock_sse_stream(
            [f"s{stream_id}_t{i}" for i in range(events_per_stream)]
        )

        start = time.monotonic()
        for event in events:
            await ctrl.push(event)

        pulled = 0
        while ctrl.pending_events > 0:
            event = await ctrl.pull(timeout=0.001)
            if event:
                pulled += 1

        ctrl.close()
        return time.monotonic() - start

    start = time.perf_counter()
    results = await asyncio.gather(
        *[simulate_stream(i) for i in range(num_streams)]
    )
    total_elapsed = time.perf_counter() - start

    total_events = num_streams * (events_per_stream + 1)  # +1 for [DONE]
    logger.info(
        "  %d concurrent streams, %d events each",
        num_streams, events_per_stream,
    )
    logger.info(
        "  Total: %d events in %.3fs = %.0f events/s",
        total_events, total_elapsed, total_events / total_elapsed,
    )
    logger.info(
        "  Avg stream time: %.1fms",
        (sum(results) / len(results)) * 1000,
    )


def main() -> None:
    """Run all benchmarks."""
    bench_sse_parsing()
    bench_token_extraction()
    asyncio.run(bench_backpressure_throughput())
    asyncio.run(bench_concurrent_streams())


if __name__ == "__main__":
    main()
