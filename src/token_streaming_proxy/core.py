"""Core streaming proxy implementation.

Proxies HTTP requests to upstream LLM APIs, parsing SSE responses
in real-time and forwarding them to clients with backpressure control.
Never buffers the full response -- tokens flow through as they arrive.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
import uuid
from typing import Any

import httpx
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import Response, StreamingResponse
from starlette.routing import Route

from token_streaming_proxy.backpressure import BackpressureController
from token_streaming_proxy.exceptions import (
    UpstreamConnectionError,
    UpstreamTimeoutError,
)
from token_streaming_proxy.models import (
    ProxyConfig,
    ProxyState,
    ProxyStats,
    StreamMetrics,
    StreamState,
)
from token_streaming_proxy.sse import (
    encode_heartbeat_comment,
    parse_sse_event,
)

logger = logging.getLogger(__name__)

# Headers that should not be forwarded
HOP_BY_HOP_HEADERS = frozenset({
    "connection", "keep-alive", "proxy-authenticate",
    "proxy-authorization", "te", "trailers",
    "transfer-encoding", "upgrade",
})


class StreamingProxy:
    """High-performance SSE streaming proxy for LLM APIs.

    Handles the full lifecycle of proxied streaming requests:
    1. Receive client request
    2. Forward to upstream LLM API
    3. Parse SSE events from upstream response
    4. Apply backpressure if client is slow
    5. Forward events to client with zero buffering
    6. Send keepalive heartbeats during gaps
    7. Record metrics for observability

    Attributes:
        config: Proxy configuration.
        stats: Aggregate statistics.
        state: Current proxy state.
    """

    def __init__(self, config: ProxyConfig | None = None) -> None:
        """Initialize the streaming proxy.

        Args:
            config: Proxy configuration (uses defaults if None).
        """
        self.config = config or ProxyConfig()
        self.stats = ProxyStats()
        self.state = ProxyState.STOPPED
        self._active_streams: dict[str, StreamMetrics] = {}
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create the upstream HTTP client.

        Returns:
            Configured async HTTP client.
        """
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(
                    connect=self.config.connect_timeout,
                    read=self.config.upstream_timeout,
                    write=30.0,
                    pool=30.0,
                ),
                limits=httpx.Limits(
                    max_connections=self.config.max_connections,
                    max_keepalive_connections=self.config.max_connections // 2,
                ),
                follow_redirects=True,
            )
        return self._client

    def _build_upstream_url(self, path: str) -> str:
        """Construct the full upstream URL.

        Args:
            path: Request path from the client.

        Returns:
            Full URL to the upstream API.
        """
        base = self.config.upstream_base_url.rstrip("/")
        return f"{base}{path}"

    def _filter_request_headers(
        self, headers: dict[str, str]
    ) -> dict[str, str]:
        """Remove hop-by-hop and proxy headers from request.

        Args:
            headers: Original request headers.

        Returns:
            Filtered headers safe for upstream.
        """
        return {
            k: v for k, v in headers.items()
            if k.lower() not in HOP_BY_HOP_HEADERS
            and k.lower() != "host"
        }

    def _build_response_headers(
        self, upstream_headers: httpx.Headers
    ) -> dict[str, str]:
        """Build response headers for the client.

        Strips hop-by-hop headers, adds streaming-friendly headers,
        and applies configured header overrides.

        Args:
            upstream_headers: Headers from upstream response.

        Returns:
            Headers to send to the client.
        """
        strip_set = frozenset(
            h.lower() for h in self.config.strip_headers
        )
        headers = {
            k: v for k, v in upstream_headers.items()
            if k.lower() not in HOP_BY_HOP_HEADERS
            and k.lower() not in strip_set
            and k.lower() != "content-length"
        }
        headers.update(self.config.add_headers)
        return headers

    async def proxy_stream(
        self,
        request: Request,
    ) -> Response:
        """Proxy a single streaming request to the upstream API.

        This is the main handler that:
        1. Forwards the request to upstream
        2. Sets up backpressure control
        3. Streams SSE events back to the client
        4. Records metrics

        Args:
            request: Incoming Starlette request.

        Returns:
            StreamingResponse with SSE content.
        """
        stream_id = str(uuid.uuid4())[:8]
        metrics = StreamMetrics(stream_id=stream_id)
        self._active_streams[stream_id] = metrics
        self.stats.active_streams += 1

        try:
            return await self._handle_stream(request, metrics)
        except Exception as exc:
            logger.error("Stream %s error: %s", stream_id, exc)
            metrics.complete(error=str(exc))
            return Response(
                content=f"Proxy error: {exc}",
                status_code=502,
                media_type="text/plain",
            )
        finally:
            self.stats.active_streams -= 1
            self.stats.record_stream(metrics)
            self._active_streams.pop(stream_id, None)

    async def _handle_stream(
        self,
        request: Request,
        metrics: StreamMetrics,
    ) -> Response:
        """Internal stream handling with backpressure.

        Args:
            request: Client request.
            metrics: Stream metrics to update.

        Returns:
            StreamingResponse.
        """
        client = await self._get_client()
        upstream_url = self._build_upstream_url(request.url.path)

        body = await request.body()
        req_headers = self._filter_request_headers(dict(request.headers))

        try:
            upstream_req = client.build_request(
                method=request.method,
                url=upstream_url,
                headers=req_headers,
                content=body,
                params=dict(request.query_params),
            )
            upstream_resp = await client.send(upstream_req, stream=True)
        except httpx.ConnectTimeout as exc:
            raise UpstreamTimeoutError(upstream_url, self.config.connect_timeout) from exc
        except httpx.ConnectError as exc:
            raise UpstreamConnectionError(upstream_url, str(exc)) from exc

        content_type = upstream_resp.headers.get("content-type", "")
        is_sse = "text/event-stream" in content_type

        if not is_sse:
            # Non-streaming response: forward as-is
            content = await upstream_resp.aread()
            metrics.bytes_received = len(content)
            metrics.bytes_sent = len(content)
            metrics.complete()
            return Response(
                content=content,
                status_code=upstream_resp.status_code,
                headers=self._build_response_headers(upstream_resp.headers),
                media_type=content_type,
            )

        # SSE streaming response with backpressure
        response_headers = self._build_response_headers(upstream_resp.headers)
        response_headers["content-type"] = "text/event-stream"

        controller = BackpressureController(
            max_buffer_size=self.config.max_buffer_size,
        )

        async def stream_generator() -> Any:
            """Generate SSE events for the StreamingResponse."""
            try:
                await self._stream_with_backpressure(
                    upstream_resp, controller, metrics,
                )
            except Exception as exc:
                logger.error(
                    "Stream %s upstream error: %s",
                    metrics.stream_id, exc,
                )
                metrics.complete(error=str(exc))
            finally:
                controller.close()
                await upstream_resp.aclose()

        # Start upstream reader as background task
        reader_task = asyncio.create_task(
            stream_generator(),
            name=f"upstream-reader-{metrics.stream_id}",
        )

        async def client_generator() -> Any:
            """Yield events to the client from the backpressure buffer."""
            heartbeat_data = encode_heartbeat_comment()
            try:
                while True:
                    event = await controller.pull(
                        timeout=self.config.heartbeat_interval
                    )
                    if event is None:
                        if controller._closed:
                            break
                        # No event within heartbeat interval -- send keepalive
                        metrics.heartbeats_sent += 1
                        yield heartbeat_data
                        continue

                    encoded = event.encode()
                    metrics.events_sent += 1
                    metrics.bytes_sent += len(encoded)
                    yield encoded

                    if event.is_done:
                        break
            except asyncio.CancelledError:
                pass
            finally:
                controller.close()
                if not reader_task.done():
                    reader_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await reader_task
                if metrics.state not in (
                    StreamState.COMPLETED, StreamState.ERRORED
                ):
                    metrics.complete()

        metrics.state = StreamState.STREAMING
        return StreamingResponse(
            client_generator(),
            status_code=upstream_resp.status_code,
            headers=response_headers,
            media_type="text/event-stream",
        )

    async def _stream_with_backpressure(
        self,
        upstream_resp: httpx.Response,
        controller: BackpressureController,
        metrics: StreamMetrics,
    ) -> None:
        """Read upstream SSE and push to backpressure buffer.

        Pauses reading from upstream when the client buffer is full,
        applying TCP-level backpressure to the LLM API.

        Args:
            upstream_resp: Upstream HTTP response (streaming).
            controller: Backpressure controller.
            metrics: Stream metrics to update.
        """
        is_first = True
        buffer = b""

        async for chunk in upstream_resp.aiter_bytes():
            if controller._closed:
                break

            metrics.bytes_received += len(chunk)
            buffer += chunk

            # Parse complete SSE events from buffer
            while b"\n\n" in buffer:
                event_raw, buffer = buffer.split(b"\n\n", 1)
                if not event_raw.strip():
                    continue

                event = parse_sse_event(event_raw)
                if event is None:
                    continue

                if is_first:
                    metrics.first_byte_time = time.monotonic()
                    is_first = False

                metrics.events_received += 1

                # Push to buffer (may trigger backpressure)
                accepted = await controller.push(event)
                if not accepted:
                    metrics.complete(error="Buffer overflow")
                    return

                # If backpressured, wait for client to catch up
                if controller.is_paused:
                    metrics.backpressure_count += 1
                    metrics.state = StreamState.BACKPRESSURED
                    await controller.wait_for_drain()
                    metrics.state = StreamState.STREAMING

        # Handle remaining buffer
        if buffer.strip():
            event = parse_sse_event(buffer)
            if event is not None:
                metrics.events_received += 1
                await controller.push(event)

        metrics.complete()

    async def health_check(self, request: Request) -> Response:
        """Health check endpoint.

        Args:
            request: Incoming request.

        Returns:
            JSON response with proxy status.
        """
        return Response(
            content=(
                f'{{"status": "{self.state.value}", '
                f'"active_streams": {self.stats.active_streams}, '
                f'"total_streams": {self.stats.total_streams}, '
                f'"total_errors": {self.stats.total_errors}, '
                f'"avg_ttfb_ms": {self.stats.avg_ttfb_ms:.1f}}}'
            ),
            media_type="application/json",
        )

    async def metrics_endpoint(self, request: Request) -> Response:
        """Metrics endpoint returning detailed stats.

        Args:
            request: Incoming request.

        Returns:
            JSON with active streams and aggregate metrics.
        """
        active = [
            {
                "stream_id": m.stream_id,
                "state": m.state.value,
                "ttfb_ms": round(m.ttfb_ms, 1),
                "duration_ms": round(m.duration_ms, 1),
                "events": m.events_sent,
                "backpressure": m.backpressure_count,
            }
            for m in self._active_streams.values()
        ]
        import json
        return Response(
            content=json.dumps({
                "stats": {
                    "total_streams": self.stats.total_streams,
                    "active_streams": self.stats.active_streams,
                    "total_events": self.stats.total_events,
                    "total_bytes": self.stats.total_bytes,
                    "total_errors": self.stats.total_errors,
                    "avg_ttfb_ms": round(self.stats.avg_ttfb_ms, 1),
                    "avg_duration_ms": round(self.stats.avg_duration_ms, 1),
                },
                "active": active,
            }),
            media_type="application/json",
        )

    def create_app(self) -> Starlette:
        """Create the Starlette ASGI application.

        Returns:
            Configured Starlette app with routing.
        """
        routes = [
            Route("/health", self.health_check, methods=["GET"]),
            Route("/metrics", self.metrics_endpoint, methods=["GET"]),
            Route(
                "/{path:path}",
                self.proxy_stream,
                methods=["GET", "POST", "PUT", "DELETE", "PATCH"],
            ),
        ]
        self.state = ProxyState.RUNNING
        return Starlette(routes=routes)

    async def close(self) -> None:
        """Shut down the proxy, closing connections.

        Drains active streams and closes the upstream client.
        """
        self.state = ProxyState.DRAINING
        if self._client and not self._client.is_closed:
            await self._client.aclose()
        self.state = ProxyState.STOPPED
