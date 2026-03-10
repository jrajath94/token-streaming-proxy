"""CLI interface for the streaming proxy server."""

from __future__ import annotations

import argparse
import logging

import uvicorn

from token_streaming_proxy.core import StreamingProxy
from token_streaming_proxy.models import ProxyConfig

logger = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> None:
    """Main CLI entry point.

    Args:
        argv: Command-line arguments.
    """
    parser = argparse.ArgumentParser(
        prog="token-proxy",
        description="High-performance SSE streaming proxy for LLM APIs",
    )
    parser.add_argument(
        "--host", default="0.0.0.0",
        help="Bind address (default: 0.0.0.0)",
    )
    parser.add_argument(
        "--port", type=int, default=8080,
        help="Bind port (default: 8080)",
    )
    parser.add_argument(
        "--upstream", required=True,
        help="Upstream LLM API base URL",
    )
    parser.add_argument(
        "--timeout", type=float, default=120.0,
        help="Upstream response timeout in seconds",
    )
    parser.add_argument(
        "--max-connections", type=int, default=100,
        help="Max concurrent connections",
    )
    parser.add_argument(
        "--max-buffer", type=int, default=1024 * 1024,
        help="Max backpressure buffer in bytes",
    )
    parser.add_argument(
        "--heartbeat", type=float, default=15.0,
        help="Heartbeat interval in seconds",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true",
        help="Enable debug logging",
    )

    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    config = ProxyConfig(
        host=args.host,
        port=args.port,
        upstream_base_url=args.upstream,
        upstream_timeout=args.timeout,
        max_connections=args.max_connections,
        max_buffer_size=args.max_buffer,
        heartbeat_interval=args.heartbeat,
    )

    proxy = StreamingProxy(config)
    app = proxy.create_app()

    logger.info(
        "Starting token-streaming-proxy on %s:%d -> %s",
        config.host, config.port, config.upstream_base_url,
    )

    uvicorn.run(
        app,
        host=config.host,
        port=config.port,
        log_level="debug" if args.verbose else "info",
    )


if __name__ == "__main__":
    main()
