"""Token Streaming Proxy: High-performance SSE proxy for LLM APIs.

A purpose-built reverse proxy that understands Server-Sent Events (SSE),
handles backpressure between LLM providers and clients, and never
buffers streaming responses.
"""

from token_streaming_proxy.core import StreamingProxy
from token_streaming_proxy.models import ProxyConfig, StreamMetrics

__version__ = "0.1.0"

__all__ = [
    "StreamingProxy",
    "ProxyConfig",
    "StreamMetrics",
]
