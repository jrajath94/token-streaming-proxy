"""Custom exceptions for the streaming proxy."""


class ProxyError(Exception):
    """Base exception for proxy errors."""


class UpstreamTimeoutError(ProxyError):
    """Raised when upstream LLM API doesn't respond in time."""

    def __init__(self, url: str, timeout: float) -> None:
        self.url = url
        self.timeout = timeout
        super().__init__(f"Upstream {url} timed out after {timeout}s")


class UpstreamConnectionError(ProxyError):
    """Raised when connection to upstream fails."""

    def __init__(self, url: str, reason: str) -> None:
        self.url = url
        self.reason = reason
        super().__init__(f"Cannot connect to {url}: {reason}")


class BackpressureExceededError(ProxyError):
    """Raised when client can't consume fast enough and buffer overflows."""

    def __init__(self, buffer_size: int, max_size: int) -> None:
        self.buffer_size = buffer_size
        self.max_size = max_size
        super().__init__(
            f"Backpressure buffer overflow: {buffer_size}/{max_size} bytes"
        )


class InvalidSSEError(ProxyError):
    """Raised when upstream sends malformed SSE data."""

    def __init__(self, raw: str) -> None:
        self.raw = raw
        super().__init__(f"Invalid SSE event: {raw[:100]}")
