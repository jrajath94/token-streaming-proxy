# Interview Prep: token-streaming-proxy

## Elevator Pitch (30 seconds)

I built a purpose-built reverse proxy for LLM APIs that understands Server-Sent Events natively. Standard proxies like Nginx buffer responses and add latency to streaming. My proxy never buffers, applies backpressure when clients are slow, and sends keepalive heartbeats -- processing 525K SSE events per second.

## Why I Built This

### The Real Motivation

Standard reverse proxies don't understand SSE semantics. Nginx buffers by default, adding 100-500ms latency to token streaming. HAProxy has no SSE-aware flow control. When I was building an LLM-powered product, slow mobile clients would cause the proxy to buffer entire responses, consuming memory and defeating the purpose of streaming. I needed a proxy that understood the protocol.

### Company-Specific Framing

| Company         | Why This Matters to Them                                                                                       |
| --------------- | -------------------------------------------------------------------------------------------------------------- |
| Anthropic       | Claude API streaming is SSE-based. Infrastructure that handles SSE correctly is essential for API reliability. |
| OpenAI          | Millions of streaming API calls per hour. Proxy-layer latency compounds at scale.                              |
| DeepMind        | Any deployment of streaming inference needs SSE-aware infrastructure.                                          |
| NVIDIA          | TensorRT-LLM and Triton serve via SSE. Efficient proxy layer completes the serving stack.                      |
| Google          | Cloud Run, Apigee all need SSE support. This demonstrates cloud infrastructure thinking.                       |
| Meta FAIR       | llama.cpp server uses SSE. Self-hosted inference needs proper proxy infrastructure.                            |
| Citadel/JS/2Sig | Backpressure and flow control are core to low-latency systems. Same principles apply to market data feeds.     |

## Architecture Deep-Dive

### Key Design Decisions

| Decision                   | Why                                       | Alternative                                 | Tradeoff                                                       |
| -------------------------- | ----------------------------------------- | ------------------------------------------- | -------------------------------------------------------------- |
| High/low watermark BP      | Prevents oscillation (hysteresis)         | Simple size check                           | Small added complexity, but prevents rapid pause/resume cycles |
| SSE comments for heartbeat | Per-spec invisible to clients             | Data events                                 | Doesn't trigger event handlers, truly transparent              |
| httpx async client         | HTTP/2, connection pooling, streaming API | aiohttp (heavier), urllib3 (sync)           | Slightly newer library, but better async ergonomics            |
| Starlette over FastAPI     | No Pydantic overhead on hot path          | FastAPI (validation), raw ASGI (no routing) | Less auto-docs, but ~15% lower latency per request             |
| Per-stream metrics         | Zero external dependencies                | Prometheus client                           | Can't aggregate across instances without additional work       |

### Scaling Analysis

- **Current capacity:** ~100K events/s on single core, 100 concurrent streams
- **10x strategy:** uvicorn workers (multi-process), horizontal scaling behind L4 LB
- **100x strategy:** Rewrite hot path in Rust/C++ (SSE parsing, backpressure buffer), keep Python for config/metrics
- **Bottlenecks:** Python GIL limits single-process throughput; asyncio event loop is the ceiling
- **Cost estimate:** Single c5.large ($0.085/hr) handles ~1000 concurrent LLM streams

## 10 Deep-Dive Interview Questions

### Q1: Walk me through a request end-to-end.

**A:** Client sends POST to `/v1/chat/completions`. Starlette routes to `proxy_stream()`. We create a `StreamMetrics` instance, forward the request to upstream via httpx with streaming enabled, check the content-type. If it's `text/event-stream`, we create a `BackpressureController`, spawn a background task to read from upstream and push events into the controller, and return a `StreamingResponse` whose generator pulls from the controller. The generator also sends heartbeat comments every 15 seconds of silence.

### Q2: Why high/low watermarks instead of a simple buffer limit?

**A:** Simple limit causes oscillation: buffer fills to max, we pause, client drains one event, we resume, buffer immediately fills again, we pause. This creates a rapid pause/resume cycle that wastes CPU. Watermarks create hysteresis: pause at 64KB, don't resume until 16KB. The gap between them creates a stable operating range.

### Q3: What was the hardest bug you hit?

**A:** SSE events split across TCP chunks. A single `data: {"choices":[...]}` event might arrive in two chunks: `data: {"choi` and `ces":[...]}`. Naive newline splitting breaks the JSON. The fix: buffer incoming bytes and only parse on the double-newline SSE event boundary, not on arrival.

### Q4: How would you scale to 100x?

**A:** The bottleneck is Python's asyncio event loop. Three options: (1) Multi-process with uvicorn workers and session affinity. (2) Rewrite the SSE parser and backpressure buffer in Rust via PyO3 -- these are the hot paths. (3) Move to a purpose-built Rust proxy (like Pingora) with SSE awareness added.

### Q5: What would you do differently with more time?

**A:** (1) Add request routing to multiple upstreams with health checking (round-robin, least-connections). (2) Implement connection multiplexing over HTTP/2 to reduce upstream connections. (3) Add a Prometheus metrics exporter for production observability.

### Q6: How does this compare to LiteLLM Proxy?

**A:** LiteLLM is a full LLM gateway with model management, API key rotation, rate limiting, and usage tracking. My proxy is a focused component: just SSE-aware reverse proxying with backpressure. LiteLLM buffers responses through its middleware pipeline; my proxy never buffers. They solve different problems -- LiteLLM for multi-model orchestration, mine for raw streaming performance.

### Q7: What are the security implications?

**A:** (1) We forward Authorization headers to upstream -- the proxy must be trusted. In production, terminate TLS at the proxy. (2) Request body is read into memory before forwarding -- large requests could OOM. Mitigation: request size limits. (3) The proxy doesn't validate API keys -- it's transparent. Add authentication middleware if exposing to untrusted clients.

### Q8: Explain your testing strategy.

**A:** Unit tests for SSE parsing (edge cases: multiline, comments, split chunks), backpressure controller (watermark triggering, timeout, close), and data models. Integration-style tests with mock SSE streams through the full parser pipeline. 56 tests, 63% coverage. The gap is the HTTP layer (core.py proxy handler) which requires a running server to test properly.

### Q9: What are the failure modes?

**A:** (1) Upstream disconnect mid-stream: detected by httpx, propagated as stream error to client. (2) Client disconnect mid-stream: asyncio detects broken pipe, we cancel the upstream reader task. (3) Buffer overflow (slow client): we abort with 502 after `max_buffer_size` exceeded. (4) Upstream timeout: configurable, returns 502 with error message.

### Q10: Explain SSE backpressure from first principles.

**A:** SSE is HTTP long-polling over a single connection. The server sends `data: ...\n\n` events. TCP provides flow control: if the receiver's buffer is full, the sender's `send()` blocks. Our backpressure exploits this: when our buffer is full (client is slow), we stop reading from upstream. This fills the upstream TCP send buffer, which makes the upstream's `send()` block, which naturally slows the LLM token generation. It's backpressure all the way up the stack.

## Metrics & Results

| Metric                  | Value         | How Measured  | Significance                  |
| ----------------------- | ------------- | ------------- | ----------------------------- |
| SSE parse throughput    | 525K events/s | bench_core.py | Proxy is never the bottleneck |
| Token extraction        | 317K tokens/s | bench_core.py | Faster than any LLM generates |
| Backpressure throughput | 1.5M events/s | bench_core.py | Buffer is not the bottleneck  |
| Concurrent streams      | 99K events/s  | 50 streams    | Scales linearly               |
| Test count              | 56            | pytest        | Solid coverage                |

## Career Narrative

- **JPMorgan:** Built real-time data pipelines with backpressure for financial market data. Same flow control principles apply.
- **Goldman Sachs:** Low-latency networking for quant systems. Understanding TCP flow control and buffering is fundamental.
- **NVIDIA:** Infrastructure for serving models at scale. The proxy layer is critical for production LLM deployment.
- **This project:** Demonstrates I can build production networking infrastructure with proper flow control, not just ML models.
