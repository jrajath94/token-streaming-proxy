# X Thread: token-streaming-proxy

**Tweet 1:**
Your LLM proxy is adding 100-500ms latency to every token.

Nginx, HAProxy, Caddy -- they all buffer SSE by default. That defeats the entire point of streaming.

I built a proxy that understands SSE natively.

Code: github.com/jrajath94/token-streaming-proxy

---

**Tweet 2:**
The problem: standard reverse proxies treat SSE like regular HTTP.

They buffer. They don't send keepalives. They don't handle backpressure when clients are slow.

Result: your "streaming" response arrives as one big chunk.

---

**Tweet 3:**
My approach: a purpose-built SSE proxy.

- Zero buffering: events forwarded as they arrive
- Backpressure: high/low watermarks prevent OOM with slow clients
- Heartbeats: SSE comments keep connections alive
- Metrics: TTFB, throughput, per-stream observability

---

**Tweet 4:**
The non-obvious insight: hysteresis in backpressure.

Simple buffer limit = oscillation (pause at 64KB, resume at 63KB, pause at 64KB...).

Watermarks = stability (pause at 64KB, resume at 16KB). The gap prevents thrashing.

Same principle as TCP flow control.

---

**Tweet 5:**
Benchmarks:

- SSE parsing: 525K events/s (91 MB/s)
- Token extraction: 317K tokens/s
- Backpressure buffer: 1.5M events/s push, 1.4M pull
- 50 concurrent streams: 99K events/s
- Zero-dependency metrics built in

---

**Tweet 6:**
Star it if you serve LLMs behind a proxy.

Works with OpenAI, Anthropic, Ollama, vLLM -- anything that streams SSE.

github.com/jrajath94/token-streaming-proxy

#LLM #Streaming #SSE #OpenSource #BuildInPublic #AsyncIO #Python
