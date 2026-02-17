# LinkedIn Post: token-streaming-proxy

I just open-sourced token-streaming-proxy -- here's why your LLM streaming is slower than it should be.

Every production LLM deployment sits behind a reverse proxy. Nginx, HAProxy, Caddy -- they're great for HTTP, but they don't understand Server-Sent Events. They buffer streaming responses by default, adding 100-500ms latency per token batch. They don't send keepalives, so connections timeout. And when a client on a slow network can't consume tokens fast enough, the proxy buffers everything in memory until it crashes.

I built a purpose-built SSE proxy that solves all three problems. It never buffers -- events flow through as they arrive. It applies backpressure using high/low watermarks (the same hysteresis principle from TCP flow control) to prevent memory overflow with slow clients. And it sends SSE comment heartbeats to keep connections alive through idle periods. The result: 525K events/s parsing throughput, sub-millisecond forwarding latency, and zero-dependency metrics for observability.

The project handles 50 concurrent streams at 99K events/s on a single process. It works with any OpenAI-compatible API (OpenAI, Anthropic, Ollama, vLLM, TGI) and can be deployed as a drop-in proxy -- just change your base URL. Built with Python's asyncio for clean concurrent I/O, Starlette for lightweight ASGI routing, and httpx for HTTP/2-capable upstream connections.

If you're deploying LLMs in production, your proxy layer matters more than you think.

-> GitHub: github.com/jrajath94/token-streaming-proxy

#AI #MachineLearning #LLM #Infrastructure #SoftwareEngineering #OpenSource #Streaming #Python
