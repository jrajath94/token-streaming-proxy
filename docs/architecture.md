# Architecture: token-streaming-proxy

## Request Flow

```
Client Request
    │
    ▼
┌──────────────────────────────────┐
│         Starlette Router         │
│  /health → health_check()        │
│  /metrics → metrics_endpoint()   │
│  /* → proxy_stream()             │
└──────────┬───────────────────────┘
           │
           ▼
┌──────────────────────────────────┐
│      StreamingProxy.proxy_stream │
│  1. Create StreamMetrics         │
│  2. Forward request to upstream  │
│  3. Check content-type           │
│     ├─ Not SSE → forward as-is  │
│     └─ SSE → stream with BP     │
└──────────┬───────────────────────┘
           │
    ┌──────┴──────┐
    ▼             ▼
┌────────┐  ┌──────────────┐
│Upstream│  │ Backpressure │
│ Reader │  │  Controller  │
│        │──►  push()      │
│  SSE   │  │  high/low    │
│ Parser │  │  watermarks  │
└────────┘  └──────┬───────┘
                   │ pull()
                   ▼
            ┌──────────────┐
            │   Client     │
            │  Generator   │
            │  + Heartbeat │
            └──────────────┘
```

## Backpressure Design

The controller uses hysteresis (high/low watermarks) to prevent oscillation:

```
Buffer Size
    ▲
    │
High├──────────── PAUSE upstream ─────
    │                                │
    │         Normal operation       │
    │                                │
Low ├──────────── RESUME upstream ───
    │
    └─────────────────────────────► Time
```

When buffer exceeds `high_watermark`: pause reading from upstream (TCP backpressure propagates to LLM).
When buffer drops below `low_watermark`: resume reading.
When buffer exceeds `max_buffer_size`: abort stream (client too slow).
