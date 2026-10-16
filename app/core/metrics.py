"""Prometheus metric objects."""

from prometheus_client import Counter, Gauge, Histogram

orders_sent_total = Counter(
    "orders_sent_total", "NewOrderSingle messages actually sent to the broker", ["symbol", "side", "order_type"]
)
orders_failed_total = Counter("orders_failed_total", "Orders that transitioned to FAILED", ["reason_code"])
fix_reconnects_total = Counter("fix_reconnects_total", "FIX session (re)connect attempts", ["session_type"])
queue_depth = Gauge("queue_depth", "Messages ready in a queue", ["queue"])
orders_received_total = Counter("orders_received_total", "POST /orders requests accepted", ["symbol"])
order_latency_seconds = Histogram(
    "order_latency_seconds",
    "Order latency by pipeline stage",
    ["stage"],  # api_to_publish, publish_to_dispatch, dispatch_to_ack, end_to_end
)
fix_session_up = Gauge("fix_session_up", "1 if the FIX session is logged on, else 0", ["session_type"])
fix_messages_total = Counter("fix_messages_total", "FIX messages sent or received", ["direction", "msg_type"])
fix_handler_errors_total = Counter(
    "fix_handler_errors_total", "Errors raised while handling an inbound FIX message", ["msg_type"]
)
quotes_received_total = Counter("quotes_received_total", "Market data updates received", ["symbol"])
quote_staleness_seconds = Gauge("quote_staleness_seconds", "Age in seconds of the last cached quote", ["symbol"])
price_stale_reads_total = Counter("price_stale_reads_total", "GET /prices reads flagged stale", ["symbol"])
outbox_pending = Gauge("outbox_pending", "Unpublished rows currently in outbox_events")
outbox_parked_total = Counter("outbox_parked_total", "Outbox rows parked after exhausting max_attempts")
cancel_dispatch_parked_total = Counter(
    "cancel_dispatch_parked_total", "Cancel commands parked by the relay without reaching the broker"
)
messages_dead_lettered_total = Counter(
    "messages_dead_lettered_total", "Messages routed to a dead-letter queue", ["queue"]
)
order_illegal_transition_total = Counter(
    "order_illegal_transition_total", "Rejected illegal order status transitions", ["from_status", "to_status"]
)
protection_gap_seconds = Histogram(
    "protection_gap_seconds", "Time from a position opening to its protection reaching ACTIVE", ["symbol"]
)
protection_failures_total = Counter(
    "protection_failures_total", "Protective order placement or fill failures", ["reason"]
)
symbols_discovered_total = Counter("symbols_discovered_total", "New symbols discovered via SecurityList")
symbols_discovery_failures_total = Counter("symbols_discovery_failures_total", "SecurityList discovery failures")
symbols_changed_total = Counter("symbols_changed_total", "Changes applied to already-known symbols", ["kind"])
http_request_duration_seconds = Histogram(
    "http_request_duration_seconds", "HTTP request duration", ["route", "method", "status"]
)
