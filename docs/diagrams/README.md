# System flow diagrams

How a request moves through the gateway, from the REST and WebSocket API to cTrader and back.

## 1. POST /orders — inside the API process

Middleware and dependencies, router, PlaceOrderHandler, unit of work and PostgreSQL, ending at PlaceOrderResponse.

```mermaid
sequenceDiagram
  autonumber
  participant C as Client
  participant MW as Middleware and deps
  participant R as orders.place_order
  participant H as PlaceOrderHandler
  participant U as SqlAlchemyUnitOfWork
  participant DB as PostgreSQL
  C->>MW: POST /api/v1/orders
  MW->>MW: CorrelationIdMiddleware.dispatch()
  MW->>MW: ApiKeyAuthenticator.authenticate() HMAC + pepper
  MW->>MW: rate_limit_orders() Redis token bucket
  MW->>R: PlaceOrderRequest + get_place_order_handler()
  R->>R: _fingerprint(body)
  R->>H: handle(PlaceOrderCommand)
  H->>H: _validate_client_order_id()
  H->>U: __aenter__() builds repositories
  H->>U: orders.get_by_client_order_id()
  U->>DB: SELECT orders
  alt order already exists
    H-->>R: _replay() fingerprint match, replayed=True
  end
  H->>U: symbols.get_by_name()
  U->>DB: SELECT symbols
  H->>H: _validate_symbol, _validate_order_type, _resolve_quantity
  H->>H: _validate_quantity, _validate_prices
  H->>H: _build_order() status PENDING, stores correlation_id
  H->>H: _build_create_order_command() mints fix_cl_ord_id
  H->>U: orders.add(Order)
  H->>U: outbox.add(CreateOrderCommand, order.create)
  H->>U: commit()
  U->>DB: INSERT orders + outbox_events, one transaction
  Note over U,DB: IntegrityError becomes DuplicateKeyError, then 409
  H-->>R: PlaceOrderResult
  R-->>C: 202 PlaceOrderResponse (200 if replayed)
  Note over R,C: errors render as ErrorResponse, RFC 9457 shape
```

## 2. POST /orders — relay to FIX worker to cTrader

The outbox row becomes a command, the dispatch guard runs, and NewOrderSingle goes out on the TRADE session.

```mermaid
sequenceDiagram
  autonumber
  participant RL as OutboxRelay
  participant DB as PostgreSQL
  participant MQ as RabbitMQ
  participant CS as EnvelopeEventConsumer
  participant OH as OrderCommandHandler
  participant SM as SessionManager
  participant CT as cTrader
  loop every outbox_poll_interval
    RL->>DB: publish_batch() SELECT FOR UPDATE SKIP LOCKED
    RL->>RL: _publish_one() build_envelope(message_id = outbox.id)
    RL->>MQ: publish ctrader.commands, key order.create
    RL->>DB: published_at = now, COMMIT
  end
  Note over RL,DB: parked: handle_publish_failure() fails the order only for _PARKING_FAILS_THE_ORDER
  MQ->>CS: deliver from commands.orders
  CS->>CS: decode_envelope() + extract trace context
  CS->>OH: handle(envelope)
  OH->>SM: is_logged_on()
  Note over OH,SM: not logged on: TransientHandlerError, requeue with backoff
  OH->>OH: _handle_create_order() CreateOrderCommand.from_payload()
  OH->>DB: inbox.claim(consumer, message_id)
  Note over OH,DB: already claimed: ack and stop
  OH->>DB: _dispatch_order() orders.get(order_id)
  alt submitted_at already set
    OH->>OH: dispatch guard, message_factory.order_status()
    OH->>SM: send(OrderStatusRequest)
  else first dispatch
    OH->>DB: orders.update(DISPATCHING, fix_cl_ord_id), COMMIT
    OH->>OH: message_factory.new_order() format_quantity / format_price
    OH->>SM: send(NewOrderSingle)
    SM->>CT: 35=D on TRADE session
    OH->>DB: _send_and_publish() status SUBMITTED
    OH->>MQ: publish OrderSubmitted
  end
  Note over OH,DB: send raises: status DISPATCH_UNCERTAIN, reconciliation re-queries
  CS->>MQ: ack()
```

## 3. Execution report — cTrader to projector

The fill returns as a raw event, and the projector turns it into order, position and protection state.

```mermaid
sequenceDiagram
  autonumber
  participant CT as cTrader
  participant SM as SessionManager
  participant FW as FixWorker
  participant MQ as RabbitMQ
  participant PJ as ProjectorHandler
  participant DB as PostgreSQL
  CT->>SM: ExecutionReport 35=8
  SM->>SM: _handle_message() routes on tag 35
  SM->>FW: on_trade_message() via ensure_future
  alt 35=8
    FW->>FW: _handle_trade_message() map_execution_report()
    FW->>MQ: publish ExecutionReportReceived, ctrader.events
  else 35=j or 35=9
    FW->>FW: map_business_message_reject() / map_order_cancel_reject()
    FW->>MQ: publish OrderFailed
  end
  MQ->>PJ: consume events.projector, handle(envelope)
  PJ->>DB: inbox.claim(consumer, message_id)
  alt ExecutionReportReceived
    PJ->>DB: _apply_execution_report() orders.get_by_fix_cl_ord_id()
    PJ->>PJ: bind_contextvars(correlation_id) from the order row
    PJ->>DB: executions.add(Execution)
    PJ->>PJ: apply_transition() vs last_broker_transact_at
    Note over PJ: illegal transition: counted, logged, order unchanged
    PJ->>PJ: _update_order_for_report() sets last_broker_transact_at
    PJ->>DB: orders.update(status, filled_qty, position_id)
    opt fill carries a position_id
      PJ->>DB: _apply_position_effects() positions.add / update
      opt stop_loss or take_profit requested
        PJ->>DB: _place_protective_orders() _build_protective_order() x2
        PJ->>DB: outbox.add(PlaceProtectiveOrdersCommand)
      end
      opt the filled order is itself a protective order
        PJ->>DB: _apply_protective_order_transition() _compute_protection_state()
        PJ->>DB: _cancel_oco_sibling() outbox.add(CancelOrderCommand)
      end
    end
  else OrderFailed
    PJ->>PJ: _apply_order_failed() apply_transition()
    PJ->>DB: orders.update(REJECTED or FAILED)
  end
  PJ->>DB: COMMIT, version_id_col guards the write
  Note over PJ,DB: duplicate exec_id: DuplicateKeyError, whole TX is a no-op
  Note over PJ,DB: StaleDataError becomes ConcurrentModificationError, requeued
```

## 4. WS /ws/prices — cTrader to client via Redis

How a quote travels from the QUOTE FIX session through Redis pub/sub to a subscribed WebSocket client.

```mermaid
sequenceDiagram
  autonumber
  participant C as Client
  participant WS as PriceWebSocketSession
  participant RD as Redis
  participant FW as FixWorker
  participant SM as SessionManager (QUOTE)
  participant CT as cTrader

  rect rgba(128,128,128,0.06)
    Note over FW,CT: producer side, running before any client connects
    SM->>CT: Logon 35=A, then SecurityListRequest 35=y
    CT-->>FW: SecurityList 35=y
    FW->>FW: map_security_list() fills _symbol_registry
    FW->>CT: market_data_requests(entry_types = BID, OFFER) per symbol
  end

  C->>WS: GET /ws/prices?api_key=...
  WS->>WS: ApiKeyAuthenticator.authenticate()
  Note over C,WS: invalid key: close(4401), never accepted
  WS->>C: accept()
  WS->>RD: pubsub.subscribe(control channel)
  C->>WS: {action: subscribe, symbols: [EURUSD]}
  WS->>WS: handle_frame() checks ws_max_symbols
  WS->>RD: pubsub.subscribe(quotes:EURUSD)

  CT-->>SM: MarketDataSnapshot 35=W / Incremental 35=X
  SM->>FW: on_quote_message() via ensure_future
  FW->>FW: map_market_data(), resolve symbol from _symbol_registry
  FW->>FW: merge entry into _last_quotes[symbol]
  Note over FW: an incremental may carry one side only - nothing is published until both bid and ask are known
  FW->>RD: price_cache.set() HSET price:account:symbol, EXPIRE price_ttl
  FW->>RD: quote_publisher.publish() PUBLISH quotes:EURUSD
  Note over FW,RD: Redis failure is logged and counted, never raised - the FIX reader must not stall

  RD-->>WS: pubsub message on quotes:EURUSD
  WS->>C: send_text(payload) type quote, bid, ask, timestamp

  alt no message within ws_heartbeat_seconds
    WS->>C: {type: heartbeat}
  end

  C->>WS: disconnect
  WS->>RD: close() pubsub.aclose()
  Note over C,RD: GET /prices/{symbol} reads the same HSET instead, so a late joiner still gets the last quote
```
