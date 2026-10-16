from app.core.sentry import _scrub_event, init_sentry


def test_init_sentry_is_a_no_op_without_a_dsn() -> None:
    # sentry_sdk.init(dsn=None) is documented as inert
    init_sentry(dsn=None, env="test", traces_sample_rate=0.0)


def test_scrub_event_redacts_credential_looking_extra_fields() -> None:
    event = {"extra": {"database_url": "postgresql://u:p@host/db", "order_id": "abc-123"}}

    scrubbed = _scrub_event(event, {})

    assert scrubbed is not None
    assert scrubbed["extra"]["database_url"] == "***REDACTED***"
    assert scrubbed["extra"]["order_id"] == "abc-123"


def test_scrub_event_redacts_credentials_embedded_in_a_request_url() -> None:
    event = {"request": {"url": "amqp://guest:guest@rabbitmq:5672/", "headers": {"X-Api-Key": "sk_live_secret"}}}

    scrubbed = _scrub_event(event, {})

    assert scrubbed is not None
    assert scrubbed["request"]["url"] == "amqp://***REDACTED***@rabbitmq:5672/"
    assert scrubbed["request"]["headers"]["X-Api-Key"] == "***REDACTED***"


def test_scrub_event_redacts_breadcrumb_data() -> None:
    event = {"breadcrumbs": {"values": [{"data": {"fix_trade_password": "hunter2", "symbol": "EURUSD"}}]}}

    scrubbed = _scrub_event(event, {})

    assert scrubbed is not None
    breadcrumb_data = scrubbed["breadcrumbs"]["values"][0]["data"]
    assert breadcrumb_data["fix_trade_password"] == "***REDACTED***"
    assert breadcrumb_data["symbol"] == "EURUSD"


def test_scrub_event_passes_through_an_event_with_nothing_to_redact() -> None:
    event = {"message": "order submitted"}

    scrubbed = _scrub_event(event, {})

    assert scrubbed == {"message": "order submitted"}
