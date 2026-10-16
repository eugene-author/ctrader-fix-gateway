import pytest
from pydantic import ValidationError

from app.core.config import FixWorkerSettings


def _env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://u:p@localhost/gateway")
    monkeypatch.setenv("RABBITMQ_URL", "amqp://guest:guest@localhost/")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv("ACCOUNT_ID", "0198c1c2-0000-7000-8000-000000000000")
    monkeypatch.setenv("FIX_TRADE_HOST", "demo-uk-eqx-01.p.c-trader.com")
    monkeypatch.setenv("FIX_TRADE_PORT", "5202")
    monkeypatch.setenv("FIX_TRADE_USERNAME", "1234567")
    monkeypatch.setenv("FIX_TRADE_PASSWORD", "hunter2")
    monkeypatch.setenv("FIX_TRADE_SENDER_COMP_ID", "demo.broker.1234567")
    monkeypatch.setenv("FIX_QUOTE_HOST", "demo-uk-eqx-01.p.c-trader.com")
    monkeypatch.setenv("FIX_QUOTE_PORT", "5201")
    monkeypatch.setenv("FIX_QUOTE_USERNAME", "1234567")
    monkeypatch.setenv("FIX_QUOTE_PASSWORD", "hunter2")
    monkeypatch.setenv("FIX_QUOTE_SENDER_COMP_ID", "demo.broker.1234567")


def test_fix_worker_settings_loads_required_values_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    _env(monkeypatch)

    settings = FixWorkerSettings()

    assert settings.fix_trade_host == "demo-uk-eqx-01.p.c-trader.com"
    assert settings.fix_trade_port == 5202
    assert settings.fix_trade_username == "1234567"
    assert settings.fix_trade_password == "hunter2"
    assert settings.fix_trade_sender_comp_id == "demo.broker.1234567"


def test_fix_worker_settings_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    _env(monkeypatch)

    settings = FixWorkerSettings()

    assert settings.fix_trade_ssl is True  # FIX TLS is on by default
    assert settings.fix_trade_sender_sub_id == "TRADE"
    assert settings.fix_trade_target_comp_id == "cServer"
    assert settings.fix_trade_target_sub_id == "TRADE"
    assert settings.fix_trade_heartbeat == 30
    assert settings.fix_quote_ssl is True
    assert settings.fix_quote_sender_sub_id == "QUOTE"
    assert settings.fix_quote_target_comp_id == "cServer"
    assert settings.fix_quote_target_sub_id == "QUOTE"
    assert settings.fix_quote_heartbeat == 30
    assert settings.fix_begin_string == "FIX.4.4"
    assert settings.fix_reconnect_max_delay == 60.0
    assert settings.graceful_timeout == 20.0
    assert settings.redis_max_connections == 200
    assert settings.quote_subscription_batch_size == 20
    assert settings.quote_subscription_batch_delay_seconds == 0.1


def test_fix_worker_settings_fails_fast_when_fix_trade_password_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    _env(monkeypatch)
    monkeypatch.delenv("FIX_TRADE_PASSWORD", raising=False)

    with pytest.raises(ValidationError):
        FixWorkerSettings()


def test_fix_worker_settings_repr_redacts_the_password(monkeypatch: pytest.MonkeyPatch) -> None:
    _env(monkeypatch)

    settings = FixWorkerSettings()

    rendered = repr(settings)
    assert "hunter2" not in rendered
    assert "***REDACTED***" in rendered


def test_trade_fix_config_renders_the_dict_ctrader_fix_expects(monkeypatch: pytest.MonkeyPatch) -> None:
    _env(monkeypatch)

    settings = FixWorkerSettings()
    cfg = settings.trade_fix_config()

    assert cfg == {
        "Host": "demo-uk-eqx-01.p.c-trader.com",
        "Port": 5202,
        "SSL": True,
        "Username": "1234567",
        "Password": "hunter2",
        "BeginString": "FIX.4.4",
        "SenderCompID": "demo.broker.1234567",
        "SenderSubID": "TRADE",
        "TargetCompID": "cServer",
        "TargetSubID": "TRADE",
        "HeartBeat": "30",
    }


def test_quote_fix_config_renders_the_dict_ctrader_fix_expects(monkeypatch: pytest.MonkeyPatch) -> None:
    _env(monkeypatch)

    settings = FixWorkerSettings()
    cfg = settings.quote_fix_config()

    assert cfg == {
        "Host": "demo-uk-eqx-01.p.c-trader.com",
        "Port": 5201,
        "SSL": True,
        "Username": "1234567",
        "Password": "hunter2",
        "BeginString": "FIX.4.4",
        "SenderCompID": "demo.broker.1234567",
        "SenderSubID": "QUOTE",
        "TargetCompID": "cServer",
        "TargetSubID": "QUOTE",
        "HeartBeat": "30",
    }
