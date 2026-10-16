"""Environment-sourced settings, composed from per-concern mixins."""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from app.core.security import redact


class AppSettingsMixin(BaseSettings):
    """App identity and logging fields."""

    model_config = SettingsConfigDict(extra="ignore")

    app_env: str = Field(default="dev", alias="APP_ENV")
    app_name: str = Field(default="ctrader-fix-gateway", alias="APP_NAME")
    app_log_level: str = Field(default="INFO", alias="APP_LOG_LEVEL")
    sentry_dsn: str | None = Field(default=None, alias="SENTRY_DSN")
    sentry_traces_sample_rate: float = Field(default=0.0, alias="SENTRY_TRACES_SAMPLE_RATE")
    metrics_port: int = Field(default=9100, alias="METRICS_PORT")
    otel_exporter_otlp_endpoint: str | None = Field(default=None, alias="OTEL_EXPORTER_OTLP_ENDPOINT")


class DatabaseSettingsMixin(BaseSettings):
    """The Postgres connection string."""

    model_config = SettingsConfigDict(extra="ignore")

    database_url: str = Field(alias="DATABASE_URL")


class RedisSettingsMixin(BaseSettings):
    """The Redis connection string."""

    model_config = SettingsConfigDict(extra="ignore")

    redis_url: str = Field(alias="REDIS_URL")


class RabbitMQSettingsMixin(BaseSettings):
    """The RabbitMQ connection string."""

    model_config = SettingsConfigDict(extra="ignore")

    rabbitmq_url: str = Field(alias="RABBITMQ_URL")


class Settings(AppSettingsMixin, DatabaseSettingsMixin, RedisSettingsMixin, RabbitMQSettingsMixin):
    """Settings for the `api` process. Fails fast if a required value is missing."""

    model_config = SettingsConfigDict(env_file=None, extra="ignore")

    price_stale_ms: int = Field(default=5000, alias="PRICE_STALE_MS")
    ws_max_symbols: int = Field(default=50, alias="WS_MAX_SYMBOLS")
    ws_heartbeat_seconds: float = Field(default=15.0, alias="WS_HEARTBEAT_SECONDS")
    rate_limit_orders_per_second: float = Field(default=10.0, alias="RATE_LIMIT_ORDERS_PER_SECOND")
    rate_limit_orders_burst: float = Field(default=20.0, alias="RATE_LIMIT_ORDERS_BURST")
    api_key_pepper: str = Field(alias="API_KEY_PEPPER")

    def __repr__(self) -> str:
        """Render with secrets redacted."""
        fields = ", ".join(f"{name}={redact(name, value)!r}" for name, value in self.__dict__.items())
        return f"Settings({fields})"


class RelaySettings(AppSettingsMixin, DatabaseSettingsMixin, RabbitMQSettingsMixin):
    """Settings for the `relay` process."""

    model_config = SettingsConfigDict(env_file=None, extra="ignore")

    outbox_poll_interval_seconds: float = Field(default=0.1, alias="OUTBOX_POLL_INTERVAL_SECONDS")
    outbox_max_attempts: int = Field(default=10, alias="OUTBOX_MAX_ATTEMPTS")

    def __repr__(self) -> str:
        """Render with secrets redacted."""
        fields = ", ".join(f"{name}={redact(name, value)!r}" for name, value in self.__dict__.items())
        return f"RelaySettings({fields})"


class ProjectorSettings(RelaySettings):
    """Settings for the `projector` process."""

    model_config = SettingsConfigDict(env_file=None, extra="ignore")

    symbols_overlay_path: str | None = Field(default=None, alias="SYMBOLS_OVERLAY_PATH")

    def __repr__(self) -> str:
        """Render with secrets redacted."""
        fields = ", ".join(f"{name}={redact(name, value)!r}" for name, value in self.__dict__.items())
        return f"ProjectorSettings({fields})"


class FixSessionSettingsMixin(BaseSettings):
    """One FIX session's connection and credentials, read from env vars."""

    model_config = SettingsConfigDict(extra="ignore")

    fix_trade_host: str = Field(alias="FIX_TRADE_HOST")
    fix_trade_port: int = Field(alias="FIX_TRADE_PORT")
    fix_trade_ssl: bool = Field(default=True, alias="FIX_TRADE_SSL")
    fix_trade_username: str = Field(alias="FIX_TRADE_USERNAME")
    fix_trade_password: str = Field(alias="FIX_TRADE_PASSWORD")
    fix_trade_sender_comp_id: str = Field(alias="FIX_TRADE_SENDER_COMP_ID")
    fix_trade_sender_sub_id: str = Field(default="TRADE", alias="FIX_TRADE_SENDER_SUB_ID")
    fix_trade_target_comp_id: str = Field(default="cServer", alias="FIX_TRADE_TARGET_COMP_ID")
    fix_trade_target_sub_id: str = Field(default="TRADE", alias="FIX_TRADE_TARGET_SUB_ID")
    fix_trade_heartbeat: int = Field(default=30, alias="FIX_TRADE_HEARTBEAT")
    fix_quote_host: str = Field(alias="FIX_QUOTE_HOST")
    fix_quote_port: int = Field(alias="FIX_QUOTE_PORT")
    fix_quote_ssl: bool = Field(default=True, alias="FIX_QUOTE_SSL")
    fix_quote_username: str = Field(alias="FIX_QUOTE_USERNAME")
    fix_quote_password: str = Field(alias="FIX_QUOTE_PASSWORD")
    fix_quote_sender_comp_id: str = Field(alias="FIX_QUOTE_SENDER_COMP_ID")
    fix_quote_sender_sub_id: str = Field(default="QUOTE", alias="FIX_QUOTE_SENDER_SUB_ID")
    fix_quote_target_comp_id: str = Field(default="cServer", alias="FIX_QUOTE_TARGET_COMP_ID")
    fix_quote_target_sub_id: str = Field(default="QUOTE", alias="FIX_QUOTE_TARGET_SUB_ID")
    fix_quote_heartbeat: int = Field(default=30, alias="FIX_QUOTE_HEARTBEAT")
    fix_begin_string: str = Field(default="FIX.4.4", alias="FIX_BEGIN_STRING")


class FixWorkerSettings(
    AppSettingsMixin, DatabaseSettingsMixin, RabbitMQSettingsMixin, RedisSettingsMixin, FixSessionSettingsMixin
):
    """Settings for the `fix-worker` process."""

    model_config = SettingsConfigDict(env_file=None, extra="ignore")

    account_id: str = Field(alias="ACCOUNT_ID")
    fix_reconnect_max_delay: float = Field(default=60.0, alias="FIX_RECONNECT_MAX_DELAY")
    graceful_timeout: float = Field(default=20.0, alias="GRACEFUL_TIMEOUT")
    price_ttl_seconds: int = Field(default=60, alias="PRICE_TTL_SECONDS")
    redis_max_connections: int = Field(default=200, alias="REDIS_MAX_CONNECTIONS")
    quote_subscription_batch_size: int = Field(default=20, alias="QUOTE_SUBSCRIPTION_BATCH_SIZE")
    quote_subscription_batch_delay_seconds: float = Field(default=0.1, alias="QUOTE_SUBSCRIPTION_BATCH_DELAY_SECONDS")

    def __repr__(self) -> str:
        """Render with secrets redacted."""
        fields = ", ".join(f"{name}={redact(name, value)!r}" for name, value in self.__dict__.items())
        return f"FixWorkerSettings({fields})"

    def trade_fix_config(self) -> dict[str, str | int | bool]:
        """Render as the plain dict ctrader_fix message classes."""
        return {
            "Host": self.fix_trade_host,
            "Port": self.fix_trade_port,
            "SSL": self.fix_trade_ssl,
            "Username": self.fix_trade_username,
            "Password": self.fix_trade_password,
            "BeginString": self.fix_begin_string,
            "SenderCompID": self.fix_trade_sender_comp_id,
            "SenderSubID": self.fix_trade_sender_sub_id,
            "TargetCompID": self.fix_trade_target_comp_id,
            "TargetSubID": self.fix_trade_target_sub_id,
            "HeartBeat": str(self.fix_trade_heartbeat),
        }

    def quote_fix_config(self) -> dict[str, str | int | bool]:
        """Render as the plain dict ctrader_fix message classes."""
        return {
            "Host": self.fix_quote_host,
            "Port": self.fix_quote_port,
            "SSL": self.fix_quote_ssl,
            "Username": self.fix_quote_username,
            "Password": self.fix_quote_password,
            "BeginString": self.fix_begin_string,
            "SenderCompID": self.fix_quote_sender_comp_id,
            "SenderSubID": self.fix_quote_sender_sub_id,
            "TargetCompID": self.fix_quote_target_comp_id,
            "TargetSubID": self.fix_quote_target_sub_id,
            "HeartBeat": str(self.fix_quote_heartbeat),
        }
