import os

# app.main builds a real Settings() at import time (for uvicorn's `app.main:app`
# entry point), so importing it - even just to reach create_app() - requires
# these to be present in the environment.
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://u:p@localhost/gateway")
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")
os.environ.setdefault("RABBITMQ_URL", "amqp://guest:guest@localhost/")
os.environ.setdefault("API_KEY_PEPPER", "test-pepper")
