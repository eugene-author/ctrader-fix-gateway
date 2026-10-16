.PHONY: up down logs test test-unit lint fmt fmt-check typecheck imports migrate

up:
	docker compose up --build -d

down:
	docker compose down

logs:
	docker compose logs -f

test:
	uv run pytest

test-unit:
	uv run pytest tests/unit -v

lint:
	uv run ruff check app tests

fmt:
	uv run ruff check --fix app tests
	uv run black app tests

fmt-check:
	uv run black --check --diff app tests

typecheck:
	uv run mypy app

imports:
	uv run lint-imports

migrate:
	uv run alembic upgrade head
