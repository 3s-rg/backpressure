check: fmt-check lint test

fmt-check:
    cd python && uv run ruff format --check .

lint:
    cd python && uv run ruff check .
    cd python && uv run basedpyright

fmt:
    cd python && uv run ruff format .
    cd python && uv run ruff check --fix .

test: test-unit test-temporal

test-unit:
    cd python/core && uv run pytest -v -m "not integration"

test-temporal:
    cd python/temporal && uv run pytest -v -m "not integration"

test-integration:
    cd python/core && uv run pytest -v -m integration
    cd python/temporal && uv run pytest -v -m integration

test-go:
    cd go && go test ./...

up:
    docker compose up -d --wait

down:
    docker compose down
