FROM ghcr.io/astral-sh/uv:0.11.30-python3.12-trixie-slim

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PATH="/app/.venv/bin:${PATH}"

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY src ./src
RUN uv sync --frozen --no-dev

RUN groupadd --gid 10001 platform && \
    useradd --uid 10001 --gid 10001 --create-home platform

USER 10001:10001

CMD ["platform-integration", "serve", "--host", "0.0.0.0", "--port", "8080"]
