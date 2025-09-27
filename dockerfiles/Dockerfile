FROM ghcr.io/astral-sh/uv:python3.12-trixie-slim

WORKDIR /app

COPY pyproject.toml .
COPY uv.lock .
RUN uv sync --locked

COPY . .

ENV AWS_REGION=us-west-2

CMD ["uv", "run", "fastapi", "run", "main.py", "--reload", "--port", "80"]