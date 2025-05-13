FROM python:3.12-slim

WORKDIR /app

COPY README.md .
COPY pyproject.toml .
COPY poetry.lock .

RUN pip install poetry && poetry install --no-root

ENV PYTHONPATH=/app
