ARG PYTHON_VERSION=3.12-slim

FROM python:${PYTHON_VERSION} AS builder

RUN pip install uv

WORKDIR /cdf_s3_replicator

COPY pyproject.toml uv.lock README.md ./
COPY cdf_s3_replicator ./cdf_s3_replicator

RUN uv sync --frozen --no-dev

FROM python:${PYTHON_VERSION} AS base

WORKDIR /cdf_s3_replicator

COPY --from=builder /cdf_s3_replicator /cdf_s3_replicator
COPY build/config_remote.yaml /config/config_remote.yaml

ENV PATH="/cdf_s3_replicator/.venv/bin:$PATH"

CMD ["python", "-m", "cdf_s3_replicator", "/config/config_remote.yaml"]