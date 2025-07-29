ARG PYTHON_VERSION=3.12-slim

FROM python:${PYTHON_VERSION} AS builder

RUN pip install uv

WORKDIR /cdf_fabric_replicator

COPY pyproject.toml uv.lock ./

RUN uv sync --frozen --no-dev --no-install-project

COPY cdf_fabric_replicator ./cdf_fabric_replicator

FROM python:${PYTHON_VERSION} AS base

WORKDIR /cdf_fabric_replicator

COPY --from=builder /cdf_fabric_replicator /cdf_fabric_replicator

COPY build/config_remote.yaml /config/config_remote.yaml

ENV PATH="/cdf_fabric_replicator/.venv/bin:$PATH"

CMD ["python", "-m", "cdf_fabric_replicator", "/config/config_remote.yaml"]