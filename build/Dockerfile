ARG PYTHON_VERSION=3.12-slim
 
FROM python:${PYTHON_VERSION} AS builder
ENV POETRY_VIRTUALENVS_IN_PROJECT=true
RUN pip install "poetry>=1.8"
RUN mkdir -p /cdf_fabric_replicator
 
COPY pyproject.toml /cdf_fabric_replicator
COPY uv.lock /cdf_fabric_replicator
COPY cdf_fabric_replicator /cdf_fabric_replicator
 
WORKDIR /cdf_fabric_replicator
RUN poetry install --no-root --without dev --sync
 
FROM python:${PYTHON_VERSION} AS base
 
COPY --from=builder /cdf_fabric_replicator/ /cdf_fabric_replicator
COPY build/config_remote.yaml /config/config_remote.yaml
 
ENV PATH="/cdf_fabric_replicator/.venv/bin:$PATH"
CMD ["python", "-m", "cdf_fabric_replicator", "/config/config_remote.yaml"]