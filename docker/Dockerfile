FROM python:3.12-bookworm

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    curl \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Install uv from official image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

RUN useradd -m -u 50000 -s /bin/bash airflow

WORKDIR /opt/airflow

# Build context is .. (project root) — copy from there
COPY pyproject.toml uv.lock ./

RUN uv sync --frozen --no-dev && \
    mkdir -p dags logs plugins && \
    chown -R airflow: /opt/airflow

ENV VIRTUAL_ENV=/opt/airflow/.venv
ENV PATH="${VIRTUAL_ENV}/bin:${PATH}"
ENV PYSPARK_PYTHON="${VIRTUAL_ENV}/bin/python3.12"
ENV PYSPARK_DRIVER_PYTHON="${VIRTUAL_ENV}/bin/python3.12"
ENV AIRFLOW_HOME=/opt/airflow

USER airflow
