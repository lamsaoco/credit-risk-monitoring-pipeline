FROM python:3.12-bookworm

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    curl \
    ca-certificates \
    gnupg \
    && install -m 0755 -d /etc/apt/keyrings \
    && curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg \
    && chmod a+r /etc/apt/keyrings/docker.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian bookworm stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null \
    && apt-get update && apt-get install -y docker-ce-cli docker-compose-plugin \
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
