ARG PYTHON_VERSION=3.11
ARG POETRY_VERSION=1.8.3


FROM ghcr.io/withlogicco/poetry:${POETRY_VERSION}-python-${PYTHON_VERSION}-slim AS builder

ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /app

COPY ./pyproject.toml ./poetry.lock ./

RUN poetry export -f requirements.txt --output requirements.txt --without-hashes

# Upgrade pip, install virtualenv and the script requirements
RUN pip install --upgrade pip virtualenv --no-cache-dir && \
    python -m venv /venv && \
    /venv/bin/pip install --no-cache-dir -r requirements.txt



FROM python:${PYTHON_VERSION}-slim AS production

ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /app

# Install system dependencies and Zabbix support
RUN apt-get update && \
    apt-get install -y --no-install-recommends zabbix-sender && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Copy virtual environment and app
COPY --from=builder /venv /venv
COPY --from=builder /app /app

# Copy application code
COPY ./src/cvmfs_repo_consumers.py ./

# Activate virtualenv
ENV PATH="/venv/bin:$PATH"

