ARG PYTHON_VERSION=3.11
ARG POETRY_VERSION=1.8.3

######################
# Stage 1 – Build
######################
FROM ghcr.io/withlogicco/poetry:${POETRY_VERSION}-python-${PYTHON_VERSION}-slim AS builder

ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /app

# Upgrade pip and install virtualenv
RUN pip install --upgrade pip virtualenv --no-cache-dir

# Create and activate virtual environment
RUN python -m venv /venv
ENV PATH="/venv/bin:$PATH"

# Install Python requirements
COPY ./src/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source code
COPY ./src/certs /app/certs/
COPY ./src/cvmfs_repo_consumers.py /app/
COPY ./src/parameters.json /app/


##########################
# Stage 2 – Production
##########################
FROM python:${PYTHON_VERSION}-slim AS production

ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends zabbix-sender && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Copy virtual environment and application
COPY --from=builder /venv /venv
COPY --from=builder /app /app

# Activate virtualenv
ENV PATH="/venv/bin:$PATH"


