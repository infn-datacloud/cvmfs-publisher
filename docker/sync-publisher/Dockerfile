ARG PYTHON_VERSION=3.11
ARG POETRY_VERSION=1.8.3

############################
# Stage 1 – Build
############################
FROM ghcr.io/withlogicco/poetry:${POETRY_VERSION}-python-${PYTHON_VERSION}-slim AS builder

ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /app

# Copy only the files required to generate requirements.txt
COPY ./pyproject.toml ./poetry.lock* ./

# Export requirements without version pinning (for system installation)
RUN poetry export -f requirements.txt --without-hashes -o requirements.txt

# Install pip + venv + dependencies
RUN pip install --upgrade pip virtualenv --no-cache-dir && \
    python -m venv /venv && \
    /venv/bin/pip install --no-cache-dir -r requirements.txt


############################
# Stage 2 – Runtime
############################
FROM python:${PYTHON_VERSION}-slim AS production

ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /app

# Install system and CVMFS dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    kmod \
    libcap2-bin \
    fuse \
    lsb-release \
    apache2 \
    apache2-utils \
    apache2-bin && \
    wget https://ecsft.cern.ch/dist/cvmfs/cvmfs-release/cvmfs-release-latest_all.deb && \
    dpkg -i cvmfs-release-latest_all.deb && \
    apt-get update && apt-get install -y --no-install-recommends \
    cvmfs \
    cvmfs-server \
    zabbix-sender && \
    rm -f cvmfs-release-latest_all.deb && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Apache suppress warning + CVMFS proxy config
RUN echo "ServerName localhost" >> /etc/apache2/apache2.conf && \
    echo "CVMFS_HTTP_PROXY=DIRECT" > /etc/cvmfs/default.local

# Copy venv and application from build stage
COPY --from=builder /venv /venv

# Copy application scripts 
COPY ./src/publisher_consumer.py ./src/cvmfs_repo_sync.py ./src/entrypoint.sh ./
RUN chmod +x ./entrypoint.sh

# Activate virtualenv
ENV PATH="/venv/bin:$PATH"

ENTRYPOINT ["/app/entrypoint.sh"]

