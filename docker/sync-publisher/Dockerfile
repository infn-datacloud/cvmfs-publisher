ARG PYTHON_VERSION=3.11
ARG POETRY_VERSION=1.8.3


FROM ghcr.io/withlogicco/poetry:${POETRY_VERSION}-python-${PYTHON_VERSION}-slim AS builder

ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /app

COPY ./pyproject.toml ./poetry.lock ./

RUN poetry export -f requirements.txt --without-hashes | sed 's/==.*$//' > requirements.txt

# Upgrade pip, install venv and script requirements
RUN pip install --upgrade pip virtualenv --no-cache-dir && \
    python -m venv /venv && \
    /venv/bin/pip install --no-cache-dir -r requirements.txt

# Copy application source code
COPY ./src/certs ./certs/
COPY ./src/cvmfs_repo_sync.py ./
COPY ./src/publisher_consumer.py ./
COPY ./src/parameters.json ./
COPY ./src/entrypoint.sh /usr/bin/entrypoint.sh
RUN chmod +x /usr/bin/entrypoint.sh


FROM python:${PYTHON_VERSION}-slim AS production

ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /app

# Install system dependencies, CVMFS server, Zabbix support
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        wget \
        kmod \
        libcap2-bin \
        apache2 \
        apache2-utils \
        apache2-bin && \
    wget https://ecsft.cern.ch/dist/cvmfs/cvmfs-release/cvmfs-release-latest_all.deb && \
    dpkg -i cvmfs-release-latest_all.deb && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        cvmfs \
        cvmfs-server \
        zabbix-sender && \
    rm -f cvmfs-release-latest_all.deb && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Suppress Apache warnings and set CernVM-FS proxy
RUN echo "ServerName localhost" >> /etc/apache2/apache2.conf && \
    echo "CVMFS_HTTP_PROXY=DIRECT" > /etc/cvmfs/default.local

# Copy virtual environment and application code
COPY --from=builder /venv /venv
COPY --from=builder /app /app
COPY --from=builder /usr/bin/entrypoint.sh /usr/bin/entrypoint.sh

# Activate virtualenv
ENV PATH="/venv/bin:$PATH"

ENTRYPOINT ["/usr/bin/entrypoint.sh"]

