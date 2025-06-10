#!/bin/sh

set -e

echo "[+] Starting Apache2 service..."
service apache2 start

echo "[+] Launching publisher-consumer.py..."
python /app/publisher_consumer.py
