#!/bin/sh

set -e

echo "[+] Starting Apache2 service..."
service apache2 start

echo "[+] Launching cvmfs_repo_sync.py..."
python /app/cvmfs_repo_sync.py
