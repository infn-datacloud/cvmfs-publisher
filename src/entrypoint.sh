#!/bin/sh

echo "[+] Starting Apache2 service..."
apache2ctl -D FOREGROUND &

exec "$@"
