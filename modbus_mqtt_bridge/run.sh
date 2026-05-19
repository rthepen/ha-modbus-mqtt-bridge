#!/usr/bin/env bash
set -e

echo "=== Starting Modbus to MQTT Bridge Daemon ==="
python3 bridge.py
