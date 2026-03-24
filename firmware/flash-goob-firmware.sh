#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TARGET_DIR="$SCRIPT_DIR/GOOB_seeed-xiao-c3"

PORT="${PORT:-/dev/ttyACM0}"
FQBN="${FQBN:-esp32:esp32:XIAO_ESP32C3}"

echo "Reminder: Do NOT run with sudo!"
echo "Compiling ${TARGET_DIR} for ${FQBN}..."
arduino-cli compile --fqbn "$FQBN" "$TARGET_DIR"
echo "Compiled! Flashing ${PORT}..."
arduino-cli upload -p "$PORT" --fqbn "$FQBN" "$TARGET_DIR"
