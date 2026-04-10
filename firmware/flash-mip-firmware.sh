#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TARGET_DIR="$SCRIPT_DIR/MIP_seeed-xiao-esp32s3-sense"

PORT="${PORT:-/dev/ttyACM0}"
FQBN="${FQBN:-esp32:esp32:XIAO_ESP32S3:PSRAM=opi}"

arduino-cli compile --fqbn "$FQBN" "$TARGET_DIR"
arduino-cli upload -p "$PORT" --fqbn "$FQBN" "$TARGET_DIR"
