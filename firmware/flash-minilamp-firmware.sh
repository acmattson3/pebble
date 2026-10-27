#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TARGET_DIR="$SCRIPT_DIR/MINILAMP_seeed-xiao-c6"

PORT="${PORT:-/dev/ttyACM0}"
FQBN="${FQBN:-esp32:esp32:XIAO_ESP32C6}"

python3 "$TARGET_DIR/generate_manifest.py"
arduino-cli compile --fqbn "$FQBN" "$TARGET_DIR"
arduino-cli upload -p "$PORT" --fqbn "$FQBN" "$TARGET_DIR"
