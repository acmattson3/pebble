#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TARGET_DIR="$SCRIPT_DIR/BUMPERBOT_wemos"

PORT="${PORT:-/dev/ttyUSB0}"
FQBN="${FQBN:-esp8266:esp8266:d1_mini}"

arduino-cli compile --fqbn "$FQBN" "$TARGET_DIR"
arduino-cli upload -p "$PORT" --fqbn "$FQBN" "$TARGET_DIR"
