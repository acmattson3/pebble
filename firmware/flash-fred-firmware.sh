#!/usr/bin/env bash
set -euo pipefail

PORT="${PORT:-/dev/ttyACM0}"
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
SKETCH="${SKETCH:-${SCRIPT_DIR}/FRED_seeed-xiao-samd21}"
# Default FQBN targets Seeed XIAO SAMD21; override for different FRED-compatible boards.
FQBN="${FQBN:-Seeeduino:samd:seeed_XIAO_m0}"

echo "Reminder: Do NOT run with sudo!"
echo "Compiling ${SKETCH} for ${FQBN}..."
arduino-cli compile --fqbn "${FQBN}" "${SKETCH}"
echo "Compiled! Flashing ${PORT}..."
arduino-cli upload -p "${PORT}" --fqbn "${FQBN}" "${SKETCH}"
