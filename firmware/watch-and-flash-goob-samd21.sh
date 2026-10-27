#!/usr/bin/env bash
set -Eeuo pipefail

# Wait for the XIAO SAMD21 UF2 boot drive, then copy a prebuilt UF2 image to it.
# Run on Goob as root. Example:
#   sudo ./watch-and-flash-goob-samd21.sh /tmp/pebble-goob-samd21.uf2

if [[ $EUID -ne 0 ]]; then
  exec sudo -- "$0" "$@"
fi

UF2_FILE="${1:-/tmp/pebble-goob-samd21.uf2}"
VOLUME="/dev/disk/by-label/Arduino"
MOUNT_DIR="/mnt/pebble-samd21-uf2"
CONTROL_SERVICE="pebble-control.service"

if [[ ! -r "$UF2_FILE" ]]; then
  echo "UF2 image is not readable: $UF2_FILE" >&2
  exit 1
fi

restart_control=0
if systemctl is-active --quiet "$CONTROL_SERVICE"; then
  restart_control=1
  systemctl stop "$CONTROL_SERVICE"
fi

cleanup() {
  mountpoint -q "$MOUNT_DIR" && umount "$MOUNT_DIR" || true
  if (( restart_control )); then
    systemctl start "$CONTROL_SERVICE" || true
  fi
}
trap cleanup EXIT INT TERM

mkdir -p "$MOUNT_DIR"
echo "Waiting for XIAO UF2 drive at $VOLUME ..."
while [[ ! -b "$VOLUME" ]]; do
  sleep 0.1
done

udevadm settle --timeout=2 || true
echo "Found $(readlink -f "$VOLUME"); mounting UF2 drive ..."
mount -t vfat -o rw,sync "$VOLUME" "$MOUNT_DIR"

dest="$MOUNT_DIR/GOOB_SAMD21.UF2"
echo "Copying $UF2_FILE to the XIAO boot drive ..."
cp -- "$UF2_FILE" "$dest"
echo "UF2 copy completed: $dest"
echo "The XIAO should reboot when it accepts the image."
