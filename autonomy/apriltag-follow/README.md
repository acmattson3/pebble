# AprilTag Follow

This directory contains AprilTag utilities that consume shared video from
`av_daemon` (no direct camera-device access in these scripts).

## Scripts

- `run.py`: autonomy-manager entrypoint that forwards to `april-follow-test.py`
  and applies default CLI values from `services.autonomy_manager.scripts.apriltag-follow.args`
  in `control/configs/config.json`.
- `april-follow-test.py`: tracks a marker and publishes drive commands.
- `calibrate_camera_aruco.py`: camera calibration from live stream or image set.
- `distance_test.py`: marker pose/distance reporting.
- `capture_samples.py`: saves sample frames from shared stream.
- `detect_marker_ids.py`: quickly inspect visible marker IDs.
- `live_marker_ids.py`: live shared-stream monitor for currently visible IDs.

## Requirements

- Python 3
- `opencv-contrib-python` (ArUco/AprilTag module)
- `numpy`
- `paho-mqtt` (for `april-follow-test.py`)

Install from this directory:

```bash
python3 -m pip install -r autonomy/apriltag-follow/requirements.txt
```

## Runtime Video Source

Scripts in this directory read AV-daemon shared video via GStreamer `shmsrc`.
Default shared socket:

- `/tmp/pebble-video.sock`

Use `--video-shm`, `--width`, `--height`, and `--fps` to override capture assumptions.

## Config-First Usage

`april-follow-test.py` supports unified runtime config directly:

```bash
python3 autonomy/apriltag-follow/april-follow-test.py \
  --config control/configs/config.json \
  --marker-size 0.25 \
  --calibration autonomy/apriltag-follow/camera_calibration.yaml
```

Notes:
- If `--config` is provided, the script loads AV-daemon shared-video defaults
  from `services.av_daemon.video` in the same runtime config used by the control stack.
- MQTT defaults come from `local_mqtt` (not `services.mqtt_bridge.remote_mqtt`).
- You can still override local MQTT values with CLI flags (`--mqtt-host`, `--mqtt-port`, etc.).

Drive topic:
- `pebble/robots/<robot-id>/incoming/drive-values`

AprilTag telemetry topic (for web overlays/telemetry consumers):
- `pebble/robots/<robot-id>/outgoing/apriltag-data`

Video overlays topic (normalized polygon vertices for web drawing):
- `pebble/robots/<robot-id>/outgoing/video-overlays`

## Autonomy Manager Usage

The control `autonomy_manager` service launches this script via `run.py`.
From the web interface, script start/stop commands flow through:

- `.../incoming/autonomy-command` (`{"action":"start","file":"apriltag-follow","config":{...}}`)

Common web-editable config fields are expected from
`services.autonomy_manager.scripts.apriltag-follow.args`, including:

- `tag_id` (`--marker-id`)
- `tag_size_m` (`--marker-size`)
- `turn_power` (`--turn-power`)
- `search_pause_s` (`--search-pause`)
- `search_reverse_after_s` (`--search-reverse-after`)

Current default behavior targets smoother steering and continuous search:
- `--turn-power 0.5`
- `--search-pause 0.2`
- `--search-reverse-after 0.0` (do not alternate search direction)

## Calibration

Live calibration from shared stream:

```bash
python3 autonomy/apriltag-follow/calibrate_camera_aruco.py \
  --video-shm /tmp/pebble-video.sock \
  --marker-size 0.035 \
  --marker-id 0 \
  --marker-dictionary APRILTAG_25H9 \
  --samples 40 \
  --min-marker-area-fraction 0.01 \
  --preview \
  --output autonomy/apriltag-follow/camera_calibration.yaml
```

Press `space` to capture a sample. Press `q` or `esc` to quit.

Calibrate from a saved image directory:

```bash
python3 autonomy/apriltag-follow/calibrate_camera_aruco.py \
  --marker-size 0.035 \
  --marker-id 0 \
  --marker-dictionary APRILTAG_25H9 \
  --input-dir /path/to/calibration-images \
  --samples 0 \
  --output autonomy/apriltag-follow/camera_calibration.yaml
```

## Distance Test

```bash
python3 autonomy/apriltag-follow/distance_test.py \
  --video-shm /tmp/pebble-video.sock \
  --marker-size 0.25 \
  --marker-id 13 \
  --marker-dictionary APRILTAG_25H9 \
  --calibration autonomy/apriltag-follow/camera_calibration.yaml
```

Expected output:

```text
X: 0.0123	Y: -0.0456	D: 0.5021
```

## Notes

- Keep this directory aligned with config-driven runtime behavior.
- Avoid adding large new CLI-only workflows when a config-backed path is possible.
