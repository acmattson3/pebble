# Control Stack

`control/` hosts a local-first multi-process runtime.

## Services

- `launcher.py`
  - Starts and supervises enabled control services.
  - Publishes retained `{system}/{type}/{id}/outgoing/capabilities` from `config.json`.
  - Publishes structured runtime logs to `{system}/{type}/{id}/outgoing/logs`.
  - Republish interval is configurable via `services.launcher.capabilities.publish_interval_seconds` (default is inherited from `services.defaults.retained_publish_interval_seconds`, normally `3600`).
- `services/av_daemon.py`
  - Owns physical camera/microphone devices with GStreamer (`gst-launch-1.0`).
  - Exposes shared local streams over `shmsink` for other local processes.
- `services/mqtt_bridge.py`
  - Bridges local MQTT and remote MQTT.
  - Publishes remote `online` heartbeat.
  - Handles MQTT media subprocess controls (audio/video) behind flags.
  - MQTT media scripts consume AV-daemon shared streams and do not access hardware directly.
- `services/ros1_bridge.py`
  - Bridges local MQTT `incoming/drive-values` into a ROS 1 `cmd_vel` publisher.
  - Re-publishes the last commanded `cmd_vel` at a configurable ROS-side cadence so stacks like Jackal `twist_mux` keep receiving a live velocity stream between MQTT updates.
  - Can publish low-bandwidth ROS telemetry back into local MQTT, including wheel-odometry, localization pose, navigation state, diagnostics, and battery voltage.
  - Can publish local `outgoing/online` heartbeat for ROS-backed robots that do not use the MCU bridge path, though normal dual-bridge deployments should leave heartbeat ownership in `mqtt_bridge`.
  - Applies the same Pebble drive topic location and watchdog semantics to a ROS 1 stack.
- `services/serial_mcu_bridge.py`
  - Bridges local MQTT command topics with MCU serial commands.
  - Keeps high-rate touch telemetry (`a0/a1/a2`) local by default (`services.serial_mcu_bridge.telemetry.publish_touch_sensors=false`).
  - Mirrors raw touch telemetry into local shared memory for wheel odometry consumers.
  - Publishes charging state as retained MQTT status with periodic re-announce.
- `services/soundboard_handler.py`
  - Handles soundboard playback plus retained files/status topics.
  - Periodically re-announces retained soundboard topics to recover from retained-state loss.
- `services/autonomy_manager.py`
  - Scans `autonomy/` for runnable script directories (`run.py`).
  - Publishes retained autonomy script list/status topics.
  - Accepts start/stop commands and runs one script at a time.
  - Starting a new script auto-stops any currently running script.
  - No child auto-restart on crash; exit/error is reflected in retained status.
- `services/serial_standard.md`
  - Compact self-describing serial MCU standard for `pebble_serial_v1`.
  - Defines discovery, framing, and dynamic `local_only` behavior for serial MCU telemetry.

## Runtime Config

Runtime config is a single local file:

- `control/configs/config.json`

Do not commit this file; it is gitignored.

Create it by copying the template:

```bash
cp control/configs/config.json.example control/configs/config.json
```

Then edit as needed.

Reference schema:

- `control/configs/config.json.example`
- Shared retained-topic default interval:
  - `services.defaults.retained_publish_interval_seconds` (default `3600`).
- ROS 1 drive bridge config:
  - `services.ros1_bridge.topics.drive_values`
  - `services.ros1_bridge.topics.wheel_odometry|charging_status|charging_level|localization_pose|navigation_status|navigation_goal|navigation_local_plan|navigation_global_plan|diagnostics`
  - `services.ros1_bridge.heartbeat.*`
  - `services.ros1_bridge.ros.cmd_vel_topic|cmd_vel_publish_interval_seconds|odometry_topic|navigation_*_topic|diagnostics_topic`
  - `services.ros1_bridge.telemetry.*`
  - `services.ros1_bridge.motion.max_linear_speed_mps|max_angular_speed_radps`
  - `services.ros1_bridge.safety.*`
- Autonomy manager config:
  - `services.autonomy_manager.topics.command|files|status`
  - `services.autonomy_manager.scripts.<name>.args[]` (web-editable runtime fields mapped to script CLI args)
## Topic Format

This runtime uses the topic format:

- `{system}/{type}/{id}/{incoming|outgoing}/{metric}`

No `/components/` segment.

## Mirror + Media Flags

Flags are standard incoming topics under `incoming/flags/`:

- `.../incoming/flags/remote-mirror`
- `.../incoming/flags/mqtt-audio`
- `.../incoming/flags/mqtt-video`
- `.../incoming/flags/reboot`
- `.../incoming/flags/git-pull`

Payload:

```json
{"value": true}
```

Flag semantics:

- Missing flag value is treated as `false`.
- If `remote-mirror=false`, only local `incoming/flags/#` is mirrored to remote.
- If `remote-mirror=true`, full local `incoming/#` is mirrored to remote.
- `incoming/audio-stream` is not bridged remote->local; MQTT audio
  scripts connect directly to the remote broker.
- Remote `outgoing/#` is never bridged into local MQTT (local `outgoing/#` is
  forwarded to remote; remote `incoming/#` is forwarded to local).
- If `mqtt-audio=false`, running MQTT audio subprocesses are stopped.
- If `mqtt-video=false`, running MQTT video subprocess is stopped.
- `incoming/flags/reboot=true` triggers a one-shot reboot command on-robot.
  - Reboot requests are expected to be non-retained.
  - Retained reboot requests are ignored by default (`services.mqtt_bridge.reboot_control.ignore_retained=true`).
- `incoming/flags/git-pull=true` triggers a one-shot git pull command on-robot.
  - Git-pull requests are expected to be non-retained.
  - Retained git-pull requests are ignored by default (`services.mqtt_bridge.git_pull_control.ignore_retained=true`).
- The web UI `audio/start` and `video/start` endpoints publish
  `mqtt-audio=true` / `mqtt-video=true` (retained) before sending
  control commands.
- Media control topic payloads (`incoming/audio`, `incoming/front-camera`)
  must contain a boolean (`true`/`false`) or a JSON object with
  boolean `value`/`enabled` keys.

Reboot control config (in `services.mqtt_bridge`):

- `topics.reboot`
  - Topic for reboot flag input (default `.../incoming/flags/reboot`).
- `reboot_control.command`
  - Command list/string to execute when a reboot request is received.
- `reboot_control.cooldown_seconds`
  - Minimum seconds between accepted reboot requests (default `30`).
- `reboot_control.ignore_retained`
  - Ignore retained reboot payloads (default `true`).
- `topics.git_pull`
  - Topic for git-pull flag input (default `.../incoming/flags/git-pull`).
- `git_pull_control.command`
  - Command list/string to execute when a git-pull request is received.
- `git_pull_control.cooldown_seconds`
  - Minimum seconds between accepted git-pull requests (default `60`).
- `git_pull_control.ignore_retained`
  - Ignore retained git-pull payloads (default `true`).

## Serial Safety

`serial_mcu_bridge` supports command safety controls:

- It can run either as one legacy service config or as multiple named instances under `services.serial_mcu_bridge.instances.<name>`.
- Each instance selects a `protocol`.
- Current built-in protocols are:
  - `goob_base_v1` for the existing drive/lights/touch/charging MCU contract
  - `imu_mpu6050_v1` for the Nano USB-serial MPU-6050 bridge that publishes IMU MQTT topics directly
  - `pebble_serial_v1` for self-describing MCUs that expose compact discovery plus compact runtime packets

- `services.serial_mcu_bridge.safety.drive_timeout_seconds`
  - If no new drive command arrives within this window, send `M 0 0`.
- `services.serial_mcu_bridge.safety.ignore_retained_drive`
  - Ignore retained drive payloads to prevent stale command replay.
- `services.serial_mcu_bridge.safety.stop_on_shutdown`
  - Send a final `M 0 0` when service stops.

`serial_mcu_bridge` telemetry controls:

- `services.serial_mcu_bridge.telemetry.publish_touch_sensors`
  - Default `false`. When `false`, `a0/a1/a2` are parsed and cached locally but not published to MQTT.
- `services.serial_mcu_bridge.retained_publish_interval_seconds`
  - Interval used to re-publish retained charging status (default inherits `services.defaults.retained_publish_interval_seconds`, normally `3600`).
  - Charging status is always published immediately when it changes.
- `services.serial_mcu_bridge.instances.<name>.topics.high_rate|low_rate`
  - IMU protocol topics for `imu_mpu6050_v1` instances.
- `services.serial_mcu_bridge.instances.<name>.publish.high_rate_qos|low_rate_qos|high_rate_retain|low_rate_retain`
  - IMU protocol publish controls for `imu_mpu6050_v1` instances.
- `services.serial_mcu_bridge.odometry_shm.enabled`
  - Default `true`. Enables writing raw touch samples to shared memory.
- `services.serial_mcu_bridge.odometry_shm.name`
  - Shared-memory segment name (default `{system}_{type}_{id}_odometry_raw`).
- `services.serial_mcu_bridge.odometry_shm.slots`
  - Ring-buffer slot count (default `2048`).

Goob currently uses this split:

- root `serial_mcu_bridge`
  - `goob_base_v1` on the XIAO SAMD21 for drive, lights, touch, and charging telemetry
- `serial_mcu_bridge.instances.imu`
  - `imu_mpu6050_v1` on a USB Arduino Nano + GY-521 (`CH340` serial adapter)
  - publishes `{base}/outgoing/sensors/imu-fast` locally and `{base}/outgoing/sensors/imu` for mirrored consumers

The wire-level standard for future serial MCUs is documented in
`control/services/serial_standard.md`.

For `pebble_serial_v1`, `mqtt_bridge` also learns dynamic `local_only` topics
from retained discovery payloads and keeps those topics on the local broker
instead of mirroring them remotely.

## Launch

Run directly from repo root:

```bash
python3 control/launcher.py --config control/configs/config.json
```

## Autonomy Topics

- Incoming command topic:
  - `{system}/{type}/{id}/incoming/autonomy-command`
  - payload examples:
    - `{"action":"start","file":"apriltag-follow","config":{"tag_id":13,"tag_size_m":0.25}}`
    - `{"action":"stop"}`
- Outgoing retained files topic:
  - `{system}/{type}/{id}/outgoing/autonomy-files`
  - payload includes script descriptors with config field schema.
- Outgoing retained status topic:
  - `{system}/{type}/{id}/outgoing/autonomy-status`
  - payload includes `running`, `file`, `pid`, `error`, and effective `config`.

## AV Daemon Notes

- `services.av_daemon.video.backend` supports:
  - `v4l2` (USB/UVC cameras via `/dev/video*`)
  - `libcamera` (Pi/libcamera stack)
  - `arducam` / `picamera2` (aliases that map to `libcamera`)
- `services.av_daemon.video.capture_format` controls V4L2 capture caps:
  - `auto`/`raw` (default raw capture path)
  - `mjpeg` (decode MJPEG with `jpegparse ! jpegdec`)
  - raw format hint (for example `yuyv`, `nv12`)
- `services.av_daemon.video.io_mode` can be used to control V4L2 buffer mode:
  - `auto` (default), `rw`, `mmap`, `userptr`, `dmabuf`, `dmabuf-import`
  - If you see V4L2 allocation failures, try `io_mode: "rw"`.
- For webcams like Logitech C922 where high fps/resolutions are exposed as MJPEG,
  set `capture_format` to `mjpeg`.
- Prefer stable V4L2 device paths from `/dev/v4l/by-id/` over `/dev/videoN`.
- Other local processes should consume shared streams, not open camera/mic devices directly.
- To reduce MQTT video CPU/bandwidth without changing AV daemon capture,
  set `services.mqtt_bridge.media.video_publisher.publish_width`,
  `publish_height`, and `publish_fps`.
- For `libcamera` backend, target systems need GStreamer libcamera support installed
  (for example package `gstreamer1.0-libcamera` on Debian/Raspberry Pi OS).
- Verify libcamera plugin availability with:
  - `gst-inspect-1.0 libcamerasrc`
- If your camera enumerates as `/dev/video*` with V4L2 formats, you can use
  `backend: "v4l2"` instead of libcamera.
- `services.av_daemon.video.camera_controls` can set libcamera controls for
  sensors like Arducam (for example `{ "AwbEnable": 1, "AeEnable": 1 }`).
  Legacy `AwbEnable`/`AeEnable` keys are mapped to `libcamerasrc`
  properties (`awb-enable`/`ae-enable`).
- For AV daemon dependencies on Debian/Raspberry Pi style systems, ensure
  `gstreamer1.0-alsa` (for `alsasrc`) and `gstreamer1.0-plugins-bad`
  (for `jpegparse`) are installed.
- OpenCV consumers (`cv2.CAP_GSTREAMER`) require an OpenCV build with GStreamer enabled.

## systemd (single process)

Use the repository example unit:

- `systemd/pebble-control.service.example`

The example unit intentionally includes:

- `PAMName=login`
- `Environment=XDG_RUNTIME_DIR=/run/user/%U`
- `Environment=DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/%U/bus`

Keep those lines when deploying AV-enabled robots. They provide the user
session context needed for the AV-daemon shared-memory audio path to stay
visible to `shmsrc` consumers under systemd.

Install flow is documented in the root `README.md` under "Setup (Example Linux Robot)".
