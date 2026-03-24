# Video Streaming

This directory handles robot camera streaming over MQTT.

Current architecture:
- `control/services/av_daemon.py` is the only process that opens the physical camera.
- `video/camera_publisher.py` reads frames from AV-daemon shared memory (`shmsrc`) and publishes to MQTT.
- Test subscribers in this directory read the published MQTT stream.

## Files

- `camera_publisher.py`
  - Publishes outgoing camera frames to MQTT from shared video input.
- `camera_config.py`
  - Reads unified runtime config and derives video profile settings.
- `video_receiver_test.py`
  - Local test subscriber/decoder that displays video from MQTT.
- `video_bitrate_monitor.py`
  - Reports MQTT video bitrate/stats.

## Unified Config

Scripts in this directory use:
- `control/configs/config.json`

Main sections used:
- `robot`
- `services.mqtt_bridge.remote_mqtt`
- `services.av_daemon.video`
- `services.mqtt_bridge.media.video_publisher`

`camera_publisher.py` profile names are derived from
`services.mqtt_bridge.media.video_publisher.profile` (default `default`).

## Data Path

1. AV daemon captures camera and exports BGR frames through shared memory.
2. `camera_publisher.py` consumes that shared stream.
3. Publisher encodes and sends MQTT payloads (keyframe/delta JSON payload).
4. Receivers decode and reconstruct frames.

You can decouple capture and MQTT stream sizes:
- Capture/input shape comes from `video_publisher.width/height/fps` (or AV daemon defaults).
- MQTT output shape/fps comes from `video_publisher.publish_width/publish_height/publish_fps`.
- If `publish_*` is omitted, publisher uses the capture/input shape.

## MQTT Topic Convention

Topic format:
- `{system}/{type}/{id}/outgoing/front-camera`

Default topic is derived from `robot` identity unless overridden in
`services.mqtt_bridge.media.video_publisher.topic`.

## Payload Format

`camera_publisher.py` publishes JSON payload with:
- `id` (int): frame counter
- `keyframe` (bool): `true` for full frame, `false` for delta frame
- `data` (str): base64-encoded zlib-compressed JPEG bytes
- `timestamp` (float): UNIX seconds at publish time

### Delta-frame details

Keyframe path:
- Encode full BGR frame as JPEG.
- Compress with zlib and base64.
- Publish with `keyframe: true`.
- Receiver stores this as reference frame.

Delta path:
- Compute per-pixel delta against previous sent frame:
  - `raw_delta = current.astype(int16) - previous.astype(int16)`
- Shift to unsigned byte domain:
  - `encoded_delta = clip(raw_delta + 128, 0, 255).astype(uint8)`
- JPEG encode `encoded_delta`, then zlib + base64.
- Publish with `keyframe: false`.

Receiver reconstruction:
- Decode payload image to `decoded`.
- If keyframe:
  - `reference = decoded`
- If delta:
  - `raw_delta = decoded.astype(int16) - 128`
  - `reconstructed = clip(reference.astype(int16) + raw_delta, 0, 255).astype(uint8)`
  - `reference = reconstructed`

Behavior notes:
- Delta frames require reference keyframe state.
- Packet loss/out-of-order can cause drift until next keyframe.
- Regular keyframes limit recovery time.

## Manual run

From repo root:

```bash
python3 video/camera_publisher.py \
  --config control/configs/config.json \
  --profile default
```

Optional overrides:
- `--robot-id`, `--topic`
- `--host`, `--port`, `--username`, `--password`
- `--input-shm`, `--input-retry`

## Integration with mqtt_bridge

Default `services.mqtt_bridge.video_control.command` in `config.json.example`:

```json
"command": [
  "python3",
  "video/camera_publisher.py",
  "--config",
  "control/configs/config.json"
]
```

Bridge media controls:
- `.../incoming/flags/mqtt-video`
- `.../incoming/front-camera`

`mqtt_bridge` controls process lifecycle only. `camera_publisher.py` publishes
video directly to the remote broker configured at
`services.mqtt_bridge.remote_mqtt`.

## Test utilities

Receiver preview:

```bash
python3 video/video_receiver_test.py --config control/configs/config.json --profile default
```

Bitrate monitor:

```bash
python3 video/video_bitrate_monitor.py --config control/configs/config.json --profile default --interval 1.0
```

## Dependencies

Python:
- `opencv-contrib-python`
- `numpy`
- `paho-mqtt`

System:
- `gstreamer1.0-tools`
- `gstreamer1.0-plugins-base`
- `gstreamer1.0-plugins-good`

## GStreamer/libcamera caveats

- AV daemon `video.backend=libcamera` requires system libcamera GStreamer integration
  (for example `gstreamer1.0-libcamera` on Debian/Raspberry Pi OS).
- OpenCV consumers require a build with GStreamer enabled (`cv2.CAP_GSTREAMER`).
- Shared stream format assumptions must match (`BGR`, width/height/fps).

## Troubleshooting

If publisher logs "Video source unavailable":
- Ensure AV daemon is running.
- Verify shared socket path matches between AV daemon and video publisher config.
- Verify required GStreamer plugins are installed.

If no frames arrive on MQTT:
- Confirm `camera_publisher.py` process is running.
- Confirm remote broker settings (`services.mqtt_bridge.remote_mqtt`) and topic config.
- Confirm robot identity in `control/configs/config.json` (`robot.system/type/id`).
  If identity remains at a placeholder/default, topic auto-derivation will publish there.

If CPU is high in `camera_publisher.py`:
- Reduce `publish_width`/`publish_height` and/or `publish_fps`.
- Keep AV daemon capture settings independent if local consumers need higher quality.

## Future enhancements

- Optional migration from delta-frame payload to a more efficient encoded transport.
- Structured error publication to `{system}/{type}/{id}/outgoing/errors`.
- Expanded subsystem/integration tests under `tests/`.
