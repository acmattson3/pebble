# Video Streaming

Pebble supports two MQTT camera formats: the original PBVD JPEG delta stream
and the newer PBVH H.264 stream. Goob is configured to use H.264. The shared
camera module defaults to PBVD for compatibility; the robot's publisher config
and advertised module codec must agree. See [H.264 setup and live observations](../modules/H264.md)
for configuration, wire framing, recovery behavior, and benchmark results.

## Data path

1. The AV daemon opens the physical camera only while video capture is needed
   and exports BGR frames over shared memory.
2. The camera publisher consumes that local stream and encodes PBVD or H.264.
3. Encoded frames are published non-retained at QoS 0 on the robot's advertised
   `o/video` topic.
4. The web gateway decodes frames; its current HTTP endpoint serves JPEG to the
   browser. H.264 is currently used on robot-to-MQTT-to-gateway, not natively in
   the browser.

Demand is requested through the camera module's View/Stop controls. Requests
are renewable leases, so publisher and capture processes stop after the last
consumer releases its request or its lease expires. Older
`incoming/flags/mqtt-video` and `incoming/front-camera` controls remain
transitional compatibility paths.

## Configuration and diagnostics

Runtime settings are in `control/configs/config.json`:

- `services.av_daemon.video` selects the camera backend, device, and capture
  settings.
- `services.mqtt_bridge.media.video_publisher` configures publisher codec,
  bitrate/quality, output dimensions, and frame rate.
- `services.module_host.modules` camera instance `formats.stream.codec` tells
  the web interface which decoder to use.

To run the publisher manually:

```bash
python3 video/camera_publisher.py --config control/configs/config.json --profile default
```

The local preview and bitrate monitor are still available:

```bash
python3 video/video_receiver_test.py --config control/configs/config.json --profile default
python3 video/video_bitrate_monitor.py --config control/configs/config.json --profile default --interval 1.0
```

OpenCV, NumPy, Paho MQTT, and GStreamer are used by the current paths. H.264
encoding/decoding uses PyAV/libav; Goob currently uses software x264. No
hardware encoder was configured. Check [the benchmark notes](../modules/H264.md)
before selecting settings for another robot: camera capture and encoding load
depend on hardware, resolution, and scene.

## Troubleshooting

- If the publisher reports an unavailable source, confirm the AV daemon is
  running and that shared socket paths match.
- If no MQTT frames arrive, confirm a live camera lease, publisher process,
  remote broker settings, and the robot's `pebble/robots/<id>` identity.
- If H.264 frames fail to decode, confirm the manifest advertises `h264` and
  that the publisher and gateway have PyAV/libav available.
- If CPU usage is high, reduce publish dimensions or frame rate; local capture
  settings can remain higher for autonomy consumers.
