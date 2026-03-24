# Audio Streaming

This directory contains robot-side MQTT audio scripts managed by `mqtt_bridge`:
- `audio_publisher.py` (AV-daemon shared microphone stream -> MQTT `outgoing/audio`)
- `audio_receiver.py` (MQTT `incoming/audio-stream` -> robot speaker via `aplay`)

## Unified Config

Both scripts now read settings from the unified runtime config:
- `control/configs/config.json`

Relevant section:
- `services.mqtt_bridge.media.audio_publisher`
- `services.mqtt_bridge.media.audio_receiver`

MQTT connection settings come from:
- `services.mqtt_bridge.remote_mqtt`

Robot identity comes from:
- `robot`

## Packet format

Audio MQTT payloads are binary with this big-endian header:
- Struct: `!4sBBBBHHIQ`
- `magic`: `PBAT`
- `version`: `1`
- `codec`: `1` (`pcm_s16le`)
- `channels`: channel count
- `flags`: reserved (`0`)
- `rate`: sample rate (Hz)
- `frame_samples`: samples per channel in this packet
- `seq`: monotonic packet sequence
- `timestamp_ms`: UNIX epoch milliseconds

Header is followed by interleaved PCM16LE bytes (`frame_samples * channels * 2`).

## Manual run

From repo root:

Publisher:

```bash
python3 audio/audio_publisher.py --config control/configs/config.json
```

Receiver:

```bash
python3 audio/audio_receiver.py --config control/configs/config.json
```

CLI flags can override config values if needed.

## Bridge-managed start/stop (recommended)

`mqtt_bridge` starts/stops these subprocesses based on:
- `.../incoming/flags/mqtt-audio`
- `.../incoming/audio`

The default `services.mqtt_bridge.audio_control` commands in `config.json.example`
already point to unified config mode.

`mqtt_bridge` controls process lifecycle only. `audio_publisher.py` and
`audio_receiver.py` connect directly to the remote broker configured at
`services.mqtt_bridge.remote_mqtt`.

## Dependencies

- `paho-mqtt`
- ALSA tools (`aplay`)
- `gst-launch-1.0` (for publisher shared-stream input)

## Notes

- Speaker playback path remains direct (`aplay`) by design.
- Microphone capture is owned by AV daemon and shared via `shmsrc`.
- Default `concealment_ms` is `100` (0.1s) in unified config examples.
- Receiver logic resets packet-loss state when stream sequence rewinds (for example
  after restart), reducing long stale-buffer recovery delays.
