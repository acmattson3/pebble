# Audio Streaming

Pebble's robot audio path is demand-controlled and uses Opus over MQTT in both
directions. The AV daemon owns the physical microphone. The MQTT audio
publisher reads its shared stream; the receiver decodes speaker audio and
plays it with ALSA. The `modules/audio` control module provides Listen, Talk,
and media demand controls. See [camera and audio modules](../modules/MEDIA.md)
for the current architecture and endpoints.

## Robot-side packet format

Audio MQTT payloads are binary with the big-endian header `!4sBBBBHHIQ`:

- `magic`: `PBAT`
- `version`: `1`
- `codec`: `1` for PCM16LE or `2` for Opus
- `channels`, `flags`, `rate`, `frame_samples`, `seq`, `timestamp_ms`

The header is followed by PCM16LE bytes for codec 1 or one Opus packet for
codec 2. The receiver accepts both. Current Goob settings use 16 kHz mono,
20 ms frames, and a 24 kbit/s Opus target. Browser-to-gateway audio currently
uses PCM16LE over HTTP before the gateway encodes Opus for MQTT; native browser
Opus transport remains future work.

## Configuration and manual use

The publisher and receiver read `control/configs/config.json`, especially
`services.mqtt_bridge.remote_mqtt` and the respective
`services.mqtt_bridge.media.audio_publisher` / `audio_receiver` sections.
Media lifecycle is normally managed by the AV daemon when demand mode is
enabled. For manual diagnostics, run from the repository root:

```bash
python3 audio/audio_publisher.py --config control/configs/config.json
python3 audio/audio_receiver.py --config control/configs/config.json
```

The publisher consumes the AV daemon's shared microphone stream and does not
open the microphone device itself. The receiver plays to the configured ALSA
output using `aplay`.

## Dependencies and notes

- Python: `paho-mqtt`; Opus runtime library (`libopus0` on Debian).
- System: `aplay`, GStreamer tools/plugins used by the shared-stream reader.
- AV-enabled systemd deployments need the login session environment shown in
  `systemd/pebble-control.service.example` so `shmsrc` can access the shared
  audio stream.
- Audio queues are bounded. Talk's browser upload queue preserves in-flight
  audio and briefly queues waiting chunks; subjective speaker quality is still
  being evaluated. A short release tail prevents the last speech samples from
  being cut off when Talk stops.
- Full-duplex acoustic echo cancellation is not implemented. It will require
  coordinated render/capture reference audio at both the operator and robot
  ends; see the future-work discussion in the [root README](../README.md).
