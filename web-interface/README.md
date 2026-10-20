# Pebble web interface

The Flask service provides two pages:

- `/modules` is the robot-advertised interface. The web gateway reads each
  robot's retained `o/manifest`, validates its module definitions, and renders
  shared controls without robot-specific page code.
- `/` is the compatibility interface for existing deployments and services
  that have not moved to module contracts yet.

A robot needs only the Pebble MQTT contract for basic interoperability. The
module host and module manifest are optional; they add discovered controls and
robot-defined views. Shared widgets include telemetry, soundboard, autonomy,
lights, media, and logs. The autonomy form is built from each robot's advertised
scripts and argument descriptors.

## Run and configure

Install dependencies and start the service from the repository root:

```bash
python3 -m pip install -r web-interface/requirements.txt
python3 web-interface/web-control.py
```

The service reads `web-interface/config.json` by default. Copy
`web-interface/config.json.example` for a new deployment, and set `PEBBLE_CONFIG`
to select another file. Robot identity and runtime services belong in
`control/configs/robot.json`; web bind, broker, robot-list, and history settings
belong in the web config. The older unified config is still accepted by the
runtime compatibility loader.

For Docker, mount both config files and expose the configured web port. The
root `Dockerfile` runs the launcher-based service; see the root README for an
example invocation.

## Robot modules

Shared module contracts and widgets are in [`../modules/README.md`](../modules/README.md).
The gateway serves module assets from `/modules/assets/` and module data through
its API. State endpoints expose the latest validated payload. Event endpoints
are kept in bounded in-memory history; the shared log viewer retains at most
100 events per endpoint and is not a durable log archive.

The launcher publishes structured records at compact `o/logs`. It also mirrors
those records to its configured compatibility topic so existing log consumers
continue to work. Robots that use another runtime can publish the same declared
event contract directly.

Camera and audio modules use demand-controlled capture, H.264 video, and Opus
audio over MQTT. See [`../modules/MEDIA.md`](../modules/MEDIA.md) and
[`../modules/H264.md`](../modules/H264.md).

## Long-term MQTT capture

The module log viewer is for recent operator diagnostics. For durable broker
capture and offline analysis, use `tools/mqtt-broker-logger.py`:

```bash
python3 web-interface/tools/mqtt-broker-logger.py \
  --config web-interface/config.json \
  --topic 'pebble/#' \
  --decode-utf8 \
  --parse-json
```

By default, JSONL files are written to `web-interface/logs/`. Repeat `--topic`
to subscribe to multiple topic filters.

The web service can also persist broker traffic to MongoDB when
`mqtt_history.enabled` is configured. This is separate from the robot module
viewer and its in-memory event history.
