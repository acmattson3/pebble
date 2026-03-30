# Web Interface

This directory contains the Pebble web UI service.

## Files

- `web-control.py` - Flask server + MQTT client.
- `requirements.txt` - Python packages for this UI.
- `Dockerfile` - container build for web UI deployment.

## Run

```bash
python3 -m pip install -r requirements.txt
python3 web-control.py
```

By default the UI reads `control/configs/config.json` from the repository root.
Override with:

```bash
PEBBLE_CONFIG=/path/to/config.json python3 web-control.py
```

For Docker usage, provide `/app/control/configs/config.json` (for example with a bind mount).

Example:

```bash
docker build -f web-interface/Dockerfile -t pebble-web .
docker run --rm -p 8080:8080 \
  -v \"$PWD/control/configs/config.json:/app/control/configs/config.json:ro\" \
  pebble-web
```

Compose example:

```yaml
services:
  pebble:
    build:
      context: .
      dockerfile: web-interface/Dockerfile
    restart: unless-stopped
    volumes:
      - ./control/configs/config.json:/app/control/configs/config.json:ro
    ports:
      - "8060:8080"
```

Default URL:

- `http://localhost:8080`

## Server-Side Broker Logger

For long-running telemetry capture directly from the server broker, use:

```bash
python3 web-interface/tools/mqtt-broker-logger.py \
  --config control/configs/config.json \
  --topic 'pebble/#' \
  --decode-utf8 \
  --parse-json
```

By default it writes JSONL files to `web-interface/logs/`.
You can subscribe to multiple topic filters by repeating `--topic`.

See [FUTURE_GOALS.md](./FUTURE_GOALS.md) for planned expansion of this logger.

## MongoDB MQTT History

`web-control.py` can persist MQTT history to MongoDB with a fixed storage cap.
When enabled, it subscribes to `#` and records all broker traffic. The preferred
storage mode is a MongoDB capped collection (oldest records overwritten automatically).
Startup uses the configured `web_interface.mqtt` broker settings automatically when
history logging is enabled.

Example `control/configs/config.json` block:

```json
"web_interface": {
  "mqtt_history": {
    "enabled": true,
    "uri": "mongodb://127.0.0.1:27017",
    "database": "pebble",
    "collection": "mqtt_history",
    "max_bytes": 536870912000
  }
}
```

If the target collection already exists and is not capped, the web interface falls
back to periodic chunked pruning of oldest documents to keep usage under `max_bytes`.
`max_bytes` applies to the configured MongoDB history collection, not total database size.

Payload notes:
- Messages are stored as raw bytes (`payload` field) plus metadata (`topic`, `ts`, `qos`, `retain`, `payload_size`).
- Very large payloads are truncated to 15 MB before insert (`payload_truncated=true`).

## Topic Format

The UI expects the repository topic format:

- `{system}/{type}/{id}/{incoming|outgoing}/{metric}`

No `/components/` segment.

For dynamic capability discovery, robots may publish retained
`{system}/{type}/{id}/outgoing/capabilities` payloads.
When capabilities + heartbeat telemetry are present, static
`web_interface.robots` entries are optional (they can still be used for friendly names/overrides).
Setting `web_interface.robots` to `[]` is supported.
If a robot is discovered without a capabilities payload, the UI defaults to
enabling video/audio/soundboard/autonomy controls (best-effort behavior).

When capability payloads include media `command_topic` / `flag_topic`, the UI
uses those for start/stop controls. Media start actions also set
`incoming/flags/mqtt-video` / `incoming/flags/mqtt-audio` to `true` (retained)
before sending start commands.
Manual control payloads on media command topics should be boolean (`true`/`false`)
or JSON objects with boolean `value`/`enabled` keys.

Reboot integration:
- publishes to:
  - `{system}/{type}/{id}/incoming/flags/reboot`
- payload:
  - `{ "value": true }` with `retain=false` (one-shot request).
- UI behavior:
  - `Reboot Robot` button appears for `type=robots` components.
  - if capabilities advertise `system.reboot.controls=false`, the button is disabled.

Service-restart integration:
- publishes to:
  - `{system}/{type}/{id}/incoming/flags/service-restart`
- payload:
  - `{ "value": true }` with `retain=false` (one-shot request).
- UI behavior:
  - `Restart Service` button appears for `type=robots` components.
  - if capabilities advertise `system.service_restart.controls=false`, the button is disabled.

Autonomy integration:
- subscribes to:
  - `{system}/{type}/{id}/outgoing/autonomy-files` (retained)
  - `{system}/{type}/{id}/outgoing/autonomy-status` (retained)
- publishes to:
  - `{system}/{type}/{id}/incoming/autonomy-command`
- UI behavior:
  - script list is rendered from `autonomy-files`.
  - per-script runtime config fields are rendered dynamically from each script's `configs` schema.
  - one `Start/Stop` control toggles selected script execution.

Video overlay integration (Phase 2):
- subscribes to:
  - `{system}/{type}/{id}/outgoing/video-overlays`
- web route:
  - `GET /robots/<id>/video/overlays`
- payload shape:
  - `{"shapes": [[[x_norm,y_norm], ...], ...], "frame_width": <int>, "frame_height": <int>, "source": <string>}`
- UI behavior:
  - `Overlays` checkbox in the camera card toggles drawing.
  - points are normalized (`0.0..1.0`) and rendered as green polygon outlines over the video stream.
