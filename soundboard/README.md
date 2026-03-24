# Soundboard

This directory documents the Pebble soundboard MQTT behavior.

Runtime implementation lives in:

- `control/services/soundboard_handler.py`

## Topics

Topic format follows:

- `{system}/{type}/{id}/{incoming|outgoing}/{metric}`

Per component:

- Command topic:
  - `{system}/{type}/{id}/incoming/soundboard-command`
- Retained file list topic:
  - `{system}/{type}/{id}/outgoing/soundboard-files`
- Retained status topic:
  - `{system}/{type}/{id}/outgoing/soundboard-status`

Defaults are generated from robot identity unless overridden in:

- `services.soundboard_handler.topics.*`

## Command payloads

Accepted command payload forms:

- `{"action":"play","file":"name.wav"}`
- `{"action":"stop"}`
- `{"enabled":true,"file":"name.wav"}` (`true` => play, `false` => stop)
- `{"value":{...}}` wrapper for the same command object
- Bare boolean:
  - `true` => play (file must still be provided elsewhere in object form)
  - `false` => stop

`file` is sanitized and constrained to the configured soundboard directory.

## Outgoing retained payloads

`soundboard-files`:

```json
{
  "files": ["a.wav", "nested/b.wav"],
  "controls": true,
  "timestamp": 1772062593.3888009
}
```

`soundboard-status`:

```json
{
  "playing": false,
  "file": null,
  "error": null,
  "controls": true,
  "timestamp": 1772062593.395611
}
```

## Retained republish behavior

To tolerate retained-state loss on broker restart/cleanup:

- `soundboard-files` and `soundboard-status` are republished at local MQTT reconnect.
- They are also republished periodically.

Periodic interval defaults to:

- `services.defaults.retained_publish_interval_seconds` (default `3600`)

Per-service override:

- `services.soundboard_handler.retained_publish_interval_seconds`

## Config keys

Primary section:

- `services.soundboard_handler`

Important keys:

- `enabled`
- `topics.command`
- `topics.files`
- `topics.status`
- `playback.directory`
- `playback.player_command`
- `playback.scan_interval`
- `playback.stop_timeout`
- `retained_publish_interval_seconds` (optional override)

## Manual checks

Subscribe to file list:

```bash
mosquitto_sub -h 127.0.0.1 -t 'pebble/robots/<component-id>/outgoing/soundboard-files' -v
```

Subscribe to status:

```bash
mosquitto_sub -h 127.0.0.1 -t 'pebble/robots/<component-id>/outgoing/soundboard-status' -v
```

Play a sound:

```bash
mosquitto_pub -h 127.0.0.1 -t 'pebble/robots/<component-id>/incoming/soundboard-command' \
  -m '{"action":"play","file":"sound.wav"}'
```

Stop playback:

```bash
mosquitto_pub -h 127.0.0.1 -t 'pebble/robots/<component-id>/incoming/soundboard-command' \
  -m '{"action":"stop"}'
```
