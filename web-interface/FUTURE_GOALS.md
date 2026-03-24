# Future Goals

## Independent Server-Side Data Logger

Current scope:
- `web-interface/tools/mqtt-broker-logger.py`
- Subscribes to broker topics and writes JSONL logs for offline analysis.

Planned next steps:
- Structured topic presets (odometry-only, autonomy-only, media/control, full capture).
- Optional CSV extractors for common telemetry channels.
- Time-based file rotation and retention policy.
- Compression options for long captures.
- Optional upload pipeline for centralized analysis.
- Lightweight web endpoint/UI hooks to start/stop named capture profiles.
