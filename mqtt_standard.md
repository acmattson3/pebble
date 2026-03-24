# MQTT Publish/Subscribe Standard

This document is a revised general-purpose MQTT topic and payload standard.

## 1. Scope

This document defines a general MQTT topic and payload convention for
heterogeneous component systems. A component may be a robot, satellite, tower,
fixed station, operator console, person-carried device, or any other uniquely
identified participant in the system.

The key words `MUST`, `MUST NOT`, `REQUIRED`, `SHALL`, `SHALL NOT`, `SHOULD`,
`SHOULD NOT`, `RECOMMENDED`, `MAY`, and `OPTIONAL` in this document are to be
interpreted as described in [RFC 2119](https://datatracker.ietf.org/doc/html/rfc2119).
When those words are not capitalized, they are used in their ordinary English
sense rather than as normative terms.

This standard is intended to be reusable beyond the exact setup implemented in
this repository. To keep that distinction clear:

- unqualified sections describe the general standard
- any section or table explicitly labeled `Example implementation in this
  repository` describes the current codebase only
- example topic names, payloads, and schemas are illustrative, not mandatory
  for future users

The standard topic base is:

```text
{system}/{type}/{id}
```

The default full topic form is:

```text
{system}/{type}/{id}/{incoming|outgoing}/{metric}
```

Any component-specific topic defined by this standard MUST follow that topic
shape. Additional path segments are permitted only inside the `{metric}`
portion.

Examples:

```text
luminsim/robots/astra1/incoming/drive-values
luminsim/satellites/lunanet-sat-1/outgoing/status
luminsim/towers/tower-3/outgoing/power
luminsim/people/operator-1/outgoing/location
```

Important notes:

- `metric` may contain additional path segments, such as `flags/remote-mirror`
  or `sensors/imu`
- not every component publishes every topic family
- future users are free to omit capabilities, video, autonomy, AprilTag, or any
  other optional topic family

## 2. MQTT Version

This topic standard is intended to be usable over either MQTT 3.1.1 or MQTT 5,
provided the implementation preserves the same publish/subscribe semantics,
retained-message behavior, and payload contracts.

### Example implementation in this repository

The current repository uses MQTT 3.1.1. Shared helpers explicitly create clients
with `MQTTv311`, the web interface also explicitly uses `MQTTv311`, and the
current codebase does not depend on MQTT 5-specific properties.

## 3. Topic Model

### 3.1 Component Identity

Each component is identified by:

- `system`: the namespace or deployment name
- `type`: the component class, such as `robots`, `satellites`, `towers`,
  `stations`, `operators`, or `people`
- `id`: a unique identifier within that component type

The tuple `{system}/{type}/{id}` MUST uniquely identify one component within a
deployment.

### 3.2 Direction Semantics

By convention:

- `incoming` means data intended to be consumed by that component
- `outgoing` means data published by that component

This is a logical direction, not necessarily a statement about broker location.
For example, an `incoming` topic may be published on a remote broker and then
bridged locally.

### 3.3 Topic Examples

Examples:

- `{system}/robots/{id}/incoming/drive-values`
- `{system}/robots/{id}/outgoing/online`
- `{system}/satellites/{id}/outgoing/status`
- `{system}/towers/{id}/outgoing/capabilities`
- `{system}/people/{id}/outgoing/location`

### 3.4 Wildcards

Examples:

- `{system}/#` for all live system traffic
- `{system}/+/+/outgoing/online` for all heartbeat topics
- `{system}/robots/+/outgoing/#` for all robot-published telemetry
- `{system}/towers/+/outgoing/status` for status of all towers

## 4. Message Conventions

### 4.1 JSON And Binary Payloads

All non-media topics MUST use JSON payloads. High-rate media topics MAY use
either JSON or binary payloads.

When binary payloads are used, the encoding SHOULD be documented for that metric
family.

### 4.2 Timestamps

This standard allows two common timestamp forms:

- `t`: UNIX epoch milliseconds as an integer
- `timestamp`: UNIX epoch seconds as a float

Implementations SHOULD be internally consistent. Consumers SHOULD tolerate both
forms when interacting with mixed-version components.

### 4.3 QoS Guidance

RECOMMENDED defaults:

- QoS 1 for commands, retained discovery topics, and low-rate telemetry where
  delivery matters
- QoS 0 for high-rate media or telemetry where freshness matters more than
  guaranteed delivery

Consumers SHOULD tolerate either QoS 0 or 1 when practical.

### 4.4 Retain Guidance

RECOMMENDED defaults:

- retained for current-state topics such as `capabilities`, `online`, or other
  latest-known status topics
- non-retained for one-shot commands and high-rate streams

One-shot commands MUST NOT be retained unless the consumer explicitly
implements stale-command filtering.

### 4.5 Command Wrappers

For command topics, both of the following patterns are acceptable:

```json
{ "x": 0.1, "z": 0.2 }
```

```json
{ "value": { "x": 0.1, "z": 0.2 } }
```

Similarly, boolean controls may use either a bare boolean or a wrapped form:

```json
true
```

```json
{ "value": true }
```

```json
{ "enabled": true }
```

Future users are not required to support every wrapper form, but if a system
mixes multiple producers, accepting both direct and wrapped payloads usually
improves compatibility.

## 5. Broker Topologies

### 5.1 Single-Broker Deployment

The simplest deployment uses one broker for all components. In this case:

- components publish `outgoing/...` directly
- components subscribe to `incoming/...` directly
- retained discovery and current-state topics are resolved by the broker alone

### 5.2 Multi-Broker Or Bridged Deployment

A deployment may also use multiple brokers, such as:

- one component-local broker plus one remote broker
- one site-local broker plus one cloud broker
- separate brokers for low-rate control and high-rate media

In those cases:

- bridge routing SHOULD be explicitly documented
- loops MUST be prevented
- one-shot command retention SHOULD be handled carefully
- implementers SHOULD make it clear which broker is authoritative for each
  topic family

### Example implementation in this repository

The current repository uses:

- a component-local broker, usually `127.0.0.1:1883`
- a remote broker configured in the runtime config

Current bridge behavior:

| Source side | Topic class | Result |
| --- | --- | --- |
| local broker | `{base}/outgoing/#` | always forwarded to remote |
| remote broker | `{base}/incoming/#` | always forwarded to local, except `incoming/audio-stream` |
| remote broker | `{base}/outgoing/#` | never forwarded back into local |
| local broker | `{base}/incoming/flags/#` | always mirrored to remote |
| local broker | other `{base}/incoming/#` | mirrored only when `incoming/flags/remote-mirror` is `true` |
| local broker | retained `outgoing/...` topics | cached and replayed to remote after remote reconnect |

The current repository also routes some media outside the local broker entirely.
That is an example deployment choice, not a requirement of this standard.

## 6. Recommended Generic Topic Families

These topic families are reusable patterns. They are recommendations, not a
complete required set.

| Topic family | Direction | Purpose | Typical retain / QoS |
| --- | --- | --- | --- |
| `{base}/incoming/<command>` | incoming | control commands for a component | non-retained, QoS 1 |
| `{base}/incoming/flags/<flag>` | incoming | mode toggles or control gating | often retained, QoS 1 |
| `{base}/outgoing/online` | outgoing | heartbeat / liveness indicator | retained, QoS 1 |
| `{base}/outgoing/capabilities` | outgoing | retained discovery and control metadata | retained, QoS 1 |
| `{base}/outgoing/status` | outgoing | summarized current state | often retained, QoS 1 |
| `{base}/outgoing/logs` | outgoing | structured diagnostics | non-retained, QoS 0 or 1 |
| `{base}/outgoing/<telemetry>` | outgoing | sensor/state telemetry | retain/QoS depend on metric |
| `{base}/outgoing/<media>` | outgoing | video/audio/media streams | non-retained, often QoS 0 |

Not every system needs all of these. A satellite-only system may never publish
`drive-values`; a tower may never publish audio; a person-carried device may
publish location and status only.

## 7. Discovery And Capability Advertisement

If a system publishes retained discovery data, a common envelope shape is:

```json
{
  "schema": "<implementation-schema-id>",
  "t": 1710969600123,
  "value": { ... }
}
```

RECOMMENDED characteristics:

- retained
- QoS 1
- published at startup, reconnect, and whenever current capabilities change
- includes topic names for control and telemetry where those names are
  configurable

Future users MAY define any schema identifier and any capability structure that
fits their system.

If a system publishes a current-state discovery or capability topic intended for
late subscribers, that topic MUST be retained.

## 8. Example Implementation In This Repository

Everything in this section is an example of the current codebase. Future users
are not expected to implement every one of these topics or payload families.

### 8.1 Example Default Topic Set

Here `{base}` means `{system}/{type}/{id}` in the current repository.

#### Incoming Topics Implemented Here

| Example topic | Current producer(s) | Current consumer(s) | Notes |
| --- | --- | --- | --- |
| `{base}/incoming/drive-values` | web UI, teleop, autonomy scripts | serial MCU bridge, wheel-odometry direction tracker | Optional; only meaningful for drive-capable components. |
| `{base}/incoming/lights-solid` | web UI | serial MCU bridge | Optional visual-control topic. |
| `{base}/incoming/lights-flash` | web UI | serial MCU bridge | Optional visual-control topic. |
| `{base}/incoming/front-camera` | web UI | MQTT bridge | Optional video start/stop request. |
| `{base}/incoming/audio` | web UI | MQTT bridge | Optional audio start/stop request. |
| `{base}/incoming/audio-stream` | remote operator audio sender | audio receiver | Optional speaker downlink topic. |
| `{base}/incoming/soundboard-command` | web UI | soundboard handler | Optional local playback command topic. |
| `{base}/incoming/autonomy-command` | web UI | autonomy manager | Optional autonomy control topic. |
| `{base}/incoming/flags/remote-mirror` | web UI, tooling, tests | MQTT bridge | Optional bridge-mode flag. |
| `{base}/incoming/flags/mqtt-audio` | web UI | MQTT bridge | Optional media gating flag. |
| `{base}/incoming/flags/mqtt-video` | web UI | MQTT bridge | Optional media gating flag. |
| `{base}/incoming/flags/reboot` | web UI | MQTT bridge | Optional one-shot reboot control. |
| `{base}/incoming/flags/git-pull` | web UI | MQTT bridge | Optional one-shot repo-update control. |

#### Outgoing Topics Implemented Here

| Example topic | Current producer(s) | Current consumer(s) | Notes |
| --- | --- | --- | --- |
| `{base}/outgoing/online` | MQTT bridge; some firmware components may publish directly | web UI, replay, analysis | Current heartbeat topic in this repository. |
| `{base}/outgoing/capabilities` | launcher | web UI, replay, tooling | Retained capability advertisement. |
| `{base}/outgoing/logs` | launcher | web UI, replay, analysis | Structured runtime diagnostics. |
| `{base}/outgoing/touch-sensors` | serial MCU bridge when enabled | web UI, analysis | Optional ADC touch telemetry. |
| `{base}/outgoing/charging-status` | serial MCU bridge; some MCU firmware | web UI, replay | Optional charging boolean. |
| `{base}/outgoing/charging-level` | some MCU firmware | web UI, replay | Optional battery/voltage metric. |
| `{base}/outgoing/front-camera` | camera publisher | web UI, replay, receivers | Optional video stream. |
| `{base}/outgoing/audio` | audio publisher | web UI, replay, receivers | Optional uplink audio stream. |
| `{base}/outgoing/soundboard-files` | soundboard handler | web UI | Optional sound catalog. |
| `{base}/outgoing/soundboard-status` | soundboard handler | web UI, replay | Optional sound playback state. |
| `{base}/outgoing/autonomy-files` | autonomy manager | web UI | Optional script catalog. |
| `{base}/outgoing/autonomy-status` | autonomy manager | web UI, replay | Optional autonomy process state. |
| `{base}/outgoing/wheel-odometry` | wheel-odometry script | web UI, replay, follow logic | Optional odometry estimate. |
| `{base}/outgoing/apriltag-locations` | AprilTag location script | follow logic, web UI, replay | Optional perception topic. |
| `{base}/outgoing/apriltag-data` | AprilTag follow scripts | web UI, replay | Optional target-tag estimate topic. |
| `{base}/outgoing/video-overlays` | perception/autonomy publishers | web UI, replay | Optional overlay geometry topic. |

### 8.2 Example Boolean Control Convention Used Here

The current repository's shared boolean parser accepts only:

```json
true
```

```json
{ "value": true }
```

```json
{ "enabled": true }
```

This currently applies to:

- `incoming/front-camera`
- `incoming/audio`
- `incoming/flags/remote-mirror`
- `incoming/flags/mqtt-audio`
- `incoming/flags/mqtt-video`
- `incoming/flags/reboot`
- `incoming/flags/git-pull`

### 8.3 Example Drive Payload Used Here

Accepted forms:

```json
{ "x": 0.2, "z": 0.5 }
```

```json
{ "value": { "x": 0.2, "z": 0.5 } }
```

Current field meanings:

- `x`: steering, `-1.0 .. 1.0`
- `z`: throttle, `-1.0 .. 1.0`
- `y`: optional auxiliary axis, not used by the serial MCU bridge

The current repository converts this to a differential-drive serial command:

```text
left  = clamp(z + x, -1.0, 1.0)
right = clamp(z - x, -1.0, 1.0)
```

Serial example:

```text
M 0.700 0.300
```

This is an example actuator mapping, not a general MQTT requirement.

### 8.4 Example Light Payloads Used Here

Solid LED command:

```json
{ "b": 0.1, "g": 0.2, "r": 0.3 }
```

Wrapped form:

```json
{ "value": { "b": 0.1, "g": 0.2, "r": 0.3 } }
```

Flash/fade LED command:

```json
{ "b": 0.4, "g": 0.5, "r": 0.6, "period": 1.2 }
```

### 8.5 Example Heartbeat Payload Used Here

Current heartbeat payload:

```json
{ "t": 1710969600123 }
```

In the current repository this is published on `outgoing/online`, typically
retained with QoS 1.

### 8.6 Example Capability Payload Used Here

The current repository uses the schema id:

```text
pebble-capabilities/v1
```

Example envelope:

```json
{
  "schema": "pebble-capabilities/v1",
  "t": 1710969600123,
  "value": {
    "identity": {
      "system": "pebble",
      "type": "robots",
      "id": "unit-1"
    },
    "video": {
      "available": true,
      "controls": true,
      "topic": "pebble/robots/unit-1/outgoing/front-camera",
      "command_topic": "pebble/robots/unit-1/incoming/front-camera",
      "flag_topic": "pebble/robots/unit-1/incoming/flags/mqtt-video",
      "overlays_topic": "pebble/robots/unit-1/outgoing/video-overlays",
      "width": 640,
      "height": 480,
      "fps": 15
    }
  }
}
```

The current repository may populate the following `value` sections:

- `identity`
- `video`
- `audio`
- `soundboard`
- `autonomy`
- `system.reboot`
- `system.git_pull`
- `drive`
- `lights`
- `telemetry`

This shape is an example schema, not a requirement for future users.

### 8.7 Example Logs Payload Used Here

Current structured log payload:

```json
{
  "t": 1710969600123,
  "level": "INFO",
  "service": "mqtt_bridge",
  "message": "Remote MQTT connected",
  "logger": "root",
  "pid": 4321
}
```

### 8.8 Example Video Stream Used Here

The current repository's example video topic is:

```text
{base}/outgoing/front-camera
```

Payload:

```json
{
  "id": 123,
  "keyframe": true,
  "data": "<base64(zlib(jpeg_bytes))>",
  "timestamp": 1710969600.123
}
```

Current encoding rules:

- keyframes contain full JPEG frames
- delta frames encode `(current - previous) + 128` before JPEG + zlib + base64
- consumers reconstruct deltas by subtracting `128` and adding to the previous
  reference frame

This is a repository-specific transport choice. Future users may replace it with
another video payload format or omit video entirely.

### 8.9 Example Audio Stream Used Here

The current repository uses:

- `{base}/outgoing/audio` for outbound component audio
- `{base}/incoming/audio-stream` for inbound playback audio

Both use the same binary packet format:

- big-endian header struct: `!4sBBBBHHIQ`
- `magic`: `PBAT`
- `version`: `1`
- `codec`: `1` (`pcm_s16le`)
- `channels`: channel count
- `flags`: currently `0`
- `rate`: sample rate in Hz
- `frame_samples`: samples per channel in the packet
- `seq`: monotonic packet sequence
- `timestamp_ms`: UNIX epoch milliseconds
- followed by interleaved PCM16LE payload bytes

Current default operating profile in this repository:

- `16000` Hz
- mono or stereo depending on component config
- `20 ms` frames
- QoS `0`

This is an example binary media format, not a requirement for future users.

### 8.10 Example Soundboard Topics Used Here

Current accepted command forms:

```json
{ "action": "play", "file": "intro.wav" }
```

```json
{ "action": "stop" }
```

```json
{ "enabled": true, "file": "warn/alarm.wav" }
```

```json
{ "value": { "action": "play", "file": "intro.wav" } }
```

```json
false
```

Example files payload:

```json
{
  "files": ["intro.wav", "warn/alarm.wav"],
  "controls": true,
  "timestamp": 1710969600.123
}
```

Example status payload:

```json
{
  "playing": true,
  "file": "intro.wav",
  "error": null,
  "controls": true,
  "timestamp": 1710969600.123
}
```

Future users do not need to implement a soundboard topic family.

### 8.11 Example Autonomy Topics Used Here

Current accepted command forms:

```json
{ "action": "start", "file": "apriltag-odom-follow", "config": { "tag_id": 13 } }
```

```json
{ "action": "stop" }
```

```json
{ "enabled": true, "file": "wheel-odometry", "config": { "use_tune_state": false } }
```

```json
{ "value": { "enabled": true, "file": "apriltag-follow" } }
```

Example files payload:

```json
{
  "files": [
    {
      "file": "apriltag-odom-follow",
      "label": "apriltag-odom-follow",
      "configs": [
        { "key": "tag_id", "label": "Tag ID", "type": "int", "default": 13 }
      ]
    }
  ],
  "controls": true,
  "timestamp": 1710969600.123
}
```

Example status payload:

```json
{
  "running": true,
  "file": "apriltag-odom-follow",
  "pid": 4321,
  "dependencies": [
    { "file": "apriltag-locations", "pid": 4310, "running": true },
    { "file": "wheel-odometry", "pid": 4311, "running": true }
  ],
  "error": null,
  "config": { "tag_id": 13 },
  "controls": true,
  "timestamp": 1710969600.123
}
```

This is an example process-control family, not a required part of the general
standard.

### 8.12 Example Odometry And Perception Topics Used Here

These are optional example telemetry families currently used in this repository.

Example `wheel-odometry` payload:

```json
{
  "value": {
    "x_mm": 0.0,
    "y_mm": 0.0,
    "heading_rad": 0.0,
    "heading_deg": 0.0,
    "left_mm": 0.0,
    "right_mm": 0.0,
    "distance_mm": 0.0,
    "left_crossings": 0,
    "right_crossings": 0,
    "unknown_direction_events": 0,
    "left_step_total": 0.0,
    "right_step_total": 0.0,
    "left_mm_per_step": 0.0,
    "right_mm_per_step": 0.0,
    "left_extrema_events": 0,
    "right_extrema_events": 0,
    "left_rejected_sign_flips": 0,
    "right_rejected_sign_flips": 0,
    "left_stall_events": 0,
    "right_stall_events": 0,
    "sample_seq": 0,
    "publish_seq": 0
  },
  "unit": "mm",
  "source": "wheel-odometry",
  "timestamp": 1710969600.123
}
```

Example `apriltag-locations` payload:

```json
{
  "source": "apriltag-locations",
  "detections": [
    {
      "marker_id": 13,
      "corners_norm": [[0.1, 0.2], [0.2, 0.2], [0.2, 0.3], [0.1, 0.3]],
      "center_norm": [0.15, 0.25]
    }
  ],
  "frame_width": 640,
  "frame_height": 480,
  "timestamp": 1710969600.123
}
```

Example `apriltag-data` payload:

```json
{
  "detected": true,
  "marker_id": 13,
  "corners_norm": [[0.1, 0.2], [0.2, 0.2], [0.2, 0.3], [0.1, 0.3]],
  "distance_m": 0.74,
  "angle_rad": -0.11,
  "timestamp": 1710969600.123
}
```

Example `video-overlays` payload:

```json
{
  "source": "apriltag-locations",
  "shapes": [
    [[0.1, 0.2], [0.2, 0.2], [0.2, 0.3], [0.1, 0.3]]
  ],
  "labels": [
    { "marker_id": 13, "center_norm": [0.15, 0.25] }
  ],
  "frame_width": 640,
  "frame_height": 480,
  "timestamp": 1710969600.123
}
```

Future users do not need to adopt any of these perception-specific metrics.

## 9. Example Compatibility Topics Still Recognized By This Repository

These are examples of older or compatibility topics still recognized by current
tools here:

| Example topic | Current status |
| --- | --- |
| `{base}/outgoing/heartbeat` | Legacy synonym accepted alongside `outgoing/online`. |
| `{base}/outgoing/status` | Legacy generic state topic still accepted by the web UI. |
| `{base}/outgoing/charging-level` | Optional battery/voltage topic still consumed by the web UI and replay tools. |

## 10. Example Topic Override Points In This Repository

These are current config-driven topic override points in this repository. They
are implementation details, not part of the general standard itself.

| Config location | Affects |
| --- | --- |
| `services.launcher.capabilities.topic` | `outgoing/capabilities` |
| `services.launcher.logs.topic` | `outgoing/logs` |
| `services.mqtt_bridge.topics.remote_mirror` | `incoming/flags/remote-mirror` |
| `services.mqtt_bridge.topics.mqtt_audio` | `incoming/flags/mqtt-audio` |
| `services.mqtt_bridge.topics.mqtt_video` | `incoming/flags/mqtt-video` |
| `services.mqtt_bridge.topics.reboot` | `incoming/flags/reboot` |
| `services.mqtt_bridge.topics.git_pull` | `incoming/flags/git-pull` |
| `services.mqtt_bridge.topics.audio_control` | `incoming/audio` |
| `services.mqtt_bridge.topics.video_control` | `incoming/front-camera` |
| `services.mqtt_bridge.heartbeat.topic` | `outgoing/online` |
| `services.mqtt_bridge.media.video_publisher.topic` | `outgoing/front-camera` |
| `services.mqtt_bridge.media.video_publisher.overlays_topic` | example overlay topic field in capabilities |
| `services.mqtt_bridge.media.audio_publisher.topic` | `outgoing/audio` |
| `services.mqtt_bridge.media.audio_receiver.topic` | `incoming/audio-stream` |
| `services.serial_mcu_bridge.topics.*` | drive, lights, touch, charging topics |
| `services.soundboard_handler.topics.*` | soundboard command/files/status topics |
| `services.autonomy_manager.topics.*` | autonomy command/files/status topics |
| autonomy CLI args such as `--topic`, `--locations-topic`, `--overlays-topic`, `--odom-topic`, `--tags-topic` | script-specific published / subscribed topics |

## 11. Implementation Notes

- Future users SHOULD treat the topic families in sections 6 and 8 as modular.
  A valid implementation may use only a small subset.
- If a deployment uses retained current-state topics, it SHOULD republish them
  at startup, reconnect, and on meaningful state changes.
- If topic names are configurable, publishing those effective topic names in a
  retained capabilities or discovery topic SHOULD be preferred because it
  improves interoperability.
- If a system bridges brokers, loop prevention and retained-message replay
  SHOULD be documented explicitly.
