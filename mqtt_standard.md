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
- any section or table explicitly labeled as repository behavior describes the
  current codebase only
- example topic names, payloads, and schemas are illustrative, not mandatory
  for future users

The topic base is:

```text
{system}/{type}/{id}
```

The compact Pebble topic form is:

```text
{system}/{type}/{id}/{i|o}/{metric}
```

New Pebble component topics SHOULD use this shape. `i` means input to the
component (commands and requests); `o` means output from it (state, telemetry,
and media). Additional path segments are permitted inside `{metric}`. Existing
deployments may retain the longer `incoming` and `outgoing` segments during
migration; implementations that need compatibility MAY support both forms.

Examples:

```text
luminsim/robots/astra1/i/drive
luminsim/satellites/lunanet-sat-1/o/status
luminsim/towers/tower-3/o/power
luminsim/people/operator-1/o/location
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

- `i` means data intended to be consumed by that component
- `o` means data published by that component

This is a logical direction, not necessarily a statement about broker location.
For example, an `incoming` topic may be published on a remote broker and then
bridged locally.

### 3.3 Topic Examples

Compact Pebble examples:

- `{system}/robots/{id}/i/drive`
- `{system}/robots/{id}/o/online`
- `{system}/satellites/{id}/o/status`
- `{system}/towers/{id}/o/manifest`
- `{system}/people/{id}/o/location`

### 3.4 Wildcards

Examples:

- `{system}/#` for all live system traffic
- `{system}/+/+/o/online` for all heartbeat topics
- `{system}/robots/+/o/#` for all robot-published telemetry
- `{system}/towers/+/o/status` for status of all towers

Legacy deployments may use equivalent `incoming/...` and `outgoing/...`
filters.

### 3.5 Robot availability

An MQTT client that represents a complete robot SHOULD publish robot-wide
availability at `{base}/o/online`. The minimal payload is:

```json
{"online":true}
```

The retained online message SHOULD use QoS 1. Before connecting, the client
SHOULD configure an MQTT Last Will on the same topic with a retained QoS 1
payload that sets `online` to `false`. After a successful connection it
publishes `online: true`; before a clean disconnect it SHOULD publish
`online: false`. The Will covers ungraceful loss of the MQTT session.

An online publisher SHOULD refresh its retained `online: true` periodically,
no slower than every 60 seconds. Pebble's Linux bridge currently refreshes it
every 30 seconds, independently of the compatibility heartbeat. Consumers can
use this refresh as a freshness signal; MQTT clients that only publish on
connect SHOULD still use a local timeout and rely on broker connection state
and LWT for prompt disconnect detection.

Publishers MAY add `boot_id` (unique for each process/device boot) and `t`
(UNIX epoch milliseconds) to help consumers recognize restarts and order live
updates. A Will's publish time is not the time the connection failed, so
consumers SHOULD use their local receive time to track offline events and
freshness. If a publisher cannot send periodic updates, a consumer SHOULD treat
an old `online: true` as stale after a locally chosen timeout. Availability
means the MQTT client is connected; it does not guarantee that every motor,
sensor, or optional Pebble service is healthy.

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

- components publish `o/...` directly
- components subscribe to `i/...` directly
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

### Current bridge behavior in this repository

Compact module topics are routed according to their manifest declarations.
Older service topics continue to use the legacy names described below. Some
media is routed outside the local broker entirely; that is an implementation
choice, not a requirement of this standard.

The current repository uses:

- a component-local broker, usually `127.0.0.1:1883`
- a remote broker configured in the runtime config

Current bridge behavior:

| Source side | Topic class | Result |
| --- | --- | --- |
| local broker | `{base}/o/#` and legacy `{base}/outgoing/#` | component output forwarded to remote |
| remote broker | `{base}/i/#` and legacy `{base}/incoming/#` | commands forwarded to local, subject to media routing exceptions |
| remote broker | `{base}/o/#` and legacy `{base}/outgoing/#` | never forwarded back into local |
| local broker | declared control inputs | mirrored according to bridge policy |
| local broker | retained `o/...` topics | cached and replayed to remote after remote reconnect |

The current repository also routes some media outside the local broker entirely.
That is an example deployment choice, not a requirement of this standard.

## 6. Recommended Generic Topic Families

These topic families are reusable patterns. They are recommendations, not a
complete required set.

| Topic family | Direction | Purpose | Typical retain / QoS |
| --- | --- | --- | --- |
| `{base}/i/<command>` | input | control commands for a component | non-retained, QoS 1 |
| `{base}/i/flags/<flag>` | input | mode toggles or control gating | often retained, QoS 1 |
| `{base}/o/online` | output | heartbeat / liveness indicator | retained, QoS 1 |
| `{base}/o/manifest` | output | retained module/control discovery | retained, QoS 1 |
| `{base}/o/status` | output | summarized current state | often retained, QoS 1 |
| `{base}/o/logs` | output | structured diagnostics | non-retained, QoS 0 or 1 |
| `{base}/o/<telemetry>` | output | sensor/state telemetry | retain/QoS depend on metric |
| `{base}/o/<media>` | output | video/audio/media streams | non-retained, often QoS 0 |

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

Future users MAY define any schema identifier and any discovery structure that
fits their system. Pebble's current robot-module discovery is `o/manifest`; the
older capabilities format remains for compatibility with the `/` interface.

If a system publishes a current-state discovery or capability topic intended for
late subscribers, that topic MUST be retained.

Pebble's minimal robot contract does not require a Linux module host or a rich
capability document. `{base}/o/online` is the small lifecycle signal. The
optional robot-owned module system uses retained `{base}/o/manifest` to
describe controls, endpoint schemas, and media formats for generic interfaces.
The module host's `{base}/o/modules/status` is service-level availability; it
does not replace robot-wide online status. Consumers SHOULD treat a retained
manifest as the latest discovery snapshot and replace their cached snapshot
when a newer manifest arrives. Publishing an empty retained manifest payload
removes that snapshot under MQTT retained-message semantics.

A module endpoint of kind `event` carries independent, schema-validated JSON
records on an output topic. Event records MUST NOT be retained. A consumer may
keep a bounded recent in-memory view, but this is not durable logging; use a
broker recorder when logs must survive consumer restarts.

## 8. Example Implementation In This Repository

Pebble is migrating its runtime services toward compact `i/` and `o/` topics.
The robot-owned module system already uses these names for declared controls,
media, state, and discovery. Existing service integrations still use many of
the legacy topic names listed below, and adapters preserve those paths during
migration. Future users are not expected to implement every example topic or
payload family.

### 8.1 Example Default Topic Set

Here `{base}` means `{system}/{type}/{id}`. Module media/control examples use
compact paths; legacy service examples are explicitly labeled.

#### Compact module topics in use

| Example topic | Purpose |
| --- | --- |
| `{base}/o/manifest` | Retained robot-advertised module definitions |
| `{base}/o/modules/status` | Module host availability |
| `{base}/o/logs` | Structured, non-retained runtime log events |
| `{base}/i/drive` | Drive command, adapted locally to the existing drive bridge |
| `{base}/i/lights/solid`, `{base}/i/lights/flash` | Solid/flash RGB commands from the shared lights module |
| `{base}/i/camera`, `{base}/i/audio`, `{base}/i/speaker` | Media requests |
| `{base}/o/video`, `{base}/o/audio` | H.264/PBVD video and Opus audio |
| `{base}/i/audio-stream` | Opus audio sent to robot speaker |
| `{base}/i/media/lease`, `{base}/o/media/state` | Capture demand and AV service state |

Media resource requests normally use a short lease (`ttl_seconds` up to 15),
renewed by the consumer and removed with `enabled: 0`. The camera and microphone
stream request endpoints may explicitly set `persistent: true` with a
`ttl_seconds` of 86400. Persistent capture survives a web page closing and is
released by a later `enabled: 0` request from the same consumer or when the AV
service restarts. The TTL field remains required by those endpoint schemas, but
the persistent flag holds the lease independently of expiry. Other media
resources, including speaker playback, remain short leases.

The shared `pebble.logs` module uses `{base}/o/logs` with one JSON object per
event: `t` is the Unix timestamp in milliseconds; `level` is `DEBUG`, `INFO`,
`WARNING`, `ERROR`, or `CRITICAL`; `service`, `message`, `logger`, and `pid`
identify the source and record. Events are non-retained. A gateway may keep a
bounded recent buffer for display; use the broker logger or MQTT history for
durable storage.

#### Legacy incoming topics still implemented here

| Example topic | Current producer(s) | Current consumer(s) | Notes |
| --- | --- | --- | --- |
| `{base}/incoming/drive-values` | web UI, teleop, autonomy scripts, compact drive adapter | serial MCU bridge, ROS 1 bridge, wheel-odometry direction tracker | Legacy actuator command destination; compact drive modules adapt to it. |
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
| `{base}/incoming/flags/service-restart` | web UI | MQTT bridge | Optional one-shot service restart control. |
| `{base}/incoming/flags/git-pull` | web UI | MQTT bridge | Optional one-shot repo-update control. |

#### Legacy outgoing topics still implemented here

| Example topic | Current producer(s) | Current consumer(s) | Notes |
| --- | --- | --- | --- |
| `{base}/outgoing/online` | MQTT bridge, ROS 1 bridge; some firmware components may publish directly | classic web UI, replay, analysis | Legacy timestamp heartbeat, kept for compatibility; robot-wide lifecycle topic is `{base}/o/online`. |
| `{base}/outgoing/capabilities` | launcher | web UI, replay, tooling | Retained capability advertisement. |
| `{base}/outgoing/logs` | launcher | compatibility UI, replay, analysis | Compatibility mirror of the structured runtime events at `{base}/o/logs`. |
| `{base}/outgoing/touch-sensors` | serial MCU bridge when enabled | web UI, analysis | Optional ADC touch telemetry. |
| `{base}/outgoing/charging-status` | serial MCU bridge; some MCU firmware | web UI, replay | Optional charging boolean. |
| `{base}/outgoing/charging-level` | some MCU firmware; ROS 1 bridge | web UI, replay | Optional battery/voltage metric. |
| `{base}/outgoing/localization-pose` | ROS 1 bridge | replay, tooling | Optional low-rate localization estimate. |
| `{base}/outgoing/navigation-status` | ROS 1 bridge | replay, tooling | Optional low-rate navigation state summary. |
| `{base}/outgoing/navigation-goal` | ROS 1 bridge | replay, tooling | Optional current navigation goal. |
| `{base}/outgoing/navigation/local-plan` | ROS 1 bridge | replay, tooling | Optional sampled local plan. |
| `{base}/outgoing/navigation/global-plan` | ROS 1 bridge | replay, tooling | Optional sampled global plan. |
| `{base}/outgoing/diagnostics` | ROS 1 bridge | replay, tooling | Optional low-rate diagnostics summary. |
| `{base}/outgoing/front-camera` | legacy camera publisher config | legacy UI, replay, receivers | Legacy video; module path is `{base}/o/video`. |
| `{base}/outgoing/audio` | legacy audio publisher config | legacy UI, replay, receivers | Legacy audio; module path is `{base}/o/audio`. |
| `{base}/outgoing/soundboard-files` | soundboard handler | web UI | Optional sound catalog. |
| `{base}/outgoing/soundboard-status` | soundboard handler | web UI, replay | Optional sound playback state. |
| `{base}/outgoing/autonomy-files` | autonomy manager | web UI | Optional script catalog. |
| `{base}/outgoing/autonomy-status` | autonomy manager | web UI, replay | Optional autonomy process state. |
| `{base}/outgoing/wheel-odometry` | wheel-odometry script; ROS 1 bridge | web UI, replay, follow logic | Optional odometry estimate. |
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
- `incoming/flags/service-restart`
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

The compact lifecycle signal is published on `o/online` as described in
[Robot availability](#35-robot-availability). The classic heartbeat remains on
`outgoing/online`, typically retained with QoS 1.

### 8.6 Legacy Capabilities Payload

The classic interface and compatibility services use the schema id:

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

The legacy capabilities publisher may populate the following `value` sections:

- `identity`
- `video`
- `audio`
- `soundboard`
- `autonomy`
- `system.reboot`
- `system.service_restart`
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

### 8.8 Legacy PBVD Video Stream

The legacy video topic is:

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

PBVD encoding rules:

- keyframes contain full JPEG frames
- delta frames encode `(current - previous) + 128` before JPEG + zlib + base64
- consumers reconstruct deltas by subtracting `128` and adding to the previous
  reference frame

This is a repository-specific transport choice. Future users may replace it with
another video payload format or omit video entirely.

### 8.9 Legacy PCM Audio Stream

The legacy audio topics are:

- `{base}/outgoing/audio` for outbound component audio
- `{base}/incoming/audio-stream` for inbound playback audio

The PBAT packet format supports PCM16LE (`codec: 1`) and Opus (`codec: 2`).
Legacy PCM topic consumers use this binary packet format:

- big-endian header struct: `!4sBBBBHHIQ`
- `magic`: `PBAT`
- `version`: `1`
- `codec`: `1` (`pcm_s16le`) for the legacy paths; compact media modules
  currently advertise Opus (`codec: 2`)
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
| `services.launcher.capabilities.subtopic` | `outgoing/capabilities` |
| `services.launcher.logs.subtopic` | compatibility topic `outgoing/logs`; launcher also publishes compact `{base}/o/logs` |
| `services.mqtt_bridge.subtopics.remote_mirror` | `incoming/flags/remote-mirror` |
| `services.mqtt_bridge.subtopics.mqtt_audio` | `incoming/flags/mqtt-audio` |
| `services.mqtt_bridge.subtopics.mqtt_video` | `incoming/flags/mqtt-video` |
| `services.mqtt_bridge.subtopics.reboot` | `incoming/flags/reboot` |
| `services.mqtt_bridge.subtopics.service_restart` | `incoming/flags/service-restart` |
| `services.mqtt_bridge.subtopics.git_pull` | `incoming/flags/git-pull` |
| `services.mqtt_bridge.subtopics.audio_control` | `incoming/audio` |
| `services.mqtt_bridge.subtopics.video_control` | `incoming/front-camera` |
| `services.mqtt_bridge.heartbeat.subtopic` | `outgoing/online` |
| `services.mqtt_bridge.media.video_publisher.subtopic` | `outgoing/front-camera` |
| `services.mqtt_bridge.media.video_publisher.overlays_subtopic` | example overlay topic field in capabilities |
| `services.mqtt_bridge.media.audio_publisher.subtopic` | `outgoing/audio` |
| `services.mqtt_bridge.media.audio_receiver.subtopic` | `incoming/audio-stream` |
| `services.serial_mcu_bridge.subtopics.*` | drive, lights, touch, charging topics |
| `services.soundboard_handler.subtopics.*` | soundboard command/files/status topics |
| `services.autonomy_manager.subtopics.*` | autonomy command/files/status topics |
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
