# Camera and audio modules

Enable `services.av_daemon.demand_enabled: true` and add these instances to
`services.module_host.modules`:

```json
[
  {"id":"camera", "use":"modules/camera/module.json"},
  {"id":"audio", "use":"modules/audio/module.json"}
]
```

The Goob example includes the full setup. Capture settings remain under
`services.av_daemon`. Publisher/receiver commands and media transport settings
remain under `services.mqtt_bridge` for configuration compatibility; **AV daemon
owns these processes when demand is enabled**. The MQTT bridge only transports
commands and state. Existing robot configurations can opt in individually.

## Interface

Open `/modules`. Camera and audio cards are generated from the robot's manifest.
View/Stop controls camera demand; Listen/Stop controls microphone demand. Hold
the Talk button or **T** to send microphone audio to the robot's speaker. **V**
starts viewing; **B** stops viewing. All these actions support the same keyboard
and controller mapping editor as drive and custom modules.

Browser camera and listening requests persist until explicitly stopped or the
AV service restarts, so navigating away or closing the page does not turn off
capture. The browser reuses its consumer identity on return; pressing **Stop**
releases that request. The
microphone uplink used by Talk remains a short lease and is released when the
page loses focus. Explicitly stopping a resource or restarting the AV service
still releases it. Capture stays active if another browser or local autonomy
consumer still needs it.
`Capture active` reflects device-process state, which can differ from this tab's
request. It does not assert that a device is delivering frames; a waiting stream
is reported separately. Speaker playback does not open the robot's microphone.

Browser microphone access and AudioWorklet require **HTTPS or localhost**. For
an HTTP-only ARL installation, tunnel the web service and open localhost:

```sh
ssh -N -L 18080:127.0.0.1:8080 acmattson3@arl-aio-acmattson3
# Open http://localhost:18080/modules
```

Camera viewing and robot audio listening can use the regular HTTP page. The
browser asks for microphone permission only when Talk is pressed. Permission
failures release the speaker request and show an error.

## Endpoints and wire formats

| Relative topic | Purpose |
| --- | --- |
| `i/camera` | Camera publishing request |
| `i/audio` | Robot microphone publishing request |
| `i/speaker` | Robot speaker playback request |
| `o/video` | PBVD delta or PBVH H.264 video, as advertised |
| `o/audio` | PBAT audio, codec 2 (Opus) |
| `i/audio-stream` | PBAT Opus audio to the robot speaker |
| `o/camera/state`, `o/audio/state` | Retained module activity state |
| `i/media/lease` | Local capture resource requests |
| `o/media/state` | Retained AV service activity |

Topics are configurable through the module instance's `subtopics` overrides and
matching publisher/receiver configuration. Legacy `topics` overrides remain
accepted during migration. Binary streams use QoS 0, never
retain packets. State is retained; commands are not. The manifest advertises
`video`, `audio`, and `uplink` endpoint kinds and the codec/sample format.

A short resource command is `{"enabled":1,"consumer":"my-viewer","ttl_seconds":8}`;
`enabled:0` releases that consumer. Omitted consumer names default to one direct
consumer per command topic. Short leases are limited to 15s. Camera and
microphone stream endpoints may opt into a persistent lease with
`{"enabled":1,"consumer":"web:...","ttl_seconds":86400,"persistent":true}`;
only those two resources accept the persistent flag. The web gateway supplies a
stable browser-profile consumer for persistent requests, so Stop works after
returning to the interface. Persistent leases are held until `enabled:0` or an
AV service restart; the TTL field is still required by the endpoint schema.
Raw local capture requests look like:

```json
{"consumer":"my-vision-process","resources":["video"],"ttl_seconds":8}
```

Resources are `video`, `microphone`, `video-stream`, `audio-stream`, and `speaker`.
Stream resources imply the corresponding capture resource; raw capture does not
start publishing. Processes restart while demand remains. Their process groups
are stopped when demand ends, including GStreamer reader subprocesses.

Classic incoming/front-camera and incoming/audio commands remain transitional
explicit on/off consumers, bounded to 24h and cleared on AV restart. They do
not have browser lease renewal; use the module interface for automatic release.

## Opus

Install the runtime library (`apt install libopus0` on Debian). Python uses a
small ctypes binding, with no additional Python codec dependency. Goob uses
16 kHz mono, 20 ms frames, 24 kbit/s target, complexity 3. Audio queues are bounded.
The existing 24-byte PBAT header stays version 1; codec 1 is PCM16LE, codec 2 is
Opus. The receiver accepts both. Decoder state is isolated per stream and resets
on sequence restart or format change.

The robot ↔ MQTT broker ↔ web gateway legs use Opus in both directions. The
browser ↔ web gateway legs currently use PCM16LE over HTTP, with 40 ms uplink
batches and bounded polling for listening. Camera supports PBVD or H.264 over MQTT and JPEG frames from the gateway to
the browser. See [H.264 setup and observations](H264.md). Native browser
Opus/WebCodecs transport remains separate work.
Concurrent talkers are serialized into one uplink stream, without audio mixing
or a control ownership policy.

## Local autonomy

Use `control.common.media.CaptureLease` around standalone shared-memory readers:

```python
with CaptureLease(config, config_path, ["video"]):
    # Open/retry the configured AV shared-memory socket and process frames.
    ...
```

The context renews independently, releases on exit, and has a non-retained MQTT
will plus an expiry if its process dies. For scripts managed by autonomy_manager,
put `["video"]` in `media-resources.json` beside run.py, or set
`services.autonomy_manager.scripts.<name>.media_resources` in the runtime config.
The manager requests the union of resources used by its live script/dependencies.
The AprilTag scripts now declare their video dependency. Standalone tools that
open the old socket directly need to adopt CaptureLease or run with an explicit
local capture consumer.

## Checks for this change

Python compilation and JavaScript syntax checks were run. No test suites or
interactive camera/microphone/speaker checks were run for this milestone.

### Binary dispatch recovery (2026-10-20 UTC)

The first live video frame exposed a routing error in ARL's older web runtime:
module media was decoded, then dispatched again to the legacy JSON handler.
Its UnicodeDecodeError terminated the MQTT receive thread and made heartbeat
availability expire. Module message dispatch now reports ownership, preventing
that fall-through. The ARL MQTT callback also has an exception boundary, rejects
non-legacy directions in the legacy dispatcher, and handles non-UTF-8 input.

A live recovery check ran camera and Opus microphone capture together for 32
seconds, renewing requests through the HTTP gateway. Goob stayed online for all
32 samples; 26 distinct camera frames were fetched after startup and 961,280
bytes of decoded microphone PCM were received. The gateway logged no callback
exceptions. Both requests were then released: capture/publishing state returned
to false and no camera/audio publisher or GStreamer capture processes remained.
This checked the gateway paths; browser microphone uplink and speaker playback
were not exercised in that recovery check.
