# Robot-owned modules

The generic page is `/modules`. A robot advertises its optional controls and
telemetry through its local broker. A robot that only implements the core MQTT
contract does not need a module host or the web interface.
`mqtt_bridge` carries compact module outputs to the remote broker and compact
commands back to the local broker. Existing topics remain available through
explicit adapters.

For a dedicated web host, set `web_interface.mqtt.auto_connect` to `true` in its
private runtime config so discovery works immediately after service restart.
The default is `false`; manual broker connection is available from the
compatibility page at `/`.

## Contracts

The retained discovery topic is `{system}/{type}/{id}/o/manifest`, with schema
`pebble-manifest/v2`. `manifest.schema.json` defines its structure. The robot
runtime expands relative endpoint topics under the identity prefix. Goob's base
is `pebble/robots/goob`.

A module definition contains:

- `type`, `version`, `title`, `description`: reusable module metadata.
- `endpoints`: relative `i/...` command and `o/...` state/event topics with JSON
  payload schemas and explicit QoS/retention. Commands and events are never
  retained.
- `actions`: named numeric values with ranges, neutral values and release behavior.
- `commands`: object fields bound to named actions, with an optional hold-repeat
  interval. All fields are published together in one command.
- `bindings`: keyboard codes, controller axes and controller buttons mapped to
  actions. Axes support dead zones, scale/inversion and offsets. Reset bindings
  release held actions. Keyboard auto-repeat is ignored.
- `ui`: a grid of generic joystick, range, button, reset, state, telemetry and
  log widgets, plus reusable media, soundboard, autonomy, and lights widgets.

Module format version 1 supports numeric action bindings and object commands.
The soundboard widget is a reusable typed control that reads the robot's WAV
list and playback status from declared state endpoints and sends validated
play/stop commands to a declared command endpoint. Definitions contain no
arbitrary JavaScript, HTML, expressions, remote schema references, or
robot-specific web routes. Schemas support the bounded local JSON Schema subset
enforced by `control/common/modules.py`.

The autonomy widget reads script names and bounded argument descriptors from
the robot, then builds typed inputs for string, integer, float, boolean, and
enumerated options. Start/stop requests use a declared command endpoint. The
robot's existing autonomy manager remains responsible for discovering allowed
scripts, validating argument bounds, managing dependencies, and stopping the
current process before starting another.

The lights widget uses declared solid and flash command endpoints. The shared
`modules/lights/module.json` defines color and flash-period payloads; robots
adapt those endpoints to their own actuators. Goob forwards them to its existing
serial bridge topics.

The telemetry widget renders robot-defined metrics from declared state
endpoints. `modules/telemetry/module.json` provides battery voltage, charging,
and touch sensor endpoints for robots that use them. Goob selects only battery
voltage and enables the widget's built-in presence rows, which show online
state and the web gateway's local receive time for Goob's last online update.
Presence comes from the standard `o/online` topic; it is not duplicated as a
telemetry endpoint and does not depend on Goob having a synchronized clock.

The logs widget reads structured records from a declared event endpoint. The
gateway keeps the most recent 100 events in memory per endpoint; logs are
non-retained and are not a durable archive. The shared `modules/logs/module.json`
declares the record fields and level filter. The launcher publishes records to
compact `o/logs` and also mirrors them to its configured compatibility topic.

The administration widget is a shared, data-only set of fixed actions. A robot
instance selects only supported endpoints and defines its confirmation text;
the browser renders those actions and requires confirmation before publishing.
Each action has a separate command endpoint with a fixed `enabled: true`
payload, and the robot maps that endpoint to its existing local handler. The
browser cannot supply commands, service names, or arbitrary topics. Goob maps
reboot, Git update, and service restart to its configured flag handlers. The
web page reports that the request was queued; the existing handlers do not yet
publish execution results.

Each configured instance supplies an `id` and `use` (a module JSON path relative
to the repo, or an absolute path for a local/private module). Optional
`subtopics`, `endpoints`, `bindings`, `ui`, `title`, and `description` override
that instance's defaults. `endpoints` can restrict an instance to a subset of
the shared module's declared endpoints. Module endpoint subtopics must remain within the robot's
identity prefix. A forward adapter's `subtopic` names its local compatibility
destination. Legacy `topics`/`topic` adapter fields remain accepted during
migration. Adapters remain private robot configuration and are not sent to the
browser.

## Goob's configured modules

Merge `robots/goob/modules.example.json`'s `services.module_host` section into
the robot's private runtime config. Install `jsonschema>=4,<5` in the runtime
environment (or Debian's `python3-jsonschema` for a system Python service).
The launcher starts `control.services.module_host` automatically when enabled.

- Shared `modules/drive/module.json`: forward/turn actions, an on-screen stick,
  sliders, WASD, Space to release, and controller axes 0/1. Its compact command
  endpoint `i/drive` forwards once to `incoming/drive-values` locally, so the
  existing MCU/ROS bridge and drive watchdog still apply.
- Shared `modules/soundboard/module.json`: renders a robot-advertised WAV list,
  playback state, and play/stop buttons. Goob's module instance forwards the
  compact command to its existing soundboard handler; the handler keeps the
  legacy topics available and publishes the declared module state endpoints.
- Shared `modules/lights/module.json`: color picker, solid/flash/off controls,
  and bounded flash period. Goob forwards its compact command endpoints to the
  serial bridge's existing light topics.
- Shared `modules/telemetry/module.json`: battery voltage, charging state, and
  optional touch sensor readings. Goob advertises battery voltage plus online
  and last-seen status; its manifest omits charging and touch endpoints.
- Shared `modules/logs/module.json`: recent structured runtime messages with
  level filtering. The launcher publishes compact events while retaining the
  configured compatibility feed for existing log consumers.
- Shared `modules/autonomy/module.json`: robot-advertised autonomy scripts,
  configurable arguments, and start/stop controls. Goob's autonomy manager
  publishes compact module state while keeping its legacy topics available.
- Example robot-local `robots/goob/modules/gripper-demo/module.json`: a virtual
  position slider, open/close buttons, G/H keys and controller buttons 0/1. It
  demonstrates defining a custom layout and can be enabled by a robot that
  needs it; its echo adapter updates retained `o/gripper-demo/state` and never
  touches hardware. Goob does not currently enable this example module.

To promote a robot-specific module into the shared library, move its definition
under `modules/`, choose its public type/version, and update `use`. Hardware
adaptation belongs in the robot runtime, not the shared control definition.

## Browser input behavior

All widgets, keys and controllers update the same named actions. Held actions
return to neutral on release; position-like actions can latch their value.
Held commands repeat at their declared cadence using a 50 ms browser tick.
Opening the page does not publish drive commands. Forms do not trigger keyboard
shortcuts; key releases still clear an earlier hold after focus changes.

Expand **Input mappings** on a module to change a key, axis/button index, action,
value, scale, dead zone or offset, or add/remove bindings. Changes are stored in
this browser per robot, module instance and version. Keyboard overrides are
independent of controller profiles; controller overrides are keyed by device ID.
**Robot defaults** restores advertised mappings. Enable the Controller checkbox
to use the selected device. Browser/controller axis layouts vary, so indexes
are editable. Releasing inputs also disables controller input until re-enabled.

Focus loss, robot changes and controller disconnection release held inputs.
There is at most one HTTP command in flight per endpoint and one pending latest
value, with final neutral commands queued in order. Unload delivery is best
effort; the existing robot watchdog remains the final stop mechanism. Explicit
control ownership is outside this milestone.

## Runtime and gateway behavior

The bridge also publishes robot-wide retained connection presence at
`o/online`, with a Last Will for unexpected MQTT disconnects. The older
`outgoing/online` heartbeat remains for existing consumers. This core presence
contract does not depend on the module host or this module manifest.

The module host publishes retained availability every five seconds at
`o/modules/status`, with a boot ID and MQTT last will. The web page treats a
module host as offline after 20 seconds without availability or when its broker
connection is down. Supported controls and actual host availability are separate.
Echo state survives broker reconnection within a process; the virtual demo starts
at its declared neutral after a process restart.

The web gateway accepts commands only for advertised command endpoints, validates
payload ranges and shape, and rejects stale manifest revisions. It accepts no
arbitrary browser-selected topic. `published` is a gateway result, not an
actuator acknowledgement. Tombstoning the retained manifest removes its controls.

On MQTT 3.1.1 live deliveries do not preserve the publisher's RETAIN flag. The
bridge therefore derives retained module-state routing from the manifest, so
new remote subscribers receive discovery/state immediately. Only the declared
forward adapter translates the compact command into its legacy destination;
there is no reverse alias or wildcard topic rewrite.

## Validation

```bash
python3 -m unittest tests.test_modules tests.test_mqtt_bridge tests.test_common tests.test_example_configs tests.test_launcher tests.test_autonomy_manager tests.test_unified_config
node --test tests/test_module_inputs.mjs
```

Optional end-to-end browser validation requires Playwright Chromium, Flask,
Paho, and a cached `eclipse-mosquitto:2.0.22` Docker image:

```bash
PYTHONPATH=. python3 tests/module_browser_smoke.py
```

It creates two temporary loopback-only brokers on ports 18884/18885 and a local
web gateway on 18086. It exercises real MQTT forwarding, generic custom controls,
keyboard remapping and persistence, controller and pointer input, neutral release,
and retained discovery/state. Its drive commands stay inside isolated brokers.

## Media modules

[Camera and audio modules](MEDIA.md) add demand-controlled capture, Opus over
MQTT, shared media widgets, and mappable view/listen/talk actions.

[H.264 backend](H264.md) documents codec selection, framing, recovery, live
measurements, and the browser Talk upload queue correction.
