# Pebble development plan

This is the working plan for Pebble's architecture and product direction. It
tracks the broader work discussed for Pebble, not only configuration cleanup.
Update the status as milestones land; keep this plan focused on decisions and
outcomes rather than duplicating implementation details from the READMEs.

## Goals and constraints

- Keep Pebble a lightweight, local-first robot system built around MQTT rather
  than requiring ROS/ROS 2.
- Make the basic MQTT contract small enough for an ESP32-class robot. A robot
  should not need the Linux runtime, module host, or web UI to participate.
- Make optional capabilities composable: a robot opts into shared modules or
  provides its own modules, while adapters connect those contracts to hardware.
- Prefer simple, versioned, inspectable JSON contracts and generic tooling.
- Preserve compatibility during migrations; avoid forcing every robot to adopt
  every feature.
- Do not add explicit control ownership until a real use case requires it.

## Current state

- Compact `i/` and `o/` topics and a robot-wide retained `o/online` presence
  signal are implemented. Legacy `incoming/` and `outgoing/` topics remain in
  use behind adapters and compatibility paths.
- The robot-owned module system supports versioned manifests, reusable module
  definitions, validated command/state endpoints, generic layouts, and editable
  keyboard/controller mappings. Shared drive controls and a Goob-local gripper
  example demonstrate reusable and robot-specific modules.
- The web gateway discovers modules and gates commands on robot and module-host
  availability. ARL's live interface has been verified with Goob; the telemetry
  widget now shows robot presence and the gateway's last-seen receive time.
- Camera and audio capture are demand-controlled. Robot-to-gateway and
  gateway-to-robot audio use Opus over MQTT. Goob uses H.264 over MQTT; the
  benchmark and live recovery results are documented in `modules/H264.md`.
- Sparkplug B was reviewed for useful ideas. Pebble has adopted lightweight
  lifecycle availability, retained discovery snapshots, boot IDs, and clear
  distinctions between robot and module-host health without requiring the
  Sparkplug payload and topic model.
- The minimal MQTT component profile is documented: a unique identity, agreed
  identity-scoped topics and payloads, and MQTT 3.1.1 semantics are enough to
  participate. Capabilities and presence remain optional. The MiniLAMP example
  uses compact drive, online, and battery topics, with retained state and an
  MQTT Last Will; presence no longer depends on NTP.
- Full-duplex acoustic echo cancellation, native browser media transport, and
  Jev integration are not implemented.
- Robot-local MQTT topic settings now use identity-relative `subtopic` and
  `subtopics` in the example configs. One shared resolver handles defaults,
  preserves legacy full `topic`/`topics` values, and rejects unsafe local
  subtopics.
- The robot/web config split is deployed: Goob uses `robot.json`, and ARL's
  web service uses `web-interface/config.json`. Goob's robot-local topic
  settings now use subtopics while resolving to the same MQTT topics. Its
  motor output swap is configured rather than patched into source.
- Goob's disconnected Nano IMU instance has been removed. Its main serial
  bridge now publishes battery voltage from the connected MCU; battery samples
  containing `chg=-1` no longer lose their `v=` voltage field.
- Goob's soundboard module is defined and exposes the robot's `/opt/sounds`
  WAV library and playback status through the generic module interface.
- Goob advertises battery voltage through telemetry and omits charging and
  touch endpoints from its module manifest. The widget also shows online state
  and last seen from the standard `o/online` presence signal. Shared telemetry
  still offers charging and touch endpoints for robots that use them.
- The log module uses non-retained events and bounded gateway history; Goob's
  example configuration includes it.
- The shared autonomy module is deployed on Goob and ARL. Goob advertises its
  four configured scripts and idle status; their payloads validate against the
  module contract. No autonomy script was started during verification.
- The module UI remains at `/modules`; the existing compatibility interface
  remains at `/` until the replacement passes the migration criteria.

## Roadmap

### 1. Simplify and separate configuration — boundary complete; profiles remain

Reduce repeated identity, topic, broker, and media settings while making the
robot runtime and web deployment independently understandable.

- [x] Make robot-local topic overrides relative to the configured identity, using
  `subtopic`; retain a clearly explicit route for fully qualified external
  topics and a compatibility path for existing configs.
- [x] Establish ownership boundaries: robot identity, hardware, and enabled
  services belong to robot configuration; web bind/history settings and its
  list of controlled robots belong to web configuration.
- [x] Split the current unified config into robot and web configs, without
  creating a separate file for every process.
- [x] Resolve robot-local topic settings centrally with documented defaults
  and precedence.
- [ ] Extend central validation/resolution to all service settings so every
  service receives fully resolved settings.
- [ ] Reduce duplicated media values by distinguishing capture formats/devices
  from published stream profiles. Derive equal values by default and allow
  explicit overrides when capture and network output differ.
- [ ] Consider named MQTT connection profiles where several services share
  broker settings. Keep local, remote, and web broker roles distinct when their
  endpoints or credentials differ.
- [x] Keep secrets out of shareable example configs, using protected files or
  environment references.
- [x] Migrate example configs first and preserve existing deployments through
  a compatibility loader. Tests cover topic resolution, identity boundaries,
  and split-config behavior.
- [ ] Expand cross-robot reference and general precedence coverage as
  configuration resolution grows.

**Completed slice:** centralized robot-local topic resolution and migrated
robot, module-adapter, and web-target examples to `subtopic` fields while
keeping legacy full topics supported. The robot/web config split is deployed
on Goob and ARL. Focused tests verify namespace safety and unchanged effective
topics.

**Acceptance:**

- [x] Goob and the primary Pebble/web deployment resolve to the expected
  topics and settings; web configs can address robots outside the local
  runtime identity; unsafe local subtopics are rejected; legacy configs have a
  migration path.

### 2. Make the minimal MQTT contract easy to implement — profile documented

- [ ] Finish migrating robot-owned services toward compact `i/` and `o/` topics;
  keep adapters for deployed legacy consumers.
- [x] Document a minimal component profile that needs only identity, MQTT
  topics, and payload contracts. Availability is recommended, but not required.
- [x] Add a conformance checklist and broker-backed simulated MCU/ESP32 client
  test. The simulator verifies command delivery, retained state, and retained
  offline Last Will behavior.
- [ ] Add an explicit Linux minimal-profile conformance/simulator test. The
  existing Linux unit and on-robot smoke tests cover runtime behavior, but do
  not yet run this profile checklist as a separate client.
- [x] Review Sparkplug-inspired lifecycle, discovery, sequence, and stale-data
  practices individually; adopt only lightweight practices that improve
  recovery or interoperability.

**Acceptance:**

- [x] The profile and embedded-style simulator demonstrate a component
  publishing state and consuming commands without Pebble's Linux services,
  module host, or web interface.
- [x] Compile the MiniLAMP firmware for the Seeed XIAO ESP32-C6.
- [ ] Flash and exercise the MiniLAMP firmware against hardware.

### 3. Grow robot-owned controls and the shared module library

- [x] Promote shared drive, soundboard, lights, telemetry, logs, autonomy, and
  administrative modules into `modules/` with versioned contracts and examples.
- [x] Keep robot-specific mechanisms such as Goob's gripper in robot-owned
  modules, while rendering them through the generic interface.
- [x] Keep input-to-action mapping in module contracts so keyboard and gamepad
  inputs use the same command path.
- [x] Keep telemetry data and presence generic. Goob advertises battery only;
  its widget shows online and last seen from `o/online`, while charging and
  touch remain optional shared endpoints for other robots.
- [ ] Add examples for more common drive variants and robot-specific controls
  as they are adopted by additional robots.
- [ ] Improve authoring with a simple layout designer or reusable widget
  library when real module needs show that hand-authored JSON is cumbersome.
- [ ] After robot modules, media controls, reconnect behavior, and multi-robot
  use are working reliably, make the module UI the root page and move the
  compatibility interface under a subpath.
- [x] Keep browser execution generic and bounded; robot modules cannot send
  arbitrary HTML or JavaScript to the gateway.
- [x] Add unit coverage for invalid manifests, command validation, and offline
  handling.
- [x] Run the Playwright browser smoke test for module rendering, controls,
  telemetry presence, soundboard behavior, and browser errors.
- [ ] Run broader reconnect and multi-robot integration checks before the
  root-page migration.

**Acceptance:**

- [x] Adding the current shared or robot-specific controls does not require a
  robot-specific web page or gateway code, and commands remain
  schema-validated.

### 4. Improve media quality and two-way conversation

- [x] Keep H.264 and Opus as the current robot-to-broker formats; benchmark and
  live Goob recovery findings are documented.
- [x] Keep media capture demand-controlled and test the main enable/disable
  lifecycle.
- [ ] Evaluate browser-native H.264/WebCodecs and native Opus paths to reduce
  gateway JPEG and browser PCM conversion work.
- [ ] Design acoustic echo cancellation for both ends of a two-way call: the
  operator client and the robot playback/capture path. Prototype against
  realistic speaker/microphone setups and measure echo reduction, speech
  quality, delay, CPU use, and push-to-talk behavior.
- [ ] Expand reconnect, competing-request, and unavailable-device testing and
  report explicit media-source-unavailable and stream-health diagnostics.

**Acceptance:**

- [ ] Media recovers cleanly after network and device interruptions; any
  echo-cancellation change is evaluated at both ends and does not impair
  one-way listening or speech intelligibility.

### 5. Improve robot services and operator feedback

- [ ] Add shared sound cue names and helper utilities for autonomy/control
  scripts.
- [x] Implement the shared log-viewer module; Goob's example configuration
  includes it.
- [x] Implement the shared autonomy module with bounded argument descriptors
  and preserve the existing manager's process/dependency behavior.
- [x] Implement the shared administrative module with fixed reboot, Git pull,
  and service-restart actions and browser confirmation.
- [ ] Publish administrative action execution status; handlers currently
  acknowledge only that a request was queued.
- [ ] Add safe, bounded audio volume controls for supported ALSA devices.
- [ ] Add soundboard repeat and timed-repeat modes with a clear stop command.
- [x] Preserve Goob's MCU flashing startup pattern, then send lights-off after
  serial connection so MCU and bridge startup can be distinguished.
- [ ] Improve structured runtime diagnostics with service metadata and
  optional health states while keeping robot-wide availability separate from
  individual service health.

### 6. Explore an optional Jev decision layer

- [ ] Treat Jev (TypeSafe System One) as an optional typed, single-step decision
  component, not an LLM chat interface and not part of the fast control loop.
- [ ] If pursued, prototype an adapter that supplies bounded observations and
  receives discrete intents such as follow, stop, or request operator help.
- [ ] Keep outputs behind local validation, safety limits, and existing
  watchdog/stop behavior. Do not make Jev a dependency of basic Pebble robots.

**Acceptance:**

- [ ] A Jev prototype is removable, cannot bypass local safety controls, and
  has clear timeout/failure behavior.

## Verification status and remaining tests

- [x] Full Python unit suite: 168 tests passed. The runner needed `pyserial`
  installed into a temporary dependency target; no repository dependency files
  were changed for the local environment.
- [x] Focused module tests, Python syntax checks, and JavaScript syntax check
  passed for the telemetry presence change.
- [x] Broker-backed `tests/minimal_profile_smoke.py` passed for compact topics,
  commands, retained state, and offline Last Will behavior.
- [x] Live Goob retained manifest advertises only the battery telemetry
  endpoint and sets `presence: true`; ARL's live API reports Goob online, its
  last-seen receive time, and battery voltage.
- [x] `tests/module_browser_smoke.py` passed with Playwright/Chromium, covering
  two-broker discovery, custom controls, soundboard behavior, online/last-seen
  telemetry display, input handling, retained state, and no browser exceptions.
- [x] MiniLAMP compiled for `esp32:esp32:XIAO_ESP32C6` with the example
  placeholder configuration, PubSubClient 2.8.0, and ArduinoJson 7.4.3. The
  resulting sketch uses 76% of program storage and 13% of dynamic memory.
- [ ] Flash and bench-test MiniLAMP; no target board is attached to this host.
- [ ] Run broader reconnect and multi-robot browser integration coverage; the
  current browser smoke covers discovery across two brokers but not the full
  reconnect/load matrix.
- [ ] Run Goob's on-robot smoke suite for broader end-to-end validation. This
  telemetry-only rollout was checked through the retained manifest and ARL
  live API, without running the full robot smoke suite.

## Deferred by choice

- Explicit control ownership/locking.
- Requiring ROS or ROS 2 for core Pebble operation.
- Requiring every robot to publish a rich module manifest.
- Arbitrary robot-provided executable UI code in the web gateway.
