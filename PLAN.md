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
  availability. ARL's live interface has been verified with Goob.
- Camera and audio capture are demand-controlled. Robot-to-gateway and
  gateway-to-robot audio use Opus over MQTT. Goob uses H.264 over MQTT; the
  benchmark and live recovery results are documented in `modules/H264.md`.
- Sparkplug B was reviewed for useful ideas. Pebble has adopted lightweight
  lifecycle availability, retained discovery snapshots, boot IDs, and clear
  distinctions between robot and module-host health without requiring the
  Sparkplug payload and topic model.
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

## Roadmap

### 1. Simplify and separate configuration — underway

Reduce repeated identity, topic, broker, and media settings while making the
robot runtime and web deployment independently understandable.

- Make robot-local topic overrides relative to the configured identity, using
  `subtopic`; retain a clearly explicit route for fully qualified external
  topics and a compatibility path for existing configs.
- Establish ownership boundaries: robot identity, hardware, and enabled
  services belong to robot configuration; web bind/history settings and its
  list of controlled robots belong to web configuration.
- Split the current unified config into robot and web configs without making
  one file per process. Keep optional audio/video profiles only where they
  improve reuse or isolate real device-specific settings.
- Resolve settings centrally with documented defaults and precedence. Services
  should receive validated, resolved settings rather than each interpreting
  partial files independently.
- Reduce duplicated media values by distinguishing capture formats/devices
  from published stream profiles. Derive equal values by default and allow
  explicit overrides when capture and network output differ.
- Consider named MQTT connection profiles where several services share broker
  settings. Keep local, remote, and web broker roles distinct when their
  endpoints or credentials differ.
- Keep secrets out of shareable example configs, using protected files or
  environment references.
- Migrate example configs first; preserve existing deployments through a
  compatibility loader or clear migration tool. Add schema validation and
  tests for resolved topics, cross-robot references, and precedence.

**Completed slice:** centralized robot-local topic resolution and migrated
robot, module-adapter, and web-target examples to `subtopic` fields while
keeping legacy full topics supported. Focused tests verify namespace safety
and unchanged effective media/capability topics.

**Acceptance:** Goob and the primary Pebble/web deployment resolve to the same
effective topics and service behavior as before; a multi-robot web config can
still address robots outside the local runtime identity; invalid local
subtopics cannot escape the robot namespace; legacy configs have a documented
migration path.

### 2. Make the minimal MQTT contract easy to implement

- Finish migrating robot-owned services toward compact `i/` and `o/` topics;
  keep adapters for deployed legacy consumers.
- Document a minimal robot profile that needs only identity, MQTT topics,
  payloads, and availability.
- Add a small conformance checklist and simulated client tests for both a
  Linux robot and an MCU/ESP32-style client.
- Continue evaluating Sparkplug-inspired lifecycle, discovery, sequence, and
  stale-data practices individually. Adopt only pieces that improve recovery
  or interoperability while keeping the Pebble standard easy to read and
  implement; do not require Sparkplug's full protocol model.

**Acceptance:** a basic robot can publish state and consume commands without
running Pebble's Linux services, module host, or web interface.

### 3. Grow robot-owned controls and the shared module library

- Continue promoting proven robot-local modules into `modules/` with stable
  type/version contracts and examples.
- Keep promoting robot-owned capabilities into reusable shared modules. The
  soundboard module now defines its command, file-list, status, and layout
  contract; each robot opts in and adapts it to its playback service and sound
  collection. The generic renderer handles the control without Goob-specific
  web code.
- Keep input-to-action mapping in the module contract so keyboard, gamepad, and
  future inputs use the same command path.
- Add module examples for common drive variants and one or more robot-specific
  mechanisms such as a gripper.
- Improve authoring with a simple layout designer or reusable widget library
  when real module needs show where hand-authored JSON is cumbersome.
- Keep browser execution generic and bounded; robot modules should not need to
  send arbitrary HTML or JavaScript to the web gateway.
- Extend browser, gateway, reconnect, invalid-manifest, and multi-robot tests.

**Acceptance:** adding a shared or robot-specific control does not require a
robot-specific web page or gateway code, and commands remain schema-validated.

### 4. Improve media quality and two-way conversation

- Keep H.264 and Opus as current robot-to-broker formats; measure on target
  hardware before adding encoder or decoder dependencies.
- Evaluate browser-native H.264/WebCodecs and native Opus paths to reduce the
  gateway JPEG and browser PCM conversion work.
- Design acoustic echo cancellation for both ends of a two-way call: the
  operator client and the robot playback/capture path. Prototype against
  realistic speaker/microphone setups and measure echo reduction, speech
  quality, delay, CPU use, and behavior during push-to-talk transitions.
- Keep media demand controls and resource lifecycle behavior robust under
  reconnects, competing requests, and unavailable devices.
- Report explicit media-source-unavailable and stream-health diagnostics.

**Acceptance:** media recovers cleanly after network and device interruptions;
any echo-cancellation change is evaluated at both ends and does not impair
one-way listening or speech intelligibility.

### 5. Improve robot services and operator feedback

- Add shared sound cue names and helper utilities for autonomy/control scripts.
- Expose autonomy scripts through a robot-defined module so robots can publish
  their available scripts and the interface can start/stop them without a
  Pebble-wide hard-coded autonomy panel.
- Add robot-defined light-control, log-viewer, and telemetry modules, using the
  existing interface behavior as a reference while keeping each module
  optional and robot-owned.
- Add safe, bounded audio volume controls for supported ALSA devices.
- Add soundboard repeat and timed-repeat modes with a clear stop command.
- Preserve the MCU's flashing startup pattern, then have `serial_mcu_bridge`
  send a lights-off command immediately after establishing serial
  communication. This gives a visible startup check for MCU power, bridge
  connectivity, and successful command delivery.
- Improve structured runtime diagnostics with service metadata and optional
  health states while keeping robot-wide availability separate from individual
  service health.

### 6. Explore an optional Jev decision layer

- Treat Jev (TypeSafe System One) as an optional typed, single-step decision
  component, not an LLM chat interface and not part of the robot's fast control
  loop.
- If pursued, prototype an adapter that supplies bounded observations and
  receives discrete intents such as follow, stop, or request operator help.
- Keep all outputs behind local validation, safety limits, and existing
  watchdog/stop behavior. Do not make Jev a dependency of basic Pebble robots.

**Acceptance:** a Jev prototype is removable, cannot bypass local safety
controls, and has a clear timeout/failure behavior.

### Next implementation slice: shared sound cues and helpers

- Build on the robot-owned soundboard module with stable cue names and helper
  utilities that autonomy and control scripts can use without hard-coded file
  paths.
- Keep the cue layer optional and robot-defined so robots can map semantic cues
  to their own recordings or playback hardware.
- Continue with bounded volume controls and repeat/timed-repeat modes when the
  soundboard contract and target hardware behavior are clear.

## Deferred by choice

- Explicit control ownership/locking.
- Requiring ROS or ROS 2 for core Pebble operation.
- Requiring every robot to publish a rich module manifest.
- Arbitrary robot-provided executable UI code in the web gateway.

## Config boundary implementation

Split robot runtime settings from web deployment settings in a reviewable
slice. Keep the existing unified config working through a compatibility
adapter, and verify that Goob's runtime and the multi-robot web interface still
resolve the same effective settings. Extract audio/video profiles only after
that boundary is clear.

The robot/web boundary is represented by separate examples and runtime paths.
The loader prefers `robot.json`, the web UI prefers its own
`web-interface/config.json`, and the old unified file remains a fallback.
Goob and the ARL web service are running with the split files. Goob's local
topics resolve through subtopics with unchanged effective MQTT endpoints.
Audio/video profile extraction remains a later step after this boundary has
been exercised on Goob and the web host.

**Completed slice:** the serial MCU bridge accepts
`services.serial_mcu_bridge.drive.swap_motor_outputs` as a boolean. The default
preserves left/right order; Goob's example enables the swap for its current
motor wiring, and Goob's live robot config enables it.

**Completed verification:** the former web-test failures are resolved; the
complete 168-test suite passes. The Goob battery and soundboard paths have
also been verified on the live robot.
