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
  keyboard/controller mappings. Shared drive controls and a robot-local
  gripper example in the repository demonstrate reusable and robot-specific
  modules.
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
  uses compact topics, retained state and an MQTT Last Will. Its current WSS
  implementation obtains SNTP time for TLS certificate validation; the minimal
  component profile itself does not require time synchronization.
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
- Goob's MCU input pins previously exposed as touch/odometry samples were
  repurposed encoder connections, not touch sensors. A brief elevated test at
  15% PWM showed strong changes on both odometry channels while the wheels
  turned, but the encoder readings have not been reliable enough for useful
  odometry. The user has now replaced the encoders with the existing MPU-6050
  IMU on the XIAO SAMD21 I²C pins (D4/SDA, D5/SCL). This leaves one MCU pin for
  future expansion. Goob also has two spare transistor-driven outputs:
  currently only the red channel is connected to the white headlight LEDs.
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
- [x] Flash current MiniLAMP firmware and verify authenticated WSS, discovery,
  telemetry and zero/invalid command handling. See its October 27 REPORT.
- [x] Restore MiniLAMP on/off drive. The user corrected reversed drivers and
  confirmed driving works; v2 preserves the original 25% command deadband and
  full-on GPIO outputs. Fractional PWM was unsuitable for this platform.

### 3. Grow robot-owned controls and the shared module library

- [x] Promote shared drive, soundboard, lights, telemetry, logs, autonomy, and
  administrative modules into `modules/` with versioned contracts and examples.
- [x] Keep robot-specific mechanisms in robot-owned modules, while rendering
  them through the generic interface. The gripper module is a repository
  example and is not enabled on Goob.
- [x] Keep input-to-action mapping in module contracts so keyboard and gamepad
  inputs use the same command path.
- [x] Support named keyboard actions for specialized shared widgets: P plays
  the selected sound, while F toggles lights between off and the last selected
  solid or flash mode. Both defaults remain remappable in the shared editor.
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

#### Goob sensor and output hardware follow-up

The physical encoder-to-IMU wiring change must happen before any dependent
software is implemented or deployed. Keep the spare MCU pin and transistor
outputs available for future robot-specific additions; do not assign them a
Pebble contract until a concrete use case is selected.

- [x] **Hardware prerequisite (user task):** replace Goob's encoder wiring with
  the existing MPU-6050 IMU on XIAO SAMD21 D4/SDA and D5/SCL; preserve one MCU
  pin for future expansion. The user reports the wiring is complete.
- [x] **Depends on hardware prerequisite:** flash Goob's updated SAMD21
  firmware. Uploaded to the replacement XIAO from Legion on COM3; BOSSA
  checksum verification succeeded. The replacement is now installed on Goob;
  its serial bridge opens successfully, but firmware logs show repeated MPU-6050
  initialization failures. Verify the I²C wiring/address, then validate stable
  sensor samples and battery/drive/lights behavior.
- [x] **Software configuration:** retire Goob's analog encoder sampling and
  disable its wheel-odometry shared-memory writer; keep generic odometry support
  only if another robot uses it.
- [x] **Software implementation:** add MPU-6050 serial parsing/publishing and a
  reusable telemetry endpoint/widget for orientation, acceleration magnitude,
  temperature, and sample health. Serial communication and live IMU telemetry
  are verified after a full Goob power cycle; the root cause of the earlier
  initialization failures remains unknown.
- [ ] Keep orientation and other direct IMU measurements available as regular
  telemetry, with high-rate sampling/filtering on the MCU and a lower-rate
  summary over MQTT. Do not define a robot-wide pose or localization estimate
  as core telemetry.
- [x] Add a reusable 3D orientation widget to the web module interface. A
  robot module selects the widget, binds it to orientation telemetry, and
  references a static GLB outside MQTT. It loads a local test GLB, applies
  roll/pitch(/yaw), supports camera orbit/zoom, and accepts a per-robot static
  `model_rotation_deg` to align model and IMU coordinate frames. Browser
  integration passes.
- [x] Configure and deploy Goob's orientation widget with its GLB URL; publish
  the model through Swag and allow cross-origin reads for public `.glb` files.
  Confirmed the live ARL interface loads it alongside Goob's IMU telemetry.
- [x] Add an operator-triggered gyro recalibration control for robots that
  advertise the optional calibration endpoint. Goob's firmware command stops
  motor outputs and averages 400 samples while the operator keeps the robot
  stationary. Compiled and flashed the Seeed on ARL with peripherals disconnected;
  BOSSA checksum verification succeeded.
- [x] Reinstall the Seeed on Goob and deploy the calibration endpoint, bridge,
  telemetry schema, and ARL web control. Verified the live manifest advertises
  `imu_calibrate`, fresh telemetry reports calibration generation and gyro bias,
  IMU samples continue without errors, and ARL serves the updated widget.
- [ ] From the UI, trigger calibration while Goob is stationary; verify the
  generation increments, the new bias is shown, and stationary yaw drift falls.
- [x] **Incident review (2026-10-26):** ARL's live log cache records repeated
  MPU-6050 I²C initialization and sample-read failures before the reported
  drive-control loss, two completed manual gyro calibrations, then an MCU USB
  disconnect/reconnect. The configured MongoDB history is disabled on ARL; the
  local Mongo archive has no current `pebble/robots/goob` records, so it cannot
  establish the exact MQTT drive values.
- [x] Add a SAMD21 independent watchdog and shorten the MCU's idle motor
  timeout to one second. Main-loop watchdog feeds occur after serial, drive,
  sensor, and telemetry work; startup latches motor pins
  low and reports watchdog resets. Compiled on ARL and flashed successfully
  with BOSSA checksum verification.
- [ ] Bench-test watchdog behavior with the Seeed connected to its IMU and
  robot peripherals. The earlier bare-board reset loop was initially attributed
  to a possible I²C stall without capturing reset cause; later review found the
  watchdog clock was never explicitly configured. Motor driver inputs were
  disconnected, so their electrical state during reset has not been measured.
- [x] Replace manual SAMD watchdog setup with Adafruit SleepyDog 1.8.4,
  which configures its independent clock. Add a read-only `V` command for
  firmware ID, uptime, reset cause, watchdog period, and IMU readiness.
  V2 retains the 1000 ms watchdog and adds IMU/motor state diagnostics. Its
  on-target simulated-sensor harness verified an intentional watchdog reset
  and IMU probing lockout for that boot; USB/drive/light handling stays available.
- [x] Make absent-IMU startup return safely with USB and commands available.
  V2's bounded adapter handles line-low/NACK/error states and retries with
  backoff. Legion's bare-board soak passed for 600.687 seconds without reset.
  Physical externally stuck-line tests remain separate pending hardware checks.
- [x] Add a Goob-side watcher that waits for the Seeed's UF2 boot drive and
  copies a prebuilt firmware image, restoring `pebble-control.service` after
  the attempt. A read of the bootloader's `CURRENT.UF2` flash snapshot shows
  the compiled application image byte-for-byte at `0x2000`, confirming the UF2
  write.
- [x] Confirm the corrected application exits bootloader and responds without
  a working IMU. V2 passed the ten-minute bench soak; see
  `firmware/GOOB_seeed-xiao-samd21/REPORT.md` and its recorded validation assets.
- [x] Reconnect the actual IMU and verify production orientation telemetry,
  manual calibration, and command responsiveness. Legion measured 100.002 Hz,
  zero sample errors over 60 seconds, and a responsive 400-sample calibration.
  Goob is now running the flashed MCU; live bridge MQTT shows advancing IMU
  sample counts with zero errors and battery voltage near 4.07 V.
- [ ] Run prolonged connected-IMU stability and repeated power-cycle/reconnect
  checks. Test physical sensor disconnect/reconnect, externally stuck SDA/SCL,
  and partial-read faults. Measure motor command-loss stop timing during sensor
  recovery and motor-driver output states during watchdog reset; check power
  integrity/noise under load. Previous simulated tests and ACKs do not measure
  these electrical behaviors.
- [x] Bound serial command length and processing per loop: 128 payload bytes,
  fixed storage, 64 received bytes per pass. Legion tested overlong/NUL input,
  malformed motor values and 185,856 bytes of sustained input with 768 V replies.
- [x] Make IMU initialization/calibration incremental and stop motors on entry
  or sensor faults. Calibration rejects nonzero drive and has a deadline;
  main-loop motor-timeout servicing continues between steps. Verified by
  on-target simulated tests and real manual calibration. Periodic automatic
  recovery attempts conservatively interrupt active driving.
- [x] Reset the estimator's sample timestamp after calibration/recovery and
  gaps over 100 ms. Tested a simulated two-second gap and actual calibration;
  fresh samples restart relative yaw rather than integrating the missing time.
- [x] Replace SAMD Wire calls with a project-owned bounded SERCOM2 adapter.
  Legion verified real MPU register writes/reads, address NACK (120 us), and
  clock-stall timeout/recovery (10,014 us including overhead). The watchdog
  remains the final fallback; no installed Arduino core patches are required.
- [ ] Add selective durable drive-command and watchdog diagnostics (timestamps
  and mapped motor values, without media payloads) so future command-loss events
  can be attributed to MQTT input, bridge forwarding, or MCU handling.
- [ ] Replace Euler-angle orientation state with a normalized quaternion
  estimator and publish the quaternion directly. Compose the IMU-to-body and
  model-to-viewer offsets as quaternions; keep Euler values as derived display
  fields. Validate yaw direction upright and inverted, and roll/pitch through
  the apex. MPU-6050 yaw remains relative and will drift without a magnetometer.
- [ ] Confirm the model axes and IMU orientation mapping against the robot.
  An OBJ/MTL source can be converted to a single GLB for delivery.
- [ ] Implement localization as opt-in autonomy scripts, which may also serve
  as utility scripts. Possible independent scripts include IMU-only relative
  localization and, once tag locations and camera geometry are established,
  localization that fuses IMU measurements with AprilTag observations. Each
  script should document its frames, assumptions, validity, and uncertainty;
  localization outputs belong to the script's interface rather than a
  universal pose topic.
- [ ] Document Goob's two spare transistor-driven outputs and current wiring
  (red channel drives white headlights). Defer firmware controls and a shared
  lights contract change until a concrete use for the remaining output(s) is
  chosen.

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
- [x] MiniLAMP v2 compiled and hash-verified on its actual C6 connected to ARL,
  using Espressif core 3.3.12 and native ESP-MQTT/cJSON. The sketch uses
  1,199,076 flash bytes (91%) and 44,952 static RAM bytes (13%). Native drive
  safety/deadband tests and a 68.558-second WSS verification passed.
- [ ] Run broader reconnect and multi-robot browser integration coverage; the
  current browser smoke covers discovery across two brokers but not the full
  reconnect/load matrix.
- [ ] Run Goob's on-robot smoke suite for broader end-to-end validation. This
  telemetry-only rollout was checked through the retained manifest and ARL
  live API, without running the full robot smoke suite.
- [x] Restore Goob's live MPU-6050 telemetry after a full battery disconnect
  and reconnect. Hardware-I²C MQTT samples were observed at sequences 16901
  and 18341, with matching successful-sample counts and zero sample errors.
  Earlier firmware diagnostics had reported `WHO_AM_I-read` address NACK
  (SAMD `Wire` status 2); changing to one-time `Wire.begin()` and matching the
  basic register-read sequence to Lawlor's AERO-Beam example did not resolve
  that failure. A temporary I²C scanner's serial output was not captured, so it
  was inconclusive. The user confirmed AD0 low, SDA/SCL pull-ups to 3.3 V, and
  approximately 3.311 V idle on both bus lines, then replaced the GY-521. A
  subsequent full battery power cycle restored valid production `Wire`
  telemetry without a firmware or wiring change during that cycle. The IMU is
  currently treated as working; the cause of the earlier failure is unknown.
- [ ] Verify recovery behavior across a controlled MCU-only reset versus a full
  power cycle, if the initialization issue returns. Do not disturb the current
  working setup just to reproduce an unexplained fault.

## MiniLAMP standalone firmware and TheMind gateway — October 27

- [x] Use verified WSS to `mqtt.sssn.us:443/`, with private credentials,
  native ESP-MQTT's separate network task, and an independent motor task.
- [x] Restore on/off GPIO control and legacy grouser mixing, with independent
  one-second drive/grouser leases, queue/validation limits and a task watchdog.
  Output permutation/inversion and pin settings remain configurable.
- [x] Generate MCU discovery from shared drive/battery modules and a custom
  grouser descriptor. Render it with existing generic web controls; remove
  variable-speed sliders for this on/off drivetrain.
- [x] Validate fresh battery readings (~3.65–3.73 V), zero/invalid commands,
  normal upload and forward/reverse/grouser software states. User confirmed
  driving works. Preserve scripts and evidence in the target firmware folder.
- [ ] Independently confirm each wheel's orientation, reverse/grouser physical
  operation, retained-command replay rejection, network-loss stop timing,
  electrical output behavior during reset, and longer loaded operation. The
  startup jolt on D6/UART0 is observed; a future hardware change can relocate
  that input or add an enable gate. Compare battery readings with a meter.
- [x] Deploy TheMind's web-only gateway on Tailscale port 8080, using the same
  active web assets as ARL and the local Mosquitto broker. Ignore private config
  in Git and Docker build context. Both robots are discovered automatically.
- [x] Move Goob's remote bridge to authenticated TCP `100.64.0.5:1883` over
  Tailscale, preserving the robot-local broker and serial bridge process.
- [x] Verify remote Tailscale HTTP access, browser rendering of both robots and
  Goob's 3D widget without page errors, and zero-drive routing through the web
  gateway to Goob's local adapter and MiniLAMP's WSS client. Updated module
  fixture tests pass (9 tests), including generated MCU manifest consistency.
  Added the missing container `libopus0` dependency; Opus and H.264 codec
  round trips pass inside the deployed image. Container health check passes.
- [ ] Investigate Goob's currently stale IMU stream at its local broker.
  Battery/control remain live; the gateway displays the last valid IMU sample.
  Do not treat cached orientation as a fresh measurement.
  Add per-topic stale indications to shared telemetry/orientation widgets so
  robot-wide availability cannot be mistaken for fresh sensor data.
- [ ] Exercise Goob's real camera/listen/talk and other actuator modules through
  TheMind's gateway. Rendering and zero-drive routing are verified; real media
  and nonzero Goob actuation were not exercised during migration. Browser Talk
  needs HTTPS or another browser-trusted secure origin; plain Tailscale HTTP
  does not grant microphone capture in a default browser configuration.

## Deferred by choice

- Explicit control ownership/locking.
- Requiring ROS or ROS 2 for core Pebble operation.
- Requiring every robot to publish a rich module manifest.
- Arbitrary robot-provided executable UI code in the web gateway.
