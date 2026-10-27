# MiniLAMP firmware bring-up — October 27, 2026

## Initial v1 bring-up history (superseded by on/off v2)

Firmware `minilamp-c6-wss-v1` replaces the old blocking MQTT client and full-power
quantizer. It preserves confirmed motor wiring/mixing and the D0 battery divider.
ESP-MQTT provides asynchronous MQTT 3.1.1 over certificate-verified WSS. Motor
safety runs in its own higher-priority task with independent one-second drive
and grouser timeouts, reconnect generation checks, bounded command buffering,
finite numeric validation, and a two-second hardware task watchdog.

Four LEDC channels are reused across motor direction changes. The robot-owned
manifest is generated from shared drive/telemetry modules and a custom grouser
module using the generic interface controls.

The configured Wi-Fi and broker credentials remain in the ignored private
header. No passwords are included in this report or its evidence.

## Build and transport checks

- Board: XIAO ESP32-C6, detected ESP32-C6FH4, USB serial 54:32:04:11:DE:A4 on ARL.
- Official Arduino CLI 1.5.1, Espressif Arduino core 3.3.12, default board settings.
- Warning-enabled compile: 1,208,500 sketch bytes, 45,152 static RAM bytes;
  no warnings. Fits the default 1,310,720-byte app partition (92%).
- Normal Arduino serial upload: all written regions hash-verified; application
  binary 1,208,640 bytes; normal application serial returned afterward.
- MQTT authentication at mqtt.sssn.us:443 with WebSocket path `/`: successful
  independently from ARL, then from the C6 itself with hostname/CA verification.
- Generated manifest: 3,430 JSON bytes, revision d042a29d2d15b853; validated
  with the shared Pebble validator. The board publishes it retained.

## Automated checks

`test_drive_safety.cpp` passed with -Wall -Wextra -Werror. It checks lease
boundaries, independence of drive/grouser refresh, stale commands, non-finite and
out-of-range values, mixing, stops, and millis rollover.

`tests/wss-v1-validation.json` records a 71.246-second WSS check (including a
60-second post-command soak), 16 status messages, 19 battery messages, increasing
uptime and one boot ID. All three zero-value commands (drive, grouser, legacy
wrapper) were accepted. Six invalid payloads (wrong type, missing field, bool,
NaN, oversized JSON, embedded NUL) were rejected without advancing the valid
motor timestamps. Software output states stayed zero. This check did not
physically measure GPIOs or motor outputs.

During that check the battery was disconnected and the board ran from USB;
0.348–0.364 V was the floating sense-input reading. The user then connected the
battery while keeping MiniLAMP lifted. Fresh telemetry reported 3.678 V; later
readings during pulse checks were about 3.64–3.65 V. The battery divider has not
been calibrated against a meter in this session.

## Motion checks and unresolved driver issue

The user confirmed MiniLAMP was lifted with wheels/grousers clear before any
nonzero command. A 30% forward pulse for 0.45 seconds was accepted over WSS;
USB diagnostics captured four software output values of 0.3, followed by zeros,
with no controller reset or LEDC API failure. The user saw no movement and
identified the DRV boards as inserted backwards.

After powering down/reseating the drivers, the user confirmed correct
orientation, power restored and continued lifted setup. Another 30% pulse and
then a 60% pulse produced the requested software output states and returned to
zero, but the user still saw no movement. Evidence is in
`tests/pwm-v1-pulse-validation.json` and `tests/pwm-v1-60-validation.json`;
their `pass` and `software_only` fields refer ONLY to software observations.
Motor function and directions are not verified. Do not infer failed hardware
solely from these software checks.

The controller was left stopped. Driver VM/VCC and SLP/nSLEEP voltage
measurements have been requested. At this stage, higher-power and sustained commands had not been sent. The
subsequent full-on comparison below resolved the movement question.

## Current production: on/off WSS v2

The user reported the motors jolting at startup and confirmed that MiniLAMP is
an on/off drivetrain. A supervised 0.45-second full-on comparison using the
initial firmware worked (`tests/pwm-v1-full-on-validation.json`). V2 therefore
restores the original behavior explicitly: quantize left/right/grouser controls
at a 25% deadband before mixing, then use direct GPIO forward/reverse/stop.
There is no PWM or variable-speed setting. The advertised drive UI now shows a
direction stick and stop, and the grouser slider has discrete -1/0/1 positions.

Current firmware ID: **minilamp-c6-onoff-wss-v2**. A warning-enabled Arduino
compile passed with 1,199,076 sketch bytes and 44,952 static RAM bytes. Normal
serial upload hash verification succeeded. Native safety/deadband tests passed.
The generated manifest is 3,343 bytes, revision **b576c760a79555b4**.

`tests/live-validation.json` records a **68.558-second** WSS check, including a
60-second post-command soak, 14 status messages and 16 battery messages. TLS,
discovery and zero/invalid command checks passed, with increasing uptime and no
reset. Battery telemetry was 3.686–3.728 V with the battery connected.

Supervised v2 forward, reverse and grouser pulses returned to zero after each
command, with no reported controller fault/reset. Their software snapshots
are in `tests/motor-pulse-validation.json`, `tests/motor-reverse-validation.json`
and `tests/motor-grouser-validation.json`. The user subsequently confirmed that
driving is working. Individual physical directions and grouser behavior were
not separately confirmed in that response.

TheMind's web-only gateway now discovers MiniLAMP from the same broker over
local TCP, renders its modules without page errors, and successfully sent two
zero commands through to the C6's WSS client. No credentials were committed or
included in the Docker image.

## Still pending

- Independently confirm each wheel's direction and reverse/grouser physical
  operation, then longer loaded operation and supply/noise checks.
- Verify retained-command replay rejection and reconnect behavior on the actual
  network; measure electrical stop timing on lost command/network/reset.
- Scope boot behavior of D6/UART0, or move that motor input to a non-UART pin
  or provide a hardware enable gate in a later hardware revision. The user
  observed the same startup jolt as with the older firmware.
- Independently compare battery voltage to a meter.
