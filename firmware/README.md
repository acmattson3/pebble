# Firmware Reference Implementations (Arduino)

This directory preserves MCU firmware examples from the original Pebble project.
These files are reference implementations, not a required Pebble firmware layout.
Pebble does not depend on these exact boards, filenames, or flashing scripts.

If you are integrating a different controller platform, treat the contents of
this directory as examples of how Pebble topics and device behavior can be
mapped onto embedded firmware.

The current USB-serial MCU direction is documented in
`control/services/serial_standard.md`. New serial-only MCU firmware should
target `pebble_serial_v1` rather than inventing a one-off text protocol.

## Included reference targets

- `firmware/FRED_seeed-xiao-samd21/`
  - Reference firmware for the original Fred platform.
- `firmware/GOOB_seeed-xiao-samd21/`
  - Goob's current Seeed XIAO SAMD21 firmware for drive, lights, battery voltage, and its MPU-6050 on I²C (`D4/SDA`, `D5/SCL`).
  - Uses the existing Goob serial command protocol and publishes IMU samples to the host bridge.
- `firmware/GOOB_seeed-xiao-c6/`
  - Experimental Goob firmware port for the XIAO ESP32-C6. It has been compiled
    and flashed/tested on ARL only; the C6 has not yet been installed on Goob.
  - Battery-voltage telemetry is intentionally disabled: Goob's battery sense
    is on D6, which is not ADC-capable on the C6. See its README before moving
    the board to Goob.
- `firmware/GOOB_arduino-nano-imu/`
  - Archived standalone Nano bridge implementation; Goob no longer uses this separate MCU.
- `firmware/HS105_seeed-xiao-c3/`
  - Legacy USB-serial station-mode bridge for four TP-Link HS105 outlets.
  - The XIAO ESP32-C3 joins a router-hosted Wi-Fi LAN as a normal client, talks to four statically addressed HS105 plugs over the legacy Kasa LAN protocol on port `9999`, and exposes four logical outlet slots over `pebble_serial_v1`.
  - Current Mind runtime no longer starts this bridge. Keep this target only as an archived reference for how the old smart-switch path worked.
- `firmware/MIP_seeed-xiao-esp32s3-sense/`
  - Direct MQTT firmware for a Seeeduino XIAO ESP32-S3 Sense with camera, wired to a WowWee MiP robot UART.
  - The sketch publishes Pebble-standard MQTT topics itself rather than using `serial_mcu_bridge`, because the XIAO owns Wi-Fi, MQTT, camera capture, and the MiP UART link.
  - Wiring:
    - `XIAO D6 / GPIO43 TX -> MiP RX`
    - `XIAO D7 / GPIO44 RX -> MiP TX`
    - common ground between XIAO and MiP
  - Do not power the XIAO directly from the MiP raw battery rail; use a suitable regulator/buck supply.
  - MQTT video is intentionally low bandwidth: off by default, 1 FPS, low resolution, keyframe-only, no delta frames.
- `firmware/MINILAMP_seeed-xiao-c6/`
  - Current standalone MiniLAMP C6 firmware: four motors, battery ADC, verified
    MQTT-over-WSS, independent motor safety task, and robot-owned discovery.
  - See its README and REPORT for the tested build and pending driver-power
    investigation. Firmware/transport checks pass; physical movement is not
    yet verified.
- `firmware/BUMPERBOT_wemos/`
  - Reference firmware for the original BumperBot platform.

## Goob IMU wiring and bring-up record

The current SAMD21 production build is **`goob-samd21-bounded-recovery-v2`**,
flashed on Legion and installed on Goob on October 27. Its project-owned
`BoundedI2C.h` replaces Wire with transactions limited to a 10 ms budget;
initialization/calibration run incrementally, serial input has fixed storage
and per-loop processing limits, and the independent one-second watchdog is
retained. After a watchdog reset the IMU is disabled for that boot to prevent
repeated sensor-related resets. Recovery retries stop active drive, so an
intermittently failing IMU can still interrupt driving.

The [target README](GOOB_seeed-xiao-samd21/README.md) describes current behavior.
The [Legion report](GOOB_seeed-xiao-samd21/REPORT.md) and accompanying test assets
record the ten-minute bare-board soak, serial stress tests, on-target simulated
tests including a watchdog reset, and connected-sensor tests. Connected IMU
sampling measured 100 Hz with zero errors; real NACK and a deliberately stalled
I2C clock returned within the adapter's bounded recovery path. Goob's live MQTT
bridge also publishes increasing IMU sample counts with zero errors and battery
readings near 4.07 V. Physical disconnect/stuck-line tests, prolonged connected
operation, and electrical motor-output timing remain pending in `PLAN.md`.

The following bring-up history includes earlier Wire-based builds and upload
attempts; those intermediate implementations are superseded by v2.

Goob's wheel encoders were removed and their MCU pins repurposed for an MPU-6050
GY-521 module. The IMU is connected directly to the existing Seeed XIAO
SAMD21; the former separate Arduino Nano IMU path is not used.

The user reports the IMU board is centered on the motor-shaft axis, with its
back (the side away from the pins) facing forward and its board plane parallel
to the chassis bottom. The Goob orientation widget currently configures
`model_rotation_deg.x = -90`; this visual alignment is being checked against
live roll, pitch, and yaw movement.

Current wiring and observed electrical state:

- GY-521 `VCC` to the Seeed XIAO's 3.3 V output and `GND` to common ground.
- GY-521 `SDA` to XIAO `D4` and `SCL` to XIAO `D5`.
- `AD0` is tied low through 4.7 kΩ, selecting I²C address `0x68`.
- External 4.7 kΩ pull-ups are installed from SDA and SCL to 3.3 V.
- SDA/SCL idle voltage was measured at approximately 3.311 V.
- The sensor powers on. These observations confirm idle levels and wiring intent,
  but do not verify signal quality during I²C transactions.

Bring-up history (October 2026):

1. Initial integration reported repeated IMU initialization errors. The firmware
   was changed to use the XIAO's hardware I²C pins and initialize `Wire` once,
   keeping the bus active through sensor probe retries. The bus is set to
   100 kHz; reads use a register-pointer write followed by a repeated-start read,
   following the general approach in Dr. Orion Lawlor's AERO-Beam MPU example.
2. The error persisted at `WHO_AM_I-read`, with SAMD `Wire` error 2 (address
   NACK) and zero bytes received. Matching the basic read sequence did not
   resolve it. A temporary address-scan firmware was attempted, but its serial
   output was not captured, so that scan provided no reliable result. The Goob
   firmware was restored afterward.
3. The user replaced the GY-521 module and confirmed AD0 low, pull-ups to 3.3 V,
   and approximately 3.311 V idle on both bus lines. These checks alone did not
   explain the NACK.
4. After a full battery disconnect/reconnect, the IMU began publishing valid
   telemetry without a firmware or wiring change during that power cycle.
   Subsequent live checks received sequence `18341`, `samples_ok: 18341`, and
   `samples_error: 0`, with acceleration, gyro, temperature, and orientation
   values present. This confirms it was still working at the latest check. The
   cold power cycle is the only change correlated with recovery; the root cause
   is not yet established.

5. On 2026-10-22, the user moved GY-521 VCC from the shared 5 V boost output to
   the XIAO's 3.3 V output. The IMU initially streamed at 100 Hz with zero
   sample errors. A later check found repeated I²C address NACKs; after the
   Seeed was physically replugged and the control service reopened its serial
   device, fresh readings resumed with zero errors. This supports the 3.3 V
   feed but needs longer runtime observation to confirm reliability.
6. The firmware source in this directory samples the IMU at 100 Hz and emits
   orientation summaries at 8 Hz. Goob's installed MCU firmware was left
   untouched during the 2026-10-22 viewer update; its SBC bridge now builds
   the viewer's 8 Hz module updates from the existing high-rate IMU stream.
   Relative yaw is integrated from the bias-corrected Z gyro rate.
   Yaw is zeroed on IMU initialization and will drift because the MPU-6050 has
   no magnetometer; it is not an absolute compass heading.

7. On 2026-10-22, a stationary live sample was collected for 20 seconds from
   Goob's high-rate IMU stream (about 100 Hz, 2,003 samples). Roll/pitch and
   acceleration were steady, but the bias-corrected Z gyro averaged
   `-4.350 dps` (standard deviation `0.014 dps`). Integrated yaw changed about
   `-87.1°` in those 20 seconds, equivalent to roughly `-261°/minute` if the
   offset persisted. The MCU's reported startup gyro bias was only
   `+0.705 dps` on Z; the current temperature was about `28.5°C`. This points
   to a large residual gyro offset, likely because the startup calibration did
   not match the sensor's later zero-rate bias or was collected under different
   conditions. It is far larger than the observed sample-to-sample noise.
   No firmware was flashed and no robot or MCU reset was performed during this
   check. The viewer's “Reset orientation” only changes its display reference;
   it does not correct the MCU's gyro bias or stop future yaw drift.

   A useful next calibration should wait for the IMU to warm up, confirm the
   robot is motionless, then average gyro readings for several seconds and
   reject/retry if the gyro variance or acceleration changes indicate motion.
   Since the MPU-6050 has no magnetometer, accelerometer data cannot determine
   yaw bias while the robot may be rotating. Any ongoing adaptive yaw-bias
   correction therefore needs a trustworthy stationary condition; blindly
   learning bias during normal operation could erase real turns.

   The Goob firmware now defines a manual serial `C` command for recalibrating
   the gyro while the robot is deliberately stationary. It stops the motors,
   averages 400 samples over roughly four seconds, keeps the existing yaw
   display reference, and reports a new calibration generation and bias in
   summary telemetry. The robot-defined orientation widget can expose this as
   “Calibrate gyro.” This command requires the matching MCU firmware to be
   flashed before enabling the button on a live robot.

8. Follow-up on 2026-10-22: restarting `pebble-control.service` did not reset
   the MCU. A subsequent USB hub port-3 power cycle disconnected/re-enumerated
   the Seeed, and the webcam and USB audio devices were present again
   afterward, but MCU uptime continued increasing and the reported startup
   bias remained `+0.705 dps` on Z. The corrected gyro was still about
   `-4.3 dps`. The USB port cycle therefore did not actually remove power from
   the MCU (it may have another power path, or the hub's reported port-power
   state may not reflect VBUS removal). A true MCU power cycle is still needed
   to test startup recalibration; do not assume restarting Pebble or
   disconnecting its USB interface performs one.

When troubleshooting a recurrence, record whether a full sensor+MCU power cycle
recovers it, and compare that with an MCU-only reset while leaving the IMU
powered. Do not treat the unsuccessful serial scan attempt as evidence that
either address was or was not detected.

### 2026-10-26 motor hold incident and watchdog update

ARL's live log cache recorded repeated MPU-6050 I²C initialization and sample
read failures before Goob stopped accepting drive input. The user confirmed the
two logged manual gyro recalibrations completed at 18:40 and 18:42 UTC. Later
logs show the MCU USB disconnect/reconnect; the ARL Mongo history setting was
disabled, and the local archive had no current `pebble/robots/goob` records, so
the exact MQTT drive values were unavailable.

The SAMD21 `Wire` implementation used for this build polls SERCOM status flags
without a transaction deadline. If a read stalls, it can prevent both serial
command handling and the firmware's software motor timeout from running, while
the PWM pins retain their last output. The sketch now enables a roughly
one-second independent SAMD21 watchdog, feeds it only after the main loop's
serial, drive, IMU, and telemetry work completes, and reduces the firmware's
idle motor timeout from five seconds to one second. It forces motor output
latches low at startup and prints a warning after a watchdog reset.

This source revision compiled on ARL for `Seeeduino:samd:seeed_XIAO_m0` and
uses 49,372 bytes of flash. It was flashed to the Seeed while disconnected from
Goob's peripherals; BOSSA reported a successful flash checksum. Afterward, the
board entered bootloader mode and repeated upload attempts could not catch its
short bootloader window. With no peripherals connected, the application
reappeared briefly and then disconnected again. This is consistent with the
watchdog recovering from an I²C transaction stalled by the unconnected,
floating bus, but a serial log of the reset cause was not captured, so that
explanation remains unconfirmed.

Do not treat this bare-board cycle as proof of a Goob hardware fault or a
successful test of the motor failsafe. The motor outputs were disconnected, so
their electrical state during reset was not measured. Verify operation with
the IMU and robot peripherals connected, confirm the MCU remains online, and
measure that motor driver inputs stay low during a deliberately stalled I²C
transaction. Bounded/recoverable I²C transactions remain future work.

The sketch now enables the SAMD21's internal pull-ups on SDA/SCL before
starting `Wire` and checks both lines before each MPU transaction. This should
make an absent IMU return a normal I²C NACK instead of leaving floating lines
to stall the SERCOM; a line already held low is skipped and reported while the
main loop continues feeding the watchdog. The updated source compiles at
49,596 bytes. A UF2 image was generated for the SAMD21 application base
(`0x2000`) and copied to Goob's `Arduino` boot drive using
[`watch-and-flash-goob-samd21.sh`](watch-and-flash-goob-samd21.sh). The copy
completed and the bootloader USB device disconnected. A later read of the
bootloader's `CURRENT.UF2` flash snapshot contained 1,024 valid SAMD21 UF2
blocks; the bytes at application address `0x2000` matched the compiled image
exactly. This confirms the image is in flash. It does not confirm that the
application boots. At that point Goob saw the Seeed in bootloader mode
(`2886:002f`), not as the running application.

On 2026-10-26, the SAMD21 was returned to ARL with the Blink sketch running.
Arduino CLI identified it as `Seeeduino XIAO` on `/dev/ttyACM1`; the Goob
firmware compiled to 49,596 bytes and was uploaded using the normal Seeed
SAMD/BOSSA serial upload. The uploader verified all 50,288 bytes by checksum.
After reset, the application USB device (`2886:802f`) repeatedly disconnected
and reappeared at roughly one-second intervals while the board was bare on ARL.
This pattern is consistent with watchdog resets during startup/IMU probing, but
the reset cause was not captured. Do not install this image on Goob until it is
shown to stay online with the IMU path connected or the absent-IMU reset loop
is fixed. No motor outputs were connected or tested.

Follow-up source review found that the manual watchdog setup did not configure
its generic clock, despite assuming a 1024 Hz clock. The Seeed Arduino core
resets the generic-clock controller during startup. The firmware now uses
[Adafruit SleepyDog](https://github.com/adafruit/Adafruit_SleepyDog) 1.8.4 to
configure the independent watchdog clock and its one-second period. It also
accepts a read-only `V` command that reports firmware ID, uptime, reset cause,
watchdog period, and IMU readiness. This build compiles at 50,292 bytes.

The subsequent [Legion bring-up report](GOOB_seeed-xiao-samd21/REPORT.md)
records successful normal BOSSA upload and checksum verification of this exact
source, followed by responsive `V` commands and increasing uptime through
52,628 ms with `watchdog_ms=1000` and `imu_ready=0`. No further sketch changes
were made on Legion. The missing SleepyDog dependency was installed there;
an upload access problem was resolved after unplugging/reconnecting and
entering bootloader mode with serial terminals closed. The report leaves the
original USB-disappearance cause unconfirmed. Address-NACK retries returned
normally during this short bench check; working-IMU operation and motor-output
behavior remain unverified.

The report identified unbounded serial-buffer growth/processing, blocking IMU
initialization that could delay motor-timeout enforcement while feeding the
watchdog, and an estimator timestamp that spanned outages or calibration. Its
October 26–27 follow-up documents v2's fixes and validation. The tested source,
adapter, scripts and evidence are preserved in the target directory. Remaining
hardware checks are tracked in `PLAN.md`.

### Quaternion orientation direction

Lawlor's [rotation lecture](https://www.cs.uaf.edu/2017/spring/cs482/lecture/01_27_spin.html)
shows quaternion integration from angular velocity and emphasizes that the
multiplication order determines whether a rotation is applied in local or
world coordinates. His [Euler/quaternion notes](https://www.cs.uaf.edu/2015/spring/cs482/lecture/02_16_rotation.html)
also explain the pole singularity and the need to normalize an integrated
quaternion. Goob currently estimates and publishes Euler angles, so upside-down
yaw direction and apex flips still need a coordinated quaternion estimator and
viewer update. A quaternion removes the representation singularity; it does
not correct gyro bias or provide absolute yaw. The MPU-6050 has no magnetometer.

## Private config

For the MiniLAMP reference firmware, create a private config header:

```bash
cp firmware/MINILAMP_seeed-xiao-c6/private_config.h.example \
   firmware/MINILAMP_seeed-xiao-c6/private_config.h
```

Then edit `private_config.h` with Wi-Fi credentials, `MQTT_URI`, and broker
credentials. The tested URI is `wss://mqtt.sssn.us:443/`.
`private_config.h` is gitignored so secrets are not committed.

MiniLAMP's identity and module selection come from
`robots/minilamp/modules.example.json`; `generate_manifest.py` compiles shared
drive/telemetry definitions and the robot's grouser module into the committed
`GeneratedManifest.h`. Pin mapping and motor-output swaps/inversion live in
`RobotConfig.h`. It uses compact topics under the generated identity:

- `i/drive` and `i/grouser` accept non-retained JSON motor commands with
  independent one-second leases.
- `o/online` publishes retained `{"online":true}` every 5 seconds and sets a
  retained offline Last Will. Presence and low-rate state use QoS 1.
- `o/manifest` advertises controls; `o/modules/status` refreshes availability.
- `o/telemetry/battery` publishes the existing `{value, unit}` JSON shape as
  retained current state.
- `o/status` and read-only serial `V` report firmware and controller health.

The example uses a 10-second MQTT keepalive. Consumers should choose their own
stale timeout; 15 seconds is suitable for this interactive MiniLAMP profile.

For the HS105 XIAO ESP32-C3 bridge firmware, create a private config header:

```bash
cp firmware/HS105_seeed-xiao-c3/private_config.h.example \
   firmware/HS105_seeed-xiao-c3/private_config.h
```

Then edit `private_config.h` with:
- the router SSID/PSK for the LAN the XIAO should join
- the XIAO's own static IP, gateway, and subnet
- the static IP for each outlet slot (`o1`..`o4`)

Legacy smart-switch communication path:
- The XIAO ESP32-C3 connected to the same router LAN as the TP-Link HS105 smart plugs.
- Each plug had a static IPv4 address configured in `private_config.h`; slot names `o1` through `o4` mapped directly to those addresses.
- Pebble host commands reached the C3 over USB serial using `pebble_serial_v1` command interfaces such as `outlets/o1/power`.
- The C3 translated each command into the HS105 local Kasa TCP protocol on port `9999`, sending `system.set_relay_state` and polling `system.get_sysinfo` for relay state.
- The C3 reported outlet status back to Pebble as retained `pebble_serial_v1` state interfaces such as `outlets/o1/status`.

For the MiP XIAO ESP32-S3 Sense firmware, create a private config header:

```bash
cp firmware/MIP_seeed-xiao-esp32s3-sense/private_config.h.example \
   firmware/MIP_seeed-xiao-esp32s3-sense/private_config.h
```

Then edit `private_config.h` with:
- Wi-Fi and MQTT credentials
- robot identity (`SYSTEM_NAME`, `ROBOT_ID`, `ROBOT_NAME`)
- optional TLS settings
- MiP UART baud/probe settings
- MQTT keepalive, video rate/quality, and MQTT packet buffer size

For the BumperBot reference firmware, create a separate private config header:

```bash
cp firmware/BUMPERBOT_wemos/private_config.h.example \
   firmware/BUMPERBOT_wemos/private_config.h
```

Then edit `private_config.h` with local Wi-Fi/MQTT credentials.

## Build + flash (arduino-cli)

The commands below are convenience steps for the preserved reference targets in
this directory. They are not intended to define a universal Pebble flashing
workflow.

Install board cores once:
```bash
arduino-cli core update-index
arduino-cli core install esp32:esp32
arduino-cli core install esp8266:esp8266
arduino-cli core install arduino:avr
arduino-cli core install Seeeduino:samd
arduino-cli lib install "Adafruit SleepyDog Library@1.8.4"
```

Reference flash scripts (from repo root):
```bash
./firmware/flash-goob-firmware.sh
./firmware/flash-hs105-firmware.sh
./firmware/flash-mip-firmware.sh
./firmware/flash-fred-firmware.sh
./firmware/flash-minilamp-firmware.sh
./firmware/flash-bumperbot-wemos-firmware.sh
```

`flash-goob-firmware.sh` targets the actual Goob XIAO SAMD21/M0 board with
`Seeeduino:samd:seeed_XIAO_m0`; do not override it to an ESP32 target.

`flash-fred-firmware.sh` defaults to:
- `PORT=/dev/ttyACM0`
- `FQBN=Seeeduino:samd:seeed_XIAO_m0`

Override as needed, e.g.:
```bash
PORT=/dev/ttyACM1 FQBN=Seeeduino:samd:seeed_XIAO_m0 ./firmware/flash-fred-firmware.sh
PORT=/dev/ttyACM0 FQBN=esp32:esp32:XIAO_ESP32C3 ./firmware/flash-hs105-firmware.sh
PORT=/dev/ttyACM0 FQBN=esp32:esp32:XIAO_ESP32S3:PSRAM=opi ./firmware/flash-mip-firmware.sh
PORT=/dev/ttyACM0 FQBN=esp32:esp32:XIAO_ESP32C6 ./firmware/flash-minilamp-firmware.sh
PORT=/dev/ttyUSB0 FQBN=esp8266:esp8266:d1_mini ./firmware/flash-bumperbot-wemos-firmware.sh
```

The archived Goob Nano IMU sketch does not have a dedicated flash script
because Nano bootloader variants differ. Typical examples are:

```bash
arduino-cli compile --fqbn arduino:avr:nano firmware/GOOB_arduino-nano-imu
arduino-cli upload -p /dev/ttyUSB0 --fqbn arduino:avr:nano firmware/GOOB_arduino-nano-imu
```

If your Nano clone still uses the old bootloader, switch to:

```bash
arduino-cli compile --fqbn arduino:avr:nano:cpu=atmega328old firmware/GOOB_arduino-nano-imu
arduino-cli upload -p /dev/ttyUSB0 --fqbn arduino:avr:nano:cpu=atmega328old firmware/GOOB_arduino-nano-imu
```

## MiniLAMP WSS and TLS

MiniLAMP uses the ESP32 core's ESP-MQTT client and built-in CA bundle to verify
the WSS broker hostname/certificate. It obtains time over SNTP before starting
MQTT so certificate dates can be checked. There is no insecure TLS fallback.
TLS connection, MQTT reconnect and publishing work runs outside the dedicated
motor task. It therefore cannot hold the previous drive output while waiting
for a server. The target README documents dependencies, control mappings,
validation scripts and remaining hardware checks.
