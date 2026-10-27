# Goob — Seeed XIAO SAMD21

Open `GOOB_seeed-xiao-samd21.ino` in Arduino IDE.

1. Install **Seeed SAMD Boards** in Boards Manager. Its package URL is
   `https://files.seeedstudio.com/arduino/package_seeeduino_boards_index.json`.
2. Select **Seeeduino XIAO** (SAMD21/M0), not an ESP32 board.
3. Install **Adafruit SleepyDog Library**, version **1.8.4**, in Library Manager.
4. Select the XIAO's serial port and use Arduino IDE's normal Upload action.

The current build identifies itself as `goob-samd21-bounded-recovery-v2`.
It retains SleepyDog's independent one-second watchdog, bounds serial input and
I2C transactions, and schedules IMU initialization/calibration incrementally.
See `REPORT.md` for measured results and remaining hardware checks.

In Serial Monitor, use **460800 baud** and **Newline**. Send `V` for firmware ID,
uptime, reset cause, watchdog period, and IMU readiness. With no IMU attached,
the board should continue accepting serial commands and report IMU unavailable.
Increasing uptime across repeated `V` commands checks for resets. Additional
key/value fields report calibration/state/error and motor command age. The
existing `M`, `L`, `F`, `C`, `V` commands and `I`, `S`, `C` telemetry remain.

Commands have a maximum of **128 payload bytes**, excluding CR/LF, and input is
serviced in batches of at most **64 bytes per loop pass**. Overlong lines and
embedded NUL bytes are discarded through the next newline, with one error.
Motor values must parse completely as finite floats; malformed/NaN/Inf values
cannot renew the valid motor-command timestamp. Finite motor values retain the
existing clamp to [-1, 1]. Nonzero motor commands are rejected throughout both
automatic and manual calibration. Light float arguments must also be finite;
fade periods must be nonnegative and at most 2,147,483 seconds.

`BoundedI2C.h` is a project-owned adapter for this exact SAMD21/SERCOM2 pinout.
Do not omit it when copying the sketch. It replaces Wire calls, exclusively owns
D4/D5, and adds no third-party dependency or installed-core patch. Each bus
setup/transaction has one **10 ms** budget including cleanup; interrupts must
remain enabled for `micros()`. I2C runs at approximately 100 kHz and requires the
documented external pull-ups. A bare board without pull-ups can report line-low
instead of NACK; both are handled as unavailable without blocking USB.

I2C error codes: 0 success, 2 address NACK, 3 data NACK, 4 bus error,
5 timeout/incomplete read, 6 line low, 7 adapter not ready. Partial read byte
counts are retained for diagnostics. Recovery disables the peripheral, releases
the pins, then retries initialization after 1, 2, 4, 8, 16, and at most 30 seconds.
It does not generate bus-clear clock pulses or power-cycle the sensor. Entering
initialization or detecting a sensor fault stops the motors; unavailable/backoff
state releases the drive-command lock. Consequently, periodic recovery attempts
also interrupt active drive. This is intentional conservative behavior.

After a watchdog reset, IMU probing is disabled for that boot. USB, drive and
light commands remain usable. A normal reset/power-cycle permits probing again.
This avoids repeatedly entering a defective peripheral after watchdog recovery.
`V` state codes: 0 unavailable/backoff, 1 bus setup, 2 identify, 3 wake,
4 wake wait, 5 configure, 6 settling wait, 7 automatic calibration, 8 running,
9 watchdog lockout. `imu_calibrating=1` also covers manual calibration while
state 8 is active. Initialization/calibration has a ten-second overall deadline.

The estimator resets its time origin/filter initialization after initialization,
recovery or calibration, and on sample gaps over 100 ms. It does not integrate
the missing interval. The first fresh sample uses zero integration time; yaw
restarts relative to that new origin, so consumers should still treat an
estimator restart as a new reference.

Goob wiring: B/G/R light controls D0/D1/D2; battery-divider input D6;
left motor D7/D8; right motor D9/D10; MPU-6050 SDA D4 and SCL D5.
The battery is 1S, maximum 4.2 V, with a 2:1 divider (maximum 2.1 V at D6).

Motor command timeout remains one second and is serviced before/after serial,
IMU and telemetry work. Initialization contains no delay loops or watchdog feeds.
Individual bounded calls and USB output can still add scheduling latency; this
is not a hard real-time electrical guarantee. Electrical motor-output timing
and reset behavior remain to be measured on an isolated/safely positioned driver.

Reproducible tests are in `tests/`. `serial_validation.py` requires host-side
pyserial **3.5** and checks the production firmware with zero motor commands.
`build_harness.py` generates a separate bare-board test sketch from the current
source, substituting a simulated IMU to exercise scheduling and estimator logic.
That harness intentionally triggers a watchdog reset; always restore production
firmware afterward. Simulated tests do not validate the real I2C electrical path.

## Repository integration and live Goob check — October 27

The tested Legion source, adapter, report, validation scripts and evidence are
preserved here. Windows CRLF line endings were normalized to LF during import;
the SHA-256 hashes in `REPORT.md` refer to the Windows originals. Both production
files match those originals after line-ending normalization. A fresh
warning-enabled ARL compile of the imported source succeeded with no warnings,
at the same 49,892 sketch bytes recorded in the report.

With the production MCU installed in Goob, its control bridge was active and
the native USB application was enumerated. A read-only MQTT capture received
100 fresh IMU module messages over 12.87 seconds (7.69 Hz), with underlying
sample counts increasing from 15,428 to 16,716 (about 100.1 Hz), zero sample
errors, and 100 startup calibration samples. Battery telemetry was near 4.07 V.
This check used broker subscriptions and did not open the MCU serial device,
send actuator/calibration commands, or restart the bridge. It supplements the
Legion report; it is not a long-duration or electrical motor-output test.

### Later installed-robot failure — October 27

Read-only inspection of Goob's journal (`journalctl _UID=1001`) found real
sensor/bus failures, rather than only stale MQTT forwarding. All times below
are UTC. Read failures occurred at 20:33:25, 20:37:15, 20:38:35, 20:41:03 and
20:41:06. Most reported `wire=4` (BUS_ERROR), sometimes after three bytes;
20:41:03 reported `wire=2` (ADDRESS_NACK). Recovery succeeded at 20:35:27,
20:38:18, 20:39:37 and briefly at 20:41:05. After the final failure, retries
continued every 30 seconds through inspection at 22:39 with
`stage=bus-start who=0x0 wire=6 bytes=0`. Error 6 is LINE_LOW: at least one
of SDA/SCL reads low with the peripheral disabled and GPIO pull-ups enabled.
The zero identity is cleared on retry; it does not prove the sensor changed
address. Firmware does not report which line is low.

Drive-stop and calibration-command log entries surround several failures,
consistent with the user's observation that driving triggers the problem.
These logs do not record each nonzero drive command, so exact onset correlation
is unproven. The serial bridge remained PID 2904 after its 20:20:59 connection;
no subsequent MCU USB disconnect was recorded through this inspection. The MCU
continued reporting retry failures and battery data: no evidence here of a
whole-MCU reset. Suspect motor-related electrical interference, supply/ground
transients or intermittent wiring; an adapter recovery defect is also possible.

Current recovery reinitializes SERCOM but does not perform GPIO bus-clear clock
pulses. Next diagnostics should distinguish SDA-low from SCL-low, then test a
bounded open-drain bus-clear procedure, following NXP UM10204. SDA stuck low
may recover after nine clock pulses; SCL stuck low can require sensor power
cycling. Inspect sensor supply and bus signals during motor startup to identify
the trigger. No firmware upload, reset, calibration or motor command was issued
during this investigation.
