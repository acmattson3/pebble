# Goob — Seeed XIAO SAMD21

Open `GOOB_seeed-xiao-samd21.ino` in Arduino IDE.

1. Install **Seeed SAMD Boards** in Boards Manager. Its package URL is
   `https://files.seeedstudio.com/arduino/package_seeeduino_boards_index.json`.
2. Select **Seeeduino XIAO** (SAMD21/M0), not an ESP32 board.
3. Install **Adafruit SleepyDog Library**, version **1.8.4**, in Library Manager.
4. Select the XIAO's serial port and use Arduino IDE's normal Upload action.

The current build identifies itself as `goob-samd21-independent-drive-v4`.
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
existing clamp to [-1, 1]. Drive commands take priority over IMU initialization,
faults, recovery and calibration. A nonzero drive command cancels calibration
and applies immediately; it does not wait for a sensor. Light float arguments must also be finite;
fade periods must be nonnegative and at most 2,147,483 seconds.

`BoundedI2C.h` and `BusClear.h` are project-owned support files for this exact
SAMD21/SERCOM2 pinout. Include both when copying the sketch. The adapter replaces Wire calls, exclusively owns
D4/D5, and adds no third-party dependency or installed-core patch. Each bus
setup/transaction has one **10 ms** budget including cleanup; interrupts must
remain enabled for `micros()`. I2C runs at approximately 100 kHz and requires the
documented external pull-ups. A bare board without pull-ups can report line-low
instead of NACK; both are handled as unavailable without blocking USB.

I2C error codes: 0 success, 2 address NACK, 3 data NACK, 4 bus error,
5 timeout/incomplete read, 6 line low, 7 adapter not ready. Partial read byte
counts are retained for diagnostics. Recovery disables the peripheral, releases
the lines and services a bounded open-drain bus-clear state machine before
backoff. If SCL is high and SDA is low, it generates up to nine recovery clocks,
stopping early if SDA releases, then attempts STOP. If SCL is held low, no
recovery clocks are forced. A released bus gets a bounded SERCOM restart.
Initialization retries follow after 1, 2, 4, 8, 16, and at most 30 seconds.
Firmware does not reset or power-cycle the sensor. IMU faults, initialization,
clear/restart and backoff never modify motor output or renew motor command age.
Drive/light commands remain available throughout. Motor shutdown still occurs
for explicit zero commands, invalid numeric motor values, the one-second valid
command timeout, or MCU reset/watchdog recovery. This supersedes the earlier
response's policy of stopping drive on sensor faults.

After a watchdog reset, IMU probing is disabled for that boot. USB, drive and
light commands remain usable. A normal reset/power-cycle permits probing again.
This avoids repeatedly entering a defective peripheral after watchdog recovery.
`V` state codes: 0 unavailable/backoff, 1 bus setup, 2 identify, 3 wake,
4 wake wait, 5 configure, 6 settling wait, 7 reserved legacy calibration state,
8 running, 9 watchdog lockout, 10 bus clear, 11 SERCOM restart.
Both automatic and manual calibration run in ready/running state 8 and report
`imu_calibrating=1`. Hardware initialization has a ten-second overall deadline.
Calibration's own ten-second deadline cancels calibration without declaring a
healthy sensor unavailable.

At first initialization, if drive is at commanded rest, a 100-sample gyro bias
calibration starts after the sensor becomes ready. A drive command can cancel
it. Recovery preserves an established gyro bias and does not repeatedly
calibrate a moving robot. `C` requests 400-sample calibration only at commanded
rest; if driving, C returns an error without stopping the motors. A new nonzero
drive command cancels an in-progress C. Cancelled calibration never overwrites
the last completed bias. If initial calibration was cancelled, bias stays zero
and `imu_bias_valid=0`; use C while physically stationary to establish a bias.
Commanded rest alone does not prove the robot has physically stopped moving.

Bus clearing follows [NXP UM10204 rev. 7, section 3.1.16](https://www.nxp.com/docs/en/user-guide/UM10204.pdf).
GPIO only switches between high-impedance input and output LOW; it never drives
SDA or SCL HIGH. The STOP setup has its own SCL rising edge, separate from the
up-to-nine recovery clocks. If SDA stays held low, STOP cannot complete and
the attempt fails safely. External 3.3 V pull-ups are required. A permanently
low SCL or SDA, failed power supply, damaged sensor or ongoing electrical noise
can require hardware reset/power cycling or an electrical fix. This code does
not promise recovery from those conditions.

The clear engine advances one phase per main-loop pass. Minimum phase holds
are 10 microseconds, SCL release wait is 2 ms, and the whole clear engine has a
250 ms deadline; main-loop scheduling adds return latency. Driver disable and
SERCOM restart each retain a separate 10 ms operation budget. Clock pulses may
therefore be much slower than normal I2C while serial and motor safety run.
No retry/calibration blocks the main loop or feeds the watchdog inside a wait.

Read-only `V` adds `sda`/`scl` live levels, `fault_sda`/`fault_scl`,
`fault_state`, `fault_wire`, total `faults`, `bus_clear` and `clear_pulses`.
Fault evidence is captured before STOP/disable/pin cleanup and retained until
the next fault. Treat the fault snapshot as meaningful only when `faults>0`.
Live levels during clear state 10 can reflect the controller's intended LOW
phase; a single transaction-fault snapshot also cannot distinguish a clock
phase from a persistently held line without the later recovery evidence.

Fault and clear-result diagnostics share a maximum of one line per second.
Repeated events retain the latest snapshot plus the cumulative fault count;
they do not allocate a growing queue. Existing valid commands and I/S/battery
telemetry remain compatible. Clear result codes: 0 not started, 1 running,
2 lines released/STOP completed, 3 SDA held after nine clocks, 4 SCL held,
5 overall clear deadline, 6 SERCOM disable failed (no GPIO clocks attempted).
The result log also reports `restart_wire`; result 2 does not itself establish
that the sensor is acknowledging or that subsequent initialization succeeded.
V also exposes `motor_left`/`motor_right` (clamped logical outputs before wiring
inversion) and `imu_bias_valid`. These are software state, not measured driver
voltage or wheel motion.

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
is not a hard real-time electrical guarantee. Motor electrical behavior must be
measured on an isolated/safely positioned driver before robot deployment.

Reproducible tests are in `tests/`. `serial_validation.py` requires host-side
pyserial **3.5** and checks the production firmware with zero motor commands.
`build_harness.py` generates a separate test sketch from the current
source, substituting a simulated IMU and suppressing all PWM calls while motor
pins remain latched LOW. It exercises recovery, scheduling and estimator logic.
That harness intentionally triggers a watchdog reset; always restore production
firmware afterward. Simulated tests do not validate the real I2C electrical path.
The current scripts write `*-v4.log/json`, preserving earlier v2/v3 evidence.
Use `serial_validation.py --port COM3 --seconds 60 --expect-imu` for a connected
sensor, or omit `--expect-imu` for a bare-board run. Loaded motor tests and
oscilloscope measurements of IMU supply/SDA/SCL remain separate pending work.
`drive_comparison.py --port COM3 --label <label> --simple` performs a lifted-
robot comparison: 3 seconds stopped, 4 seconds both motors at 50%, 3 seconds
stopped, 4 seconds at 50%, then 3 seconds stopped. It requires current user
authorization and a safe lifted setup, logs telemetry, and sends repeated final
zero commands. Wheels-off comparison changes inertia/load/vibration as well as
magnet proximity, so it cannot by itself isolate magnetic interference.
