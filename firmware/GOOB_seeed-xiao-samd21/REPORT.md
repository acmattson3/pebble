# Firmware review and hardware bring-up report

Date: October 26, 2026 (America/Anchorage)

## Result

The firmware in `GOOB_seeed-xiao-samd21.ino` was compiled, uploaded, and verified successfully on the connected Seeed XIAO SAMD21. The source was not modified during this review.

After upload, Windows enumerated the application as USB VID `2886`, PID `802F`, with its serial interface on **COM3**. The firmware responded to `V` commands and reported increasing uptime through **52,628 ms**, without an observed restart. This establishes short-term stability in the tested configuration; it does not establish long-term stability or operation with motors and a working IMU.

The root cause of the earlier USB disappearance remains unconfirmed.

## Build and upload

- Board: `Seeeduino:samd:seeed_XIAO_m0`.
- Board package: Seeed SAMD Boards **1.8.6**.
- Library: Adafruit SleepyDog Library **1.8.4**, installed during this session because it was initially missing.
- USB stack: default Arduino stack.
- Build: successful; reported sketch usage **50,292 bytes**. Two signed/unsigned comparison warnings appeared in the command tokenizer (lines 539–540).
- Uploader: bundled BOSSA/bossac **1.7.0-arduino3**.
- Device identified by uploader: **ATSAMD21G18A**.
- Application start address: **0x2000**, preserving the bootloader region.
- Flash operation: **50,992 bytes** written and checksum verification successful, followed by CPU reset.

The flash byte count includes the generated binary contents and differs from the compiler's reported sketch usage.

Bootloader mode enumerated as VID `2886`, PID `002F`, on **COM6**. Upload attempts initially stalled; an independent serial-open attempt returned `Access denied`. Closing serial applications alone did not immediately resolve access. After unplugging, reconnecting, and double-tapping reset with serial terminals closed, upload succeeded. The process holding the port was not identified, so the access error should not be treated as proof of a firmware failure.

## Runtime observations

Firmware identity and status were confirmed over native USB serial:

```text
INFO firmware=goob-samd21-watchdog-clock-v1 uptime_ms=5623 reset_cause=0x40 watchdog_ms=1000 imu_ready=0
INFO firmware=goob-samd21-watchdog-clock-v1 uptime_ms=10600 reset_cause=0x40 watchdog_ms=1000 imu_ready=0
INFO firmware=goob-samd21-watchdog-clock-v1 uptime_ms=52628 reset_cause=0x40 watchdog_ms=1000 imu_ready=0
```

The reset-cause value was recorded following the uploader's CPU reset; it does not diagnose the earlier failure. IMU retries and battery telemetry continued while serial commands remained responsive.

The IMU repeatedly reported:

```text
WARN IMU init failed stage=WHO_AM_I-read who=0x0 wire=2 bytes=0
```

The configured I2C address is **0x68**. `wire=2` indicates an address NACK in the installed Wire implementation. This means the sensor did not acknowledge that transaction; it does not distinguish an absent sensor, wiring or power problems, or a sensor configured at another address. `who=0x0` is not a successful chip-ID read in this case.

Check IMU power and common ground, SDA on D4, SCL on D5, pull-ups to 3.3 V, and address selection. The code accepts WHO_AM_I values 0x68/0x69 but always communicates at address 0x68; accepting both identity values does not probe address 0x69.

## Significant code findings

Line numbers refer to the sketch as reviewed and may change after edits.

### 1. I2C hangs can become recurring watchdog resets

Relevant locations: `mpuWrite()` and `mpuRead()`, approximately lines 173–248; watchdog configuration at lines 159–164; IMU startup/retry at lines 448–451 and 507–514.

The installed SAMD21 core contains unbounded waits in SERCOM I2C operations. Checking SDA and SCL before calling Wire prevents some failure cases, but does not bound a transaction if a line becomes stuck or another fault occurs after the check.

The one-second watchdog can recover from a hang, but startup immediately attempts sensor initialization again. A persistent fault could therefore produce repeated resets and interrupt USB enumeration. This is a plausible explanation for the reported symptom, not a reproduced failure in this session: the observed address-NACK condition returned normally.

Recommended correction: use bounded I2C operations with explicit error recovery. Add a recovery policy that can keep USB and command handling available while disabling or backing off a repeatedly failing IMU, especially after watchdog resets. Preserve motor shutdown on fault.

### 2. Serial command accumulation has no length or processing bound

Relevant locations: `String buffer` at line 458 and the receive loop at lines 486–495.

Every non-newline character is appended to a dynamically allocated String. A sender that omits the newline can grow the buffer until memory is exhausted. Repeated allocation also creates avoidable heap pressure. A sustained stream can keep the receive loop busy and delay the motor timeout and watchdog servicing.

Recommended correction: impose a command-length limit, discard an overlong command through its next newline, and cap the bytes or time processed per loop iteration. A fixed-size receive buffer would make memory use predictable.

### 3. IMU reinitialization delays motor timeout enforcement

Relevant locations: `initMpu6050()` at lines 260–303 and the main-loop retry at lines 507–514.

Initialization contains two 100 ms delays and 100 calibration iterations with 5 ms delays, plus I2C transaction time: roughly 700 ms or more. Calibration feeds the watchdog, but does not check the motor command timeout. During a runtime reinitialization, existing motor output can therefore remain active beyond the intended one-second timeout. Continued watchdog feeding does not enforce that command timeout.

Recommended correction: make initialization/calibration incremental so the main loop continues servicing commands and motor safety, or force the motors off before entering blocking reinitialization and define how subsequent commands are handled.

## Additional correctness issue

`lastImuSampleUs` (lines 149 and 319–320) is not reset when IMU initialization or manual calibration completes. After a sampling gap, the next orientation update can use an elapsed interval that includes the outage or calibration period. This can produce an orientation jump. Reset the sample timestamp when restarting the estimator and consider bounding the integration interval. This finding is not a likely direct cause of USB disappearance.

## Remaining validation

- Test with a powered, acknowledging MPU-6050 and confirm stable sampling and USB operation.
- Reproduce sensor disconnection and bus faults under controlled conditions; confirm recovery does not trap the board in a reset loop.
- After bounding the parser, test overlong commands and sustained serial input.
- Measure motor shutdown timing during normal command loss, IMU reinitialization, and watchdog resets.
- Check power integrity and motor-induced noise under load if failures coincide with motor activity; this was not measured during this session.
- Run a longer stability test and repeat power-cycle/reconnection checks.

No motor movement commands were issued during this review. Electrical output behavior, working-sensor operation, and the original failure mechanism remain unverified.

---

## Follow-up: bounded recovery firmware

Date: October 26, 2026. This section follows the development instructions in
`RESPONSE.md`; the original findings above describe the v1 baseline.

### Implemented changes

- Distinct production ID: **`goob-samd21-bounded-recovery-v2`**.
- IMU bus setup, identification, wake/settle waits, register configuration and
  100-sample startup/recovery calibration now progress as a state machine. Each
  initialization call performs at most one bus operation or one calibration
  sample. The waits use timestamps rather than `delay()`. Manual calibration
  retains 400 samples, collected by the ordinary sample scheduler.
- Entering initialization or calibration stops motors. Every nonzero motor
  command, including values below the former 0.001 threshold, is rejected while
  calibration is active. A fault aborts calibration and releases the drive lock
  while the IMU is unavailable. `serviceMotorTimeout()` enforces the one-second
  command age before/after serial, IMU, and battery work. No calibration loop
  feeds the watchdog independently of the main loop.
- Serial reception uses a 129-byte array (128 payload bytes plus NUL), with a
  maximum of 64 received bytes per main-loop pass. Overlong lines and embedded
  NUL are discarded through newline and produce one error. Tokenization and
  numeric parsing use fixed storage, not dynamic Strings. Motor float parsing
  rejects incomplete tokens, range errors, NaN and infinity before applying
  output or renewing the valid-command timestamp. Invalid numeric motor values
  also stop existing motor output. CR/LF and valid Goob commands are retained.
- Light arguments also require finite values; fade time is range checked before
  conversion to milliseconds. Existing `I`, `S`, and battery `C` telemetry
  formats are retained. `V` remains read-only and retains its original fields,
  appending IMU calibration/state/error and motor command timestamp/timeout.
- `BoundedI2C.h` replaces Wire calls with a project-owned SAMD21 SERCOM2 adapter
  for D4/D5. The actual installed Seeed 1.8.6 Wire/SERCOM source was inspected;
  it has unbounded register-polling loops, and Stream timeouts do not fix them.
  No installed Arduino core files were edited. The adapter adds no external
  dependency. Its setup, write and combined register-read operations each share
  one **10 ms** deadline across all polling and cleanup, using `micros()` with
  interrupts enabled. It preserves partial-read counts and distinguishes NACK,
  bus error, timeout, line-low and not-ready conditions.
- On a transaction fault, the IMU becomes unavailable, calibration is aborted,
  motors stop, and the peripheral is disabled/pins released. Initialization
  retries back off through 1, 2, 4, 8, 16, then 30 seconds. No bus-clear pulses
  or sensor power switching are performed. A still-held bus can remain
  unavailable, but no longer requires monopolizing the main loop.
- The independent SleepyDog watchdog remains enabled at 1000 ms. A boot after
  watchdog reset disables further IMU probing for that boot while keeping USB,
  normal drive/light commands available. Reset/power-cycle allows probing again.
- Initialization, recovery, manual calibration and gaps over 100 ms reset the
  estimator's timestamp and initialization flags. The first fresh sample uses
  zero integration time. Yaw restarts as a relative origin; this avoids
  integrating an outage but is not a promise of absolute heading continuity.

Conservative behavior to retain during integration: each automatic recovery
attempt stops active drive, even if an absent IMU had previously released the
command lock. Individual bounded bus calls, battery sampling and native USB
output can add scheduling latency; this firmware does not establish a hard
real-time electrical motor-shutdown guarantee.

### Build configuration

The production revision compiles with Seeed SAMD Boards **1.8.6**, FQBN
`Seeeduino:samd:seeed_XIAO_m0`, default Arduino USB stack, and Adafruit SleepyDog
**1.8.4**. The warning-enabled build reports **49,892 bytes** with no warnings.
The generated binary is **50,584 bytes**. The normal Arduino upload workflow
reported a successful flash and checksum verification, then normal USB serial
returned on COM3. Native serial remains configured at 460800 baud.

### Reproducible validation assets

- `tests/serial_validation.py`: real production USB/parser checks and a timed
  soak, using host-side pyserial **3.5**. It sends only zero valid motor values.
  Raw RX/TX evidence and structured results are written beside the script.
- `tests/build_harness.py`, `tests/HarnessBody.txt` and
  `tests/SimulatedI2C.txt`: generate a separate SAMD21 test sketch from the
  current production source with only the I2C adapter replaced by a simulated
  MPU-6050. This exercises production scheduling, calibration, command
  admission, timeout and estimator functions. The harness deliberately triggers
  a watchdog reset to test the actual recovery boot policy. It must only be
  uploaded to a confirmed bare board, and production must be restored afterward.

Simulated sensor tests do not validate the real adapter's acknowledging-device
path or electrical fault handling. Bare-board errors likewise do not establish
that an IMU is defective: this board has no sensor or external bus pull-ups
attached. The new adapter can report line-low (`imu_error=6`) in that condition
instead of the old implementation's address NACK. External pull-ups remain
required for the intended sensor connection.

### Measured follow-up validation

The user confirmed a **bare XIAO** before these tests. Production source was
unchanged between the final build and the following soak/harness generation.

**Production serial/parser soak: PASS.** Actual host-observed duration was
**600.687 seconds**. Firmware uptime increased from 5,593 to **606,090 ms**;
66 explicit status checks succeeded, with no observed reset or serial read
error. The IMU stayed unavailable, periodic recovery attempts returned safely,
and valid zero-drive/light/fade commands remained responsive.

The 20-second sustained-input phase transmitted **185,856 bytes**, with
**768 V responses** collected (21 seconds including drain time). No decreasing
uptime was observed. Additional passing checks included:

- Valid CR/LF, whitespace/lowercase commands and zero motor values.
- Rejection of NaN, infinity, malformed numeric suffixes, overflow/underflow and
  incomplete motor arguments. Invalid motor values did not change the last
  valid motor-command timestamp, as observed through read-only V fields.
- Exactly 128 payload bytes accepted; 129 bytes discarded with one error and
  successful parsing of the following command.
- An 8,192-byte line without newline stayed in discard state without repeated
  errors; newline produced one error and restored parsing.
- Embedded NUL rejected through newline, followed by successful parser recovery.

Raw and structured evidence: `tests/serial-validation.log` and
`tests/serial-validation.json`. The fixed receive buffer establishes a storage
bound by construction; no heap-usage profiler was used.

**On-target simulated-sensor tests: PASS, 24 checks, 8.625 seconds.** The harness
used production functions with a simulated bus on the actual SAMD21. Startup
calibration completed with 100 samples and manual calibration with 400. The
initialization test took 154 scheduler passes, with a maximum observed step
duration of 85 microseconds for the simulated bus. This timing is not a measure
of real I2C transaction duration. The test checked:

- Motor shutdown on initialization, rejection of even very small nonzero motor
  values during automatic calibration, and equivalent manual-calibration policy.
- One-second software motor timeout and no timestamp renewal for invalid values.
- Serial/main-loop servicing between calibration steps, one bus operation per
  initialization call, fault/backoff/recovery state transitions, and release of
  command lock after failed calibration.
- Safe abort on the overall initialization and manual-calibration deadlines.
- A two-second simulated sample gap was not integrated; subsequent fresh samples
  resumed integration. Recovery/calibration reset the estimator time origin.
- An **intentional hardware watchdog reset** entered the production IMU lockout
  state, and subsequent main-loop passes did not probe the sensor.

Evidence: `tests/harness-validation.log` and
`tests/harness-validation.json`. `tests/run_harness.py` captures these results.
The production v2 firmware was then restored with the normal Arduino upload
workflow and checksum verification succeeded. No test harness is intended to
remain installed on the board.

CLI used: **arduino-cli 1.5.1**, commit `01f3d4f2b`. Temporary harness builds
were separate from the production sketch and replaced only the bus adapter
for the simulated tests. None of these checks measured physical driver outputs
or robot movement.

### Remaining hardware checks

At this point, real acknowledging-device reads/writes, physical IMU
disconnect/reconnect and stuck-bus behavior have not been validated. A further
optional adapter diagnostic is supplied in `tests/build_adapter_check.py`: it
uses the real adapter with an attached MPU-6050, probes an unused address for
NACK, and temporarily gates only SERCOM2's core clock to exercise the finite
transaction deadline without shorting pins. That diagnostic must be followed
by restoration of production firmware. Its presence is not evidence it passed.

Actual motor shutdown timing, driver output states during watchdog reset,
loaded power integrity, and electrical motor safety remain pending appropriate
equipment and safe physical setup. Software flags/ACKs and simulated sensor
tests do not establish electrical safety. The original USB failure mechanism
has not been reproduced or conclusively diagnosed.

---

## Connected IMU validation — October 27, 2026

The user connected the XIAO to the MPU-6050 and the rest of the peripherals.
They confirmed the motors were powered but the robot was lifted, so motor
movement would have no consequence. All valid motor commands issued during
these checks were nevertheless **zero**. No motor-motion test or electrical
output measurement was performed. The temporary diagnostic's safety guidance
was updated to allow this currently confirmed safe physical setup, as well as
unpowered/disconnected motors.

The source and adapter SHA-256 hashes matched the tested October 26 revision:

```text
GOOB_seeed-xiao-samd21.ino
D6BF179B3B04E4BBBDA96F357374D11E4FDE8ACACB33A0B3B23F5C47301438B7
BoundedI2C.h
084AB04F18FCA941D85AFFD17EEAB6A4A1CEF0701C98532D4E23843923EAD575
```

No production firmware code changes were necessary for these tests. The
following results supersede the earlier pending status for real-device I2C
reads/writes, acknowledging-sensor calibration, NACK and clock-stall timeout
recovery. Physical disconnect/reconnect and externally stuck-line tests remain
pending.

### Production telemetry capture: PASS

Windows enumerated normal application USB VID `2886`, PID `802F`, on **COM3**.
Firmware ID was `goob-samd21-bounded-recovery-v2`; startup status indicated the
IMU was ready and calibration had collected 100 samples. Only read-only V
commands were sent during this initial capture.

Over **60.0 seconds**, `tests/monitor_production.py` recorded:

- **5,999 sample lines** and **461 summary lines**.
- Mean sample rate **100.002 Hz** from device sample timestamps.
- Largest observed sample gap **23 ms**.
- All status checks: `imu_ready=1`, `imu_error=0`, state 8 (running).
- No warning/error lines; IMU summary error counter remained zero.
- Status uptime increased from **57,790 to 112,795 ms**, with no observed reset.
- Battery telemetry ranged **4.074–4.084 V**. This is an ADC-derived reading,
  not an independently measured battery voltage.

Evidence: `tests/connected-monitor.log` and `tests/connected-monitor.json`.
This is a short connected-hardware run, separate from yesterday's ten-minute
bare-board soak.

### Real manual calibration and responsiveness: PASS

`tests/sensor_validation.py` ran for **31.063 seconds**, recording **2,706
sample lines** and **36 status responses**. It used V, a zero motor command,
a zero light command and C (manual calibration).

The IMU remained ready and error-free. Calibration-active status was observed
across repeated half-second V queries, then calibration completed with **400
samples** after approximately four seconds. Motor timeout/off status remained
asserted during calibration. Regular telemetry resumed afterward, and uptime
increased from **138,316 to 168,379 ms** without an observed reset.

The first post-calibration sample reported yaw **0.00**, and the following
sample was approximately **-0.00**. This is consistent with restarting relative
yaw at a fresh time origin rather than integrating the calibration gap. It is
not an absolute-orientation accuracy test, nor proof that a new relative
reference is continuous with the old one.

All nine automated checks passed. Evidence: `tests/sensor-validation.log` and
`tests/sensor-validation.json`.

### Real I2C adapter diagnostic: PASS, 8 checks

A separate `AdapterCheck` sketch was generated from the unchanged project-owned
adapter and compiled with the same Seeed SAMD Boards 1.8.6 / SleepyDog 1.8.4
configuration. The warning-enabled compile succeeded (37,212 sketch bytes;
37,832-byte binary). Normal Arduino upload and flash checksum verification
succeeded. Its setup latched motor pins D7–D10 low; it issued no drive commands.

Results:

| Check | Observed result |
| --- | --- |
| Bus initialization | Passed |
| MPU identity read at 0x68 | WHO_AM_I = **0x68** |
| Real register write | Passed |
| Repeated-start 14-byte sample read | Passed, full byte count |
| Read at unused address 0x67 | Address NACK, returned in **120 microseconds** |
| Reinitialization after NACK | Passed |
| Transaction with SERCOM2 core clock temporarily gated | Timeout error 5, returned in **10,014 microseconds** including adapter cleanup |
| Reinitialize and read actual MPU after clock restoration | Passed |

Clock gating affected only the I2C peripheral clock; USB and system timing
remained available. This exercises a real peripheral-stall timeout without
shorting powered pins. It does not substitute for external stuck-line,
mid-read disconnect, or incomplete-read electrical tests. The nominal budget
is 10 ms; measured return includes small execution/interrupt overhead, so it
must not be described as an exact 10.000 ms hard deadline.

Evidence: `tests/adapter-validation.log` and `tests/adapter-validation.json`.
Reproduction helpers: `tests/build_adapter_check.py` and
`tests/run_adapter_check.py`. The diagnostic completed with zero failures.

### Final installed state and remaining work

The normal Arduino workflow **restored production v2 firmware** after the
temporary diagnostic. All **50,584 bytes** were written and checksum-verified.
COM3 returned normally, and a subsequent V response confirmed:

```text
INFO firmware=goob-samd21-bounded-recovery-v2 uptime_ms=17068 reset_cause=0x40 watchdog_ms=1000 imu_ready=1 imu_calibrating=0 imu_state=8 imu_error=0 imu_stage=none motor_cmd_ms=2 motor_timeout=1
```

The production firmware, not a diagnostic sketch, is installed. Serial ports
were closed after testing. No reset-button intervention was required today.

Still pending: physical sensor disconnect/reconnect, externally stuck SDA/SCL,
partial-read fault injection, long-duration operation with the real sensor,
motor shutdown timing measured at the driver, output behavior during watchdog
reset with peripherals attached, and loaded power/noise tests. Today's results
provide real-hardware evidence for normal I2C, manual calibration, bounded NACK
and a clock-stall timeout/recovery. They do not conclusively identify the
original USB failure or establish electrical motor safety.

---

## Motor-period I2C faults: diagnostics and bus clear — October 27, 2026

This follow-up implements the new final section of `RESPONSE.md`. The supplied
Goob journal summary describes BUS_ERROR/partial reads, NACK, then LINE_LOW
while the MCU continued responding. Motor activity is a plausible trigger, not
a demonstrated cause. The live board was healthy on v2 before this change;
the reported motor-period fault was not reproduced during this session.

The user confirmed the XIAO was connected to the IMU/peripherals. No current
safe motor-motion setup was assumed from earlier messages. **No nonzero motor
command was sent to physical motor outputs.** For simulated tests, PWM calls
were replaced by a no-op sink and boot motor pins remained latched LOW. That
fixture change permits logic testing without motor movement even if a command
admission assertion were to fail.

### Production changes

New firmware identity: **`goob-samd21-bus-clear-v3`**.

1. `BoundedI2C.h` captures SDA and SCL independently at the point of failure,
   **before** STOP, disable or GPIO cleanup can change the evidence. The sketch
   retains the latest snapshot with the failing firmware state, error code,
   partial-byte count, WHO_AM_I value, calibration state and cumulative fault
   count. Repeated faults coalesce into fixed storage rather than a growing
   queue.
2. The fault WARN line retains `stage`, `who`, `wire` and `bytes`, adding
   `sda`, `scl`, `state`, `calibrating`, and `faults`. Clear-result logs report
   result, pulse count, final separate line levels and SERCOM restart error.
   These two diagnostic event types share a **one-line-per-second limit**.
   Read-only V retains its prior fields and adds live line levels, sticky fault
   levels/state/error/count and clear progress/result. A live LOW during an
   intentional clear phase is different from the stored pre-cleanup fault
   snapshot; neither should be treated as a scope measurement of a supply/noise
   problem. Initial fault fields are not evidence of a fault when `faults=0`.
3. New `BusClear.h` contains a project-owned, incremental recovery state machine
   based on [NXP UM10204 rev. 7, section 3.1.16](https://www.nxp.com/docs/en/user-guide/UM10204.pdf).
   After a transaction/startup fault, motors stop and calibration is aborted.
   SERCOM relinquishes the bus within its existing finite operation budget.
   Both lines are released, then separately sampled after a settling phase.
   With SCL high and SDA low, the controller generates up to **nine recovery
   clocks**, stopping early if SDA releases, and attempts STOP. An initially
   held-low SCL causes **zero recovery pulses**. SCL held low after a release
   also terminates the attempt within a finite wait. A released bus gets a
   bounded SERCOM restart, followed by the existing initialization retry backoff.
4. GPIO operations only set input/high impedance or output LOW, with LOW latched
   before enabling output. They **never actively drive either line HIGH**.
   External pull-ups to 3.3 V are required. The installed Seeed core's actual
   `digitalWrite`/`pinMode` implementation was inspected to verify that ordering.
5. Clearing advances one phase per normal main-loop pass; there are no new
   delay loops, spin waits or watchdog feeds inside the clear engine. Minimum
   phase holds are 10 microseconds, SCL release wait is 2 ms, and the clear
   engine has a **250 ms nominal overall deadline**. Main-loop scheduling adds
   return latency. SERCOM disable/restart each retain a separate 10 ms operation
   budget. Motor timeout, serial input and lights continue to be serviced.
   The independent watchdog and post-watchdog sensor lockout remain enabled.

The STOP setup includes a separate SCL rising edge after the recovery clocks;
`clear_pulses` counts only the up-to-nine recovery clocks. If SDA remains low,
the controller can attempt but cannot physically complete STOP. Final line
levels and failure result make that condition explicit.

State codes 0–9 remain unchanged; **10 = bus clear**, **11 = SERCOM restart**.
Clear result codes are 0 not started, 1 running, 2 lines released/STOP completed,
3 SDA still held, 4 SCL held, 5 clear deadline, 6 SERCOM disable failed/no GPIO
clocks attempted. `restart_wire` separately reports failure to restart SERCOM.
Result 2 does not promise that the sensor subsequently acknowledges or
initializes. Initialization and motor-command lock policies remain as documented
above; unavailable/recovery states do not permanently lock out drive/light
commands. Recovery/calibration attempts still conservatively stop drive.

### Recoverable cases and limits

Firmware can attempt to resynchronize a target whose interrupted transfer holds
SDA low and which releases it within nine recovery clocks. It can also restore
its own peripheral after a bounded transaction error and a successful STOP.
NACK from an absent target remains an unavailable-device condition rather than
proof of damaged hardware.

Firmware cannot force a held-low SCL high, guarantee release of SDA after nine
clocks, power-cycle the sensor, or repair supply dips, ground movement, noise,
vibration, wiring faults or damaged hardware. UM10204 recommends device hardware
reset or power cycling when bus clearing is insufficient. No dedicated sensor
reset/power-switch facility was added. Failed attempts release the GPIO drivers
and return to bounded backoff instead of looping continuously or disabling the
watchdog.

### Build and flash

Seeed SAMD Boards **1.8.6**, FQBN `Seeeduino:samd:seeed_XIAO_m0`, default Arduino
USB stack, Adafruit SleepyDog **1.8.4**, arduino-cli **1.5.1**. No installed core
patch or new third-party firmware dependency was introduced. Copy **both**
`BoundedI2C.h` and `BusClear.h` alongside the sketch when retrieving this build.

The final warning-enabled production build succeeded without warnings:
**51,644 sketch bytes**, **52,348-byte binary**. Normal Arduino upload on the
positively identified COM3 XIAO wrote and checksum-verified the full binary.
Normal application USB returned on COM3. No reset-button intervention or port
access error occurred in this follow-up.

Final production SHA-256 hashes:

```text
GOOB_seeed-xiao-samd21.ino
183CD64672B281C75E47073DFB0025C928A46BC81CAFC8E488A0E0762870E18B
BoundedI2C.h
C201A8D6F2EEBB4811EDF06FF4F1BE78E72FDA7C1004BD7193FD516A8C348DF9
BusClear.h
EDDED1A506CBD7ED27F748BA0531F9C4E958F1B52FFDBD3C61885284EA06E83F
```

### Validation performed

**On-MCU simulated-line/firmware harness: 39 checks passed, 8.719 seconds.**
The exact production `BusClear.h` ran against simulated wired-AND lines on the
SAMD21. Checks covered idle-bus STOP without recovery clocks, SDA releasing
after three clocks, SDA held through nine clocks, SCL held initially with zero
pulses, both lines held, SCL becoming held during recovery, finite deadlines,
released output drivers, and micros wraparound. Integration tests verified
separate fault-line capture before cleanup, scheduled recovery, abort/release
of calibration locks, backoff, motor timeout, estimator restart and diagnostic
rate limiting. One intentional watchdog reset again entered sensor lockout.
These are logic tests, not physical shorted-line or motor-output measurements.
Evidence: `tests/harness-validation-v3.log` and `.json`.

**Real adapter/MPU-6050: 10 checks passed.** Actual WHO_AM_I was 0x68; real
register writes and repeated-start 14-byte reads succeeded. An unused-address
NACK returned in **124 microseconds**. A deliberately gated SERCOM2 core clock
produced timeout error 5 and returned in **10,017 microseconds**, including
adapter cleanup. Communication recovered after clock restoration/reinitialization.
The real idle-bus open-drain STOP completed with zero recovery pulses and both
lines high; SERCOM restart and the following real MPU read succeeded. The
diagnostic did not short powered pins. Its idle-bus test does not establish
physical recovery from an actually stuck target.
Evidence: `tests/adapter-validation-v3.log` and `.json`.

**Production parser/stress regression with IMU attached: PASS, 60.641 seconds.**
The run retained increasing uptime, responsive V/light/zero-drive commands,
strict finite motor parsing without timestamp renewal for invalid values,
bounded overlong/missing-newline handling, and parser recovery. Sustained input
transmitted **184,404 bytes** and collected **762 V responses**. Initial/final
status showed a ready IMU, error 0, fault count 0, and both live lines high.
Uptime increased from **19,123 to 79,314 ms** without an observed reset.
This is a connected-hardware regression, not a new ten-minute bare-board soak.
Evidence: `tests/serial-validation-v3.log` and `.json`.

**Production calibration regression: all 9 checks passed, 31.141 seconds.**
Recorded **2,715 sample lines** and **36 status responses**. Real manual
calibration completed 400 samples while V remained responsive. Sensor readiness
and zero errors were retained, normal samples resumed, and uptime increased from
**107,103 to 137,247 ms** without an observed reset. Valid motor commands were
zero only; motor timeout/off status stayed asserted throughout calibration.
Evidence: `tests/sensor-validation-v3.log` and `.json`.

**Final production telemetry capture: PASS, 60.0 seconds.** Recorded **6,005
sample lines** and **462 summaries**, averaging **100.008 Hz** with a largest
sample gap of **26 ms**. No warnings, sensor errors or observed resets occurred.
Status uptime increased from **162,069 to 217,134 ms**; the final sample timestamp
was **222,109 ms**, so the final production boot ran for more than 222 seconds
across the regression/calibration/capture sequence. All status checks reported
both live lines HIGH, IMU ready, `imu_error=0`, and `faults=0`. Thus no natural
stuck-bus fault occurred during the final production run, and no claim of
recovery under actual motor noise can be made. Evidence:
`tests/connected-monitor-v3.log` and `.json`.

The final installed firmware is **production v3**, not either test sketch.
All test serial connections were closed. COM3 is available for the SBC/serial
monitor, and no user reset is required.

The v3 test scripts write distinct `*-v3` evidence files, preserving earlier v2
logs. `README.md` documents the new support file, diagnostic/result fields,
recoverable cases, limits and test commands.

### Pending electrical and loaded-motor validation

No oscilloscope or independent IMU-supply measurement was available in this
session. IMU supply, SDA and SCL **were not observed electrically while motors
started**. No loaded motor start, vibration test, external stuck-line test,
physical disconnect/reconnect, or driver-output timing measurement was run.
Those remain pending, as does distinguishing a bus lockup from motor-induced
supply dip, ground movement or noise. Simulated line faults plus a real
idle-bus STOP do not prove recovery under the reported motor-use conditions.
The MCU/reset-versus-I2C distinction in the supplied logs remains the current
evidence; this firmware does not establish a new diagnosis of the original fault.

---

## Independent drive and wheel comparison — October 28, 2026

The user reported drive freezing during IMU dropouts and explicitly required
drive to work independently of IMU availability. They authorized drive testing
and confirmed the robot was lifted. This requirement **supersedes** the prior
`RESPONSE.md` instruction to stop motors on sensor faults/recovery and reject
drive commands during calibration. Earlier sections describe those previous
policies and must not be read as current v4 behavior.

### Reproduced v3 interruption with wheels installed

A first 29.407-second exploratory sequence used brief positive drive at 30–50%
duty, with zero-command stop intervals between both-wheel and individual-wheel
tests. It recorded five IMU fault reports and repeated `ERR M ignored during IMU
calibration` responses. The user's initial visual observation was uncertain
because the exact powered durations had not been clear.

The observed repeat therefore used a simple announced pattern: **3 seconds
stopped, 4 seconds both wheels at 50%, 3 seconds stopped, 4 seconds at 50%, then
3 seconds stopped**. The user confirmed that **both wheels stopped during the
first powered interval**. At approximately 0.219 seconds after that interval
started, the log recorded an address NACK. Later it recorded BUS_ERROR with a
partial read, SDA low/SCL high during failed startup, and rejected motor commands
through recovery calibration. USB remained responsive; device uptime did not
indicate a reset during this run. The longest motor-ACK gap was **1.25 seconds**.

This established an actionable firmware cause of the drive interruption: v3
deliberately cleared motor outputs on IMU faults/recovery and rejected commands
during calibration. It does not establish what electrical/mechanical condition
caused the sensor fault itself.

Evidence: `tests/drive-wheels_on_v3.log/json` and
`tests/drive-wheels_on_v3_observed.log/json`.

### Implemented v4 behavior

Identity: **`goob-samd21-independent-drive-v4`**.

- Sensor sample/startup faults, bus clearing, SERCOM restart and backoff no
  longer modify motor output or renew valid motor-command age.
- Valid drive commands are accepted through every IMU state, including sensor
  lockout after watchdog recovery. A drive command applies first and cancels
  any in-progress gyro calibration; calibration never makes drive wait.
- The one-second command timeout remains active. Explicit zero commands,
  invalid numeric motor commands and MCU reset/watchdog recovery still stop
  drive. This does not make motors immune to MCU power/reset faults.
- Hardware IMU initialization reaches ready state separately from bias
  calibration. First initialization can collect 100 bias samples at commanded
  rest, using the normal sample scheduler. Nonzero drive cancels that collection.
  An established gyro bias survives sensor recovery, avoiding repeated
  recalibration while the robot is moving.
- C requests 400-sample calibration only at commanded rest. C while driving
  returns an error without changing drive. A later drive command cancels C and
  retains the last completed bias. Calibration timeout likewise cancels
  calibration without marking a healthy sensor unavailable. Physical rest
  remains the user's responsibility; commanded rest alone is not a motion
  measurement.
- If first calibration was cancelled before any successful bias estimate,
  `imu_bias_valid=0` and zero bias remains in use. C while stationary establishes
  a bias. The telemetry must not imply that such a moving-start estimate is
  already calibrated.
- Read-only V adds `motor_left` and `motor_right` (clamped logical outputs
  before wiring inversion) and `imu_bias_valid`. These are software output
  state, not measured wheel velocity or driver voltage.

`BoundedI2C.h` and `BusClear.h` are unchanged from v3. The new source SHA-256 is:

```text
GOOB_seeed-xiao-samd21.ino
9B2197AB3CDEE1C81B8F7219EDBEB09F6B586DA1260915055B1FDC0334DFA655
```

Same build configuration: Seeed SAMD Boards 1.8.6, FQBN
`Seeeduino:samd:seeed_XIAO_m0`, default Arduino USB stack, SleepyDog 1.8.4,
arduino-cli 1.5.1. Warning-enabled compilation succeeded without warnings:
**51,932 sketch bytes**, **52,636-byte binary**. Normal Arduino upload on COM3
wrote and checksum-verified the production binary.

### v4 logic checks and observed wheels-on test

The on-MCU harness passed **34 checks** in **7.609 seconds**, substituting a
simulated bus and suppressing physical PWM calls. It verified preservation of
motor output/command age through sample faults, failed clears/restarts and
reprobes; command acceptance with an unavailable IMU; bias retention across
recovery; drive preemption of automatic/manual calibration; non-disruptive
rejection of C while driving; stationary calibration completion; cancellation
on calibration deadline; command-timeout/invalid-command/explicit-zero stops;
outage integration rejection; existing bounded bus-clear cases; and actual
watchdog reset with boot output shutdown and subsequent drive acceptance in
sensor lockout. Evidence: `tests/harness-validation-v4.log/json`.

The production v4 build was then installed and the identical observed simple
wheels-on sequence repeated, lasting **17.422 seconds**. The user reported
**"Never turned off; looked good"** for the powered intervals.

The IMU still produced **six fault reports** across this sequence, including
BUS_ERROR/partial reads and SDA-low startup failures. Nevertheless:

- **Zero drive-command errors/rejections** occurred.
- All **40 powered-interval V samples** retained logical left/right outputs
  **0.500/0.500**, with no command timeout applied.
- **276 motor ACKs** were received; longest ACK gap **0.109 seconds**.
- Status uptime increased monotonically from **14,076 to 30,932 ms**, with no
  observed reset. Samples continued/recovered when the bus became available.
- Repeated final zero commands were sent and the serial port was closed.

This is user-observed continuous wheel motion supported by software state and
serial timing. It is not a scope measurement of uninterrupted PWM voltage.
The count of IMU faults is not a cross-version reliability metric: v4's different
recovery/calibration behavior also changes how soon another fault can be seen.
Evidence: `tests/drive-wheels_on_v4_observed.log/json`.

### Wheel-magnet hypothesis and comparison procedure

The MPU-6050 contains a MEMS accelerometer and gyroscope, not an integrated
magnetometer. Direct compass-heading disturbance is therefore not the ordinary
failure mode for this sensor. See the [TDK/InvenSense MPU-6000/MPU-6050 product
specification](https://invensense.tdk.com/wp-content/uploads/2015/02/MPU-6000-Datasheet.pdf).
This fact does not prove immunity of the complete board/wiring to strong or
time-varying fields. Motor switching noise, supply/ground disturbance and
vibration remain plausible alternatives, not measured diagnoses.

The user was asked to power down, remove the wheels, keep their magnets away
from the IMU/wiring, then reconnect while leaving the robot lifted. The user
instead reported removing the wheels without powering down; no power cycle was
assumed or required after that confirmation. The comparison used the **same
production v4 and MCU boot**, drive duty and 4-on/3-off pattern.
Removing wheels also changes inertia, motor current and vibration. Therefore
even a disappearance of faults would implicate the wheel assembly/setup, not
uniquely identify magnets. No loaded supply or SDA/SCL scope measurements have
been made.

### Observed wheels-off comparison

The wheels-off sequence lasted **17.406 seconds**. The user reported **"No
issues"** with motor continuity. All **40 powered-interval status samples**
again retained logical **0.500/0.500** with no motor command timeout. There were
**zero command errors**, **276 motor ACKs**, and a longest ACK gap of **0.109
seconds**. Uptime increased from **202,980 to 219,823 ms** in the same boot.

The IMU nevertheless faulted at **both motor starts**: BUS_ERROR with two bytes
received near host time 3.0 seconds, then BUS_ERROR with zero bytes near 10.0
seconds. Both reported SDA LOW/SCL HIGH at the fault snapshot. The fault count
increased from 6 to 8. The sensor subsequently recovered; the final status was
ready with both live lines HIGH. Thus wheel removal did **not** eliminate the
motor-start sensor communication fault.

| Same v4 sequence | Wheels installed | Wheels removed |
| --- | ---: | ---: |
| Test duration | 17.422 s | 17.406 s |
| IMU fault reports across entire run | 6 | 2 |
| V statuses with IMU unavailable | 41 of 85 | 12 of 85 |
| Motor-command errors | 0 | 0 |
| Powered statuses retaining both requested outputs | 40 of 40 | 40 of 40 |
| Longest motor ACK gap | 0.109 s | 0.109 s |
| User-observed premature motor stop | None | None |

These are single short sequences, not a statistical reliability study. Reduced
fault count with wheels removed cannot isolate magnetic effects from changed
load/inertia/vibration. Wheel-magnet rotation was absent from the second run,
yet motor-start faults persisted. Motor-start electrical disturbance and
mechanical effects therefore remain more useful hypotheses to investigate than
direct compass interference. No field-strength measurement or controlled test
of magnet distance alone was performed; the removed wheels' precise location
was not recorded. Sensor availability changes are I2C observations, **not proof
that IMU power itself switched on/off**.

Evidence: `tests/drive-wheels_off_v4_observed.log/json`.

### Remaining limitations and installed state

Production v4 remains installed on COM3. The independent-drive behavior is
demonstrated by the user's wheel/motor observations while logs recorded real
IMU faults. The underlying sensor fault remains unresolved. Supply/ground and
SDA/SCL waveforms at motor startup, motor suppression/decoupling, wiring and
vibration still need investigation with suitable electrical measurements. No
claim of physical power integrity, magnetic immunity or uninterrupted driver
voltage is made from serial ACKs alone.

All drive-comparison sequences ended with repeated zero commands and closed
their serial connection. Test scripts and `README.md` now reflect v4's new
policy; v2/v3 logs remain preserved under their earlier names.

### Final v4 zero-drive regression

The production parser/stress regression passed in **60.532 seconds** with the
IMU attached and only zero valid drive commands. It again verified bounded
overlong/missing-newline/NUL handling, finite numeric parsing, no timestamp
renewal for invalid motor values and continued V responses. Sustained input
sent **164,318 bytes** and collected **679 V responses**. Uptime increased from
**269,602 to 329,672 ms**, IMU readiness remained 1, and the cumulative fault
count stayed **8** from start to finish. The final state reported zero logical
motor outputs, no active calibration, both bus lines HIGH and current error 0.
This provides a no-drive comparison for the earlier motor-start faults; it
still does not identify an electrical component as the cause.
Evidence: `tests/serial-validation-v4.log/json`.

The final stationary real-sensor calibration regression also passed all nine
checks in **31.078 seconds**, recording **2,708 sample lines** and **36 status
responses**. Manual calibration completed 400 samples while V remained
responsive. Fault count stayed **8** throughout; uptime increased from
**445,988 to 476,069 ms**. Final current error was 0, IMU ready was 1, calibration
was inactive, both live lines were HIGH, and logical motor outputs were zero.
Evidence: `tests/sensor-validation-v4.log/json`.

Both regression scripts finished and closed COM3. Production v4 remains
installed, motors are stopped, and no reset-button action is needed. The
remaining work concerns the motor-start sensor communication fault, not the
now-demonstrated firmware drive/calibration interlock removal.
