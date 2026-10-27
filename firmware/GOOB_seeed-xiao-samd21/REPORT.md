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
