# IMU Daemon

This directory contains the local GPIO/I2C IMU service for an `MPU-6050`-class
sensor on the Orange Pi Zero 2W header bus.

## Current Hardware Status

This daemon is no longer the active Goob IMU path.

As of `2026-03-28`, Goob's working IMU path is a dedicated Arduino Nano over
USB serial running `firmware/GOOB_arduino-nano-imu/`, with the Orange Pi
consuming IMU data through `serial_mcu_bridge` rather than direct GPIO/I2C.

This README is kept because the Orange Pi header-bus bring-up and overlay work
are still useful reference material if direct-host IMU access is revisited on a
future robot.

Goob has a `GY-521` breakout with an `MPU-6050` connected directly to the
Orange Pi Zero 2W GPIO header with:

- `pin 1` -> `VCC`
- `pin 3` -> `SDA`
- `pin 5` -> `SCL`
- `pin 6` -> `GND`
- `AD0` tied low for I2C address `0x68`
- `INT` left disconnected

On Goob, the GPIO header pair on `pin 3/5` requires the Orange Pi overlay
`pi-i2c1`. With that overlay enabled, the header bus appears as Linux
`/dev/i2c-2`. The MPU-6050 is expected at address `0x68`; earlier bring-up did
produce successful reads there, but that behavior is not currently reliable.

The current Goob bring-up also uses the user overlay source in
`control/services/imu_daemon/pebble-imu-i2c1-tune.dts`, which adds
`bias-pull-up` to the header pins and lowers the I2C bus speed during
troubleshooting.

During troubleshooting, Goob temporarily had an added `1uF` capacitor across
`3.3V/GND` at the IMU breakout. That capacitor has since been removed.

## Current Software Status

Historical bring-up confirmed that the MPU-6050 answered on `/dev/i2c-2`
address `0x68`, including successful `WHO_AM_I` (`0x75`) and `PWR_MGMT_1`
(`0x6b`) reads. As of the latest live Goob checks on `2026-03-28`, that is no
longer reproducible with the current physical setup:

- direct reads on `/dev/i2c-2` now consistently fail with
  `TimeoutError(110, "Connection timed out")`
- the kernel logs `i2c i2c-2: mv64xxx: I2C bus locked, block: 1, time_left: 0`
  after each probe attempt
- the original Goob tuning overlay (`10kHz` plus `bias-pull-up`) still locks
  the controller
- a temporary `50kHz` overlay with no Orange Pi internal pull-up also locked
  the controller

So the IMU is still assigned to the correct bus in software, but the bus is
currently in a hardware-level fault state rather than a merely noisy state.

The daemon now exists with a conservative first-pass runtime contract:

- it owns local IMU register access
- it reads full motion frames with combined I2C register transfers
- it retries transient read failures and reopens the device after repeated
  errors
- it publishes:
  - high-rate filtered samples on `{base}/outgoing/sensors/imu-fast`
  - low-rate filtered state plus health/bias info on `{base}/outgoing/sensors/imu`
- it performs startup gyro-bias calibration when the robot is stationary

This is intentionally a bring-up-oriented implementation, not a final sensor
fusion stack. For Goob specifically, it should now be treated as the archived
direct-host IMU path rather than the active deployment path.

## Bus Tuning Notes

The overlay source currently sets:

- `clock-frequency = <10000>` on the header I2C controller
- `bias-pull-up` on the header pinmux

Those settings were chosen during failure analysis, not because they are known
to be ideal. The latest live test matrix has ruled out one obvious alternative:

- `50kHz` without Orange Pi internal pull-ups still hard-locks `/dev/i2c-2`

Future live testing should explicitly revisit:

- whether the `GY-521` board's own pull-ups make Orange Pi internal pull-ups
  unnecessary or excessive
- whether a faster but still conservative bus clock such as `50kHz` or
  `100kHz` is more stable than the current `10kHz`
- whether the added `1uF` decoupling capacitor or any wiring change is now
  holding `SDA` or `SCL` low
- whether the `GY-521` breakout is still powered from `3.3V` and not exposing
  `5V` pull-ups onto the Orange Pi header
- whether the board can be recovered by temporarily disconnecting the IMU or
  capacitor and confirming the controller stops reporting `bus locked`

## Current Payload Intent

- `sensors/imu-fast`
  - non-retained filtered motion/orientation samples for high-rate local consumers
  - intentionally not mirrored by `mqtt_bridge`
- `sensors/imu`
  - lower-rate filtered state plus health counters, calibration metadata, and
    bias estimates
  - intended to be safe to mirror through `mqtt_bridge`

The daemon currently focuses on filtered acceleration, gyro rates, roll/pitch,
temperature, and transport health. More advanced fusion can be layered on top
after the bus itself is proven stable.
