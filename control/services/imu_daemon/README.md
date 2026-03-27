# IMU Daemon

This directory is reserved for a future local IMU service.

## Current Hardware Status

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
`/dev/i2c-2` and the MPU-6050 is detectable at address `0x68`.

The current Goob bring-up also uses the user overlay source in
`control/services/imu_daemon/pebble-imu-i2c1-tune.dts`, which adds
`bias-pull-up` to the header pins and lowers the I2C bus speed during
troubleshooting.

## Current Software Status

The MPU-6050 is present on the correct header I2C bus, but reliable register
reads are still only partially stable:

- `i2cdetect -r` now shows `0x68` reliably on `/dev/i2c-2`
- after wake-up writes, direct register reads can succeed:
  - `WHO_AM_I` (`0x75`) has returned `0x68`
  - `PWR_MGMT_1` (`0x6b`) has returned `0x00`
- `i2ctransfer` is behaving better than `i2cget` for repeated reads
- repeated reads still produce intermittent NACK/timeouts, so the bus is not
  stable enough yet for a daemon to depend on without retries
- earlier failed transactions were able to lock the controller badly enough to
  require a reboot; the current tuning overlay has improved that behavior but
  has not eliminated transient failures

Because of that, this daemon is intentionally not implemented yet. The next
step is to make low-level register reads consistently stable before
introducing a runtime service contract.

## Intended Scope

The eventual daemon should:

- own the local MPU-6050 access
- publish low-bandwidth IMU telemetry into local MQTT
- keep raw/high-rate sensor access local-first if needed
- avoid mixing IMU hardware ownership into unrelated services
