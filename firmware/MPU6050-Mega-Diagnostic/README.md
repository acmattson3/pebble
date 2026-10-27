# MPU-6050 Mega diagnostic

Diagnostic firmware for an Arduino Mega 2560 wired to an MPU-6050 breakout on SDA 20 / SCL 21. It probes `0x68` and `0x69`, reads `WHO_AM_I`, configures a 50 Hz data rate with the digital low-pass filter enabled, averages 200 still gyro samples for bias calibration, then streams CSV over USB serial at 115200 baud.

The CSV includes acceleration in m/s², bias-corrected gyro in degrees/second, sensor temperature, a Mahony gravity-corrected quaternion, derived roll/pitch/yaw, and acceleration magnitude in g. Yaw is relative and drifts because there is no magnetometer. Check axis mounting before using the orientation output on a robot.

## Bench result on ARL

On 2026-10-22, this sketch was compiled and uploaded to the Mega at `/dev/ttyACM1` on `arl-aio-acmattson3` (the Uno on `/dev/ttyACM0` was left untouched). With no external SDA/SCL pull-ups and no external AD0 pull-down, it found address `0x68`, read `WHO_AM_I=0x68`, completed all 200 calibration samples, and streamed 261 valid samples during an 8 second capture without read errors. In the initial stationary segment (40 samples), acceleration norm averaged `1.0282 g` (SD `0.0015 g`), and gyro SD was `0.020–0.027 deg/s` per axis after bias correction. Quaternion components stayed within `0.0002` SD in that segment. A previous capture showed clear gyro response while the assembly was moved.

This proves the current breakout and wiring communicate under the current setup. Many MPU-6050 breakouts include their own I²C pull-ups and AD0 strap; therefore this result proves no *additional external* resistors were needed in this test, but does not prove the breakout lacks onboard resistors. Address `0x68` was observed with AD0 left as wired.

## Upload and monitor

Use the Mega board target and its current serial device. For this ARL setup:

```sh
arduino --upload --board arduino:avr:mega:cpu=atmega2560 --port /dev/ttyACM1 MPU6050-Mega-Diagnostic.ino
stty -F /dev/ttyACM1 115200 raw -echo
cat /dev/ttyACM1
```

Keep the sensor still during the startup `CALIBRATING` interval. A new serial connection may reset the Mega, so allow calibration to finish before judging the output.

The monitor is available in the repo and was also copied to `~/monitor_imu.py` on ARL. Run it from an ARL terminal:

```sh
~/monitor_imu.py
```

PySerial 3.5 and Tk are installed on ARL. The monitor auto-selects the Mega by USB ID; to specify the device explicitly, use `~/monitor_imu.py --port /dev/ttyACM1`. Add `--visual` for the graphical view:

```sh
~/monitor_imu.py --visual
```

The view shows a rotating, labeled X/Y/Z body-axis triad and a spring-damped acceleration marker. The marker subtracts a slow gravity estimate, so it represents a motion cue rather than a precision linear-acceleration measurement. Use `Ctrl+C` or close the window to stop. Opening the port may reset the Mega and starts a fresh stillness calibration; keep the IMU still until data appears. Near pitch +/-90°, Euler yaw/roll are singular and can appear to shift even when the quaternion is steady; use the quaternion for orientation there. The script does not publish to MQTT.
