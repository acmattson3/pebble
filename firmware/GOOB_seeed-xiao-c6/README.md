# Goob firmware for Seeed XIAO ESP32-C6 (experimental)

This is a Goob MCU firmware port for the XIAO ESP32-C6. It preserves Goob's
existing USB serial command protocol, motor and RGB output behavior, and
MPU-6050 telemetry. It is a separate target from Goob's currently installed
XIAO SAMD21 firmware.

## Important hardware difference

The existing Goob battery divider is wired to XIAO D6. On the ESP32-C6 board,
D6 is GPIO16 and is not ADC-capable. The C6 build therefore does not read or
publish battery voltage; it must not be mistaken for a valid `0 V` reading.
The available XIAO C6 ADC pins are D0-D2, which Goob currently assigns to the
three RGB output channels. Do not move the battery divider to one of those
pins without also changing the LED wiring and firmware mapping. A dedicated
ADC-capable input and board-level wiring change are needed to restore battery
telemetry while retaining all three light channels.

The C6 I²C pins remain D4/D5. Its firmware sets a 100 ms I²C timeout and a
5-second task watchdog. The motor command timeout remains 1 second. The IMU
and motor outputs have not been tested on a C6 installed in Goob.

## Build and upload

Install the Espressif Arduino core, select `XIAO_ESP32C6`, and use the board's
USB serial port:

```sh
arduino-cli core install esp32:esp32
arduino-cli compile --fqbn esp32:esp32:XIAO_ESP32C6 \
  firmware/GOOB_seeed-xiao-c6
arduino-cli board list
arduino-cli upload --fqbn esp32:esp32:XIAO_ESP32C6 --port <C6-serial-port> \
  firmware/GOOB_seeed-xiao-c6
```

The upload was verified on 2026-10-26 from ARL through `/dev/ttyACM1`. With no
peripherals connected, it remained enumerated and emitted the expected
`WHO_AM_I-read` IMU-unavailable warnings. Motor movement and battery voltage
were not tested.
