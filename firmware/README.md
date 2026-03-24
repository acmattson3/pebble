# Firmware Reference Implementations (Arduino)

This directory preserves MCU firmware examples from the original Pebble project.
These files are reference implementations, not a required Pebble firmware layout.
Pebble does not depend on these exact boards, filenames, or flashing scripts.

If you are integrating a different controller platform, treat the contents of
this directory as examples of how Pebble topics and device behavior can be
mapped onto embedded firmware.

## Included reference targets

- `firmware/FRED_seeed-xiao-samd21/`
  - Reference firmware for the original Fred platform.
- `firmware/GOOB_seeed-xiao-c3/`
  - Reference firmware for the original Goob platform.
- `firmware/MINILAMP_seeed-xiao-c6/`
  - Reference firmware for the original MiniLAMP platform.
- `firmware/BUMPERBOT_wemos/`
  - Reference firmware for the original BumperBot platform.
- `firmware/MINILAMP_serial_test/`
  - Small serial-focused helper sketch kept with the historical firmware set.

## Private config

For the MiniLAMP reference firmware, create a private config header:

```bash
cp firmware/MINILAMP_seeed-xiao-c6/private_config.h.example \
   firmware/MINILAMP_seeed-xiao-c6/private_config.h
```

Then edit `private_config.h` with local Wi-Fi/MQTT credentials and any TLS settings.
`private_config.h` is gitignored so secrets are not committed.

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
arduino-cli core install Seeeduino:samd
```

Reference flash scripts (from repo root):
```bash
./firmware/flash-goob-firmware.sh
./firmware/flash-fred-firmware.sh
./firmware/flash-minilamp-firmware.sh
./firmware/flash-bumperbot-wemos-firmware.sh
```

`flash-fred-firmware.sh` defaults to:
- `PORT=/dev/ttyACM0`
- `FQBN=Seeeduino:samd:seeed_XIAO_m0`

Override as needed, e.g.:
```bash
PORT=/dev/ttyACM1 FQBN=Seeeduino:samd:seeed_XIAO_m0 ./firmware/flash-fred-firmware.sh
PORT=/dev/ttyACM0 FQBN=esp32:esp32:XIAO_ESP32C6 ./firmware/flash-minilamp-firmware.sh
PORT=/dev/ttyUSB0 FQBN=esp8266:esp8266:d1_mini ./firmware/flash-bumperbot-wemos-firmware.sh
```

## MQTT TLS (ESP32 targets)

For the ESP32-based reference firmware, TLS is controlled by compile-time flags
in `firmware/MINILAMP_seeed-xiao-c6/private_config.h`:
- `MQTT_USE_TLS` (0/1)
- `MQTT_TLS_INSECURE` (0/1)
- `MQTT_CA_CERT` (PEM-encoded CA certificate)

Typical settings for a TLS broker:
```cpp
#define MQTT_USE_TLS 1
const uint16_t MQTT_PORT = 8883;
```

For production, provide the broker CA in `MQTT_CA_CERT`. If you cannot, you can set
`MQTT_TLS_INSECURE` to 1 for testing, but this disables certificate validation.

## Notes
- ESP32 does not have a system trust store. A CA certificate is required for
  proper verification.
- If you rotate certificates, update the PEM string in the firmware.
