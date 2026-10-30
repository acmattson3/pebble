# MiniLAMP — XIAO ESP32-C6

MiniLAMP runs MQTT directly on its C6; no SBC or Pebble Linux runtime is required.
The current firmware is `minilamp-c6-onoff-arl-tcp-v3`, using MQTT 3.1.1 over
plain TCP to ARL's anonymous broker at `10.10.10.163:1883`. Its motors, battery
sensing, and module discovery follow the current Pebble topic standard.

## Configuration and upload

1. Install Espressif Arduino core **3.3.12** and select **XIAO_ESP32C6**.
2. Copy `private_config.h.example` to `private_config.h`; configure Wi-Fi and,
   if needed, the MQTT URI and credentials. The private header is gitignored.
   The current Goob/MiniLAMP LAN broker is `mqtt://10.10.10.163:1883` and permits
   anonymous clients. For other brokers, set the appropriate URI and credentials.
3. Hardware mapping, motor inversion/output permutation, divider ratio, antenna
   and on/off settings live in `RobotConfig.h`. The existing external-antenna
   setting is preserved.
4. Robot identity and module selection live in
   `robots/minilamp/modules.example.json`. Run `generate_manifest.py` after
   changing those or shared module definitions. It requires the repo's
   `jsonschema` dependency, validates the manifest, and writes
   `GeneratedManifest.h`. Arduino IDE users can use the committed header.
5. Use normal Arduino Upload, or from the repo root:

   ```sh
   PORT=/dev/ttyACM1 ./firmware/flash-minilamp-firmware.sh
   ```

All runtime libraries (ESP-MQTT, cJSON, TLS certificate bundle, Wi-Fi, LEDC and
watchdog) are supplied by the ESP32 core. PubSubClient, ArduinoJson and SleepyDog
are not needed by this target. SNTP is only needed when using a TLS broker; the
current LAN TCP connection does not use TLS. TLS checks the hostname and the
core's CA bundle when enabled; there is no insecure TLS fallback.

## Hardware

| Function | XIAO pins |
| --- | --- |
| Battery, existing 2:1 divider | D0 |
| Front-left motor forward/reverse | D1 / D2 |
| Front-right motor forward/reverse | D4 / D3 |
| Rear-left motor forward/reverse | D5 / D6 |
| Rear-right motor forward/reverse | D8 / D7 |

These are the old sketch's actual definitions, confirmed as unchanged by the
user. Its old rear D9/D10 comments were stale. Driver modules must be correctly
oriented and have motor power and sleep/enable configured. D6 is also UART0 TX
at boot; application code cannot guarantee its state before setup. Reset-output
behavior needs an electrical check; moving this driver input to an unused
non-UART pin or adding a hardware enable gate is a future hardware option.

The drivetrain uses on/off GPIO outputs, matching the original robot. Inputs
below a 25% deadband select stop; otherwise the requested direction is driven
fully on. Left/right and grouser controls are quantized before mixing, matching
the old firmware. Reversal lowers the inactive input before raising the active
one. There is no PWM or variable-speed setting on this platform.

## Controls and safety

Normal drive mixes `left=z+x`, `right=z-x`. Grouser rotation mixes the front and
rear motors in opposite directions as in the previous firmware. Drive and
grouser commands have **independent one-second leases**; refreshing one cannot
keep an abandoned command on the other alive. Inputs and mixed outputs are
bounded to [-1,1]. Invalid commands stop the current outputs without renewing a
lease. Retained command replays are ignored; commands must never be retained.

A dedicated higher-priority motor task owns all motor pins and deadline checks.
Networking runs in ESP-MQTT's separate task, and publishing uses its bounded
outbox. Wi-Fi/MQTT disconnects invalidate queued commands and stop motors;
reconnects require fresh input. A two-second task watchdog is the final fallback.
Startup latches all driver inputs low before Wi-Fi/serial setup. These software
properties do not establish motor-driver electrical states during boot/reset.

Under `pebble/robots/minilamp` (derived once from the generated identity):

| Topic | Contract |
| --- | --- |
| `i/drive` | Non-retained `{ "x": 0, "z": 0 }`; optional legacy `y` and `{ "value": {...} }` wrapper remain accepted |
| `i/grouser` | Non-retained `{ "value": 0 }` |
| `o/manifest` | Retained `pebble-manifest/v2`, republished on connect and every 60 seconds |
| `o/online` | Retained presence every 5 seconds; retained offline Last Will |
| `o/modules/status` | Retained module availability every 5 seconds; consumers must expire stale presence |
| `o/telemetry/battery` | Retained `{ "value": 3.678, "unit": "v" }` every 5 seconds |
| `o/status` | Retained firmware, uptime, network, motor and command diagnostics every 5 seconds |

Discovery reuses shared drive and telemetry modules. A small robot-defined
`minilamp.grouser` module uses generic controls and Q/E bindings, with Space
resetting both drive and grousers. No robot-specific web code or CSS is required.
Presence, discovery, battery and status publish at QoS 1; command subscriptions
use QoS 0 and a clean session. A Serial Monitor at **115200 baud**, Newline,
accepts read-only `V` for the same diagnostics.

A disconnected battery-sense input can float; its observed USB-only reading
near 0.35 V was not a battery measurement. With battery power connected, readings
were around 3.65–3.68 V. Independent meter comparison remains pending.

## Verification

See `REPORT.md` for actual results and limitations. In particular, software output snapshots alone are not proof of motor movement. Initial
fractional-PWM pulses did not move the motors; a brief full-on comparison worked,
and the user confirmed this platform uses on/off drive. V2 now uses digital
outputs explicitly. The user confirmed driving works. The report distinguishes that observation
from the recorded forward/reverse/grouser software states.

```sh
g++ -std=c++11 -Wall -Wextra -Werror \
  firmware/MINILAMP_seeed-xiao-c6/tests/test_drive_safety.cpp -o /tmp/minilamp-drive-test
/tmp/minilamp-drive-test
python3 firmware/MINILAMP_seeed-xiao-c6/tests/verify_mqtt.py \
  --username YOUR_USER --password-file /path/to/password
```

The WSS verifier sends zero valid motor commands and bounded invalid payloads.
`motor_pulse.py` sends a real supervised 0.45-second full-on pulse and requires a currently confirmed lifted setup. It captures
software states through USB serial and always attempts a stop. Avoid running
motion tests with a serial terminal holding the device port.
