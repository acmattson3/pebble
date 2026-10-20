# Tests

This directory contains unit tests for the Pebble runtime stack.
The tests avoid on-system dependencies (real MQTT brokers, serial devices, media binaries)
by using local fakes/mocks.

## Run

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -v
```

## On-Robot Smoke Test

Run on the robot to exercise real launcher + MQTT behavior:

```bash
python3 tests/on_robot_smoke.py --config control/configs/config.json
```

If launcher is already running:

```bash
python3 tests/on_robot_smoke.py --config control/configs/config.json --no-launch
```

Options:

- `--no-launch`: run checks against an already-running stack.
- `--skip-remote`: only run local checks and skip remote broker verification.

If remote checks fail from transient network/broker issues, rerun with
`--skip-remote` to isolate local stack health.

## Current coverage

- `test_modules.py`: versioned manifests, command validation, compatibility adapters, compact bridge routing/retention, generic web discovery and offline handling.
- `test_module_inputs.mjs`: shared input action semantics; run with `node --test tests/test_module_inputs.mjs`.
- `module_browser_smoke.py`: optional two-broker browser integration checks; see [module validation](../modules/README.md#validation).
- `test_common.py`: config loading, service config helpers, topic identity helpers.
- `test_capabilities.py`: capability topic/payload derivation from unified config.
- `test_mqtt_bridge.py`: local/remote forwarding behavior, mirror-flag rules, media flag handling, heartbeat scope.
- `test_serial_mcu_bridge.py`: incoming command handling and serial telemetry publishing.
- `test_soundboard_handler.py`: action parsing, file path safety, retained file-list publishing, subprocess lifecycle.
- `test_autonomy_manager.py`: autonomy discovery, command parsing, one-at-a-time subprocess lifecycle, retained files/status publishes.
- `test_launcher.py`: service enable/disable behavior and child shutdown semantics.
- `test_av_daemon.py`: GStreamer pipeline command generation for AV daemon backends and config gating.
- `test_unified_config.py`: unified `config.json` adapter coverage for audio/video/web runtime config derivation.
- `on_robot_smoke.py`: real on-robot launcher + local/remote MQTT smoke checks (including retained capabilities).
  - also validates retained soundboard/autonomy topics when those services are enabled.

## Notes

- This is a first-pass safety net for development, including broker routing,
  media lifecycle, module discovery, and browser input behavior.
- The suite does not replace on-robot checks. Additional fault injection,
  reconnect coverage, real device checks, and browser microphone/speaker
  validation remain useful follow-up work.
