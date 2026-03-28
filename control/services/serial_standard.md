# Pebble Serial Standard v1

`pebble_serial_v1` is a self-describing MCU-to-host serial protocol for Pebble.

The intent is:

- new MCUs can be attached or removed without adding a new host-side bridge
- `config.json` continues to own robot identity (`system`, `type`, `id`)
- the MCU owns its own interface manifest
- `serial_mcu_bridge` combines the two to generate MQTT topics dynamically

This document defines the compact on-wire standard. The bridge may still
publish the discovered manifest to MQTT as JSON for inspection, but JSON is not
required on the serial link.

## Ownership Boundary

- `config.json`
  - robot identity
  - serial port / baud
  - broker settings
  - optional host-side policy overrides
- MCU
  - protocol version
  - stable `device_uid`
  - firmware version
  - compact interface manifest
  - compact runtime packets
- `serial_mcu_bridge`
  - serial framing
  - discovery caching
  - MQTT topic generation
  - generic encode / decode
  - profile-aware compatibility aliases when possible

## Topic Generation

The host derives `base = {system}/{type}/{id}` from `config.json`.

For each discovered interface, the bridge should generate:

- generic topic
  - outgoing: `{base}/outgoing/mcu/{device_uid}/{name}`
  - incoming: `{base}/incoming/mcu/{device_uid}/{name}`
- canonical alias topic
  - outgoing: `{base}/outgoing/{channel}`
  - incoming: `{base}/incoming/{channel}`

Rules:

- the MCU declares `channel` as a relative suffix, never a full MQTT topic
- generic topics are always safe and collision-free
- canonical alias topics are optional compatibility paths
- if two MCUs claim the same canonical alias topic, the bridge must keep the
  generic topics and suppress the conflicting alias until the conflict is
  resolved

## Transport

All packets are compact binary frames.

- framing: `COBS`
- packet delimiter: `0x00`
- integrity: `CRC16-CCITT`
- byte order: little-endian

All decoded packets use this layout:

- `version` `u8`
- `msg_type` `u8`
- `flags` `u8`
- `interface_id` `u16`
- `seq` `u32`
- `timestamp_ms` `u32`
- `payload`
- `crc16` `u16`

`interface_id` is `0` for protocol-wide packets such as discovery.

## Strings

Variable-length strings use `short-string` encoding:

- `length` `u8`
- `utf8_bytes[length]`

The maximum encoded string length is `255` bytes.

## Message Types

- `0x01` `HELLO`
  - compact device summary
- `0x02` `DESCRIBE_REQ`
  - empty payload
- `0x03` `DESCRIBE`
  - compact full interface manifest
- `0x10` `SAMPLE`
  - binary payload for a high-rate outgoing interface
- `0x11` `STATE`
  - binary payload for a low-rate outgoing interface
- `0x20` `CMD`
  - binary payload for an incoming interface
- `0x21` `ACK`
- `0x22` `NACK`
- `0x30` `LOG`
  - UTF-8 text payload for human-readable logs
- `0x31` `PING`
- `0x32` `PONG`

## HELLO Payload

`HELLO` is a compact device summary:

- `schema_version` `u8`
- `interface_count` `u8`
- `schema_hash` `short-string`
- `device_uid` `short-string`
- `firmware_version` `short-string`
- `model` `short-string`

The host uses `schema_hash` to decide whether it can reuse cached discovery or
should request `DESCRIBE`.

## DESCRIBE Payload

`DESCRIBE` contains the full manifest:

- `schema_version` `u8`
- `interface_count` `u8`
- `schema_hash` `short-string`
- `device_uid` `short-string`
- `firmware_version` `short-string`
- `model` `short-string`
- repeated `interface_count` times:
  - `id` `u16`
  - `dir_code` `u8`
  - `kind_code` `u8`
  - `flags` `u8`
  - `rate_hz` `u16`
  - `name` `short-string`
  - `channel` `short-string`
  - `profile` `short-string`
  - `encoding_kind` `u8`
  - `field_count` `u8`
  - repeated `field_count` times:
    - `name` `short-string`
    - `type_code` `u8`
    - `scale` `f32`
    - `offset` `f32`
    - `unit` `short-string`

### Interface `dir_code`

- `1` = `in`
- `2` = `out`

### Interface `kind_code`

- `1` = `sample`
- `2` = `state`
- `3` = `command`
- `4` = `event`

### Interface Flags

- bit `0x01`: `retain`
- bit `0x02`: `local_only`
  - the host may still publish the topic on local MQTT
  - `mqtt_bridge` should not mirror that interface's generic or alias topics to the remote broker

### Encoding Kinds

- `1` = `struct_v1`

## `struct_v1`

`struct_v1` is the required baseline runtime encoding.

Fields are packed in the declared order with no separators.

Supported field type codes:

- `1` = `bool`
- `2` = `u8`
- `3` = `i8`
- `4` = `u16`
- `5` = `i16`
- `6` = `u32`
- `7` = `i32`
- `8` = `f32`

Decoded values use:

- `decoded = raw * scale + offset`

Recommendations:

- use scaled integers instead of text or `f32` wherever reasonable
- reserve `f32` for values that truly need it

## Discovery Lifecycle

Expected host lifecycle:

1. open the serial port
2. wait for `HELLO`, or proactively send `DESCRIBE_REQ`
3. decode `DESCRIBE`
4. register MQTT topics for every interface
5. begin processing `SAMPLE` / `STATE`

The MCU may emit `HELLO` and `DESCRIBE` automatically on boot. It must also
respond to `DESCRIBE_REQ`.

## Profiles

Profiles define standard semantics on top of transport.

Example profile names:

- `battery.voltage.v1`
- `battery.charging.v1`
- `imu.motion.v1`
- `imu.summary.v1`
- `drive.diff.v1`
- `light.rgb.v1`
- `touch.scalar.v1`
- `gpio.digital_in.v1`
- `gpio.digital_out.v1`

The bridge can always expose generic per-MCU topics. Known profiles allow it to
also generate compatibility payloads and canonical aliases automatically.

## Current Implementation Scope

The first implementation target for `pebble_serial_v1` is the Goob Arduino Nano
IMU bridge:

- compact `HELLO`
- compact `DESCRIBE`
- compact binary `SAMPLE` / `STATE`
- profile-aware publishing for:
  - `imu.motion.v1`
  - `imu.summary.v1`

Legacy protocols such as `goob_base_v1` and `imu_mpu6050_v1` remain supported
while the new standard is rolled out.
