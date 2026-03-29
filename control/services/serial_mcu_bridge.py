#!/usr/bin/env python3
"""Bridge local MQTT command/telemetry topics with one serial MCU instance."""

from __future__ import annotations

import argparse
import json
import logging
import math
import signal
import sys
import threading
import time
from pathlib import Path
from typing import Any, Callable, Optional

import paho.mqtt.client as mqtt
import serial

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parents[2]))

from control.common.config import (
    default_retained_publish_interval_seconds,
    load_config,
    log_level,
    service_instance_cfg,
)
from control.common.mqtt import mqtt_auth_and_tls
from control.common.odometry_shm import OdometryRawShmWriter
from control.common.topics import identity_from_config
from control.services.serial_standard import (
    MSG_ACK,
    MSG_CMD,
    MSG_DESCRIBE,
    MSG_DESCRIBE_REQ,
    MSG_HELLO,
    MSG_LOG,
    MSG_NACK,
    MSG_PING,
    MSG_PONG,
    MSG_SAMPLE,
    MSG_STATE,
    PROTOCOL_NAME as STANDARD_PROTOCOL_NAME,
    Packet as StandardPacket,
    SerialStandardError,
    decode_discovery,
    decode_hello,
    decode_packet,
    decode_struct_payload,
    encode_packet,
    encode_struct_payload,
)


PROTOCOL_GOOB_BASE_V1 = "goob_base_v1"
PROTOCOL_IMU_MPU6050_V1 = "imu_mpu6050_v1"
PROTOCOL_PEBBLE_SERIAL_V1 = STANDARD_PROTOCOL_NAME


def clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _parse_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _vector_payload(values: dict[str, float], digits: int = 4) -> dict[str, float]:
    return {
        "x": round(float(values.get("x", 0.0)), digits),
        "y": round(float(values.get("y", 0.0)), digits),
        "z": round(float(values.get("z", 0.0)), digits),
    }


class SerialMcuBridge:
    def __init__(self, config: dict[str, Any], config_path: Path, instance_name: str | None = None) -> None:
        self.config = config
        self.config_path = config_path
        self.identity = identity_from_config(config)
        self.instance_name = instance_name
        self.service_name = (
            "serial_mcu_bridge" if self.instance_name is None else f"serial_mcu_bridge[{self.instance_name}]"
        )

        self.service_cfg = service_instance_cfg(config, "serial_mcu_bridge", instance_name)
        if instance_name is not None and not self.service_cfg:
            raise SystemExit(f"serial_mcu_bridge instance not found: {instance_name}")
        if not self.service_cfg.get("enabled", True):
            raise SystemExit(f"{self.service_name} is disabled in config.")

        serial_cfg = self.service_cfg.get("serial") if isinstance(self.service_cfg.get("serial"), dict) else {}
        self.serial_port = str(serial_cfg.get("port") or "").strip()
        if not self.serial_port:
            raise SystemExit(f"{self.service_name}: serial.port is required")
        self.baud = int(serial_cfg.get("baud") or 115200)
        self.serial_timeout = float(serial_cfg.get("timeout_seconds") or 0.1)
        self.protocol_name = str(self.service_cfg.get("protocol") or PROTOCOL_GOOB_BASE_V1).strip().lower()

        self.local_mqtt_cfg = config.get("local_mqtt") if isinstance(config.get("local_mqtt"), dict) else {}
        self.local_host = str(self.local_mqtt_cfg.get("host") or "127.0.0.1")
        self.local_port = int(self.local_mqtt_cfg.get("port") or 1883)
        self.local_keepalive = int(self.local_mqtt_cfg.get("keepalive") or 60)

        self.stop_event = threading.Event()
        self.serial_lock = threading.Lock()
        self.telemetry_lock = threading.Lock()
        self.reconnect_lock = threading.Lock()

        self.ser: Optional[serial.Serial] = None
        self.client: Optional[mqtt.Client] = None
        self.reader_thread: Optional[threading.Thread] = None
        self.reconnect_thread: Optional[threading.Thread] = None
        self.odometry_writer: Optional[OdometryRawShmWriter] = None

        self.handlers: dict[str, Callable[[dict[str, Any]], None]] = {}

        self.stop_on_shutdown = False
        self.drive_timeout_seconds = 0.0
        self.ignore_retained_drive = True
        self.last_drive_command_at: Optional[float] = None
        self.drive_timed_out = False

        self.retained_publish_interval_seconds = default_retained_publish_interval_seconds(config, default=3600.0)
        self.next_charge_republish_at = 0.0
        self.next_charge_level_republish_at = 0.0
        self.last_touch: Optional[dict[str, Any]] = None
        self.last_charge: Optional[dict[str, Any]] = None
        self.last_charge_value: Optional[bool] = None
        self.last_charge_level: Optional[dict[str, Any]] = None

        self.last_imu_motion_value: Optional[dict[str, Any]] = None
        self.last_imu_low_payload: Optional[dict[str, Any]] = None

        self.standard_rx_buffer = bytearray()
        self.standard_discovery: Optional[dict[str, Any]] = None
        self.standard_device_uid: Optional[str] = None
        self.standard_schema_hash: Optional[str] = None
        self.standard_interfaces_by_id: dict[int, dict[str, Any]] = {}
        self.standard_next_describe_request_at = 0.0
        self.standard_command_seq = 0

        self._configure_protocol()

    def _configure_protocol(self) -> None:
        topics_cfg = self.service_cfg.get("topics") if isinstance(self.service_cfg.get("topics"), dict) else {}
        telemetry_cfg = self.service_cfg.get("telemetry") if isinstance(self.service_cfg.get("telemetry"), dict) else {}
        safety_cfg = self.service_cfg.get("safety") if isinstance(self.service_cfg.get("safety"), dict) else {}
        publish_cfg = self.service_cfg.get("publish") if isinstance(self.service_cfg.get("publish"), dict) else {}

        interval_raw = self.service_cfg.get("retained_publish_interval_seconds")
        try:
            interval_value = float(
                interval_raw if interval_raw is not None else default_retained_publish_interval_seconds(self.config, default=3600.0)
            )
        except (TypeError, ValueError):
            interval_value = default_retained_publish_interval_seconds(self.config, default=3600.0)
        self.retained_publish_interval_seconds = interval_value if interval_value > 0 else 3600.0

        if self.protocol_name == PROTOCOL_GOOB_BASE_V1:
            self.drive_topic = str(topics_cfg.get("drive_values") or self.identity.topic("incoming", "drive-values"))
            self.lights_solid_topic = str(topics_cfg.get("lights_solid") or self.identity.topic("incoming", "lights-solid"))
            self.lights_flash_topic = str(topics_cfg.get("lights_flash") or self.identity.topic("incoming", "lights-flash"))
            self.touch_topic = str(topics_cfg.get("touch_sensors") or self.identity.topic("outgoing", "touch-sensors"))
            self.charge_topic = str(topics_cfg.get("charging_status") or self.identity.topic("outgoing", "charging-status"))
            self.charge_level_topic = str(
                topics_cfg.get("charging_level") or self.identity.topic("outgoing", "charging-level")
            )
            self.publish_touch_sensors = bool(telemetry_cfg.get("publish_touch_sensors", False))

            odometry_shm_cfg = (
                self.service_cfg.get("odometry_shm") if isinstance(self.service_cfg.get("odometry_shm"), dict) else {}
            )
            default_shm_name = f"{self.identity.system}_{self.identity.type}_{self.identity.robot_id}_odometry_raw"
            self.odometry_shm_enabled = bool(odometry_shm_cfg.get("enabled", True))
            self.odometry_shm_name = str(odometry_shm_cfg.get("name") or default_shm_name)
            self.odometry_shm_slots = max(64, int(odometry_shm_cfg.get("slots") or 2048))

            self.drive_timeout_seconds = max(0.0, float(safety_cfg.get("drive_timeout_seconds") or 0.75))
            self.ignore_retained_drive = bool(safety_cfg.get("ignore_retained_drive", True))
            self.stop_on_shutdown = bool(safety_cfg.get("stop_on_shutdown", True))

            self.handlers = {
                self.drive_topic: self._handle_drive_values,
                self.lights_solid_topic: self._handle_lights_solid,
                self.lights_flash_topic: self._handle_lights_flash,
            }
            return

        if self.protocol_name == PROTOCOL_IMU_MPU6050_V1:
            self.odometry_shm_enabled = False
            self.publish_touch_sensors = False
            self.drive_timeout_seconds = 0.0
            self.ignore_retained_drive = True
            self.stop_on_shutdown = False

            self.imu_high_rate_topic = str(topics_cfg.get("high_rate") or self.identity.topic("outgoing", "sensors/imu-fast"))
            self.imu_low_rate_topic = str(topics_cfg.get("low_rate") or self.identity.topic("outgoing", "sensors/imu"))
            self.imu_high_rate_qos = max(0, min(2, _parse_int(publish_cfg.get("high_rate_qos"), 0)))
            self.imu_low_rate_qos = max(0, min(2, _parse_int(publish_cfg.get("low_rate_qos"), 1)))
            self.imu_high_rate_retain = bool(publish_cfg.get("high_rate_retain", False))
            self.imu_low_rate_retain = bool(publish_cfg.get("low_rate_retain", False))
            self.handlers = {}
            return

        if self.protocol_name == PROTOCOL_PEBBLE_SERIAL_V1:
            self.odometry_shm_enabled = False
            self.publish_touch_sensors = False
            self.drive_timeout_seconds = 0.0
            self.ignore_retained_drive = True
            self.stop_on_shutdown = False
            self.handlers = {}
            self.standard_next_describe_request_at = time.monotonic() + 1.0
            return

        raise SystemExit(f"{self.service_name}: unsupported protocol {self.protocol_name}")

    def start(self) -> None:
        self._open_odometry_shm()
        self._open_serial()
        self._start_serial_reader()
        self._connect_local_mqtt()
        while not self.stop_event.is_set():
            if self.protocol_name == PROTOCOL_GOOB_BASE_V1:
                self._drive_watchdog_tick()
                self._republish_charge_if_due()
                self._republish_charge_level_if_due()
            elif self.protocol_name == PROTOCOL_PEBBLE_SERIAL_V1:
                self._standard_discovery_tick()
            self.stop_event.wait(0.05)

    def stop(self) -> None:
        self.stop_event.set()
        if self.reader_thread and self.reader_thread.is_alive():
            self.reader_thread.join(timeout=2.0)
        if self.stop_on_shutdown:
            self._send_drive_stop(reason="shutdown")
        if self.client:
            try:
                self.client.loop_stop()
                self.client.disconnect()
            except Exception:
                logging.debug("MQTT disconnect failed", exc_info=True)
            self.client = None
        if self.ser:
            try:
                self.ser.close()
            except Exception:
                logging.debug("Serial close failed", exc_info=True)
            self.ser = None
        if self.odometry_writer:
            try:
                self.odometry_writer.close()
            except Exception:
                logging.debug("Odometry shm close failed", exc_info=True)
            self.odometry_writer = None

    def _open_odometry_shm(self) -> None:
        if not getattr(self, "odometry_shm_enabled", False):
            return
        try:
            writer = OdometryRawShmWriter(name=self.odometry_shm_name, slots=self.odometry_shm_slots)
        except Exception as exc:
            logging.error("Odometry shared-memory init failed (%s): %s", self.odometry_shm_name, exc)
            self.odometry_writer = None
            return
        self.odometry_writer = writer
        action = "created" if writer.created else "attached"
        logging.info(
            "Odometry shared-memory %s (%s, slots=%d)", action, self.odometry_shm_name, self.odometry_shm_slots
        )

    def _open_serial(self) -> None:
        try:
            ser = serial.Serial(self.serial_port, self.baud, timeout=self.serial_timeout)
        except serial.SerialException as exc:
            raise SystemExit(f"Failed to open serial port {self.serial_port}: {exc}") from exc
        time.sleep(0.5)
        ser.reset_input_buffer()
        self.ser = ser
        logging.info("Serial connected on %s @ %d for %s", self.serial_port, self.baud, self.service_name)

    def _start_serial_reader(self) -> None:
        thread = threading.Thread(target=self._serial_reader, name=f"{self.service_name}-reader", daemon=True)
        thread.start()
        self.reader_thread = thread

    def _serial_reader(self) -> None:
        assert self.ser is not None
        while not self.stop_event.is_set():
            if self.protocol_name == PROTOCOL_PEBBLE_SERIAL_V1:
                try:
                    read_size = getattr(self.ser, "in_waiting", 0) or 1
                    raw = self.ser.read(read_size)
                except serial.SerialException as exc:
                    logging.error("Serial read failed for %s: %s", self.service_name, exc)
                    time.sleep(0.5)
                    continue
                if not raw:
                    continue
                self._handle_standard_bytes(raw)
                continue

            try:
                raw = self.ser.readline()
            except serial.SerialException as exc:
                logging.error("Serial read failed for %s: %s", self.service_name, exc)
                time.sleep(0.5)
                continue
            if not raw:
                continue
            line = raw.decode("utf-8", errors="ignore").strip()
            if not line:
                continue
            if self.protocol_name == PROTOCOL_GOOB_BASE_V1:
                self._handle_goob_base_line(line)
            elif self.protocol_name == PROTOCOL_IMU_MPU6050_V1:
                self._handle_imu_line(line)

    def _handle_goob_base_line(self, line: str) -> None:
        touch = self._parse_touch_telemetry_line(line)
        if touch:
            self._handle_touch_telemetry(touch)
            return
        charge = self._parse_charge_telemetry_line(line)
        if charge is not None:
            self._handle_charge_telemetry(charge)
            return
        charge_level = self._parse_charge_level_telemetry_line(line)
        if charge_level is not None:
            self._handle_charge_level_telemetry(charge_level)
            return
        self._handle_non_telemetry_line(line)

    def _handle_imu_line(self, line: str) -> None:
        high_rate = self._parse_imu_high_rate_telemetry_line(line)
        if high_rate is not None:
            self._handle_imu_high_rate_telemetry(high_rate)
            return
        low_rate = self._parse_imu_low_rate_telemetry_line(line)
        if low_rate is not None:
            self._handle_imu_low_rate_telemetry(low_rate)
            return
        self._handle_non_telemetry_line(line)

    def _handle_standard_bytes(self, raw: bytes) -> None:
        self.standard_rx_buffer.extend(raw)
        while True:
            try:
                frame_end = self.standard_rx_buffer.index(0)
            except ValueError:
                return
            frame = bytes(self.standard_rx_buffer[:frame_end])
            del self.standard_rx_buffer[: frame_end + 1]
            if not frame:
                continue
            try:
                packet = decode_packet(frame)
            except SerialStandardError as exc:
                logging.warning("Bad %s frame: %s", PROTOCOL_PEBBLE_SERIAL_V1, exc)
                continue
            self._handle_standard_packet(packet)

    def _handle_standard_packet(self, packet: StandardPacket) -> None:
        if packet.msg_type == MSG_HELLO:
            self._handle_standard_hello(packet)
            return
        if packet.msg_type == MSG_DESCRIBE:
            self._handle_standard_describe(packet)
            return
        if packet.msg_type == MSG_SAMPLE:
            self._handle_standard_outbound(packet, is_state=False)
            return
        if packet.msg_type == MSG_STATE:
            self._handle_standard_outbound(packet, is_state=True)
            return
        if packet.msg_type == MSG_LOG:
            try:
                message = packet.payload.decode("utf-8", errors="replace").strip()
            except Exception:
                message = ""
            if message:
                logging.info("[%s] %s", self.service_name, message)
            return
        if packet.msg_type == MSG_ACK:
            logging.info("[%s] ack interface=%d seq=%d", self.service_name, packet.interface_id, packet.seq)
            return
        if packet.msg_type == MSG_NACK:
            try:
                reason = packet.payload.decode("utf-8", errors="replace").strip()
            except Exception:
                reason = ""
            logging.warning("[%s] nack interface=%d seq=%d %s", self.service_name, packet.interface_id, packet.seq, reason)
            return
        if packet.msg_type == MSG_PONG:
            return
        logging.debug("[%s] Ignored standard packet type=%d", self.service_name, packet.msg_type)

    def _handle_standard_hello(self, packet: StandardPacket) -> None:
        try:
            payload = decode_hello(packet.payload)
        except SerialStandardError:
            logging.warning("Bad %s hello payload", PROTOCOL_PEBBLE_SERIAL_V1)
            return
        device_uid = str(payload.get("device_uid") or "").strip()
        schema_hash = str(payload.get("schema_hash") or "").strip()
        if device_uid:
            self.standard_device_uid = device_uid
        if schema_hash:
            self.standard_schema_hash = schema_hash
        self.standard_next_describe_request_at = time.monotonic() + 0.25
        logging.info(
            "[%s] hello device_uid=%s schema_hash=%s",
            self.service_name,
            self.standard_device_uid or "?",
            self.standard_schema_hash or "?",
        )

    def _handle_standard_describe(self, packet: StandardPacket) -> None:
        try:
            payload = decode_discovery(packet.payload)
        except SerialStandardError:
            logging.warning("Bad %s describe payload", PROTOCOL_PEBBLE_SERIAL_V1)
            return

        device_uid = str(payload.get("device_uid") or "").strip()
        schema_hash = str(payload.get("schema_hash") or "").strip()
        if device_uid:
            self.standard_device_uid = device_uid
        if schema_hash:
            self.standard_schema_hash = schema_hash

        interfaces = payload.get("interfaces")
        if not isinstance(interfaces, list):
            logging.warning("Bad %s describe interfaces", PROTOCOL_PEBBLE_SERIAL_V1)
            return

        self.standard_discovery = payload
        self.standard_interfaces_by_id = {}
        for interface in interfaces:
            if not isinstance(interface, dict):
                continue
            try:
                interface_id = int(interface["id"])
            except (KeyError, TypeError, ValueError):
                continue
            self.standard_interfaces_by_id[interface_id] = interface
            if str(interface.get("dir") or "").strip().lower() == "in":
                self._register_standard_command_topics(interface_id, interface)

        if self.standard_discovery is not None:
            self._publish_json(self._standard_discovery_topic(), self.standard_discovery, qos=1, retain=True)
        logging.info(
            "[%s] describe device_uid=%s interfaces=%d",
            self.service_name,
            self.standard_device_uid or "?",
            len(self.standard_interfaces_by_id),
        )

    def _register_standard_command_topics(self, interface_id: int, interface: dict[str, Any]) -> None:
        topic_specs = self._standard_command_topics(interface)
        for topic in topic_specs:
            if topic in self.handlers:
                continue

            def _handler(payload: dict[str, Any], *, _interface_id: int = interface_id) -> None:
                self._handle_standard_command(_interface_id, payload)

            self.handlers[topic] = _handler
            client = self.client
            is_connected = getattr(client, "is_connected", None) if client is not None else None
            if client is not None and callable(is_connected) and is_connected():
                client.subscribe(topic, qos=1)

    def _handle_standard_outbound(self, packet: StandardPacket, *, is_state: bool) -> None:
        interface = self.standard_interfaces_by_id.get(packet.interface_id)
        if not interface:
            logging.debug("Unknown %s interface_id=%d", PROTOCOL_PEBBLE_SERIAL_V1, packet.interface_id)
            return
        if str(interface.get("dir") or "").strip().lower() != "out":
            logging.debug("Ignored outbound packet for input interface_id=%d", packet.interface_id)
            return
        encoding = interface.get("encoding")
        if not isinstance(encoding, dict):
            logging.warning("Missing encoding for interface_id=%d", packet.interface_id)
            return
        if str(encoding.get("kind") or "").strip().lower() != "struct_v1":
            logging.warning("Unsupported encoding kind for interface_id=%d", packet.interface_id)
            return
        fields = encoding.get("fields")
        if not isinstance(fields, list):
            logging.warning("Missing fields for interface_id=%d", packet.interface_id)
            return
        try:
            values = decode_struct_payload(packet.payload, fields)
        except SerialStandardError as exc:
            logging.warning("Failed to decode interface_id=%d: %s", packet.interface_id, exc)
            return

        generic_payload = self._standard_generic_payload(interface, packet, values)
        generic_topic = self._standard_generic_topic("outgoing", str(interface.get("name") or f"iface-{packet.interface_id}"))
        qos = 1 if is_state else 0
        retain = bool(interface.get("retain", False))
        self._publish_json(generic_topic, generic_payload, qos=qos, retain=retain)

        alias_topic = self._standard_alias_topic(interface)
        if alias_topic is not None:
            alias_payload = self._standard_alias_payload(interface, packet, values, generic_payload)
            self._publish_json(alias_topic, alias_payload, qos=qos, retain=retain)
            profile = str(interface.get("profile") or "").strip().lower()
            if profile == "imu.summary.v1":
                with self.telemetry_lock:
                    self.last_imu_low_payload = alias_payload

    def _handle_standard_command(self, interface_id: int, payload: dict[str, Any]) -> None:
        interface = self.standard_interfaces_by_id.get(interface_id)
        if not interface:
            logging.warning("Unknown command interface_id=%d", interface_id)
            return
        encoding = interface.get("encoding")
        if not isinstance(encoding, dict):
            logging.warning("Missing command encoding for interface_id=%d", interface_id)
            return
        if str(encoding.get("kind") or "").strip().lower() != "struct_v1":
            logging.warning("Unsupported command encoding for interface_id=%d", interface_id)
            return
        fields = encoding.get("fields")
        if not isinstance(fields, list):
            logging.warning("Missing command fields for interface_id=%d", interface_id)
            return
        values = self._extract_value(payload)
        try:
            encoded_payload = encode_struct_payload(values, fields)
        except SerialStandardError as exc:
            logging.warning("Bad command payload for interface_id=%d: %s", interface_id, exc)
            return
        self.standard_command_seq = (self.standard_command_seq + 1) & 0xFFFFFFFF
        packet = encode_packet(
            MSG_CMD,
            payload=encoded_payload,
            interface_id=interface_id,
            seq=self.standard_command_seq,
            timestamp_ms=int(time.time() * 1000.0) & 0xFFFFFFFF,
        )
        self._send_serial_bytes(packet, description=f"cmd[{interface_id}]")

    def _standard_discovery_tick(self) -> None:
        if self.standard_discovery is not None:
            return
        now = time.monotonic()
        if now < self.standard_next_describe_request_at:
            return
        self.standard_next_describe_request_at = now + 2.0
        packet = encode_packet(MSG_DESCRIBE_REQ)
        self._send_serial_bytes(packet, description="describe_req")

    def _standard_device_key(self) -> str:
        candidate = self.standard_device_uid or (self.instance_name or "default")
        return candidate.strip("/") or "default"

    def _standard_discovery_topic(self) -> str:
        return self.identity.topic("outgoing", f"mcu/{self._standard_device_key()}/describe")

    def _standard_generic_topic(self, direction: str, name: str) -> str:
        clean_name = name.strip("/") or "unnamed"
        return self.identity.topic(direction, f"mcu/{self._standard_device_key()}/{clean_name}")

    def _standard_alias_topic(self, interface: dict[str, Any]) -> Optional[str]:
        channel = str(interface.get("channel") or "").strip().strip("/")
        if not channel:
            return None
        direction = "outgoing" if str(interface.get("dir") or "").strip().lower() == "out" else "incoming"
        return self.identity.topic(direction, channel)

    def _standard_command_topics(self, interface: dict[str, Any]) -> list[str]:
        name = str(interface.get("name") or "").strip()
        topics = [self._standard_generic_topic("incoming", name or f"iface-{interface.get('id', 'unknown')}")]
        alias_topic = self._standard_alias_topic(interface)
        if alias_topic is not None and alias_topic not in topics:
            topics.append(alias_topic)
        return topics

    def _standard_generic_payload(
        self,
        interface: dict[str, Any],
        packet: StandardPacket,
        values: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "t": int(time.time() * 1000.0),
            "seq": int(packet.seq),
            "mcu_ms": int(packet.timestamp_ms),
            "device_uid": self._standard_device_key(),
            "interface": str(interface.get("name") or f"iface-{packet.interface_id}"),
            "profile": str(interface.get("profile") or ""),
            "value": values,
        }

    def _standard_motion_value(self, values: dict[str, Any]) -> dict[str, Any]:
        motion_value: dict[str, Any] = {}
        if all(axis in values for axis in ("ax", "ay", "az")):
            motion_value["accel_mps2"] = _vector_payload(
                {"x": values["ax"], "y": values["ay"], "z": values["az"]}
            )
        if all(axis in values for axis in ("gx", "gy", "gz")):
            motion_value["gyro_dps"] = _vector_payload(
                {"x": values["gx"], "y": values["gy"], "z": values["gz"]}
            )
        if "temp_c" in values:
            motion_value["temp_c"] = round(float(values["temp_c"]), 3)
        orientation: dict[str, float] = {}
        if "roll_deg" in values:
            orientation["roll"] = round(float(values["roll_deg"]), 3)
        if "pitch_deg" in values:
            orientation["pitch"] = round(float(values["pitch_deg"]), 3)
        if orientation:
            motion_value["orientation_deg"] = orientation
        return motion_value

    def _standard_alias_payload(
        self,
        interface: dict[str, Any],
        packet: StandardPacket,
        values: dict[str, Any],
        generic_payload: dict[str, Any],
    ) -> dict[str, Any]:
        profile = str(interface.get("profile") or "").strip().lower()
        if profile == "imu.motion.v1":
            motion_value = self._standard_motion_value(values)
            with self.telemetry_lock:
                self.last_imu_motion_value = dict(motion_value)
            return {
                "t": generic_payload["t"],
                "seq": int(packet.seq),
                "mcu_ms": int(packet.timestamp_ms),
                "value": motion_value,
            }
        if profile == "imu.summary.v1":
            with self.telemetry_lock:
                motion_value = dict(self.last_imu_motion_value or {})
            motion_value.update(self._standard_motion_value(values))
            if "accel_norm_g" in values:
                motion_value["accel_norm_g"] = round(float(values["accel_norm_g"]), 4)
            if any(key in values for key in ("gyro_bias_x_dps", "gyro_bias_y_dps", "gyro_bias_z_dps")):
                motion_value["gyro_bias_dps"] = _vector_payload(
                    {
                        "x": values.get("gyro_bias_x_dps", 0.0),
                        "y": values.get("gyro_bias_y_dps", 0.0),
                        "z": values.get("gyro_bias_z_dps", 0.0),
                    }
                )
            health: dict[str, Any] = {
                "device": self.serial_port,
                "protocol": self.protocol_name,
                "instance": self.instance_name or "default",
            }
            if "samples_ok" in values:
                health["samples_ok"] = int(values["samples_ok"])
            if "samples_error" in values:
                health["samples_error"] = int(values["samples_error"])
            if "calibration_samples" in values:
                health["calibration_samples"] = int(values["calibration_samples"])
            motion_value["health"] = health
            return {
                "t": generic_payload["t"],
                "seq": int(packet.seq),
                "mcu_ms": int(packet.timestamp_ms),
                "value": motion_value,
            }
        return generic_payload

    def _handle_non_telemetry_line(self, line: str) -> None:
        if line.startswith("WARN "):
            logging.warning("[%s] %s", self.service_name, line[5:].strip())
            return
        if line.startswith("ERR "):
            logging.error("[%s] %s", self.service_name, line[4:].strip())
            return
        if line.startswith("OK "):
            logging.debug("[%s] %s", self.service_name, line)
            return
        logging.info("[%s] %s", self.service_name, line)

    @staticmethod
    def _parse_key_value_tokens(line: str) -> dict[str, str]:
        values: dict[str, str] = {}
        for token in line.split()[1:]:
            if "=" not in token:
                continue
            key, val = token.split("=", 1)
            values[key] = val
        return values

    @staticmethod
    def _parse_touch_telemetry_line(line: str) -> Optional[dict[str, int]]:
        if not line.startswith("T "):
            return None
        values = SerialMcuBridge._parse_key_value_tokens(line)
        try:
            return {
                "a0": int(values["a0"]),
                "a1": int(values["a1"]),
                "a2": int(values["a2"]),
            }
        except (KeyError, ValueError):
            try:
                return {
                    "a0": int(values["t0"]),
                    "a1": int(values["t1"]),
                    "a2": int(values["t2"]),
                }
            except (KeyError, ValueError):
                logging.debug("Bad touch telemetry line: %s", line)
                return None

    @staticmethod
    def _parse_charge_telemetry_line(line: str) -> Optional[int]:
        if not (line.startswith("C ") or line.startswith("T ")):
            return None
        values = SerialMcuBridge._parse_key_value_tokens(line)
        try:
            return int(values["chg"])
        except (KeyError, ValueError):
            return None

    @staticmethod
    def _parse_charge_level_telemetry_line(line: str) -> Optional[float]:
        if not (line.startswith("C ") or line.startswith("T ")):
            return None
        values = SerialMcuBridge._parse_key_value_tokens(line)
        try:
            value = float(values["v"])
        except (KeyError, ValueError):
            return None
        if not math.isfinite(value):
            return None
        return value

    @staticmethod
    def _parse_imu_high_rate_telemetry_line(line: str) -> Optional[dict[str, Any]]:
        if not line.startswith("I "):
            return None
        values = SerialMcuBridge._parse_key_value_tokens(line)
        try:
            parsed = {
                "n": int(values["n"]),
                "ms": int(values["ms"]),
                "ax": float(values["ax"]),
                "ay": float(values["ay"]),
                "az": float(values["az"]),
                "gx": float(values["gx"]),
                "gy": float(values["gy"]),
                "gz": float(values["gz"]),
                "t": float(values["t"]),
                "r": float(values["r"]),
                "p": float(values["p"]),
            }
        except (KeyError, ValueError):
            return None
        if not all(math.isfinite(float(parsed[key])) for key in ("ax", "ay", "az", "gx", "gy", "gz", "t", "r", "p")):
            return None
        return parsed

    @staticmethod
    def _parse_imu_low_rate_telemetry_line(line: str) -> Optional[dict[str, Any]]:
        if not line.startswith("S "):
            return None
        values = SerialMcuBridge._parse_key_value_tokens(line)
        try:
            parsed = {
                "n": int(values["n"]),
                "ms": int(values["ms"]),
                "r": float(values["r"]),
                "p": float(values["p"]),
                "t": float(values["t"]),
                "an": float(values["an"]),
                "gbx": float(values["gbx"]),
                "gby": float(values["gby"]),
                "gbz": float(values["gbz"]),
                "ok": int(values["ok"]),
                "err": int(values["err"]),
                "cal": int(values["cal"]),
            }
        except (KeyError, ValueError):
            return None
        if not all(math.isfinite(float(parsed[key])) for key in ("r", "p", "t", "an", "gbx", "gby", "gbz")):
            return None
        return parsed

    @staticmethod
    def _parse_telemetry_line(line: str) -> Optional[dict[str, Any]]:
        touch = SerialMcuBridge._parse_touch_telemetry_line(line)
        charge = SerialMcuBridge._parse_charge_telemetry_line(line)
        if not touch or charge is None:
            return None
        parsed = {
            "t0": touch["a0"],
            "t1": touch["a1"],
            "t2": touch["a2"],
            "chg": charge,
        }
        charge_level = SerialMcuBridge._parse_charge_level_telemetry_line(line)
        if charge_level is not None:
            parsed["v"] = charge_level
        return parsed

    def _handle_touch_telemetry(self, data: dict[str, int]) -> None:
        a0 = data.get("a0")
        a1 = data.get("a1")
        a2 = data.get("a2")
        if a0 is None:
            a0 = data.get("t0")
        if a1 is None:
            a1 = data.get("t1")
        if a2 is None:
            a2 = data.get("t2")
        if a0 is None or a1 is None or a2 is None:
            logging.debug("Bad touch telemetry payload: %s", data)
            return
        touch = {"value": {"a0": int(a0), "a1": int(a1), "a2": int(a2)}, "unit": "adc"}
        with self.telemetry_lock:
            self.last_touch = touch
            charge = self.last_charge_value
        if self.odometry_writer is not None:
            try:
                self.odometry_writer.write_sample(int(a0), int(a1), int(a2), chg=charge)
            except Exception as exc:
                logging.error("Odometry shared-memory write failed: %s", exc)
                try:
                    self.odometry_writer.close()
                except Exception:
                    logging.debug("Odometry shm close after write failure failed", exc_info=True)
                self.odometry_writer = None
        if self.publish_touch_sensors:
            self._publish_json(self.touch_topic, touch)

    def _handle_charge_telemetry(self, charge: int) -> None:
        charge_value = None if int(charge) < 0 else bool(charge)
        payload = {"value": charge_value}
        should_publish = False
        now = time.time()
        with self.telemetry_lock:
            had_charge = self.last_charge is not None
            self.last_charge = payload
            if (not had_charge) or self.last_charge_value != charge_value:
                should_publish = True
            self.last_charge_value = charge_value
        if not should_publish:
            return
        self._publish_json(self.charge_topic, payload, qos=1, retain=True)
        self.next_charge_republish_at = now + self.retained_publish_interval_seconds

    def _handle_charge_level_telemetry(self, charge_level: float) -> None:
        level = float(charge_level)
        if not math.isfinite(level):
            return
        payload = {"value": level, "unit": "v"}
        with self.telemetry_lock:
            self.last_charge_level = payload
        self._publish_json(self.charge_level_topic, payload, qos=1, retain=True)
        self.next_charge_level_republish_at = time.time() + self.retained_publish_interval_seconds

    def _handle_imu_high_rate_telemetry(self, data: dict[str, Any]) -> None:
        motion_value = {
            "accel_mps2": _vector_payload({"x": data["ax"], "y": data["ay"], "z": data["az"]}),
            "gyro_dps": _vector_payload({"x": data["gx"], "y": data["gy"], "z": data["gz"]}),
            "temp_c": round(float(data["t"]), 3),
            "orientation_deg": {
                "roll": round(float(data["r"]), 3),
                "pitch": round(float(data["p"]), 3),
            },
        }
        payload = {
            "t": int(time.time() * 1000.0),
            "seq": int(data["n"]),
            "mcu_ms": int(data["ms"]),
            "value": motion_value,
        }
        with self.telemetry_lock:
            self.last_imu_motion_value = motion_value
        self._publish_json(
            self.imu_high_rate_topic,
            payload,
            qos=self.imu_high_rate_qos,
            retain=self.imu_high_rate_retain,
        )

    def _handle_imu_low_rate_telemetry(self, data: dict[str, Any]) -> None:
        with self.telemetry_lock:
            motion_value = dict(self.last_imu_motion_value or {})

        if "orientation_deg" not in motion_value:
            motion_value["orientation_deg"] = {
                "roll": round(float(data["r"]), 3),
                "pitch": round(float(data["p"]), 3),
            }
        if "temp_c" not in motion_value:
            motion_value["temp_c"] = round(float(data["t"]), 3)

        motion_value["accel_norm_g"] = round(float(data["an"]), 4)
        motion_value["gyro_bias_dps"] = _vector_payload(
            {"x": data["gbx"], "y": data["gby"], "z": data["gbz"]}
        )
        motion_value["health"] = {
            "device": self.serial_port,
            "protocol": self.protocol_name,
            "instance": self.instance_name or "default",
            "samples_ok": int(data["ok"]),
            "samples_error": int(data["err"]),
            "calibration_samples": int(data["cal"]),
        }

        payload = {
            "t": int(time.time() * 1000.0),
            "seq": int(data["n"]),
            "mcu_ms": int(data["ms"]),
            "value": motion_value,
        }
        with self.telemetry_lock:
            self.last_imu_low_payload = payload
        self._publish_json(
            self.imu_low_rate_topic,
            payload,
            qos=self.imu_low_rate_qos,
            retain=self.imu_low_rate_retain,
        )

    def _handle_telemetry(self, data: dict[str, Any]) -> None:
        self._handle_touch_telemetry({"t0": int(data["t0"]), "t1": int(data["t1"]), "t2": int(data["t2"])})
        if "chg" in data:
            self._handle_charge_telemetry(int(data["chg"]))
        if "v" in data:
            self._handle_charge_level_telemetry(float(data["v"]))

    def _connect_local_mqtt(self) -> None:
        client = mqtt.Client()
        mqtt_auth_and_tls(client, self.local_mqtt_cfg, self.config_path.parent)
        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_message = self._on_message
        self.client = client

        retry_delay = 2.0
        while not self.stop_event.is_set():
            logging.info("Connecting to local MQTT %s:%d for %s", self.local_host, self.local_port, self.service_name)
            try:
                client.connect(self.local_host, self.local_port, self.local_keepalive)
                client.loop_start()
                return
            except OSError as exc:
                logging.warning("Local MQTT connection failed for %s: %s", self.service_name, exc)
                self.stop_event.wait(retry_delay)
                retry_delay = min(retry_delay * 2, 60.0)

    def _on_connect(self, client: mqtt.Client, _userdata: Any, _flags: dict[str, Any], rc: int) -> None:
        if rc != 0:
            logging.error("Local MQTT connect failed rc=%s", rc)
            return
        logging.info("Local MQTT connected for %s", self.service_name)
        for topic in self.handlers:
            client.subscribe(topic, qos=1)
        self._republish_cached_telemetry()

    def _on_disconnect(self, _client: mqtt.Client, _userdata: Any, rc: int) -> None:
        if rc != mqtt.MQTT_ERR_SUCCESS:
            logging.warning("Unexpected local MQTT disconnect rc=%s", rc)
            self._start_reconnect_thread()

    def _start_reconnect_thread(self) -> None:
        if self.stop_event.is_set():
            return
        with self.reconnect_lock:
            if self.reconnect_thread and self.reconnect_thread.is_alive():
                return
            thread = threading.Thread(target=self._reconnect_loop, name=f"{self.service_name}-mqtt-reconnect", daemon=True)
            thread.start()
            self.reconnect_thread = thread

    def _reconnect_loop(self) -> None:
        delay = 2.0
        while not self.stop_event.is_set():
            client = self.client
            if client is None:
                return
            is_connected = getattr(client, "is_connected", None)
            if callable(is_connected) and is_connected():
                return
            try:
                client.connect(self.local_host, self.local_port, self.local_keepalive)
                return
            except OSError as exc:
                logging.warning("Local MQTT reconnect failed for %s: %s", self.service_name, exc)
                self.stop_event.wait(delay)
                delay = min(delay * 2, 60.0)

    def _on_message(self, _client: mqtt.Client, _userdata: Any, msg: mqtt.MQTTMessage) -> None:
        handler = self.handlers.get(msg.topic)
        if not handler:
            return
        if msg.topic == getattr(self, "drive_topic", None) and self.ignore_retained_drive and bool(msg.retain):
            logging.info("Ignoring retained drive command on %s", msg.topic)
            return
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
        except json.JSONDecodeError:
            logging.warning("Non-JSON payload on %s", msg.topic)
            return
        try:
            handler(payload)
        except Exception:
            logging.exception("Command handler failed for %s", msg.topic)

    def _republish_cached_telemetry(self) -> None:
        if self.protocol_name == PROTOCOL_GOOB_BASE_V1:
            with self.telemetry_lock:
                charge = self.last_charge
                charge_level = self.last_charge_level
            if charge is not None:
                self._publish_json(self.charge_topic, charge, qos=1, retain=True)
                self.next_charge_republish_at = time.time() + self.retained_publish_interval_seconds
            if charge_level is not None:
                self._publish_json(self.charge_level_topic, charge_level, qos=1, retain=True)
                self.next_charge_level_republish_at = time.time() + self.retained_publish_interval_seconds
            return

        if self.protocol_name == PROTOCOL_IMU_MPU6050_V1 and self.imu_low_rate_retain:
            with self.telemetry_lock:
                payload = self.last_imu_low_payload
            if payload is not None:
                self._publish_json(self.imu_low_rate_topic, payload, qos=self.imu_low_rate_qos, retain=True)
            return

        if self.protocol_name == PROTOCOL_PEBBLE_SERIAL_V1:
            if self.standard_discovery is not None:
                self._publish_json(self._standard_discovery_topic(), self.standard_discovery, qos=1, retain=True)

    def _republish_charge_if_due(self) -> None:
        if self.next_charge_republish_at <= 0:
            return
        now = time.time()
        if now < self.next_charge_republish_at:
            return
        with self.telemetry_lock:
            charge = self.last_charge
        self.next_charge_republish_at = now + self.retained_publish_interval_seconds
        if not charge:
            return
        self._publish_json(self.charge_topic, charge, qos=1, retain=True)

    def _republish_charge_level_if_due(self) -> None:
        if self.next_charge_level_republish_at <= 0:
            return
        now = time.time()
        if now < self.next_charge_level_republish_at:
            return
        with self.telemetry_lock:
            charge_level = self.last_charge_level
        self.next_charge_level_republish_at = now + self.retained_publish_interval_seconds
        if charge_level is None:
            return
        self._publish_json(self.charge_level_topic, charge_level, qos=1, retain=True)

    def _publish_json(self, topic: str, payload: dict[str, Any], qos: int = 1, retain: bool = False) -> None:
        client = self.client
        if client is None:
            return
        is_connected = getattr(client, "is_connected", None)
        if callable(is_connected) and not is_connected():
            return
        client.publish(topic, json.dumps(payload), qos=qos, retain=retain)

    @staticmethod
    def _extract_value(payload: dict[str, Any]) -> dict[str, Any]:
        value = payload.get("value")
        return value if isinstance(value, dict) else payload

    def _handle_drive_values(self, payload: dict[str, Any]) -> None:
        values = self._extract_value(payload)
        x = _parse_float(values.get("x"), 0.0)
        z = _parse_float(values.get("z"), 0.0)
        left = clamp(z + x, -1.0, 1.0)
        right = clamp(z - x, -1.0, 1.0)
        self._send_serial_line("M", f"{left:.3f}", f"{right:.3f}")
        self.last_drive_command_at = time.monotonic()
        self.drive_timed_out = False

    def _handle_lights_solid(self, payload: dict[str, Any]) -> None:
        values = self._extract_value(payload)
        b = clamp(_parse_float(values.get("b"), 0.0), 0.0, 1.0)
        g = clamp(_parse_float(values.get("g"), 0.0), 0.0, 1.0)
        r = clamp(_parse_float(values.get("r"), 0.0), 0.0, 1.0)
        self._send_serial_line("L", f"{b:.3f}", f"{g:.3f}", f"{r:.3f}")

    def _handle_lights_flash(self, payload: dict[str, Any]) -> None:
        values = self._extract_value(payload)
        b = clamp(_parse_float(values.get("b"), 0.0), 0.0, 1.0)
        g = clamp(_parse_float(values.get("g"), 0.0), 0.0, 1.0)
        r = clamp(_parse_float(values.get("r"), 0.0), 0.0, 1.0)
        period = max(0.05, _parse_float(values.get("period"), 2.0))
        self._send_serial_line("F", f"{b:.3f}", f"{g:.3f}", f"{r:.3f}", f"{period:.3f}")

    def _send_serial_line(self, *parts: str) -> None:
        line = " ".join(parts) + "\n"
        self._send_serial_bytes(line.encode("utf-8"), description=parts[0])

    def _send_serial_bytes(self, data: bytes, *, description: str) -> None:
        if not self.ser:
            logging.warning("Serial unavailable; dropped %s", description)
            return
        with self.serial_lock:
            try:
                self.ser.write(data)
                self.ser.flush()
            except serial.SerialException as exc:
                logging.error("Failed serial write for %s: %s", description, exc)

    def _send_drive_stop(self, reason: str) -> None:
        self._send_serial_line("M", "0.000", "0.000")
        logging.info("Issued drive stop (%s)", reason)

    def _drive_watchdog_tick(self) -> None:
        timeout = self.drive_timeout_seconds
        if timeout <= 0:
            return
        last = self.last_drive_command_at
        if last is None:
            return
        if self.drive_timed_out:
            return
        if (time.monotonic() - last) <= timeout:
            return
        self._send_drive_stop(reason="command-timeout")
        self.drive_timed_out = True


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bridge local MQTT and one MCU serial instance.")
    parser.add_argument("--config", help="Path to runtime config JSON.")
    parser.add_argument("--instance", help="Named serial_mcu_bridge instance to run.")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    config, config_path = load_config(args.config)
    svc_cfg = service_instance_cfg(config, "serial_mcu_bridge", args.instance)
    if args.instance is not None and not svc_cfg:
        raise SystemExit(f"serial_mcu_bridge instance not found: {args.instance}")
    logging.basicConfig(
        level=getattr(logging, log_level(config, svc_cfg), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    bridge = SerialMcuBridge(config, config_path, instance_name=args.instance)

    def _shutdown(_signum: int, _frame: Any) -> None:
        bridge.stop()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    try:
        bridge.start()
    finally:
        bridge.stop()


if __name__ == "__main__":
    main()
