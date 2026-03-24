#!/usr/bin/env python3
"""Bridge local MQTT command/telemetry topics with an MCU serial link."""

from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
import threading
import time
from pathlib import Path
from typing import Any, Optional

import paho.mqtt.client as mqtt
import serial

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parents[2]))

from control.common.config import default_retained_publish_interval_seconds, load_config, log_level, service_cfg
from control.common.mqtt import mqtt_auth_and_tls
from control.common.odometry_shm import OdometryRawShmWriter
from control.common.topics import identity_from_config


def clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _parse_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


class SerialMcuBridge:
    def __init__(self, config: dict[str, Any], config_path: Path) -> None:
        self.config = config
        self.config_path = config_path
        self.identity = identity_from_config(config)
        self.service_cfg = service_cfg(config, "serial_mcu_bridge")
        if not self.service_cfg.get("enabled", True):
            raise SystemExit("serial_mcu_bridge is disabled in config.")

        serial_cfg = self.service_cfg.get("serial") if isinstance(self.service_cfg.get("serial"), dict) else {}
        self.serial_port = str(serial_cfg.get("port") or "").strip()
        if not self.serial_port:
            raise SystemExit("services.serial_mcu_bridge.serial.port is required")
        self.baud = int(serial_cfg.get("baud") or 115200)
        self.serial_timeout = float(serial_cfg.get("timeout_seconds") or 0.1)

        topics_cfg = self.service_cfg.get("topics") if isinstance(self.service_cfg.get("topics"), dict) else {}
        self.drive_topic = str(topics_cfg.get("drive_values") or self.identity.topic("incoming", "drive-values"))
        self.lights_solid_topic = str(topics_cfg.get("lights_solid") or self.identity.topic("incoming", "lights-solid"))
        self.lights_flash_topic = str(topics_cfg.get("lights_flash") or self.identity.topic("incoming", "lights-flash"))
        self.touch_topic = str(topics_cfg.get("touch_sensors") or self.identity.topic("outgoing", "touch-sensors"))
        self.charge_topic = str(topics_cfg.get("charging_status") or self.identity.topic("outgoing", "charging-status"))
        telemetry_cfg = self.service_cfg.get("telemetry") if isinstance(self.service_cfg.get("telemetry"), dict) else {}
        self.publish_touch_sensors = bool(telemetry_cfg.get("publish_touch_sensors", False))
        odometry_shm_cfg = (
            self.service_cfg.get("odometry_shm") if isinstance(self.service_cfg.get("odometry_shm"), dict) else {}
        )
        default_shm_name = f"{self.identity.system}_{self.identity.type}_{self.identity.robot_id}_odometry_raw"
        self.odometry_shm_enabled = bool(odometry_shm_cfg.get("enabled", True))
        self.odometry_shm_name = str(odometry_shm_cfg.get("name") or default_shm_name)
        self.odometry_shm_slots = max(64, int(odometry_shm_cfg.get("slots") or 2048))
        safety_cfg = self.service_cfg.get("safety") if isinstance(self.service_cfg.get("safety"), dict) else {}
        self.drive_timeout_seconds = max(0.0, float(safety_cfg.get("drive_timeout_seconds") or 0.75))
        self.ignore_retained_drive = bool(safety_cfg.get("ignore_retained_drive", True))
        self.stop_on_shutdown = bool(safety_cfg.get("stop_on_shutdown", True))
        default_retain_interval = default_retained_publish_interval_seconds(config, default=3600.0)
        interval_raw = self.service_cfg.get("retained_publish_interval_seconds")
        try:
            interval_value = float(interval_raw if interval_raw is not None else default_retain_interval)
        except (TypeError, ValueError):
            interval_value = default_retain_interval
        self.retained_publish_interval_seconds = interval_value if interval_value > 0 else default_retain_interval

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

        self.last_touch: Optional[dict[str, Any]] = None
        self.last_charge: Optional[dict[str, Any]] = None
        self.last_charge_value: Optional[bool] = None
        self.next_charge_republish_at = 0.0
        self.last_drive_command_at: Optional[float] = None
        self.drive_timed_out = False

        self.handlers = {
            self.drive_topic: self._handle_drive_values,
            self.lights_solid_topic: self._handle_lights_solid,
            self.lights_flash_topic: self._handle_lights_flash,
        }

    def start(self) -> None:
        self._open_odometry_shm()
        self._open_serial()
        self._start_serial_reader()
        self._connect_local_mqtt()
        while not self.stop_event.is_set():
            self._drive_watchdog_tick()
            self._republish_charge_if_due()
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
        if not self.odometry_shm_enabled:
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
        logging.info("Serial connected on %s @ %d", self.serial_port, self.baud)

    def _start_serial_reader(self) -> None:
        thread = threading.Thread(target=self._serial_reader, name="serial-reader", daemon=True)
        thread.start()
        self.reader_thread = thread

    def _serial_reader(self) -> None:
        assert self.ser is not None
        while not self.stop_event.is_set():
            try:
                raw = self.ser.readline()
            except serial.SerialException as exc:
                logging.error("Serial read failed: %s", exc)
                time.sleep(0.5)
                continue
            if not raw:
                continue
            line = raw.decode("utf-8", errors="ignore").strip()
            if not line:
                continue
            touch = self._parse_touch_telemetry_line(line)
            if touch:
                self._handle_touch_telemetry(touch)
            charge = self._parse_charge_telemetry_line(line)
            if charge is not None:
                self._handle_charge_telemetry(charge)

    @staticmethod
    def _parse_touch_telemetry_line(line: str) -> Optional[dict[str, int]]:
        if not line.startswith("T "):
            return None
        values: dict[str, str] = {}
        for token in line.split()[1:]:
            if "=" not in token:
                continue
            key, val = token.split("=", 1)
            values[key] = val
        try:
            return {
                "a0": int(values["a0"]),
                "a1": int(values["a1"]),
                "a2": int(values["a2"]),
            }
        except (KeyError, ValueError):
            try:
                # Backward-compat with legacy touch labels.
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
        values: dict[str, str] = {}
        for token in line.split()[1:]:
            if "=" not in token:
                continue
            key, val = token.split("=", 1)
            values[key] = val
        try:
            return int(values["chg"])
        except (KeyError, ValueError):
            return None

    @staticmethod
    def _parse_telemetry_line(line: str) -> Optional[dict[str, Any]]:
        # Backward-compatible parser for legacy tests/consumers expecting a single line.
        touch = SerialMcuBridge._parse_touch_telemetry_line(line)
        charge = SerialMcuBridge._parse_charge_telemetry_line(line)
        if not touch or charge is None:
            return None
        return {
            "t0": touch["a0"],
            "t1": touch["a1"],
            "t2": touch["a2"],
            "chg": charge,
        }

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
        payload = {"value": bool(charge)}
        should_publish = False
        now = time.time()
        with self.telemetry_lock:
            self.last_charge = payload
            if self.last_charge_value != bool(charge):
                self.last_charge_value = bool(charge)
                should_publish = True
        if not should_publish:
            return
        self._publish_json(self.charge_topic, payload, qos=1, retain=True)
        self.next_charge_republish_at = now + self.retained_publish_interval_seconds

    def _handle_telemetry(self, data: dict[str, Any]) -> None:
        # Compatibility shim used in tests and any direct callers.
        self._handle_touch_telemetry({"t0": int(data["t0"]), "t1": int(data["t1"]), "t2": int(data["t2"])})
        self._handle_charge_telemetry(int(data["chg"]))

    def _connect_local_mqtt(self) -> None:
        client = mqtt.Client()
        mqtt_auth_and_tls(client, self.local_mqtt_cfg, self.config_path.parent)
        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_message = self._on_message
        self.client = client

        retry_delay = 2.0
        while not self.stop_event.is_set():
            logging.info("Connecting to local MQTT %s:%d", self.local_host, self.local_port)
            try:
                client.connect(self.local_host, self.local_port, self.local_keepalive)
                client.loop_start()
                return
            except OSError as exc:
                logging.warning("Local MQTT connection failed: %s", exc)
                self.stop_event.wait(retry_delay)
                retry_delay = min(retry_delay * 2, 60.0)

    def _on_connect(self, client: mqtt.Client, _userdata: Any, _flags: dict[str, Any], rc: int) -> None:
        if rc != 0:
            logging.error("Local MQTT connect failed rc=%s", rc)
            return
        logging.info("Local MQTT connected; subscribing command topics.")
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
            thread = threading.Thread(target=self._reconnect_loop, name="mqtt-reconnect", daemon=True)
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
                logging.warning("Local MQTT reconnect failed: %s", exc)
                self.stop_event.wait(delay)
                delay = min(delay * 2, 60.0)

    def _on_message(self, _client: mqtt.Client, _userdata: Any, msg: mqtt.MQTTMessage) -> None:
        handler = self.handlers.get(msg.topic)
        if not handler:
            return
        if msg.topic == self.drive_topic and self.ignore_retained_drive and bool(msg.retain):
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
        with self.telemetry_lock:
            charge = self.last_charge
        if charge:
            self._publish_json(self.charge_topic, charge, qos=1, retain=True)
            self.next_charge_republish_at = time.time() + self.retained_publish_interval_seconds

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
        if not self.ser:
            logging.warning("Serial unavailable; dropped command %s", parts[0])
            return
        line = " ".join(parts) + "\n"
        with self.serial_lock:
            try:
                self.ser.write(line.encode("utf-8"))
                self.ser.flush()
            except serial.SerialException as exc:
                logging.error("Failed serial write for %s: %s", parts[0], exc)

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
    parser = argparse.ArgumentParser(description="Bridge local MQTT and MCU serial topics.")
    parser.add_argument("--config", help="Path to runtime config JSON.")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    config, config_path = load_config(args.config)
    svc_cfg = service_cfg(config, "serial_mcu_bridge")
    logging.basicConfig(
        level=getattr(logging, log_level(config, svc_cfg), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    bridge = SerialMcuBridge(config, config_path)

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
