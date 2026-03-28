from __future__ import annotations

import json
import tempfile
import time
import unittest
from pathlib import Path

from control.services.serial_mcu_bridge import SerialMcuBridge
from tests.helpers import FakeMqttClient, FakeMqttMessage, FakeSerial, make_base_config


class SerialMcuBridgeTests(unittest.TestCase):
    def _bridge(self, *, instance_name: str | None = None) -> SerialMcuBridge:
        config = make_base_config("serialbot")
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "config.json"
            path.write_text(json.dumps(config))
            return SerialMcuBridge(config, path, instance_name=instance_name)

    def test_parse_telemetry_line(self):
        parsed = SerialMcuBridge._parse_telemetry_line("T t0=1 t1=2 t2=3 chg=1")
        self.assertEqual(parsed, {"t0": 1, "t1": 2, "t2": 3, "chg": 1})
        parsed_new = SerialMcuBridge._parse_telemetry_line("T a0=4 a1=5 a2=6 chg=0")
        self.assertEqual(parsed_new, {"t0": 4, "t1": 5, "t2": 6, "chg": 0})
        self.assertIsNone(SerialMcuBridge._parse_telemetry_line("T bad=1"))

    def test_parse_split_touch_and_charge_lines(self):
        touch = SerialMcuBridge._parse_touch_telemetry_line("T a0=11 a1=22 a2=33")
        charge = SerialMcuBridge._parse_charge_telemetry_line("C chg=1")
        charge_level = SerialMcuBridge._parse_charge_level_telemetry_line("C chg=-1 v=3.742")
        self.assertEqual(touch, {"a0": 11, "a1": 22, "a2": 33})
        self.assertEqual(charge, 1)
        self.assertEqual(charge_level, 3.742)

    def test_parse_imu_lines(self):
        high = SerialMcuBridge._parse_imu_high_rate_telemetry_line(
            "I n=12 ms=345 ax=1.1 ay=2.2 az=3.3 gx=4.4 gy=5.5 gz=6.6 t=25.1 r=7.7 p=8.8"
        )
        low = SerialMcuBridge._parse_imu_low_rate_telemetry_line(
            "S n=12 ms=400 r=7.7 p=8.8 t=25.1 an=1.02 gbx=0.1 gby=0.2 gbz=0.3 ok=10 err=1 cal=200"
        )
        self.assertEqual(high["n"], 12)
        self.assertAlmostEqual(high["ax"], 1.1)
        self.assertEqual(low["ok"], 10)
        self.assertAlmostEqual(low["an"], 1.02)

    def test_named_imu_instance_merges_base_config(self):
        config = make_base_config("serialbot")
        config["services"]["serial_mcu_bridge"]["enabled"] = False
        config["services"]["serial_mcu_bridge"]["serial"] = {
            "port": "/dev/serial/by-id/base",
            "baud": 115200,
            "timeout_seconds": 0.1,
        }
        config["services"]["serial_mcu_bridge"]["instances"] = {
            "imu": {
                "enabled": True,
                "protocol": "imu_mpu6050_v1",
                "serial": {"port": "/dev/serial/by-id/imu"},
            }
        }
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "config.json"
            path.write_text(json.dumps(config))
            bridge = SerialMcuBridge(config, path, instance_name="imu")
        self.assertEqual("imu_mpu6050_v1", bridge.protocol_name)
        self.assertEqual("/dev/serial/by-id/imu", bridge.serial_port)
        self.assertEqual(115200, bridge.baud)
        self.assertEqual("pebble/robots/serialbot/outgoing/sensors/imu-fast", bridge.imu_high_rate_topic)

    def test_drive_values_send_serial_motor_command(self):
        bridge = self._bridge()
        bridge.ser = FakeSerial()
        bridge._handle_drive_values({"value": {"x": 0.2, "z": 0.5}})
        self.assertEqual(bridge.ser.writes[-1].decode("utf-8"), "M 0.700 0.300\n")

    def test_lights_commands_send_serial(self):
        bridge = self._bridge()
        bridge.ser = FakeSerial()
        bridge._handle_lights_solid({"value": {"b": 0.1, "g": 0.2, "r": 0.3}})
        bridge._handle_lights_flash({"value": {"b": 0.4, "g": 0.5, "r": 0.6, "period": 1.2}})
        self.assertIn("L 0.100 0.200 0.300\n", [w.decode("utf-8") for w in bridge.ser.writes])
        self.assertIn("F 0.400 0.500 0.600 1.200\n", [w.decode("utf-8") for w in bridge.ser.writes])

    def test_handle_telemetry_publishes_charge_only_retained(self):
        bridge = self._bridge()
        client = FakeMqttClient()
        bridge.client = client
        bridge._handle_telemetry({"t0": 10, "t1": 20, "t2": 30, "chg": 1})
        self.assertEqual(len(client.publish_calls), 1)
        topic, _payload, _qos, retain = client.publish_calls[0]
        self.assertEqual(topic, bridge.charge_topic)
        self.assertTrue(retain)
        self.assertIsNotNone(bridge.last_touch)
        self.assertEqual(bridge.last_touch["value"], {"a0": 10, "a1": 20, "a2": 30})

    def test_handle_charge_level_telemetry_publishes_retained(self):
        bridge = self._bridge()
        client = FakeMqttClient()
        bridge.client = client
        bridge._handle_charge_level_telemetry(3.91)
        self.assertEqual(len(client.publish_calls), 1)
        topic, payload, _qos, retain = client.publish_calls[0]
        self.assertEqual(topic, bridge.charge_level_topic)
        self.assertTrue(retain)
        self.assertEqual(json.loads(payload), {"value": 3.91, "unit": "v"})

    def test_unknown_charge_publishes_null_and_keeps_odometry_unknown(self):
        bridge = self._bridge()
        client = FakeMqttClient()
        bridge.client = client
        bridge._handle_charge_telemetry(-1)
        self.assertEqual(len(client.publish_calls), 1)
        topic, payload, _qos, retain = client.publish_calls[0]
        self.assertEqual(topic, bridge.charge_topic)
        self.assertTrue(retain)
        self.assertEqual(json.loads(payload), {"value": None})
        self.assertIsNone(bridge.last_charge_value)

    def test_handle_touch_telemetry_accepts_a_labels(self):
        bridge = self._bridge()
        bridge._handle_touch_telemetry({"a0": 7, "a1": 8, "a2": 9})
        self.assertEqual(bridge.last_touch["value"], {"a0": 7, "a1": 8, "a2": 9})

    def test_handle_imu_high_rate_telemetry_publishes_fast_topic(self):
        config = make_base_config("serialbot")
        config["services"]["serial_mcu_bridge"]["enabled"] = False
        config["services"]["serial_mcu_bridge"]["instances"] = {
            "imu": {
                "enabled": True,
                "protocol": "imu_mpu6050_v1",
                "serial": {"port": "/dev/ttyUSB9"},
            }
        }
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "config.json"
            path.write_text(json.dumps(config))
            bridge = SerialMcuBridge(config, path, instance_name="imu")
        bridge.client = FakeMqttClient()
        bridge._handle_imu_high_rate_telemetry(
            {"n": 1, "ms": 25, "ax": 1.0, "ay": 2.0, "az": 3.0, "gx": 4.0, "gy": 5.0, "gz": 6.0, "t": 22.5, "r": 7.0, "p": 8.0}
        )
        self.assertEqual(len(bridge.client.publish_calls), 1)  # type: ignore[union-attr]
        topic, payload, _qos, retain = bridge.client.publish_calls[0]  # type: ignore[index,union-attr]
        self.assertEqual(topic, bridge.imu_high_rate_topic)
        self.assertFalse(retain)
        decoded = json.loads(payload)
        self.assertEqual(decoded["seq"], 1)
        self.assertEqual(decoded["value"]["orientation_deg"], {"roll": 7.0, "pitch": 8.0})

    def test_handle_imu_low_rate_telemetry_uses_latest_motion(self):
        config = make_base_config("serialbot")
        config["services"]["serial_mcu_bridge"]["enabled"] = False
        config["services"]["serial_mcu_bridge"]["instances"] = {
            "imu": {
                "enabled": True,
                "protocol": "imu_mpu6050_v1",
                "serial": {"port": "/dev/ttyUSB9"},
            }
        }
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "config.json"
            path.write_text(json.dumps(config))
            bridge = SerialMcuBridge(config, path, instance_name="imu")
        bridge.client = FakeMqttClient()
        bridge._handle_imu_high_rate_telemetry(
            {"n": 3, "ms": 125, "ax": 1.0, "ay": 2.0, "az": 3.0, "gx": 4.0, "gy": 5.0, "gz": 6.0, "t": 22.5, "r": 7.0, "p": 8.0}
        )
        bridge._handle_imu_low_rate_telemetry(
            {"n": 4, "ms": 200, "r": 7.1, "p": 8.1, "t": 22.6, "an": 1.01, "gbx": 0.1, "gby": 0.2, "gbz": 0.3, "ok": 44, "err": 2, "cal": 200}
        )
        self.assertEqual(len(bridge.client.publish_calls), 2)  # type: ignore[union-attr]
        topic, payload, _qos, _retain = bridge.client.publish_calls[-1]  # type: ignore[index,union-attr]
        self.assertEqual(topic, bridge.imu_low_rate_topic)
        decoded = json.loads(payload)
        self.assertEqual(decoded["seq"], 4)
        self.assertEqual(decoded["value"]["accel_mps2"], {"x": 1.0, "y": 2.0, "z": 3.0})
        self.assertEqual(decoded["value"]["health"]["samples_ok"], 44)

    def test_charge_publish_only_on_change(self):
        bridge = self._bridge()
        client = FakeMqttClient()
        bridge.client = client
        bridge._handle_charge_telemetry(1)
        bridge._handle_charge_telemetry(1)
        bridge._handle_charge_telemetry(0)
        self.assertEqual(len(client.publish_calls), 2)

    def test_charge_republish_if_due(self):
        bridge = self._bridge()
        client = FakeMqttClient()
        bridge.client = client
        bridge._handle_charge_telemetry(1)
        self.assertEqual(len(client.publish_calls), 1)
        bridge.next_charge_republish_at = time.time() - 1.0
        bridge._republish_charge_if_due()
        self.assertEqual(len(client.publish_calls), 2)
        self.assertTrue(client.publish_calls[-1][3])

    def test_charge_level_republish_if_due(self):
        bridge = self._bridge()
        client = FakeMqttClient()
        bridge.client = client
        bridge._handle_charge_level_telemetry(3.88)
        self.assertEqual(len(client.publish_calls), 1)
        bridge.next_charge_level_republish_at = time.time() - 1.0
        bridge._republish_charge_level_if_due()
        self.assertEqual(len(client.publish_calls), 2)
        self.assertTrue(client.publish_calls[-1][3])

    def test_on_message_ignores_non_json(self):
        bridge = self._bridge()
        bridge.ser = FakeSerial()
        msg = FakeMqttMessage(topic=bridge.drive_topic, payload=b"not-json")
        bridge._on_message(None, None, msg)  # type: ignore[arg-type]
        self.assertEqual(bridge.ser.writes, [])

    def test_on_message_ignores_retained_drive_command(self):
        bridge = self._bridge()
        bridge.ser = FakeSerial()
        msg = FakeMqttMessage(topic=bridge.drive_topic, payload=b'{"value":{"x":0.1,"z":0.2}}', retain=True)
        bridge._on_message(None, None, msg)  # type: ignore[arg-type]
        self.assertEqual(bridge.ser.writes, [])

    def test_drive_watchdog_stops_when_command_times_out(self):
        bridge = self._bridge()
        bridge.ser = FakeSerial()
        bridge.drive_timeout_seconds = 0.2
        bridge.last_drive_command_at = time.monotonic() - 1.0
        bridge.drive_timed_out = False

        bridge._drive_watchdog_tick()
        self.assertEqual(bridge.ser.writes[-1].decode("utf-8"), "M 0.000 0.000\n")
        self.assertTrue(bridge.drive_timed_out)


if __name__ == "__main__":
    unittest.main()
