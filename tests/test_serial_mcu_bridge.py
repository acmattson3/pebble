from __future__ import annotations

import json
import tempfile
import time
import unittest
from pathlib import Path

from control.services.serial_mcu_bridge import SerialMcuBridge
from tests.helpers import FakeMqttClient, FakeMqttMessage, FakeSerial, make_base_config


class SerialMcuBridgeTests(unittest.TestCase):
    def _bridge(self) -> SerialMcuBridge:
        config = make_base_config("serialbot")
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "config.json"
            path.write_text(json.dumps(config))
            return SerialMcuBridge(config, path)

    def test_parse_telemetry_line(self):
        parsed = SerialMcuBridge._parse_telemetry_line("T t0=1 t1=2 t2=3 chg=1")
        self.assertEqual(parsed, {"t0": 1, "t1": 2, "t2": 3, "chg": 1})
        parsed_new = SerialMcuBridge._parse_telemetry_line("T a0=4 a1=5 a2=6 chg=0")
        self.assertEqual(parsed_new, {"t0": 4, "t1": 5, "t2": 6, "chg": 0})
        self.assertIsNone(SerialMcuBridge._parse_telemetry_line("T bad=1"))

    def test_parse_split_touch_and_charge_lines(self):
        touch = SerialMcuBridge._parse_touch_telemetry_line("T a0=11 a1=22 a2=33")
        charge = SerialMcuBridge._parse_charge_telemetry_line("C chg=1")
        self.assertEqual(touch, {"a0": 11, "a1": 22, "a2": 33})
        self.assertEqual(charge, 1)

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

    def test_handle_touch_telemetry_accepts_a_labels(self):
        bridge = self._bridge()
        bridge._handle_touch_telemetry({"a0": 7, "a1": 8, "a2": 9})
        self.assertEqual(bridge.last_touch["value"], {"a0": 7, "a1": 8, "a2": 9})

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
