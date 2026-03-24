from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from control.common.config import load_config, log_level, service_cfg
from control.common.mqtt import parse_bool_payload
from control.common.topics import RobotIdentity, identity_from_config


class CommonModuleTests(unittest.TestCase):
    def test_parse_bool_payload_variants(self):
        self.assertTrue(parse_bool_payload(True))
        self.assertFalse(parse_bool_payload(False))
        self.assertTrue(parse_bool_payload({"value": True}))
        self.assertFalse(parse_bool_payload({"enabled": False}))
        self.assertIsNone(parse_bool_payload({"value": "no"}))
        self.assertIsNone(parse_bool_payload({}))

    def test_identity_from_config_and_topic(self):
        identity = identity_from_config({"robot": {"system": "pebble", "type": "robots", "id": "unit-1"}})
        self.assertEqual(identity, RobotIdentity(system="pebble", type="robots", robot_id="unit-1"))
        self.assertEqual(identity.base, "pebble/robots/unit-1")
        self.assertEqual(identity.topic("incoming", "drive-values"), "pebble/robots/unit-1/incoming/drive-values")

    def test_identity_requires_robot_id(self):
        with self.assertRaises(SystemExit):
            identity_from_config({"robot": {"system": "pebble", "type": "robots"}})

    def test_load_config_and_service_cfg(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "config.json"
            path.write_text(json.dumps({"log_level": "DEBUG", "services": {"mqtt_bridge": {"enabled": True}}}))
            cfg, resolved = load_config(str(path))
            self.assertEqual(resolved, path)
            self.assertEqual(service_cfg(cfg, "mqtt_bridge"), {"enabled": True})
            self.assertEqual(service_cfg(cfg, "missing"), {})
            self.assertEqual(log_level(cfg, {}), "DEBUG")
            self.assertEqual(log_level(cfg, {"log_level": "warning"}), "WARNING")

    def test_load_config_rejects_missing_and_invalid(self):
        with self.assertRaises(SystemExit):
            load_config("/definitely/missing/config.json")

        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "bad.json"
            path.write_text("[")
            with self.assertRaises(SystemExit):
                load_config(str(path))


if __name__ == "__main__":
    unittest.main()
