from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from control.common.config import default_config_path, load_config, log_level, service_cfg
from control.common.mqtt import parse_bool_payload
from control.common.topics import (
    RobotIdentity,
    identity_from_config,
    resolve_mapped_topic,
    resolve_topic,
    robot_topic,
)


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

    def test_robot_local_subtopics_resolve_from_identity(self):
        identity = RobotIdentity(system="pebble", type="robots", robot_id="goob")
        self.assertEqual(robot_topic(identity, "o/video"), "pebble/robots/goob/o/video")
        self.assertEqual(
            resolve_topic(identity, {"subtopic": "incoming/drive-values"}, "incoming/default"),
            "pebble/robots/goob/incoming/drive-values",
        )
        self.assertEqual(
            resolve_topic(identity, {}, "o/status"),
            "pebble/robots/goob/o/status",
        )

    def test_legacy_full_topics_and_mapped_subtopics_resolve(self):
        identity = RobotIdentity(system="pebble", type="robots", robot_id="goob")
        external = "pebble/robots/other/o/status"
        self.assertEqual(resolve_topic(identity, {"topic": external}, "o/status"), external)
        self.assertEqual(
            resolve_mapped_topic(identity, {"subtopics": {"drive": "i/drive"}}, "drive", "i/default"),
            "pebble/robots/goob/i/drive",
        )
        self.assertEqual(
            resolve_mapped_topic(identity, {"topics": {"drive": external}}, "drive", "i/default"),
            external,
        )

    def test_robot_subtopics_reject_absolute_escape_and_wildcards(self):
        identity = RobotIdentity(system="pebble", type="robots", robot_id="goob")
        for value in ("/o/status", "o/../drive", "o/+/status", "o/#", "o//status"):
            with self.subTest(subtopic=value), self.assertRaises(ValueError):
                robot_topic(identity, value)
        with self.assertRaises(ValueError):
            resolve_topic(identity, {"topic": "external/topic", "subtopic": "o/status"}, "o/status")

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

    def test_default_config_prefers_split_robot_config_and_falls_back(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            robot = root / "control/configs/robot.json"
            legacy = root / "control/configs/config.json"
            robot.parent.mkdir(parents=True)
            legacy.write_text("{}")
            self.assertEqual(default_config_path(root), legacy)
            robot.write_text("{}")
            self.assertEqual(default_config_path(root), robot)

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
