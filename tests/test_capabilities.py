from __future__ import annotations

import unittest

from control.common.capabilities import build_capabilities_value, capabilities_topic
from control.common.config import service_cfg
from control.common.topics import identity_from_config
from tests.helpers import make_base_config


class CapabilitiesTests(unittest.TestCase):
    def test_capabilities_topic_default_and_override(self):
        config = make_base_config("capbot")
        identity = identity_from_config(config)
        launcher_cfg = service_cfg(config, "launcher")

        self.assertEqual(
            capabilities_topic(identity, launcher_cfg),
            "pebble/robots/capbot/outgoing/capabilities",
        )

        launcher_cfg["capabilities"] = {"topic": "custom/topic"}
        self.assertEqual(capabilities_topic(identity, launcher_cfg), "custom/topic")

    def test_build_capabilities_value_uses_service_enablement(self):
        config = make_base_config("capbot")
        config["services"]["av_daemon"]["enabled"] = True
        config["services"]["soundboard_handler"]["enabled"] = True
        config["services"]["autonomy_manager"]["enabled"] = True
        value = build_capabilities_value(config)

        self.assertEqual(value["identity"]["id"], "capbot")
        self.assertTrue(value["video"]["available"])
        self.assertTrue(value["audio"]["available"])
        self.assertTrue(value["drive"]["available"])
        self.assertTrue(value["soundboard"]["available"])
        self.assertTrue(value["autonomy"]["available"])
        self.assertEqual(value["video"]["topic"], "pebble/robots/capbot/outgoing/front-camera")
        self.assertEqual(value["video"]["overlays_topic"], "pebble/robots/capbot/outgoing/video-overlays")
        self.assertEqual(value["audio"]["uplink_topic"], "pebble/robots/capbot/incoming/audio-stream")
        self.assertEqual(value["autonomy"]["command_topic"], "pebble/robots/capbot/incoming/autonomy-command")
        self.assertEqual(value["system"]["git_pull"]["flag_topic"], "pebble/robots/capbot/incoming/flags/git-pull")

    def test_build_capabilities_value_disabled_services(self):
        config = make_base_config("capbot")
        config["services"]["av_daemon"]["enabled"] = False
        config["services"]["ros1_bridge"]["enabled"] = False
        config["services"]["serial_mcu_bridge"]["enabled"] = False
        config["services"]["soundboard_handler"]["enabled"] = False
        config["services"]["autonomy_manager"]["enabled"] = False
        value = build_capabilities_value(config)

        self.assertFalse(value["video"]["available"])
        self.assertFalse(value["audio"]["available"])
        self.assertFalse(value["drive"]["available"])
        self.assertFalse(value["lights"]["solid"])
        self.assertFalse(value["telemetry"]["touch_sensors"])
        self.assertFalse(value["soundboard"]["available"])
        self.assertFalse(value["autonomy"]["available"])
        self.assertFalse(value["system"]["git_pull"]["controls"])

    def test_drive_capability_can_come_from_ros1_bridge(self):
        config = make_base_config("capbot")
        config["services"]["serial_mcu_bridge"]["enabled"] = False
        config["services"]["ros1_bridge"]["enabled"] = True
        config["services"]["ros1_bridge"]["topics"] = {"drive_values": "custom/drive"}

        value = build_capabilities_value(config)
        self.assertTrue(value["drive"]["available"])
        self.assertEqual("custom/drive", value["drive"]["topic"])

    def test_charging_status_capability_can_come_from_ros1_bridge(self):
        config = make_base_config("capbot")
        config["services"]["serial_mcu_bridge"]["enabled"] = False
        config["services"]["ros1_bridge"]["enabled"] = True
        config["services"]["ros1_bridge"]["topics"]["charging_status"] = "custom/charging-status"
        config["services"]["ros1_bridge"]["telemetry"]["charging_status"]["enabled"] = True

        value = build_capabilities_value(config)
        self.assertTrue(value["telemetry"]["charging_status"])
        self.assertEqual("custom/charging-status", value["telemetry"]["charging_topic"])

    def test_build_capabilities_value_prefers_publish_video_shape(self):
        config = make_base_config("capbot")
        config["services"]["av_daemon"]["enabled"] = True
        bridge = config["services"]["mqtt_bridge"]
        media_cfg = bridge.setdefault("media", {})
        media = media_cfg.setdefault("video_publisher", {})
        media["publish_width"] = 320
        media["publish_height"] = 180
        media["publish_fps"] = 10

        value = build_capabilities_value(config)
        self.assertEqual(value["video"]["width"], 320)
        self.assertEqual(value["video"]["height"], 180)
        self.assertEqual(value["video"]["fps"], 10)

    def test_touch_telemetry_capability_follows_publish_setting(self):
        config = make_base_config("capbot")
        config["services"]["serial_mcu_bridge"]["enabled"] = True
        value = build_capabilities_value(config)
        self.assertFalse(value["telemetry"]["touch_sensors"])

        config["services"]["serial_mcu_bridge"]["telemetry"] = {"publish_touch_sensors": True}
        value = build_capabilities_value(config)
        self.assertTrue(value["telemetry"]["touch_sensors"])

    def test_reboot_capability_follows_reboot_control_command(self):
        config = make_base_config("capbot")
        value = build_capabilities_value(config)
        reboot = value["system"]["reboot"]
        self.assertTrue(reboot["available"])
        self.assertFalse(reboot["controls"])
        self.assertEqual(reboot["flag_topic"], "pebble/robots/capbot/incoming/flags/reboot")

        config["services"]["mqtt_bridge"]["reboot_control"] = {"command": ["sudo", "-n", "/sbin/reboot"]}
        config["services"]["mqtt_bridge"]["topics"] = {"reboot": "custom/reboot"}
        value = build_capabilities_value(config)
        reboot = value["system"]["reboot"]
        self.assertTrue(reboot["controls"])
        self.assertEqual(reboot["flag_topic"], "custom/reboot")


if __name__ == "__main__":
    unittest.main()
