from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from control.services.mqtt_bridge import MqttBridge
from tests.helpers import FakeMqttClient, FakeMqttMessage, FakePopenFactory, make_base_config


class MqttBridgeTests(unittest.TestCase):
    def _bridge(self) -> MqttBridge:
        config = make_base_config("mqbot")
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.json"
            cfg_path.write_text(json.dumps(config))
            return MqttBridge(config, cfg_path)

    def test_mirror_subscription_switches_on_remote_mirror_flag(self):
        bridge = self._bridge()
        bridge.local_client = FakeMqttClient()

        bridge._refresh_mirror_subscription(force=True)
        self.assertEqual(bridge.local_client.subscribe_calls[-1][0], f"{bridge.flags_prefix}#")

        bridge._handle_flag_topics(bridge.remote_mirror_topic, {"value": True})
        self.assertIn(f"{bridge.flags_prefix}#", bridge.local_client.unsubscribe_calls)
        self.assertEqual(bridge.local_client.subscribe_calls[-1][0], f"{bridge.incoming_prefix}#")

    def test_incoming_mirror_behavior_flag_only_then_full(self):
        bridge = self._bridge()
        bridge.local_client = FakeMqttClient()
        bridge.remote_client = FakeMqttClient()

        drive_payload = b'{"value":{"x":0.1,"z":0.2}}'
        drive_msg = FakeMqttMessage(topic=f"{bridge.incoming_prefix}drive-values", payload=drive_payload)
        bridge._on_local_message(None, None, drive_msg)  # type: ignore[arg-type]
        self.assertEqual(len(bridge.remote_client.publish_calls), 0)

        flag_payload = b'{"value":true}'
        flag_msg = FakeMqttMessage(topic=bridge.mqtt_video_flag_topic, payload=flag_payload)
        bridge._on_local_message(None, None, flag_msg)  # type: ignore[arg-type]
        self.assertEqual(len(bridge.remote_client.publish_calls), 1)
        self.assertEqual(bridge.remote_client.publish_calls[0][0], bridge.mqtt_video_flag_topic)

        mirror_msg = FakeMqttMessage(topic=bridge.remote_mirror_topic, payload=b'{"value":true}')
        bridge._on_local_message(None, None, mirror_msg)  # type: ignore[arg-type]
        drive_msg2 = FakeMqttMessage(topic=f"{bridge.incoming_prefix}drive-values", payload=drive_payload)
        bridge._on_local_message(None, None, drive_msg2)  # type: ignore[arg-type]
        self.assertGreaterEqual(len(bridge.remote_client.publish_calls), 3)
        self.assertEqual(bridge.remote_client.publish_calls[-1][0], f"{bridge.incoming_prefix}drive-values")

    def test_remote_to_local_forward_and_loop_prevention(self):
        bridge = self._bridge()
        bridge.local_client = FakeMqttClient()
        bridge.remote_client = FakeMqttClient()
        bridge.remote_mirror_enabled = True

        topic = f"{bridge.incoming_prefix}drive-values"
        payload = b'{"value":{"x":0.4,"z":0.5}}'
        remote_msg = FakeMqttMessage(topic=topic, payload=payload)
        bridge._on_remote_message(None, None, remote_msg)  # type: ignore[arg-type]
        self.assertEqual(len(bridge.local_client.publish_calls), 1)

        local_echo = FakeMqttMessage(topic=topic, payload=payload)
        bridge._on_local_message(None, None, local_echo)  # type: ignore[arg-type]
        self.assertEqual(len(bridge.remote_client.publish_calls), 0)

        local_new = FakeMqttMessage(topic=topic, payload=b'{"value":{"x":1.0,"z":0.0}}')
        bridge._on_local_message(None, None, local_new)  # type: ignore[arg-type]
        self.assertEqual(len(bridge.remote_client.publish_calls), 1)

    def test_remote_audio_stream_not_forwarded_to_local(self):
        bridge = self._bridge()
        bridge.local_client = FakeMqttClient()
        bridge.remote_client = FakeMqttClient()

        topic = bridge.audio_stream_topic
        payload = b"\x00\x01\x02"
        msg = FakeMqttMessage(topic=topic, payload=payload)
        bridge._on_remote_message(None, None, msg)  # type: ignore[arg-type]
        self.assertEqual(len(bridge.local_client.publish_calls), 0)

    def test_media_flags_gate_subprocesses_and_disable_stops(self):
        bridge = self._bridge()
        bridge.local_client = FakeMqttClient()
        bridge.remote_client = FakeMqttClient()
        popen_factory = FakePopenFactory()

        with mock.patch("control.services.mqtt_bridge.subprocess.Popen", new=popen_factory):
            audio_on = FakeMqttMessage(topic=bridge.audio_control_topic, payload=b'{"enabled":true}')
            bridge._on_local_message(None, None, audio_on)  # type: ignore[arg-type]
            self.assertEqual(len(popen_factory.calls), 0)

            audio_flag_true = FakeMqttMessage(topic=bridge.mqtt_audio_flag_topic, payload=b'{"value":true}')
            bridge._on_local_message(None, None, audio_flag_true)  # type: ignore[arg-type]
            self.assertEqual(len(popen_factory.calls), 2)

            audio_flag_false = FakeMqttMessage(topic=bridge.mqtt_audio_flag_topic, payload=b'{"value":false}')
            bridge._on_local_message(None, None, audio_flag_false)  # type: ignore[arg-type]
            self.assertIsNone(bridge.audio_processes["publisher"])
            self.assertIsNone(bridge.audio_processes["receiver"])

            video_on = FakeMqttMessage(topic=bridge.video_control_topic, payload=b'{"enabled":true}')
            bridge._on_local_message(None, None, video_on)  # type: ignore[arg-type]
            video_flag_true = FakeMqttMessage(topic=bridge.mqtt_video_flag_topic, payload=b'{"value":true}')
            bridge._on_local_message(None, None, video_flag_true)  # type: ignore[arg-type]
            self.assertEqual(len(popen_factory.calls), 3)

            video_flag_false = FakeMqttMessage(topic=bridge.mqtt_video_flag_topic, payload=b'{"value":false}')
            bridge._on_local_message(None, None, video_flag_false)  # type: ignore[arg-type]
            self.assertIsNone(bridge.video_process)

    def test_local_connect_subscribes_reboot_and_git_pull_flag_topics(self):
        bridge = self._bridge()
        bridge.local_client = FakeMqttClient()

        bridge._on_local_connect(bridge.local_client, None, {}, 0)  # type: ignore[arg-type]
        subscribed_topics = {topic for topic, _qos in bridge.local_client.subscribe_calls}
        self.assertIn(bridge.reboot_flag_topic, subscribed_topics)
        self.assertIn(bridge.git_pull_flag_topic, subscribed_topics)

    def test_reboot_flag_triggers_reboot_command(self):
        bridge = self._bridge()
        bridge.reboot_control_cfg = {"command": ["echo", "reboot"]}
        bridge.reboot_cooldown_seconds = 0.0
        popen_factory = FakePopenFactory()

        with mock.patch("control.services.mqtt_bridge.subprocess.Popen", new=popen_factory):
            reboot_msg = FakeMqttMessage(topic=bridge.reboot_flag_topic, payload=b'{"value":true}', retain=False)
            bridge._on_local_message(None, None, reboot_msg)  # type: ignore[arg-type]
            self.assertEqual(len(popen_factory.calls), 1)

    def test_reboot_flag_ignores_false_and_retained_messages(self):
        bridge = self._bridge()
        bridge.reboot_control_cfg = {"command": ["echo", "reboot"]}
        bridge.reboot_cooldown_seconds = 0.0
        bridge.reboot_ignore_retained = True
        popen_factory = FakePopenFactory()

        with mock.patch("control.services.mqtt_bridge.subprocess.Popen", new=popen_factory):
            false_msg = FakeMqttMessage(topic=bridge.reboot_flag_topic, payload=b'{"value":false}', retain=False)
            bridge._on_local_message(None, None, false_msg)  # type: ignore[arg-type]
            retained_true_msg = FakeMqttMessage(topic=bridge.reboot_flag_topic, payload=b'{"value":true}', retain=True)
            bridge._on_local_message(None, None, retained_true_msg)  # type: ignore[arg-type]
            self.assertEqual(len(popen_factory.calls), 0)

    def test_git_pull_flag_triggers_git_pull_command(self):
        bridge = self._bridge()
        bridge.git_pull_control_cfg = {"command": ["git", "-C", "/opt/pebble", "pull", "--ff-only"]}
        bridge.git_pull_cooldown_seconds = 0.0
        popen_factory = FakePopenFactory()

        with mock.patch("control.services.mqtt_bridge.subprocess.Popen", new=popen_factory):
            git_pull_msg = FakeMqttMessage(topic=bridge.git_pull_flag_topic, payload=b'{"value":true}', retain=False)
            bridge._on_local_message(None, None, git_pull_msg)  # type: ignore[arg-type]
            self.assertEqual(len(popen_factory.calls), 1)

    def test_git_pull_flag_ignores_false_and_retained_messages(self):
        bridge = self._bridge()
        bridge.git_pull_control_cfg = {"command": ["git", "-C", "/opt/pebble", "pull", "--ff-only"]}
        bridge.git_pull_cooldown_seconds = 0.0
        bridge.git_pull_ignore_retained = True
        popen_factory = FakePopenFactory()

        with mock.patch("control.services.mqtt_bridge.subprocess.Popen", new=popen_factory):
            false_msg = FakeMqttMessage(topic=bridge.git_pull_flag_topic, payload=b'{"value":false}', retain=False)
            bridge._on_local_message(None, None, false_msg)  # type: ignore[arg-type]
            retained_true_msg = FakeMqttMessage(topic=bridge.git_pull_flag_topic, payload=b'{"value":true}', retain=True)
            bridge._on_local_message(None, None, retained_true_msg)  # type: ignore[arg-type]
            self.assertEqual(len(popen_factory.calls), 0)

    def test_git_pull_flag_skips_when_command_already_running(self):
        bridge = self._bridge()
        bridge.git_pull_control_cfg = {"command": ["git", "-C", "/opt/pebble", "pull", "--ff-only"]}
        bridge.git_pull_cooldown_seconds = 0.0
        popen_factory = FakePopenFactory()

        with mock.patch("control.services.mqtt_bridge.subprocess.Popen", new=popen_factory):
            msg = FakeMqttMessage(topic=bridge.git_pull_flag_topic, payload=b'{"value":true}', retain=False)
            bridge._on_local_message(None, None, msg)  # type: ignore[arg-type]
            bridge._on_local_message(None, None, msg)  # type: ignore[arg-type]
            self.assertEqual(len(popen_factory.calls), 1)

    def test_heartbeat_publishes_remote_only(self):
        bridge = self._bridge()
        bridge.local_client = FakeMqttClient()
        bridge.remote_client = FakeMqttClient()
        bridge._publish_heartbeat()
        self.assertEqual(len(bridge.remote_client.publish_calls), 1)
        self.assertEqual(bridge.remote_client.publish_calls[0][0], bridge.heartbeat_topic)
        self.assertEqual(len(bridge.local_client.publish_calls), 0)

    def test_high_rate_imu_topic_is_not_forwarded_but_low_rate_is(self):
        bridge = self._bridge()
        bridge.local_client = FakeMqttClient()
        bridge.remote_client = FakeMqttClient()

        high_rate_topic = f"{bridge.outgoing_prefix}sensors/imu-fast"
        high_rate_msg = FakeMqttMessage(topic=high_rate_topic, payload=b'{"seq":1}', qos=0, retain=False)
        bridge._on_local_message(None, None, high_rate_msg)  # type: ignore[arg-type]
        self.assertEqual(len(bridge.remote_client.publish_calls), 0)

        low_rate_topic = f"{bridge.outgoing_prefix}sensors/imu"
        low_rate_msg = FakeMqttMessage(topic=low_rate_topic, payload=b'{"seq":2}', qos=1, retain=False)
        bridge._on_local_message(None, None, low_rate_msg)  # type: ignore[arg-type]
        self.assertEqual(len(bridge.remote_client.publish_calls), 1)
        self.assertEqual(bridge.remote_client.publish_calls[0][0], low_rate_topic)

    def test_serial_imu_instance_high_rate_topic_is_not_forwarded(self):
        config = make_base_config("mqbot")
        config["services"]["serial_mcu_bridge"]["enabled"] = False
        config["services"]["serial_mcu_bridge"]["instances"] = {
            "imu": {
                "enabled": True,
                "protocol": "imu_mpu6050_v1",
                "serial": {"port": "/dev/ttyUSB0"},
                "topics": {
                    "high_rate": "pebble/robots/mqbot/outgoing/custom-imu-fast",
                    "low_rate": "pebble/robots/mqbot/outgoing/custom-imu",
                },
            }
        }
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.json"
            cfg_path.write_text(json.dumps(config))
            bridge = MqttBridge(config, cfg_path)

        bridge.local_client = FakeMqttClient()
        bridge.remote_client = FakeMqttClient()

        high_rate_msg = FakeMqttMessage(
            topic="pebble/robots/mqbot/outgoing/custom-imu-fast",
            payload=b'{"seq":1}',
            qos=0,
            retain=False,
        )
        bridge._on_local_message(None, None, high_rate_msg)  # type: ignore[arg-type]
        self.assertEqual(len(bridge.remote_client.publish_calls), 0)

        low_rate_msg = FakeMqttMessage(
            topic="pebble/robots/mqbot/outgoing/custom-imu",
            payload=b'{"seq":2}',
            qos=1,
            retain=False,
        )
        bridge._on_local_message(None, None, low_rate_msg)  # type: ignore[arg-type]
        self.assertEqual(len(bridge.remote_client.publish_calls), 1)
        self.assertEqual(bridge.remote_client.publish_calls[0][0], "pebble/robots/mqbot/outgoing/custom-imu")

    def test_remote_connect_replays_cached_retained_outgoing(self):
        bridge = self._bridge()
        bridge.local_client = FakeMqttClient()
        bridge.remote_client = FakeMqttClient()

        retained_topic = f"{bridge.outgoing_prefix}capabilities"
        retained_payload = b'{"value":{"ok":true}}'
        retained_msg = FakeMqttMessage(topic=retained_topic, payload=retained_payload, qos=1, retain=True)
        bridge._on_local_message(None, None, retained_msg)  # type: ignore[arg-type]

        bridge.remote_client.publish_calls.clear()
        bridge._on_remote_connect(bridge.remote_client, None, {}, 0)  # type: ignore[arg-type]

        published_topics = [item[0] for item in bridge.remote_client.publish_calls]
        self.assertIn(bridge.heartbeat_topic, published_topics)
        self.assertIn(retained_topic, published_topics)


if __name__ == "__main__":
    unittest.main()
