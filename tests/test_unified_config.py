from __future__ import annotations

import importlib.util
import json
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

from audio import audio_publisher as audio_pub
from audio import audio_receiver as audio_rx
from video.camera_config import load_profile


class UnifiedConfigTests(unittest.TestCase):
    @staticmethod
    def _fake_flask_module() -> types.ModuleType:
        fake_flask = types.ModuleType("flask")

        class _FakeFlask:
            def __init__(self, *_args, **_kwargs):
                pass

            def route(self, *_args, **_kwargs):
                def _deco(func):
                    return func

                return _deco

        fake_flask.Flask = _FakeFlask
        fake_flask.Response = object
        fake_flask.abort = lambda *_a, **_k: None
        fake_flask.jsonify = lambda *_a, **_k: {}
        fake_flask.request = types.SimpleNamespace(get_json=lambda *_a, **_k: {}, args={})
        fake_flask.stream_with_context = lambda f: f
        return fake_flask

    def _load_web_module(self, cfg_path: Path):
        module_path = Path("web-interface/web-control.py").resolve()
        spec = importlib.util.spec_from_file_location("web_control_under_test", module_path)
        self.assertIsNotNone(spec)
        module = importlib.util.module_from_spec(spec)

        fake_cv2 = types.ModuleType("cv2")
        fake_np = types.ModuleType("numpy")

        with mock.patch.dict(
            "os.environ",
            {"PEBBLE_CONFIG": str(cfg_path)},
        ), mock.patch.dict(
            "sys.modules",
            {
                "flask": self._fake_flask_module(),
                "cv2": fake_cv2,
                "numpy": fake_np,
            },
        ):
            assert spec is not None and spec.loader is not None
            spec.loader.exec_module(module)
        return module

    def _runtime_cfg(self) -> dict:
        return {
            "log_level": "INFO",
            "robot": {
                "system": "pebble",
                "type": "robots",
                "id": "testbot",
            },
            "local_mqtt": {
                "host": "127.0.0.1",
                "port": 1883,
                "keepalive": 60,
                "username": "",
                "password": "",
                "tls": {"enabled": False},
            },
            "services": {
                "av_daemon": {
                    "video": {
                        "width": 800,
                        "height": 600,
                        "fps": 20,
                        "socket_path": "/tmp/test-video.sock",
                        "rotate_degrees": 180,
                    }
                },
                "mqtt_bridge": {
                    "remote_mqtt": {
                        "host": "remote-broker",
                        "port": 1883,
                        "keepalive": 45,
                        "username": "robot",
                        "password": "secret",
                        "tls": {"enabled": False},
                    },
                    "media": {
                        "video_publisher": {
                            "profile": "robot-video",
                            "topic": "pebble/robots/testbot/outgoing/front-camera",
                            "input_shm": "/tmp/test-video.sock",
                            "width": 800,
                            "height": 600,
                            "fps": 20,
                            "publish_width": 400,
                            "publish_height": 300,
                            "publish_fps": 12,
                            "target_fps": 15,
                            "keyframe_interval": 4,
                            "jpeg_quality": 11,
                        },
                        "audio_publisher": {
                            "topic": "pebble/robots/testbot/outgoing/audio",
                            "input_shm": "/tmp/test-audio.sock",
                            "rate": 16000,
                            "channels": 1,
                            "chunk_ms": 20,
                            "qos": 0,
                        },
                        "audio_receiver": {
                            "topic": "pebble/robots/testbot/incoming/audio-stream",
                            "device": "default",
                            "rate": 16000,
                            "channels": 1,
                            "queue_limit": 48,
                            "concealment_ms": 500,
                            "qos": 0,
                        },
                    }
                },
            },
            "web_interface": {
                "host": "0.0.0.0",
                "port": 8080,
                "robot_discovery": {"seed_configured": True},
                "robots": [{"id": "testbot", "name": "Test Bot"}],
            },
        }

    def test_video_profile_derived_from_runtime_config(self):
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.json"
            cfg_path.write_text(json.dumps(self._runtime_cfg()))
            profile = load_profile("robot-video", str(cfg_path))

        self.assertEqual(profile["mqtt"]["topic"], "pebble/robots/testbot/outgoing/front-camera")
        self.assertEqual(profile["mqtt"]["broker"], "remote-broker")
        self.assertEqual(profile["mqtt"]["keepalive"], 45)
        self.assertEqual(profile["source"]["socket_path"], "/tmp/test-video.sock")
        self.assertEqual(profile["source"]["width"], 800)
        self.assertEqual(profile["source"]["height"], 600)
        self.assertEqual(profile["encoding"]["publish_width"], 400)
        self.assertEqual(profile["encoding"]["publish_height"], 300)
        self.assertEqual(profile["encoding"]["target_fps"], 12)
        self.assertEqual(profile["encoding"]["keyframe_interval"], 4)
        self.assertEqual(profile["encoding"]["jpeg_quality"], 11)
        self.assertEqual(profile["frame_transform"]["rotate_degrees"], 180)

    def test_video_profile_allows_zero_rotate_override(self):
        cfg = self._runtime_cfg()
        cfg["services"]["mqtt_bridge"]["media"]["video_publisher"]["rotate_degrees"] = 0

        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.json"
            cfg_path.write_text(json.dumps(cfg))
            profile = load_profile("robot-video", str(cfg_path))

        self.assertEqual(profile["frame_transform"]["rotate_degrees"], 0)

    def test_audio_runtime_config_extractors(self):
        cfg = self._runtime_cfg()
        pub = audio_pub._runtime_audio_config(cfg)
        rx = audio_rx._runtime_audio_config(cfg)

        assert pub is not None
        assert rx is not None

        self.assertEqual(pub["host"], "remote-broker")
        self.assertEqual(pub["publisher"]["topic"], "pebble/robots/testbot/outgoing/audio")
        self.assertEqual(pub["publisher"]["input_shm"], "/tmp/test-audio.sock")

        self.assertEqual(rx["host"], "remote-broker")
        self.assertEqual(rx["receiver"]["topic"], "pebble/robots/testbot/incoming/audio-stream")
        self.assertEqual(rx["receiver"]["device"], "default")

    def test_web_normalize_runtime_config(self):
        cfg = self._runtime_cfg()

        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.json"
            cfg_path.write_text(json.dumps(cfg))
            module = self._load_web_module(cfg_path)

            normalized = module._normalize_config(cfg)

        self.assertIn("mqtt", normalized)
        self.assertEqual(normalized["host"], "0.0.0.0")
        self.assertEqual(normalized["port"], 8080)
        self.assertEqual(normalized["mqtt"]["host"], "127.0.0.1")
        self.assertEqual(len(normalized["robots"]), 1)
        self.assertEqual(normalized["robots"][0]["id"], "testbot")
        self.assertEqual(normalized["robots"][0]["system"], "pebble")
        self.assertEqual(normalized["robots"][0]["key"], "pebble:robots:testbot")
        self.assertEqual(
            normalized["robots"][0]["video"]["topic"],
            "pebble/robots/testbot/outgoing/front-camera",
        )

    def test_web_capabilities_payload_updates_robot_features(self):
        cfg = self._runtime_cfg()

        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.json"
            cfg_path.write_text(json.dumps(cfg))
            module = self._load_web_module(cfg_path)

            payload = {
                "schema": "pebble-capabilities/v1",
                "value": {
                    "video": {"available": True, "controls": False},
                    "audio": {"available": True, "controls": True},
                    "soundboard": {"available": False, "controls": False},
                    "autonomy": {"available": True, "controls": True},
                    "drive": {"available": True, "controls": True},
                    "lights": {"solid": True, "flash": False},
                    "telemetry": {
                        "touch_sensors": False,
                        "charging_status": True,
                        "wheel_odometry": True,
                        "imu": True,
                    },
                },
            }
            module._handle_capabilities_payload("testbot", payload, "robots")
            robot = module._get_robot("testbot")

        self.assertIsNotNone(robot)
        assert robot is not None
        self.assertTrue(robot.get("hasVideo"))
        self.assertFalse(robot.get("videoControls"))
        self.assertTrue(robot.get("hasAudio"))
        self.assertTrue(robot.get("audioControls"))
        self.assertFalse(robot.get("hasSoundboard"))
        self.assertFalse(robot.get("soundboardControls"))
        self.assertTrue(robot.get("hasAutonomy"))
        self.assertTrue(robot.get("autonomyControls"))
        self.assertTrue(robot.get("hasDrive"))
        self.assertTrue(robot.get("hasLights"))
        self.assertTrue(robot.get("hasTelemetry"))
        self.assertTrue(robot.get("hasOdometry"))
        self.assertTrue(robot.get("hasImu"))

    def test_web_capability_values_are_not_overridden_by_config_hints(self):
        cfg = self._runtime_cfg()

        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.json"
            cfg_path.write_text(json.dumps(cfg))
            module = self._load_web_module(cfg_path)

            payload = {
                "schema": "pebble-capabilities/v1",
                "value": {
                    "video": {"available": False, "controls": False},
                    "audio": {"available": False, "controls": False},
                },
            }
            module._handle_capabilities_payload("testbot", payload, "robots")
            module._ensure_robot_placeholder("testbot", "robots")
            robot = module._get_robot("testbot")
            assert robot is not None
            self.assertFalse(robot.get("hasVideo"))
            self.assertFalse(robot.get("videoControls"))
            self.assertFalse(robot.get("hasAudio"))
            self.assertFalse(robot.get("audioControls"))

            module._mark_robot_video_capable("testbot", True)
            module._mark_robot_audio_capable("testbot", True)
            robot = module._get_robot("testbot")

        self.assertIsNotNone(robot)
        assert robot is not None
        self.assertTrue(robot.get("hasVideo"))
        self.assertFalse(robot.get("videoControls"))
        self.assertTrue(robot.get("hasAudio"))
        self.assertFalse(robot.get("audioControls"))

    def test_web_capabilities_command_and_flag_topics_are_used(self):
        cfg = self._runtime_cfg()

        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.json"
            cfg_path.write_text(json.dumps(cfg))
            module = self._load_web_module(cfg_path)

            payload = {
                "schema": "pebble-capabilities/v1",
                "value": {
                    "video": {
                        "available": True,
                        "controls": True,
                        "command_topic": "pebble/robots/testbot/incoming/front-camera-custom",
                        "flag_topic": "pebble/robots/testbot/incoming/flags/mqtt-video-custom",
                        "overlays_topic": "pebble/robots/testbot/outgoing/video-overlays-custom",
                    },
                    "audio": {
                        "available": True,
                        "controls": True,
                        "command_topic": "pebble/robots/testbot/incoming/audio-custom",
                        "flag_topic": "pebble/robots/testbot/incoming/flags/mqtt-audio-custom",
                    },
                    "autonomy": {
                        "available": True,
                        "controls": True,
                        "command_topic": "pebble/robots/testbot/incoming/autonomy-command-custom",
                        "files_topic": "pebble/robots/testbot/outgoing/autonomy-files-custom",
                        "status_topic": "pebble/robots/testbot/outgoing/autonomy-status-custom",
                    },
                    "system": {
                        "service_restart": {
                            "available": True,
                            "controls": True,
                            "flag_topic": "pebble/robots/testbot/incoming/flags/service-restart-custom",
                        }
                    },
                },
            }
            module._handle_capabilities_payload("testbot", payload, "robots")

            self.assertEqual(
                module._video_command_topic_for_robot("testbot"),
                "pebble/robots/testbot/incoming/front-camera-custom",
            )
            self.assertEqual(
                module._video_flag_topic_for_robot("testbot"),
                "pebble/robots/testbot/incoming/flags/mqtt-video-custom",
            )
            self.assertEqual(
                module._video_overlays_topic_for_robot("testbot"),
                "pebble/robots/testbot/outgoing/video-overlays-custom",
            )
            self.assertEqual(
                module._audio_command_topic_for_robot("testbot"),
                "pebble/robots/testbot/incoming/audio-custom",
            )
            self.assertEqual(
                module._audio_flag_topic_for_robot("testbot"),
                "pebble/robots/testbot/incoming/flags/mqtt-audio-custom",
            )
            self.assertEqual(
                module._autonomy_command_topic_for_robot("testbot"),
                "pebble/robots/testbot/incoming/autonomy-command-custom",
            )
            self.assertEqual(
                module._autonomy_files_topic_for_robot("testbot"),
                "pebble/robots/testbot/outgoing/autonomy-files-custom",
            )
            self.assertEqual(
                module._autonomy_status_topic_for_robot("testbot"),
                "pebble/robots/testbot/outgoing/autonomy-status-custom",
            )
            self.assertEqual(
                module._service_restart_flag_topic_for_robot("testbot"),
                "pebble/robots/testbot/incoming/flags/service-restart-custom",
            )

    def test_web_audio_buffer_resets_on_sequence_rewind(self):
        cfg = self._runtime_cfg()

        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.json"
            cfg_path.write_text(json.dumps(cfg))
            module = self._load_web_module(cfg_path)

            pcm = bytes(320)
            first = module._encode_audio_packet(100, 16000, 1, pcm)
            second = module._encode_audio_packet(1, 16000, 1, pcm)
            assert first is not None and second is not None

            module._handle_audio_payload("testbot", first)
            module._handle_audio_payload("testbot", second)

            robot_key = module._robot_key("testbot")
            with module.audio_cache_lock:
                cache = module.audio_cache.get(robot_key) or {}
                buffer = list(module.audio_buffers.get(robot_key, []))

        self.assertEqual(cache.get("seq"), 1)
        self.assertEqual(cache.get("generation"), 1)
        self.assertEqual(len(buffer), 1)
        self.assertEqual(buffer[0].get("seq"), 1)

    def test_web_ui_template_has_system_dropdown_and_map_location_label(self):
        cfg = self._runtime_cfg()

        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.json"
            cfg_path.write_text(json.dumps(cfg))
            module = self._load_web_module(cfg_path)

            html = module._robot_options_html()

        self.assertIn('label for="systemSelect">System:</label>', html)
        self.assertIn("<h2>Map & Location</h2>", html)

    def test_web_does_not_seed_configured_robots_into_ui_snapshot(self):
        cfg = self._runtime_cfg()

        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.json"
            cfg_path.write_text(json.dumps(cfg))
            module = self._load_web_module(cfg_path)

            snapshot = module._ui_robot_snapshot()

        self.assertEqual(snapshot, [])

    def test_web_topic_discovery_tracks_multiple_systems_without_collisions(self):
        cfg = self._runtime_cfg()

        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.json"
            cfg_path.write_text(json.dumps(cfg))
            module = self._load_web_module(cfg_path)

            payload = json.dumps({"t": 1_800_000_000.0}).encode("utf-8")
            msg1 = types.SimpleNamespace(
                topic="pebble/robots/testbot/outgoing/heartbeat",
                payload=payload,
                qos=1,
                retain=False,
            )
            msg2 = types.SimpleNamespace(
                topic="pebblebot/robots/testbot/outgoing/heartbeat",
                payload=payload,
                qos=1,
                retain=False,
            )

            module._on_mqtt_message(None, None, msg1)
            module._on_mqtt_message(None, None, msg2)
            snapshot = module._ui_robot_snapshot()

        systems = {(item["system"], item["id"], item["key"]) for item in snapshot}
        self.assertIn(("pebble", "testbot", "pebble:robots:testbot"), systems)
        self.assertIn(("pebblebot", "testbot", "pebblebot:robots:testbot"), systems)

    def test_web_last_will_offline_payload_marks_robot_offline(self):
        cfg = self._runtime_cfg()

        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.json"
            cfg_path.write_text(json.dumps(cfg))
            module = self._load_web_module(cfg_path)

            online_msg = types.SimpleNamespace(
                topic="pebble/robots/mip/outgoing/online",
                payload=json.dumps({"t": 1_800_000_000.0, "online": True, "status": "online"}).encode("utf-8"),
                qos=1,
                retain=True,
            )
            offline_msg = types.SimpleNamespace(
                topic="pebble/robots/mip/outgoing/online",
                payload=json.dumps({"online": False, "status": "offline"}).encode("utf-8"),
                qos=1,
                retain=True,
            )

            module._on_mqtt_message(None, None, online_msg)
            module._on_mqtt_message(None, None, offline_msg)
            snapshot = module._ui_robot_snapshot()

        mip = next(item for item in snapshot if item["key"] == "pebble:robots:mip")
        self.assertFalse(mip["online"])
        self.assertEqual("offline", mip["connectionStatus"])

    def test_web_mqtt_connect_subscribes_all_systems(self):
        cfg = self._runtime_cfg()

        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.json"
            cfg_path.write_text(json.dumps(cfg))
            module = self._load_web_module(cfg_path)

            subscribe_calls: list[tuple[str, int]] = []

            class _FakeClient:
                def subscribe(self, topic, qos=0):
                    subscribe_calls.append((topic, qos))

            module._on_mqtt_connect(_FakeClient(), None, {}, 0)

        self.assertIn(("+/infrastructure", 1), subscribe_calls)
        self.assertIn(("+/+/+/outgoing/heartbeat", 1), subscribe_calls)
        self.assertIn(("+/+/+/outgoing/front-camera", 0), subscribe_calls)

    def test_web_autonomy_payload_handlers(self):
        cfg = self._runtime_cfg()

        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.json"
            cfg_path.write_text(json.dumps(cfg))
            module = self._load_web_module(cfg_path)

            module._handle_autonomy_files_payload(
                "testbot",
                {
                    "files": [
                        {
                            "file": "apriltag-follow",
                            "label": "AprilTag Follow",
                            "configs": [
                                {"key": "tag_id", "type": "int", "default": 13},
                                {"key": "tag_size_m", "type": "float", "default": 0.25},
                            ],
                        }
                    ],
                    "controls": True,
                },
            )
            module._handle_autonomy_status_payload(
                "testbot",
                {"running": True, "file": "apriltag-follow", "pid": 4321, "config": {"tag_id": 13}},
            )

            files_payload = module._autonomy_files_payload("testbot")
            status_payload = module._autonomy_status_payload("testbot")

        self.assertTrue(files_payload.get("enabled"))
        self.assertTrue(files_payload.get("controls"))
        self.assertEqual(files_payload.get("files")[0].get("file"), "apriltag-follow")
        self.assertTrue(status_payload.get("running"))
        self.assertEqual(status_payload.get("file"), "apriltag-follow")
        self.assertEqual(status_payload.get("pid"), 4321)

    def test_web_video_overlay_payload_handlers(self):
        cfg = self._runtime_cfg()

        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.json"
            cfg_path.write_text(json.dumps(cfg))
            module = self._load_web_module(cfg_path)

            module._handle_video_overlays_payload(
                "testbot",
                {
                    "source": "apriltag-follow",
                    "frame_width": 1280,
                    "frame_height": 720,
                    "shapes": [
                        [[0.1, 0.2], [0.8, 0.2], [0.8, 0.7], [0.1, 0.7]],
                        [[-1, 0.5], [2, 0.5]],  # clamped
                    ],
                },
            )
            payload = module._video_overlays_payload("testbot")

        self.assertTrue(payload.get("enabled"))
        self.assertEqual(payload.get("source"), "apriltag-follow")
        self.assertEqual(payload.get("frameWidth"), 1280)
        self.assertEqual(payload.get("frameHeight"), 720)
        self.assertEqual(len(payload.get("shapes") or []), 2)
        first_point = payload["shapes"][1][0]
        self.assertEqual(first_point, [0.0, 0.5])


if __name__ == "__main__":
    unittest.main()
