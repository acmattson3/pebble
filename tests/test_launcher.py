from __future__ import annotations

import json
import logging
import threading
import tempfile
import time
import unittest
from pathlib import Path

from control.launcher import Launcher, LauncherCapabilitiesPublisher, LauncherLogMqttHandler, LauncherLogsPublisher
from tests.helpers import FakeMqttClient, FakeProcess, make_base_config


class LauncherTests(unittest.TestCase):
    def _launcher(self, *, enabled: bool = True) -> Launcher:
        config = make_base_config("launchbot")
        config["services"]["av_daemon"]["enabled"] = False
        config["services"]["mqtt_bridge"]["enabled"] = enabled
        config["services"]["serial_mcu_bridge"]["enabled"] = False
        config["services"]["soundboard_handler"]["enabled"] = False
        config["services"]["autonomy_manager"]["enabled"] = False
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.json"
            cfg_path.write_text(json.dumps(config))
            return Launcher(config, cfg_path)

    def test_service_enabled_lookup(self):
        launcher = self._launcher(enabled=True)
        self.assertTrue(launcher._service_enabled("mqtt_bridge"))
        self.assertFalse(launcher._service_enabled("serial_mcu_bridge"))

    def test_stop_terminates_running_children(self):
        launcher = self._launcher(enabled=True)
        proc = FakeProcess(running=True)
        launcher.children = {"mqtt_bridge": proc}
        launcher.shutdown_timeout = 0.0
        launcher.stop()
        self.assertTrue(proc.terminate_called)
        self.assertFalse(proc.kill_called)

    def test_stop_kills_stubborn_children_after_timeout(self):
        launcher = self._launcher(enabled=True)
        proc = FakeProcess(running=True)

        def _no_exit_terminate() -> None:
            proc.terminate_called = True

        proc.terminate = _no_exit_terminate  # type: ignore[assignment]

        launcher.children = {"mqtt_bridge": proc}
        launcher.shutdown_timeout = 0.0
        launcher.stop()
        self.assertTrue(proc.terminate_called)
        self.assertTrue(proc.kill_called)

    def test_no_enabled_service_would_raise_from_start(self):
        launcher = self._launcher(enabled=False)
        with self.assertRaises(SystemExit):
            launcher.start()

    def test_imu_daemon_is_not_launcher_managed(self):
        config = make_base_config("launchbot")
        config["services"]["mqtt_bridge"]["enabled"] = False
        config["services"]["serial_mcu_bridge"]["enabled"] = False
        config["services"]["soundboard_handler"]["enabled"] = False
        config["services"]["autonomy_manager"]["enabled"] = False
        config["services"]["imu_daemon"]["enabled"] = True
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.json"
            cfg_path.write_text(json.dumps(config))
            launcher = Launcher(config, cfg_path)
        with self.assertRaises(SystemExit):
            launcher.start()

    def test_serial_bridge_instances_are_launcher_managed(self):
        config = make_base_config("launchbot")
        config["services"]["mqtt_bridge"]["enabled"] = False
        config["services"]["serial_mcu_bridge"]["enabled"] = False
        config["services"]["serial_mcu_bridge"]["instances"] = {
            "imu": {
                "enabled": True,
                "protocol": "imu_mpu6050_v1",
                "serial": {"port": "/dev/ttyUSB0"},
            }
        }
        config["services"]["soundboard_handler"]["enabled"] = False
        config["services"]["autonomy_manager"]["enabled"] = False
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.json"
            cfg_path.write_text(json.dumps(config))
            launcher = Launcher(config, cfg_path)
        enabled = launcher._enabled_children()
        self.assertIn("serial_mcu_bridge:imu", enabled)
        self.assertEqual(
            enabled["serial_mcu_bridge:imu"],
            ("control.services.serial_mcu_bridge", ["--instance", "imu"]),
        )

    def test_capabilities_publisher_default_interval_is_hourly(self):
        config = make_base_config("launchbot")
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.json"
            cfg_path.write_text(json.dumps(config))
            publisher = LauncherCapabilitiesPublisher(config, cfg_path)
        self.assertEqual(3600.0, publisher.publish_interval_seconds)

    def test_capabilities_publisher_interval_override(self):
        config = make_base_config("launchbot")
        config["services"]["launcher"]["capabilities"] = {"publish_interval_seconds": 120.0}
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.json"
            cfg_path.write_text(json.dumps(config))
            publisher = LauncherCapabilitiesPublisher(config, cfg_path)
        self.assertEqual(120.0, publisher.publish_interval_seconds)

    def test_capabilities_publisher_interval_inherits_shared_default(self):
        config = make_base_config("launchbot")
        config["services"]["defaults"] = {"retained_publish_interval_seconds": 900.0}
        config["services"]["launcher"]["capabilities"] = {"enabled": True}
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.json"
            cfg_path.write_text(json.dumps(config))
            publisher = LauncherCapabilitiesPublisher(config, cfg_path)
        self.assertEqual(900.0, publisher.publish_interval_seconds)

    def test_capabilities_periodic_publish_loop_connected(self):
        config = make_base_config("launchbot")
        config["services"]["launcher"]["capabilities"] = {"publish_interval_seconds": 0.02}
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.json"
            cfg_path.write_text(json.dumps(config))
            publisher = LauncherCapabilitiesPublisher(config, cfg_path)

        publisher.client = FakeMqttClient(connected=True)  # type: ignore[assignment]
        thread = threading.Thread(target=publisher._periodic_publish_loop, daemon=True)
        thread.start()
        time.sleep(0.06)
        publisher._stop_event.set()
        thread.join(timeout=1.0)

        self.assertGreaterEqual(len(publisher.client.publish_calls), 1)  # type: ignore[union-attr]

    def test_capabilities_periodic_publish_loop_disconnected(self):
        config = make_base_config("launchbot")
        config["services"]["launcher"]["capabilities"] = {"publish_interval_seconds": 0.02}
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.json"
            cfg_path.write_text(json.dumps(config))
            publisher = LauncherCapabilitiesPublisher(config, cfg_path)

        publisher.client = FakeMqttClient(connected=False)  # type: ignore[assignment]
        thread = threading.Thread(target=publisher._periodic_publish_loop, daemon=True)
        thread.start()
        time.sleep(0.06)
        publisher._stop_event.set()
        thread.join(timeout=1.0)

        self.assertEqual(0, len(publisher.client.publish_calls))  # type: ignore[union-attr]

    def test_logs_publisher_default_topic(self):
        config = make_base_config("launchbot")
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.json"
            cfg_path.write_text(json.dumps(config))
            publisher = LauncherLogsPublisher(config, cfg_path)
        self.assertEqual("pebble/robots/launchbot/outgoing/logs", publisher.topic)

    def test_logs_handler_publishes_payload(self):
        config = make_base_config("launchbot")
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.json"
            cfg_path.write_text(json.dumps(config))
            publisher = LauncherLogsPublisher(config, cfg_path)

        publisher.client = FakeMqttClient(connected=True)  # type: ignore[assignment]
        handler = LauncherLogMqttHandler(publisher)
        record = logging.getLogger("launcher-test").makeRecord(
            "launcher-test",
            logging.WARNING,
            __file__,
            1,
            "service failure: %s",
            ("boom",),
            None,
            extra={"pb_service": "mqtt_bridge", "pb_pid": 4321},
        )
        handler.emit(record)

        self.assertEqual(1, len(publisher.client.publish_calls))  # type: ignore[union-attr]
        topic, payload_raw, qos, retain = publisher.client.publish_calls[0]  # type: ignore[index,union-attr]
        self.assertEqual("pebble/robots/launchbot/outgoing/logs", topic)
        self.assertEqual(0, qos)
        self.assertFalse(retain)
        payload = json.loads(payload_raw)
        self.assertEqual("WARNING", payload["level"])
        self.assertEqual("mqtt_bridge", payload["service"])
        self.assertEqual(4321, payload["pid"])
        self.assertEqual("service failure: boom", payload["message"])

    def test_logs_handler_respects_min_level(self):
        config = make_base_config("launchbot")
        config["services"]["launcher"]["logs"] = {"min_level": "WARNING"}
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.json"
            cfg_path.write_text(json.dumps(config))
            publisher = LauncherLogsPublisher(config, cfg_path)

        publisher.client = FakeMqttClient(connected=True)  # type: ignore[assignment]
        handler = LauncherLogMqttHandler(publisher)
        info_record = logging.getLogger("launcher-test").makeRecord(
            "launcher-test",
            logging.INFO,
            __file__,
            1,
            "hello",
            (),
            None,
        )
        handler.emit(info_record)
        self.assertEqual(0, len(publisher.client.publish_calls))  # type: ignore[union-attr]


if __name__ == "__main__":
    unittest.main()
