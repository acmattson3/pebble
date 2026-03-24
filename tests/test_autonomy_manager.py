from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from control.services.autonomy_manager import AutonomyManager
from tests.helpers import FakeMqttClient, FakePopenFactory, make_base_config


class _ExitedProcess:
    def __init__(self, rc: int) -> None:
        self.pid = 9999
        self._rc = rc

    def poll(self):
        return self._rc


class AutonomyManagerTests(unittest.TestCase):
    def _manager(self) -> tuple[AutonomyManager, Path]:
        td_obj = tempfile.TemporaryDirectory()
        td = Path(td_obj.name)
        autonomy_root = td / "autonomy"
        script_dir = autonomy_root / "apriltag-follow"
        script_dir.mkdir(parents=True)
        (script_dir / "run.py").write_text("print('ok')\n")

        config = make_base_config("autobot")
        config["services"]["autonomy_manager"]["enabled"] = True
        config["services"]["autonomy_manager"]["autonomy_root"] = str(autonomy_root)

        cfg_path = td / "config.json"
        cfg_path.write_text(json.dumps(config))

        manager = AutonomyManager(config, cfg_path)
        self.addCleanup(td_obj.cleanup)
        return manager, cfg_path

    def test_parse_action(self):
        self.assertEqual(
            AutonomyManager._parse_action({"action": "start", "file": "apriltag-follow", "config": {"x": 1}}),
            ("start", "apriltag-follow", {"x": 1}),
        )
        self.assertEqual(AutonomyManager._parse_action({"action": "stop"}), ("stop", None, {}))
        self.assertEqual(
            AutonomyManager._parse_action({"value": {"enabled": True, "file": "apriltag-follow"}}),
            ("start", "apriltag-follow", {}),
        )

    def test_refresh_scripts_publishes_retained_files_payload(self):
        manager, _ = self._manager()
        manager.client = FakeMqttClient()

        manager._refresh_scripts(force=True)

        self.assertEqual(len(manager.client.publish_calls), 1)
        topic, payload, qos, retain = manager.client.publish_calls[0]
        self.assertEqual(topic, manager.files_topic)
        self.assertEqual(qos, 1)
        self.assertTrue(retain)
        self.assertIn("apriltag-follow", payload)

    def test_start_command_launches_selected_script_with_mapped_args(self):
        manager, cfg_path = self._manager()
        manager.client = FakeMqttClient()
        manager._refresh_scripts(force=True)
        popen_factory = FakePopenFactory()

        with mock.patch("control.services.autonomy_manager.subprocess.Popen", new=popen_factory):
            manager._handle_command(
                {
                    "action": "start",
                    "file": "apriltag-follow",
                    "config": {
                        "tag_id": 7,
                        "tag_size_m": 0.12,
                    },
                }
            )

        self.assertEqual(len(popen_factory.calls), 1)
        call = popen_factory.calls[0]
        cmd = call["cmd"]
        self.assertIn(str(cfg_path), cmd)
        self.assertIn("--marker-id", cmd)
        self.assertIn("7", cmd)
        self.assertIn("--marker-size", cmd)
        self.assertIn("0.12", cmd)
        self.assertEqual(manager.running_file, "apriltag-follow")

    def test_start_replaces_existing_process(self):
        manager, _ = self._manager()
        manager.client = FakeMqttClient()
        manager._refresh_scripts(force=True)
        popen_factory = FakePopenFactory()

        with mock.patch("control.services.autonomy_manager.subprocess.Popen", new=popen_factory):
            manager._handle_command({"action": "start", "file": "apriltag-follow"})
            first_proc = manager.process
            manager._handle_command({"action": "start", "file": "apriltag-follow", "config": {"tag_id": 99}})

        self.assertIsNotNone(first_proc)
        assert first_proc is not None
        self.assertTrue(first_proc.terminate_called)
        self.assertEqual(len(popen_factory.calls), 2)

    def test_stop_command_stops_running_process(self):
        manager, _ = self._manager()
        manager.client = FakeMqttClient()
        manager._refresh_scripts(force=True)
        popen_factory = FakePopenFactory()

        with mock.patch("control.services.autonomy_manager.subprocess.Popen", new=popen_factory):
            manager._handle_command({"action": "start", "file": "apriltag-follow"})
            proc = manager.process
            manager._handle_command({"action": "stop"})

        self.assertIsNotNone(proc)
        assert proc is not None
        self.assertTrue(proc.terminate_called)
        self.assertIsNone(manager.process)

    def test_poll_process_sets_error_for_nonzero_exit(self):
        manager, _ = self._manager()
        manager.client = FakeMqttClient()
        manager.running_file = "apriltag-follow"
        manager.process = _ExitedProcess(3)  # type: ignore[assignment]

        manager._poll_process()

        self.assertIsNone(manager.process)
        self.assertIn("exited with code 3", str(manager.last_error))

    def test_republish_retained_if_due_publishes_files_and_status(self):
        manager, _ = self._manager()
        manager.client = FakeMqttClient()
        manager._refresh_scripts(force=True)
        manager.client.publish_calls.clear()

        manager.next_retained_republish_at = 0.0
        manager._republish_retained_if_due()

        topics = [call[0] for call in manager.client.publish_calls]
        self.assertIn(manager.files_topic, topics)
        self.assertIn(manager.status_topic, topics)


if __name__ == "__main__":
    unittest.main()
