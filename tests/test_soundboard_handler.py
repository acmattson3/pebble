from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from control.services.soundboard_handler import SoundboardHandler
from tests.helpers import FakeMqttClient, FakePopenFactory, make_base_config


class SoundboardHandlerTests(unittest.TestCase):
    def _handler(self, sounds_dir: Path) -> tuple[SoundboardHandler, Path]:
        config = make_base_config("soundbot")
        config["services"]["soundboard_handler"]["playback"]["directory"] = str(sounds_dir)
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.json"
            cfg_path.write_text(json.dumps(config))
            handler = SoundboardHandler(config, cfg_path)
            return handler, cfg_path

    def test_parse_action_variants(self):
        self.assertEqual(SoundboardHandler._parse_action(True), ("play", None))
        self.assertEqual(SoundboardHandler._parse_action({"action": "stop"}), ("stop", None))
        self.assertEqual(SoundboardHandler._parse_action({"enabled": True, "file": "x.wav"}), ("play", "x.wav"))
        self.assertEqual(SoundboardHandler._parse_action({"value": {"action": "play", "file": "x.wav"}}), ("play", "x.wav"))

    def test_resolve_file_security(self):
        with tempfile.TemporaryDirectory() as td:
            sounds = Path(td) / "sounds"
            sounds.mkdir()
            (sounds / "ok.wav").write_bytes(b"wav")
            outside = Path(td) / "outside.wav"
            outside.write_bytes(b"wav")

            handler, _ = self._handler(sounds)
            self.assertIsNotNone(handler._resolve_file("ok.wav"))
            self.assertIsNone(handler._resolve_file("../outside.wav"))
            self.assertIsNone(handler._resolve_file(str(outside)))

    def test_refresh_files_publishes_retained_payload(self):
        with tempfile.TemporaryDirectory() as td:
            sounds = Path(td) / "sounds"
            sounds.mkdir()
            (sounds / "a.wav").write_bytes(b"wav")
            handler, _ = self._handler(sounds)
            handler.client = FakeMqttClient()
            handler._refresh_files(force=True)
            self.assertEqual(len(handler.client.publish_calls), 1)
            topic, payload, qos, retain = handler.client.publish_calls[0]
            self.assertEqual(topic, handler.files_topic)
            self.assertTrue(retain)
            self.assertIn("a.wav", payload)

    def test_play_and_stop_commands_manage_process(self):
        with tempfile.TemporaryDirectory() as td:
            sounds = Path(td) / "sounds"
            sounds.mkdir()
            (sounds / "beep.wav").write_bytes(b"wav")
            handler, _ = self._handler(sounds)
            handler.client = FakeMqttClient()
            popen_factory = FakePopenFactory()

            with mock.patch("control.services.soundboard_handler.subprocess.Popen", new=popen_factory):
                handler._handle_command({"action": "play", "file": "beep.wav"})
                self.assertIsNotNone(handler.process)
                self.assertEqual(len(popen_factory.calls), 1)
                self.assertEqual(handler.current_file, "beep.wav")

                proc = handler.process
                handler._handle_command({"action": "stop"})
                self.assertIsNone(handler.process)
                self.assertTrue(proc.terminate_called)

    def test_republish_retained_if_due_publishes_files_and_status(self):
        with tempfile.TemporaryDirectory() as td:
            sounds = Path(td) / "sounds"
            sounds.mkdir()
            (sounds / "a.wav").write_bytes(b"wav")
            handler, _ = self._handler(sounds)
            handler.client = FakeMqttClient()
            handler.next_retained_republish_at = 0.0
            handler._republish_retained_if_due()
            self.assertGreaterEqual(len(handler.client.publish_calls), 2)
            topics = [call[0] for call in handler.client.publish_calls]
            self.assertIn(handler.files_topic, topics)
            self.assertIn(handler.status_topic, topics)


if __name__ == "__main__":
    unittest.main()
