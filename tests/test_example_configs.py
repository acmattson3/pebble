from __future__ import annotations

import json
import unittest
from pathlib import Path

from control.common.capabilities import build_capabilities_value, capabilities_topic
from control.common.config import service_cfg
from control.common.topics import identity_from_config


REPO_ROOT = Path(__file__).resolve().parents[1]
EXAMPLE_CONFIGS = [
    REPO_ROOT / "control" / "configs" / "config.json.example",
    REPO_ROOT / "control" / "configs" / "fred.example.json",
    REPO_ROOT / "control" / "configs" / "goob.example.json",
    REPO_ROOT / "control" / "configs" / "husky.example.json",
]


class ExampleConfigTests(unittest.TestCase):
    def _load_config(self, path: Path) -> dict:
        data = json.loads(path.read_text(encoding="utf-8"))
        self.assertIsInstance(data, dict, path.as_posix())
        return data

    def _assert_command_script_exists(self, config_path: Path, command: list[str]) -> None:
        for token in command:
            if isinstance(token, str) and token.endswith(".py"):
                script_path = (REPO_ROOT / token).resolve()
                self.assertTrue(script_path.exists(), f"{config_path.name}: missing script {token}")
                return
        self.fail(f"{config_path.name}: no python script path found in command {command!r}")

    def test_example_configs_build_identity_and_capabilities(self):
        for path in EXAMPLE_CONFIGS:
            with self.subTest(config=path.name):
                config = self._load_config(path)
                identity = identity_from_config(config)
                launcher_cfg = service_cfg(config, "launcher")
                caps = build_capabilities_value(config)

                self.assertEqual(caps["identity"]["system"], identity.system)
                self.assertEqual(caps["identity"]["type"], identity.type)
                self.assertEqual(caps["identity"]["id"], identity.robot_id)
                self.assertTrue(capabilities_topic(identity, launcher_cfg).startswith(identity.base))
                self.assertTrue(capabilities_topic(identity, launcher_cfg).endswith("/outgoing/capabilities"))
                self.assertTrue(caps["video"]["topic"].startswith(identity.base))
                self.assertTrue(caps["audio"]["topic"].startswith(identity.base))
                self.assertTrue(caps["drive"]["topic"].startswith(identity.base))

    def test_example_configs_reference_existing_runtime_scripts(self):
        for path in EXAMPLE_CONFIGS:
            with self.subTest(config=path.name):
                config = self._load_config(path)
                av_cfg = service_cfg(config, "av_daemon")
                av_enabled = bool(av_cfg.get("enabled", False))
                video_cfg = av_cfg.get("video") if isinstance(av_cfg.get("video"), dict) else {}
                audio_cfg = av_cfg.get("audio") if isinstance(av_cfg.get("audio"), dict) else {}
                video_expected = av_enabled and bool(video_cfg.get("enabled", True))
                audio_expected = av_enabled and bool(audio_cfg.get("enabled", True))
                bridge_cfg = service_cfg(config, "mqtt_bridge")
                video_ctl = bridge_cfg.get("video_control") if isinstance(bridge_cfg.get("video_control"), dict) else {}
                audio_ctl = bridge_cfg.get("audio_control") if isinstance(bridge_cfg.get("audio_control"), dict) else {}

                if video_expected:
                    self._assert_command_script_exists(path, list(video_ctl.get("command") or []))
                if audio_expected:
                    self._assert_command_script_exists(path, list(audio_ctl.get("publisher_command") or []))
                    self._assert_command_script_exists(path, list(audio_ctl.get("receiver_command") or []))

    def test_example_configs_reference_existing_autonomy_entrypoints(self):
        for path in EXAMPLE_CONFIGS:
            with self.subTest(config=path.name):
                config = self._load_config(path)
                autonomy_cfg = service_cfg(config, "autonomy_manager")
                autonomy_root = (REPO_ROOT / str(autonomy_cfg.get("autonomy_root") or "autonomy")).resolve()
                scripts = autonomy_cfg.get("scripts") if isinstance(autonomy_cfg.get("scripts"), dict) else {}

                for script_name, script_cfg in scripts.items():
                    self.assertIsInstance(script_cfg, dict, f"{path.name}: script config for {script_name} must be an object")
                    entrypoint = str(script_cfg.get("entrypoint") or "run.py")
                    entry_path = autonomy_root / script_name / entrypoint
                    self.assertTrue(entry_path.exists(), f"{path.name}: missing autonomy entrypoint {entry_path.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    unittest.main()
