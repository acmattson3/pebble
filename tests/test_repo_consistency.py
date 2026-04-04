from __future__ import annotations

import re
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]

DOCUMENTED_PATHS = {
    "README.md": [
        ("control/configs/config.json.example", "control/configs/config.json.example"),
        ("firmware/MINILAMP_seeed-xiao-c6/private_config.h.example", "firmware/MINILAMP_seeed-xiao-c6/private_config.h.example"),
        ("systemd/pebble-control.service.example", "systemd/pebble-control.service.example"),
        ("tests/on_robot_smoke.py", "tests/on_robot_smoke.py"),
        ("web-interface/web-control.py", "web-interface/web-control.py"),
    ],
    "control/README.md": [
        ("control/configs/config.json.example", "control/configs/config.json.example"),
        ("systemd/pebble-control.service.example", "systemd/pebble-control.service.example"),
        ("control/services/serial_standard.md", "control/services/serial_standard.md"),
    ],
    "firmware/README.md": [
        ("firmware/MINILAMP_seeed-xiao-c6/private_config.h.example", "firmware/MINILAMP_seeed-xiao-c6/private_config.h.example"),
        ("firmware/HS105_seeed-xiao-c3/private_config.h.example", "firmware/HS105_seeed-xiao-c3/private_config.h.example"),
        ("firmware/BUMPERBOT_wemos/private_config.h.example", "firmware/BUMPERBOT_wemos/private_config.h.example"),
        ("firmware/flash-goob-firmware.sh", "firmware/flash-goob-firmware.sh"),
        ("firmware/flash-hs105-firmware.sh", "firmware/flash-hs105-firmware.sh"),
        ("firmware/flash-fred-firmware.sh", "firmware/flash-fred-firmware.sh"),
        ("firmware/flash-minilamp-firmware.sh", "firmware/flash-minilamp-firmware.sh"),
        ("firmware/flash-bumperbot-wemos-firmware.sh", "firmware/flash-bumperbot-wemos-firmware.sh"),
    ],
    "web-interface/README.md": [
        ("requirements.txt", "web-interface/requirements.txt"),
        ("web-interface/tools/mqtt-broker-logger.py", "web-interface/tools/mqtt-broker-logger.py"),
        ("FUTURE_GOALS.md", "web-interface/FUTURE_GOALS.md"),
    ],
    "autonomy/apriltag-follow/README.md": [
        ("autonomy/apriltag-follow/requirements.txt", "autonomy/apriltag-follow/requirements.txt"),
        ("autonomy/apriltag-follow/camera_calibration.yaml", "autonomy/apriltag-follow/camera_calibration.yaml"),
    ],
}


class RepoConsistencyTests(unittest.TestCase):
    def test_requirements_files_exist(self):
        for rel_path in [
            "requirements.txt",
            "web-interface/requirements.txt",
            "autonomy/apriltag-follow/requirements.txt",
        ]:
            with self.subTest(path=rel_path):
                self.assertTrue((REPO_ROOT / rel_path).exists(), rel_path)

    def test_documented_paths_exist(self):
        for doc_path, referenced in DOCUMENTED_PATHS.items():
            doc_text = (REPO_ROOT / doc_path).read_text(encoding="utf-8")
            for display_path, target_path in referenced:
                with self.subTest(doc=doc_path, path=target_path):
                    self.assertIn(display_path, doc_text)
                    self.assertTrue((REPO_ROOT / target_path).exists(), target_path)

    def test_web_dockerfile_copy_sources_exist(self):
        dockerfile = (REPO_ROOT / "web-interface" / "Dockerfile").read_text(encoding="utf-8")
        copy_sources: list[str] = []
        for line in dockerfile.splitlines():
            stripped = line.strip()
            if not stripped.startswith("COPY "):
                continue
            parts = stripped.split()
            if len(parts) >= 3:
                copy_sources.append(parts[1])

        self.assertEqual(
            copy_sources,
            ["./web-interface/requirements.txt", "./web-interface", "./control/configs"],
        )
        for rel_path in copy_sources:
            with self.subTest(copy_source=rel_path):
                self.assertTrue((REPO_ROOT / rel_path[2:]).exists(), rel_path)

    def test_readmes_do_not_reference_removed_web_config_or_old_name(self):
        for rel_path in [
            "README.md",
            "control/README.md",
            "web-interface/README.md",
            "autonomy/apriltag-follow/README.md",
        ]:
            with self.subTest(path=rel_path):
                text = (REPO_ROOT / rel_path).read_text(encoding="utf-8")
                self.assertNotIn("control/configs/web.json", text)
                self.assertNotRegex(text, re.compile(r"\bPebbleBot\b|\bpebblebot\b"))

    def test_systemd_example_includes_login_session_runtime_env(self):
        text = (REPO_ROOT / "systemd" / "pebble-control.service.example").read_text(encoding="utf-8")
        self.assertIn("PAMName=login", text)
        self.assertIn("Environment=XDG_RUNTIME_DIR=/run/user/%U", text)
        self.assertIn("Environment=DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/%U/bus", text)


if __name__ == "__main__":
    unittest.main()
