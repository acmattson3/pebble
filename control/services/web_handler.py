#!/usr/bin/env python3
"""Launcher-managed wrapper for the Pebble web interface."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from control.common.config import load_config, service_cfg


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Launch Pebble web interface.")
    parser.add_argument("--config", help="Path to runtime config JSON.")
    return parser.parse_args()


def _web_config_path(service: dict, runtime_path: Path, repo_root: Path) -> Path:
    configured = service.get("web_config")
    if not isinstance(configured, str) or not configured.strip():
        return runtime_path
    path = Path(configured).expanduser()
    return path.resolve() if path.is_absolute() else (repo_root / path).resolve()


def main() -> None:
    args = _parse_args()
    _config, config_path = load_config(args.config)

    repo_root = Path(__file__).resolve().parents[2]
    web_control = repo_root / "web-interface" / "web-control.py"
    if not web_control.exists():
        raise SystemExit(f"Web interface entrypoint not found: {web_control}")

    service = service_cfg(_config, "web_handler")
    env = os.environ.copy()
    if service.get("web_config"):
        env["PEBBLE_CONFIG"] = str(_web_config_path(service, config_path, repo_root))
    else:
        env.setdefault("PEBBLE_CONFIG", str(config_path))
    env.setdefault("PYTHONUNBUFFERED", "1")

    service_env = service.get("env")
    if isinstance(service_env, dict):
        for key, value in service_env.items():
            if isinstance(key, str):
                env[key] = "" if value is None else str(value)

    os.chdir(repo_root)
    os.execvpe(sys.executable, [sys.executable, str(web_control)], env)


if __name__ == "__main__":
    main()
