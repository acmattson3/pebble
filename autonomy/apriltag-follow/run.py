#!/usr/bin/env python3
"""Autonomy-manager entrypoint for AprilTag follow."""

from __future__ import annotations

import argparse
import json
import runpy
import sys
from pathlib import Path
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent
TARGET_SCRIPT = SCRIPT_DIR / "april-follow-test.py"


def _load_defaults(config_path: Path) -> dict[str, dict[str, Any]]:
    if not config_path.exists():
        return {}
    try:
        raw = json.loads(config_path.read_text())
    except json.JSONDecodeError:
        return {}
    if not isinstance(raw, dict):
        return {}
    services = raw.get("services") if isinstance(raw.get("services"), dict) else {}
    autonomy_cfg = services.get("autonomy_manager") if isinstance(services.get("autonomy_manager"), dict) else {}
    scripts_cfg = autonomy_cfg.get("scripts") if isinstance(autonomy_cfg.get("scripts"), dict) else {}
    apriltag_cfg = scripts_cfg.get("apriltag-follow") if isinstance(scripts_cfg.get("apriltag-follow"), dict) else {}
    args_schema = apriltag_cfg.get("args") if isinstance(apriltag_cfg.get("args"), list) else []

    defaults: dict[str, dict[str, Any]] = {}
    for item in args_schema:
        if not isinstance(item, dict):
            continue
        arg = item.get("arg")
        if not isinstance(arg, str) or not arg.strip():
            continue
        defaults[arg.strip()] = item
    return defaults


def _append_default_args(argv: list[str], defaults_by_arg: dict[str, dict[str, Any]]) -> list[str]:
    provided_args = set()
    for index, value in enumerate(argv):
        if value in defaults_by_arg:
            provided_args.add(value)
            continue
        if value.startswith("--"):
            key = value.split("=", 1)[0]
            if key in defaults_by_arg:
                provided_args.add(key)
                if "=" in value:
                    continue
                # Consume inline value argument from the next token.
                if index + 1 < len(argv) and not argv[index + 1].startswith("--"):
                    continue

    out = list(argv)
    for arg, item in defaults_by_arg.items():
        if arg in provided_args:
            continue
        default = item.get("default")
        required = bool(item.get("required", False))
        field_type = str(item.get("type") or "string").strip().lower()
        mode = str(item.get("mode") or "").strip().lower()

        if default is None and not required:
            continue
        if field_type == "bool" and mode == "flag":
            if bool(default):
                out.append(arg)
            continue
        if default is None:
            continue

        out.extend([arg, str(default)])
    return out


def _run_target(target_argv: list[str]) -> int:
    old_argv = list(sys.argv)
    sys.argv = [str(TARGET_SCRIPT)] + target_argv
    try:
        runpy.run_path(str(TARGET_SCRIPT), run_name="__main__")
    except SystemExit as exc:
        code = exc.code
        if code is None:
            return 0
        if isinstance(code, int):
            return code
        return 1
    finally:
        sys.argv = old_argv
    return 0


def _parse_args() -> tuple[argparse.Namespace, list[str]]:
    parser = argparse.ArgumentParser(description="Run AprilTag follow with config-backed defaults.")
    parser.add_argument("--config", type=str, default="control/configs/config.json", help="Unified runtime config JSON.")
    return parser.parse_known_args()


def main() -> int:
    args, passthrough = _parse_args()
    cfg_path = Path(args.config).expanduser()
    if not cfg_path.is_absolute():
        cfg_path = (Path.cwd() / cfg_path).resolve()

    defaults_by_arg = _load_defaults(cfg_path)
    forwarded = _append_default_args(passthrough, defaults_by_arg)

    # Ensure downstream script sees the same unified runtime config path.
    if "--config" not in forwarded:
        forwarded = ["--config", str(cfg_path)] + forwarded

    return _run_target(forwarded)


if __name__ == "__main__":
    raise SystemExit(main())
