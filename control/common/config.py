from __future__ import annotations

import json
from pathlib import Path
from typing import Any


DEFAULT_CONFIG_PATH = Path("control/configs/config.json")


def load_config(path_value: str | None) -> tuple[dict[str, Any], Path]:
    path = Path(path_value).expanduser() if path_value else DEFAULT_CONFIG_PATH
    if not path.is_absolute():
        path = path.resolve()
    if not path.exists():
        raise SystemExit(f"Configuration file not found: {path}")
    try:
        data = json.loads(path.read_text())
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Failed to parse config file {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise SystemExit(f"Configuration root must be a JSON object: {path}")
    return data, path


def service_cfg(config: dict, name: str) -> dict:
    services = config.get("services")
    if not isinstance(services, dict):
        return {}
    value = services.get(name)
    return value if isinstance(value, dict) else {}


def log_level(config: dict, service: dict) -> str:
    level = service.get("log_level") or config.get("log_level") or "INFO"
    return str(level).upper()


def default_retained_publish_interval_seconds(config: dict, default: float = 3600.0) -> float:
    services = config.get("services")
    if not isinstance(services, dict):
        return float(default)
    defaults_cfg = services.get("defaults")
    if not isinstance(defaults_cfg, dict):
        return float(default)
    raw = defaults_cfg.get("retained_publish_interval_seconds")
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return float(default)
    return value if value > 0 else float(default)
