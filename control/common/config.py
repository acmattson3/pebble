from __future__ import annotations

import json
from copy import deepcopy
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


def merge_dicts(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = deepcopy(base)
    for key, value in override.items():
        existing = merged.get(key)
        if isinstance(existing, dict) and isinstance(value, dict):
            merged[key] = merge_dicts(existing, value)
        else:
            merged[key] = deepcopy(value)
    return merged


def service_instances_cfg(config: dict, name: str) -> dict[str, dict[str, Any]]:
    cfg = service_cfg(config, name)
    raw_instances = cfg.get("instances")
    if not isinstance(raw_instances, dict):
        return {}
    instances: dict[str, dict[str, Any]] = {}
    for instance_name, instance_cfg in raw_instances.items():
        if not isinstance(instance_name, str) or not isinstance(instance_cfg, dict):
            continue
        instances[instance_name] = instance_cfg
    return instances


def service_instance_cfg(config: dict, name: str, instance: str | None = None) -> dict[str, Any]:
    cfg = service_cfg(config, name)
    if instance is None:
        return cfg
    instances = service_instances_cfg(config, name)
    instance_cfg = instances.get(instance)
    if instance_cfg is None:
        return {}
    base_cfg = {key: deepcopy(value) for key, value in cfg.items() if key != "instances"}
    return merge_dicts(base_cfg, instance_cfg)


def enabled_service_instances(config: dict, name: str) -> list[tuple[str | None, dict[str, Any]]]:
    enabled: list[tuple[str | None, dict[str, Any]]] = []

    root_cfg = service_cfg(config, name)
    if bool(root_cfg.get("enabled", False)):
        enabled.append((None, root_cfg))

    for instance_name in service_instances_cfg(config, name):
        instance_cfg = service_instance_cfg(config, name, instance_name)
        if bool(instance_cfg.get("enabled", False)):
            enabled.append((instance_name, instance_cfg))

    return enabled


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
