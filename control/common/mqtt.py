from __future__ import annotations

import ssl
from pathlib import Path
from typing import Any

import paho.mqtt.client as mqtt


def create_client(*, client_id: str = "") -> mqtt.Client:
    # Force MQTT 3.1.1 so the client does not negotiate MQTT 5 flow-control
    # properties like receive-maximum with the broker.
    return mqtt.Client(client_id=client_id, protocol=mqtt.MQTTv311)


def normalize_tls_config(value: Any) -> dict[str, Any]:
    if isinstance(value, bool):
        return {"enabled": value}
    if isinstance(value, dict):
        return {
            "enabled": bool(value.get("enabled", False)),
            "ca_cert": value.get("ca_cert") or value.get("ca_certs"),
            "client_cert": value.get("client_cert"),
            "client_key": value.get("client_key"),
            "insecure": bool(value.get("insecure", False)),
            "ciphers": value.get("ciphers"),
        }
    return {"enabled": False}


def resolve_tls_paths(tls_cfg: dict[str, Any], base_dir: Path) -> dict[str, Any]:
    def _resolve(path_value: Any) -> str | None:
        if not path_value:
            return None
        raw = str(path_value).strip()
        if not raw:
            return None
        path = Path(raw)
        if not path.is_absolute():
            path = (base_dir / path).resolve()
        if not path.exists():
            raise SystemExit(f"TLS file not found: {path}")
        return str(path)

    if not tls_cfg.get("enabled"):
        return tls_cfg
    resolved = dict(tls_cfg)
    resolved["ca_cert"] = _resolve(tls_cfg.get("ca_cert"))
    resolved["client_cert"] = _resolve(tls_cfg.get("client_cert"))
    resolved["client_key"] = _resolve(tls_cfg.get("client_key"))
    return resolved


def apply_tls(client: mqtt.Client, tls_cfg: dict[str, Any], base_dir: Path) -> None:
    if not tls_cfg.get("enabled"):
        return
    tls_cfg = resolve_tls_paths(tls_cfg, base_dir)
    client.tls_set(
        ca_certs=tls_cfg.get("ca_cert"),
        certfile=tls_cfg.get("client_cert"),
        keyfile=tls_cfg.get("client_key"),
        cert_reqs=ssl.CERT_REQUIRED,
        tls_version=ssl.PROTOCOL_TLS_CLIENT,
        ciphers=tls_cfg.get("ciphers") or None,
    )
    if tls_cfg.get("insecure"):
        client.tls_insecure_set(True)


def parse_bool_payload(payload: Any) -> bool | None:
    if isinstance(payload, bool):
        return payload
    if isinstance(payload, dict):
        value = payload.get("value")
        if isinstance(value, bool):
            return value
        enabled = payload.get("enabled")
        if isinstance(enabled, bool):
            return enabled
    return None


def mqtt_auth_and_tls(client: mqtt.Client, mqtt_cfg: dict[str, Any], base_dir: Path) -> None:
    username = mqtt_cfg.get("username")
    password = mqtt_cfg.get("password")
    if username:
        client.username_pw_set(str(username), str(password) if password is not None else None)
    apply_tls(client, normalize_tls_config(mqtt_cfg.get("tls")), base_dir)
