#!/usr/bin/env python3
"""Server-side MQTT broker logger.

Subscribes to one or more topics and writes every message to JSONL.
Designed for long-running data collection on the server side.
"""

from __future__ import annotations

import argparse
import base64
import json
import signal
import ssl
import sys
import time
from pathlib import Path
from typing import Any, Optional

import paho.mqtt.client as mqtt

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = REPO_ROOT / "control" / "configs" / "config.json"


def _normalize_tls_config(value: Any) -> dict[str, Any]:
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


def _resolve_tls_paths(tls_cfg: dict[str, Any], base_dir: Path) -> dict[str, Any]:
    def _resolve(path_value: Any) -> Optional[str]:
        if not path_value:
            return None
        path = Path(str(path_value).strip()).expanduser()
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


def _apply_tls(client: mqtt.Client, mqtt_cfg: dict[str, Any], base_dir: Path) -> None:
    tls_cfg = _normalize_tls_config(mqtt_cfg.get("tls"))
    if not tls_cfg.get("enabled"):
        return

    tls_cfg = _resolve_tls_paths(tls_cfg, base_dir)
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


def _load_config(path_value: str) -> tuple[dict[str, Any], Path]:
    cfg_path = Path(path_value).expanduser() if path_value else DEFAULT_CONFIG_PATH
    if not cfg_path.is_absolute():
        cfg_path = (Path.cwd() / cfg_path).resolve()
    if not cfg_path.exists():
        raise SystemExit(f"Config file not found: {cfg_path}")
    try:
        data = json.loads(cfg_path.read_text())
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Failed to parse config {cfg_path}: {exc}") from exc
    if not isinstance(data, dict):
        raise SystemExit("Config root must be JSON object")
    return data, cfg_path


def _broker_cfg(config: dict[str, Any]) -> dict[str, Any]:
    web_cfg = config.get("web_interface") if isinstance(config.get("web_interface"), dict) else {}
    web_mqtt = web_cfg.get("mqtt") if isinstance(web_cfg.get("mqtt"), dict) else {}
    if web_mqtt:
        return web_mqtt

    services = config.get("services") if isinstance(config.get("services"), dict) else {}
    bridge = services.get("mqtt_bridge") if isinstance(services.get("mqtt_bridge"), dict) else {}
    remote = bridge.get("remote_mqtt") if isinstance(bridge.get("remote_mqtt"), dict) else {}
    if remote:
        return remote

    local = config.get("local_mqtt") if isinstance(config.get("local_mqtt"), dict) else {}
    return local


def _default_log_path(log_dir_raw: str) -> Path:
    stamp = time.strftime("%Y%m%d_%H%M%S")
    log_dir = Path(log_dir_raw).expanduser()
    if not log_dir.is_absolute():
        log_dir = (REPO_ROOT / log_dir).resolve()
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir / f"mqtt_broker_log_{stamp}.jsonl"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Subscribe to MQTT topics and log all messages to JSONL.")
    parser.add_argument("--config", type=str, default=str(DEFAULT_CONFIG_PATH), help="Path to config JSON.")
    parser.add_argument("--host", type=str, default="", help="MQTT host override.")
    parser.add_argument("--port", type=int, default=0, help="MQTT port override.")
    parser.add_argument("--username", type=str, default="", help="MQTT username override.")
    parser.add_argument("--password", type=str, default="", help="MQTT password override.")
    parser.add_argument("--keepalive", type=int, default=60, help="MQTT keepalive.")
    parser.add_argument("--topic", action="append", default=[], help="Topic filter to subscribe (repeatable). Default: #")
    parser.add_argument("--qos", type=int, choices=[0, 1], default=0, help="Subscription QoS.")
    parser.add_argument("--log-file", type=str, default="", help="Output JSONL file path.")
    parser.add_argument("--log-dir", type=str, default="web-interface/logs", help="Output dir when --log-file is not set.")
    parser.add_argument("--flush-lines", type=int, default=25, help="Flush output every N lines.")
    parser.add_argument("--max-payload-bytes", type=int, default=8192, help="Max raw payload bytes retained per message.")
    parser.add_argument("--decode-utf8", action="store_true", help="Store UTF-8 decoded payload text when possible.")
    parser.add_argument("--parse-json", action="store_true", help="If payload is UTF-8 JSON, store parsed object.")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    config, cfg_path = _load_config(args.config)
    broker_cfg = _broker_cfg(config)

    host = args.host or str(broker_cfg.get("host") or "127.0.0.1")
    port = args.port or int(broker_cfg.get("port") or 1883)
    keepalive = int(args.keepalive or broker_cfg.get("keepalive") or 60)
    username = args.username or str(broker_cfg.get("username") or "")
    password = args.password or str(broker_cfg.get("password") or "")

    topics = list(args.topic) if args.topic else ["#"]
    log_path = Path(args.log_file).expanduser() if args.log_file else _default_log_path(args.log_dir)
    if not log_path.is_absolute():
        log_path = (REPO_ROOT / log_path).resolve()
    log_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"[mqtt-broker-logger] broker={host}:{port} topics={topics}", file=sys.stderr)
    print(f"[mqtt-broker-logger] writing {log_path}", file=sys.stderr)

    stop_requested = False
    line_count = 0
    max_payload_bytes = max(0, int(args.max_payload_bytes))
    flush_every = max(1, int(args.flush_lines))

    fh = log_path.open("a", encoding="utf-8")

    def _on_signal(_signum: int, _frame: Any) -> None:
        nonlocal stop_requested
        stop_requested = True

    signal.signal(signal.SIGINT, _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)

    def _on_connect(client: mqtt.Client, _userdata: Any, _flags: dict[str, Any], rc: int) -> None:
        if rc != 0:
            print(f"[mqtt-broker-logger] connect failed rc={rc}", file=sys.stderr)
            return
        for topic in topics:
            client.subscribe(topic, qos=int(args.qos))

    def _on_message(_client: mqtt.Client, _userdata: Any, msg: mqtt.MQTTMessage) -> None:
        nonlocal line_count
        payload = bytes(msg.payload or b"")
        payload_truncated = False
        if max_payload_bytes > 0 and len(payload) > max_payload_bytes:
            payload = payload[:max_payload_bytes]
            payload_truncated = True

        entry: dict[str, Any] = {
            "timestamp": time.time(),
            "topic": msg.topic,
            "qos": int(getattr(msg, "qos", 0)),
            "retain": bool(getattr(msg, "retain", False)),
            "payload_size_bytes": len(msg.payload or b""),
            "payload_truncated": payload_truncated,
            "payload_b64": base64.b64encode(payload).decode("ascii"),
        }

        if args.decode_utf8:
            try:
                text = payload.decode("utf-8")
                entry["payload_text"] = text
                if args.parse_json:
                    try:
                        parsed = json.loads(text)
                        entry["payload_json"] = parsed
                    except json.JSONDecodeError:
                        pass
            except UnicodeDecodeError:
                pass

        fh.write(json.dumps(entry, ensure_ascii=True) + "\n")
        line_count += 1
        if (line_count % flush_every) == 0:
            fh.flush()

    client = mqtt.Client(client_id=f"pebble-broker-logger-{int(time.time())}")
    client.on_connect = _on_connect
    client.on_message = _on_message

    if username:
        client.username_pw_set(username, password or None)
    _apply_tls(client, broker_cfg, cfg_path.parent)

    try:
        client.connect(host, port, keepalive)
    except Exception as exc:
        fh.close()
        print(f"[mqtt-broker-logger] connect failed: {exc}", file=sys.stderr)
        return 1

    client.loop_start()
    try:
        while not stop_requested:
            time.sleep(0.2)
    finally:
        client.loop_stop()
        try:
            client.disconnect()
        except Exception:
            pass
        fh.flush()
        fh.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
