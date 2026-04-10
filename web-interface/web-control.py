#!/usr/bin/env python3
"""
Web control UI for Pebble systems that interacts solely through MQTT topics.

This application can run on any system with network access to the MQTT broker. The
HTML UI mirrors the earlier serial-based version but publishes drive and light
commands to the standard incoming topics while displaying telemetry received from
the outgoing topics.
"""
import base64
import datetime
import json
import logging
import math
import os
import ssl
import struct
import sys
import threading
import time
import zlib
from collections import deque
from pathlib import Path
from typing import Any, Deque, Dict, Optional

from flask import Flask, Response, abort, jsonify, request, stream_with_context
import paho.mqtt.client as mqtt

try:
    import cv2  # type: ignore
    import numpy as np  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    cv2 = None
    np = None

try:
    from pymongo import ASCENDING, MongoClient
except ImportError:  # pragma: no cover - optional dependency
    ASCENDING = None  # type: ignore[assignment]
    MongoClient = None


REPO_ROOT = Path(__file__).resolve().parents[1]
REPLAY_DIR = Path(__file__).resolve().parent / "replay"
if str(REPLAY_DIR) not in sys.path:
    sys.path.insert(0, str(REPLAY_DIR))
try:
    from replay_support import ReplayArchive
except ImportError:  # pragma: no cover - optional dependency
    ReplayArchive = None  # type: ignore[assignment]
_CONFIG_OVERRIDE = os.environ.get("PEBBLE_CONFIG", "").strip()
if _CONFIG_OVERRIDE:
    CONFIG_PATH = Path(_CONFIG_OVERRIDE).expanduser()
    if not CONFIG_PATH.is_absolute():
        CONFIG_PATH = (Path.cwd() / CONFIG_PATH).resolve()
else:
    CONFIG_PATH = REPO_ROOT / "control" / "configs" / "config.json"
AUDIO_MAGIC = b"PBAT"
AUDIO_CODEC_PCM_S16LE = 1
AUDIO_PACKET_HEADER = struct.Struct("!4sBBBBHHIQ")


def _coerce_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _coerce_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return None


def _normalize_mqtt_history_config(value: Any) -> Dict[str, Any]:
    cfg = value if isinstance(value, dict) else {}
    max_bytes_default = 500 * 1024 * 1024 * 1024
    return {
        "enabled": bool(cfg.get("enabled", False)),
        "uri": str(cfg.get("uri") or "mongodb://127.0.0.1:27017"),
        "database": str(cfg.get("database") or "pebble"),
        "collection": str(cfg.get("collection") or "mqtt_history"),
        "max_bytes": max(1_000_000, _coerce_int(cfg.get("max_bytes"), max_bytes_default)),
        "subscribe_all_topics": bool(cfg.get("subscribe_all_topics", True)),
        "prune_check_every_messages": max(100, _coerce_int(cfg.get("prune_check_every_messages"), 2000)),
        "prune_chunk_documents": max(1000, _coerce_int(cfg.get("prune_chunk_documents"), 50000)),
        "prune_target_ratio": min(0.99, max(0.5, float(_coerce_float(cfg.get("prune_target_ratio")) or 0.9))),
    }


def _normalize_tls_config(value: Any) -> Dict[str, Any]:
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


def _resolve_tls_paths(tls_cfg: Dict[str, Any], base_dir: Path) -> Dict[str, Any]:
    def _resolve(path_value: Any) -> Optional[str]:
        if not path_value:
            return None
        raw = str(path_value).strip()
        if not raw:
            return None
        path = Path(raw)
        if not path.is_absolute():
            path = (base_dir / path).resolve()
        if not path.exists():
            raise ValueError(f"TLS file not found: {path}")
        return str(path)

    if not tls_cfg.get("enabled"):
        return tls_cfg
    resolved = dict(tls_cfg)
    resolved["ca_cert"] = _resolve(tls_cfg.get("ca_cert"))
    resolved["client_cert"] = _resolve(tls_cfg.get("client_cert"))
    resolved["client_key"] = _resolve(tls_cfg.get("client_key"))
    return resolved


def _configure_tls(client: mqtt.Client, tls_cfg: Dict[str, Any]) -> None:
    if not tls_cfg.get("enabled"):
        return
    ca_cert = tls_cfg.get("ca_cert")
    client_cert = tls_cfg.get("client_cert")
    client_key = tls_cfg.get("client_key")
    ciphers = tls_cfg.get("ciphers")
    client.tls_set(
        ca_certs=ca_cert,
        certfile=client_cert,
        keyfile=client_key,
        cert_reqs=ssl.CERT_REQUIRED,
        tls_version=ssl.PROTOCOL_TLS_CLIENT,
        ciphers=ciphers or None,
    )
    if tls_cfg.get("insecure"):
        client.tls_insecure_set(True)


def _create_mqtt_client(*, client_id: str = "") -> mqtt.Client:
    # Use MQTT 3.1.1 for the web UI to avoid MQTT 5 receive-maximum flow control.
    return mqtt.Client(client_id=client_id, protocol=mqtt.MQTTv311)


def _load_config() -> Dict[str, Any]:
    if not CONFIG_PATH.exists():
        raise SystemExit(f"Missing configuration file: {CONFIG_PATH}")
    try:
        raw = json.loads(CONFIG_PATH.read_text())
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Failed to parse configuration file {CONFIG_PATH}: {exc}") from exc
    if not isinstance(raw, dict):
        raise SystemExit(f"Configuration root must be a JSON object: {CONFIG_PATH}")
    return _normalize_config(raw)


def _topic_from_identity(system: str, component_type: str, component_id: str, direction: str, metric: str) -> str:
    return f"{system}/{component_type}/{component_id}/{direction}/{metric}"


def _component_key(system: str, component_type: str, component_id: str) -> str:
    return f"{system}:{component_type}:{component_id}"


def _identity_hint_from_topic(topic: Any) -> Optional[Dict[str, str]]:
    if not isinstance(topic, str):
        return None
    parts = [part for part in topic.strip().split("/") if part]
    if len(parts) < 3:
        return None
    return {
        "system": parts[0],
        "type": parts[1],
        "id": parts[2],
    }


def _normalize_web_robot(
    robot: Dict[str, Any],
    *,
    system: str,
    component_type: str,
    default_video_topic: str,
    default_audio_topic: str,
    default_audio_uplink_topic: str,
) -> Optional[Dict[str, Any]]:
    robot_id = str(robot.get("id") or "").strip()
    if not robot_id:
        return None
    video_cfg = robot.get("video") if isinstance(robot.get("video"), dict) else {}
    audio_cfg = robot.get("audio") if isinstance(robot.get("audio"), dict) else {}
    autonomy_cfg = robot.get("autonomy") if isinstance(robot.get("autonomy"), dict) else {}
    reboot_cfg = robot.get("reboot") if isinstance(robot.get("reboot"), dict) else {}
    service_restart_cfg = robot.get("service_restart") if isinstance(robot.get("service_restart"), dict) else {}
    git_pull_cfg = robot.get("git_pull") if isinstance(robot.get("git_pull"), dict) else {}

    identity_hint = (
        _identity_hint_from_topic(video_cfg.get("topic"))
        or _identity_hint_from_topic(audio_cfg.get("topic"))
        or _identity_hint_from_topic(audio_cfg.get("uplink_topic"))
    )
    robot_system = str(robot.get("system") or (identity_hint or {}).get("system") or system)
    robot_type = str(robot.get("type") or (identity_hint or {}).get("type") or component_type)
    robot_key = _component_key(robot_system, robot_type, robot_id)
    base_video_topic = _topic_from_identity(robot_system, robot_type, robot_id, "outgoing", "front-camera")
    base_audio_topic = _topic_from_identity(robot_system, robot_type, robot_id, "outgoing", "audio")
    base_audio_uplink = _topic_from_identity(robot_system, robot_type, robot_id, "incoming", "audio-stream")

    normalized = {
        "key": robot_key,
        "id": robot_id,
        "system": robot_system,
        "name": str(robot.get("name") or robot_id),
        "type": robot_type,
        "model": robot.get("model"),
        "video": {
            "topic": str(video_cfg.get("topic") or default_video_topic or base_video_topic),
            "controls": bool(video_cfg.get("controls", True)),
        },
        "audio": {
            "topic": str(audio_cfg.get("topic") or default_audio_topic or base_audio_topic),
            "uplink_topic": str(audio_cfg.get("uplink_topic") or default_audio_uplink_topic or base_audio_uplink),
            "rate": _coerce_int(audio_cfg.get("rate"), 16000),
            "channels": _coerce_int(audio_cfg.get("channels"), 1),
            "chunk_ms": _coerce_int(audio_cfg.get("chunk_ms"), 20),
            "concealment_ms": _coerce_int(audio_cfg.get("concealment_ms"), 100),
            "controls": bool(audio_cfg.get("controls", True)),
        },
        "autonomy": {
            "controls": bool(autonomy_cfg.get("controls", True)),
        },
        "reboot": {
            "controls": bool(reboot_cfg.get("controls", True)),
        },
        "service_restart": {
            "controls": bool(service_restart_cfg.get("controls", True)),
        },
        "git_pull": {
            "controls": bool(git_pull_cfg.get("controls", True)),
        },
    }
    return normalized


def _normalize_config(raw: Dict[str, Any]) -> Dict[str, Any]:
    # Already in web-interface-specific schema.
    if isinstance(raw.get("mqtt"), dict) and isinstance(raw.get("robots"), list):
        robots_cfg = raw.get("robots") if isinstance(raw.get("robots"), list) else []
        default_system = str(raw.get("system") or "pebble")
        default_component_type = str(raw.get("component_type") or "robots")
        normalized_robots = []
        for robot in robots_cfg:
            if not isinstance(robot, dict):
                continue
            normalized_robot = _normalize_web_robot(
                robot,
                system=default_system,
                component_type=default_component_type,
                default_video_topic="",
                default_audio_topic="",
                default_audio_uplink_topic="",
            )
            if normalized_robot:
                normalized_robots.append(normalized_robot)
        normalized = dict(raw)
        normalized["robots"] = normalized_robots
        normalized["mqtt_history"] = _normalize_mqtt_history_config(raw.get("mqtt_history"))
        normalized["log_level"] = str(raw.get("log_level") or "INFO")
        return normalized

    robot_cfg = raw.get("robot") if isinstance(raw.get("robot"), dict) else {}
    local_mqtt_cfg = raw.get("local_mqtt") if isinstance(raw.get("local_mqtt"), dict) else {}
    services_cfg = raw.get("services") if isinstance(raw.get("services"), dict) else {}
    web_cfg = raw.get("web_interface") if isinstance(raw.get("web_interface"), dict) else {}

    system = str(robot_cfg.get("system") or "pebble")
    component_type = str(robot_cfg.get("type") or "robots")
    component_id = str(robot_cfg.get("id") or "").strip()

    bridge_cfg = services_cfg.get("mqtt_bridge") if isinstance(services_cfg.get("mqtt_bridge"), dict) else {}
    media_cfg = bridge_cfg.get("media") if isinstance(bridge_cfg.get("media"), dict) else {}
    video_pub_cfg = media_cfg.get("video_publisher") if isinstance(media_cfg.get("video_publisher"), dict) else {}
    audio_pub_cfg = media_cfg.get("audio_publisher") if isinstance(media_cfg.get("audio_publisher"), dict) else {}
    audio_rx_cfg = media_cfg.get("audio_receiver") if isinstance(media_cfg.get("audio_receiver"), dict) else {}

    default_video_topic = str(
        video_pub_cfg.get("topic")
        or (_topic_from_identity(system, component_type, component_id, "outgoing", "front-camera") if component_id else "")
    )
    default_audio_topic = str(
        audio_pub_cfg.get("topic")
        or (_topic_from_identity(system, component_type, component_id, "outgoing", "audio") if component_id else "")
    )
    default_audio_uplink_topic = str(
        audio_rx_cfg.get("topic")
        or (_topic_from_identity(system, component_type, component_id, "incoming", "audio-stream") if component_id else "")
    )

    robots_cfg = web_cfg.get("robots") if isinstance(web_cfg.get("robots"), list) else []
    if not robots_cfg and component_id:
        robots_cfg = [
            {
                "id": component_id,
                "name": component_id,
                "type": component_type,
                "video": {
                    "topic": default_video_topic,
                    "controls": True,
                },
                "audio": {
                    "topic": default_audio_topic,
                    "uplink_topic": default_audio_uplink_topic,
                    "rate": _coerce_int(audio_pub_cfg.get("rate"), 16000),
                    "channels": _coerce_int(audio_pub_cfg.get("channels"), 1),
                    "chunk_ms": _coerce_int(audio_pub_cfg.get("chunk_ms"), 20),
                    "concealment_ms": _coerce_int(audio_rx_cfg.get("concealment_ms"), 100),
                    "controls": True,
                },
                "autonomy": {
                    "controls": True,
                },
                "reboot": {
                    "controls": True,
                },
                "service_restart": {
                    "controls": True,
                },
                "git_pull": {
                    "controls": True,
                },
            }
        ]

    normalized_robots = []
    for robot in robots_cfg:
        if not isinstance(robot, dict):
            continue
        normalized = _normalize_web_robot(
            robot,
            system=system,
            component_type=component_type,
            default_video_topic=default_video_topic,
            default_audio_topic=default_audio_topic,
            default_audio_uplink_topic=default_audio_uplink_topic,
        )
        if normalized:
            normalized_robots.append(normalized)

    mqtt_override = web_cfg.get("mqtt") if isinstance(web_cfg.get("mqtt"), dict) else {}
    mqtt_cfg = {
        "host": str(mqtt_override.get("host") or local_mqtt_cfg.get("host") or "127.0.0.1"),
        "port": _coerce_int(mqtt_override.get("port"), _coerce_int(local_mqtt_cfg.get("port"), 1883)),
        "keepalive": _coerce_int(mqtt_override.get("keepalive"), _coerce_int(local_mqtt_cfg.get("keepalive"), 60)),
        "username": str(mqtt_override.get("username") or local_mqtt_cfg.get("username") or ""),
        "password": str(mqtt_override.get("password") or local_mqtt_cfg.get("password") or ""),
        "tls": mqtt_override.get("tls") if isinstance(mqtt_override.get("tls"), dict) else local_mqtt_cfg.get("tls"),
    }

    return {
        "mqtt": mqtt_cfg,
        "host": str(web_cfg.get("host") or "0.0.0.0"),
        "port": _coerce_int(web_cfg.get("port"), 8080),
        "robot_discovery": web_cfg.get("robot_discovery")
        if isinstance(web_cfg.get("robot_discovery"), dict)
        else {"seed_configured": bool(normalized_robots)},
        "robots": normalized_robots,
        "log_level": str(web_cfg.get("log_level") or raw.get("log_level") or "INFO"),
        "mqtt_history": _normalize_mqtt_history_config(web_cfg.get("mqtt_history")),
    }


CONFIG = _load_config()
MQTT_CFG = CONFIG.get("mqtt", {})
WEB_HOST = str(CONFIG.get("host") or "0.0.0.0")
WEB_PORT = _coerce_int(CONFIG.get("port"), 8080)
MQTT_HISTORY_CFG = CONFIG.get("mqtt_history", {}) if isinstance(CONFIG.get("mqtt_history"), dict) else {}
REPLAY_ARCHIVE = ReplayArchive(MQTT_HISTORY_CFG) if ReplayArchive is not None else None
ROBOT_DISCOVERY = CONFIG.get("robot_discovery", {}) if isinstance(CONFIG.get("robot_discovery"), dict) else {}
# Configured robots are discovery hints only; the UI list comes from live MQTT topics.
SEED_CONFIGURED_ROBOTS = False
CONFIG_ROBOTS = CONFIG.get("robots", []) or []
CONFIG_ROBOT_MAP = {robot["key"]: robot for robot in CONFIG_ROBOTS if robot.get("key")}
CONFIG_ROBOT_KEYS_BY_ID: Dict[str, list[str]] = {}

CONFIG_VIDEO_HINTS: Dict[str, Dict[str, Any]] = {}
CONFIG_VIDEO_TOPICS: Dict[str, str] = {}
CONFIG_AUDIO_HINTS: Dict[str, Dict[str, Any]] = {}
CONFIG_AUDIO_TOPICS: Dict[str, str] = {}
CONFIG_AUDIO_UPLINK_HINTS: Dict[str, Dict[str, Any]] = {}
CONFIG_AUDIO_UPLINK_TOPICS: Dict[str, str] = {}
CONFIG_AUTONOMY_HINTS: Dict[str, Dict[str, Any]] = {}
CONFIG_REBOOT_HINTS: Dict[str, Dict[str, Any]] = {}
CONFIG_SERVICE_RESTART_HINTS: Dict[str, Dict[str, Any]] = {}
CONFIG_GIT_PULL_HINTS: Dict[str, Dict[str, Any]] = {}
for robot in CONFIG_ROBOTS:
    robot_key = str(robot.get("key") or "").strip()
    robot_id = robot.get("id")
    if not robot_id or not robot_key:
        continue
    CONFIG_ROBOT_KEYS_BY_ID.setdefault(str(robot_id), []).append(robot_key)
    video_cfg = robot.get("video")
    if isinstance(video_cfg, dict):
        if video_cfg.get("topic"):
            CONFIG_VIDEO_TOPICS[video_cfg["topic"]] = robot_key
        CONFIG_VIDEO_HINTS[robot_key] = {
            "controls": bool(video_cfg.get("controls", True)),
        }
    audio_cfg = robot.get("audio")
    if isinstance(audio_cfg, dict):
        if audio_cfg.get("topic"):
            CONFIG_AUDIO_TOPICS[audio_cfg["topic"]] = robot_key
        CONFIG_AUDIO_HINTS[robot_key] = {
            "rate": _coerce_int(audio_cfg.get("rate"), 16000),
            "channels": _coerce_int(audio_cfg.get("channels"), 1),
            "chunk_ms": _coerce_int(audio_cfg.get("chunk_ms"), 20),
            "concealment_ms": _coerce_int(audio_cfg.get("concealment_ms"), 100),
            "controls": bool(audio_cfg.get("controls", True)),
        }
        uplink_topic = audio_cfg.get("uplink_topic")
        if uplink_topic:
            CONFIG_AUDIO_UPLINK_TOPICS[robot_key] = str(uplink_topic)
        CONFIG_AUDIO_UPLINK_HINTS[robot_key] = {
            "topic": uplink_topic,
            "rate": _coerce_int(audio_cfg.get("uplink_rate"), _coerce_int(audio_cfg.get("rate"), 16000)),
            "channels": _coerce_int(audio_cfg.get("uplink_channels"), _coerce_int(audio_cfg.get("channels"), 1)),
            "chunk_ms": _coerce_int(audio_cfg.get("uplink_chunk_ms"), _coerce_int(audio_cfg.get("chunk_ms"), 20)),
            "qos": _coerce_int(audio_cfg.get("uplink_qos"), _coerce_int(audio_cfg.get("qos"), 0)),
        }
    autonomy_cfg = robot.get("autonomy")
    if isinstance(autonomy_cfg, dict):
        CONFIG_AUTONOMY_HINTS[robot_key] = {
            "controls": bool(autonomy_cfg.get("controls", True)),
        }
    reboot_cfg = robot.get("reboot")
    if isinstance(reboot_cfg, dict):
        CONFIG_REBOOT_HINTS[robot_key] = {
            "controls": bool(reboot_cfg.get("controls", True)),
        }
    service_restart_cfg = robot.get("service_restart")
    if isinstance(service_restart_cfg, dict):
        CONFIG_SERVICE_RESTART_HINTS[robot_key] = {
            "controls": bool(service_restart_cfg.get("controls", True)),
        }
    git_pull_cfg = robot.get("git_pull")
    if isinstance(git_pull_cfg, dict):
        CONFIG_GIT_PULL_HINTS[robot_key] = {
            "controls": bool(git_pull_cfg.get("controls", True)),
        }

LOG_LEVEL = CONFIG.get("log_level", "INFO")
VIDEO_SUPPORT = cv2 is not None and np is not None

robots_lock = threading.Lock()
discovered_robots: Dict[str, Dict[str, Any]] = {}
infrastructure_last_update: Optional[float] = None


_DEFAULT_TLS = _normalize_tls_config(MQTT_CFG.get("tls"))
MQTT_DEFAULTS = {
    "host": str(MQTT_CFG.get("host") or ""),
    "port": _coerce_int(MQTT_CFG.get("port"), 1883),
    "username": str(MQTT_CFG.get("username") or ""),
    "password": str(MQTT_CFG.get("password") or ""),
    "keepalive": _coerce_int(MQTT_CFG.get("keepalive"), 60),
    "tls": {
        "enabled": bool(_DEFAULT_TLS.get("enabled", False)),
        "ca_cert": _DEFAULT_TLS.get("ca_cert") or "",
        "client_cert": _DEFAULT_TLS.get("client_cert") or "",
        "client_key": _DEFAULT_TLS.get("client_key") or "",
        "insecure": bool(_DEFAULT_TLS.get("insecure", False)),
        "ciphers": _DEFAULT_TLS.get("ciphers") or "",
    },
}

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logging.getLogger("werkzeug").setLevel(logging.ERROR)

app = Flask(__name__)
if hasattr(app, "logger"):
    app.logger.disabled = True

telemetry_cache: Dict[str, Dict[str, Any]] = {}
telemetry_lock = threading.Lock()
video_cache: Dict[str, Dict[str, Any]] = {}
video_last_frames: Dict[str, Optional[Any]] = {}
video_cache_lock = threading.Lock()
video_conditions: Dict[str, threading.Condition] = {}
audio_cache: Dict[str, Dict[str, Any]] = {}
audio_buffers: Dict[str, Deque[Dict[str, Any]]] = {}
audio_conditions: Dict[str, threading.Condition] = {}
audio_cache_lock = threading.Lock()
audio_uplink_state: Dict[str, Dict[str, Any]] = {}
audio_uplink_lock = threading.Lock()
AUDIO_STREAM_BUFFER_PACKETS = 40
soundboard_cache_lock = threading.Lock()
soundboard_files_cache: Dict[str, Dict[str, Any]] = {}
soundboard_status_cache: Dict[str, Dict[str, Any]] = {}
outlets_cache_lock = threading.Lock()
outlets_status_cache: Dict[str, Dict[str, Dict[str, Any]]] = {}
autonomy_cache_lock = threading.Lock()
autonomy_files_cache: Dict[str, Dict[str, Any]] = {}
autonomy_status_cache: Dict[str, Dict[str, Any]] = {}
component_logs_cache_lock = threading.Lock()
component_logs_cache: Dict[str, Deque[Dict[str, Any]]] = {}
COMPONENT_LOGS_MAX_ENTRIES = 500
video_overlays_cache_lock = threading.Lock()
video_overlays_cache: Dict[str, Dict[str, Any]] = {}

mqtt_client: Optional[mqtt.Client] = None
mqtt_connected = threading.Event()
mqtt_state_lock = threading.Lock()
mqtt_connecting = False
mqtt_last_error: Optional[str] = None
mqtt_current_settings: Dict[str, Any] = {}
mqtt_history_collection: Optional[Any] = None
mqtt_history_client: Optional[Any] = None
mqtt_history_enabled = bool(MQTT_HISTORY_CFG.get("enabled", False))
mqtt_history_is_capped = False
mqtt_history_prune_counter = 0
mqtt_history_next_retry_at = 0.0
mqtt_history_lock = threading.Lock()

HEARTBEAT_LATENCY_SAMPLES = 10
HEARTBEAT_INTERVAL_SAMPLES = 10
HEARTBEAT_OFFLINE_MULTIPLIER = 4.0
HEARTBEAT_MAX_LATENCY_SECONDS = 10.0
HEARTBEAT_TOPIC_FRESH_SECONDS = 2.0
HEARTBEAT_MIN_INTERVAL_SAMPLE_SECONDS = 0.05


def _init_mqtt_history_logger() -> None:
    global mqtt_history_collection, mqtt_history_client, mqtt_history_enabled, mqtt_history_is_capped, mqtt_history_next_retry_at
    if not mqtt_history_enabled:
        return
    if MongoClient is None:
        logging.error("MQTT history logging enabled but pymongo is not installed.")
        mqtt_history_enabled = False
        return

    uri = str(MQTT_HISTORY_CFG.get("uri") or "mongodb://127.0.0.1:27017").strip()
    database_name = str(MQTT_HISTORY_CFG.get("database") or "pebble").strip() or "pebble"
    collection_name = str(MQTT_HISTORY_CFG.get("collection") or "mqtt_history").strip() or "mqtt_history"
    max_bytes = max(1_000_000, _coerce_int(MQTT_HISTORY_CFG.get("max_bytes"), 500 * 1024 * 1024 * 1024))

    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        db = client[database_name]

        collections = db.command("listCollections", filter={"name": collection_name}).get("cursor", {}).get("firstBatch", [])
        options: Dict[str, Any] = collections[0].get("options", {}) if collections else {}
        if not collections:
            db.create_collection(collection_name, capped=True, size=max_bytes)
            options = {"capped": True, "size": max_bytes}

        collection = db[collection_name]
        is_capped = bool(options.get("capped", False))
        configured_size = int(max_bytes)
        current_size = _coerce_int(options.get("size"), configured_size)
        if is_capped:
            if current_size != configured_size:
                logging.info(
                    "MQTT history collection '%s' is capped at %s bytes (config requests %s bytes).",
                    collection_name,
                    current_size,
                    configured_size,
                )
            logging.info(
                "MQTT history logging enabled: %s/%s (capped collection).",
                database_name,
                collection_name,
            )
        else:
            # Keep existing data and enforce cap with batched pruning of oldest documents.
            logging.warning(
                "MQTT history collection '%s' is not capped; using chunked prune fallback.",
                collection_name,
            )
            collection.create_index([("ts", ASCENDING)])

        mqtt_history_client = client
        mqtt_history_collection = collection
        mqtt_history_is_capped = is_capped
        mqtt_history_next_retry_at = 0.0
    except Exception as exc:
        logging.error("Failed to initialize MQTT history logging: %s", exc)
        mqtt_history_client = None
        mqtt_history_collection = None
        mqtt_history_is_capped = False


def _prune_mqtt_history_if_needed(force: bool = False) -> None:
    global mqtt_history_prune_counter
    if not mqtt_history_enabled or mqtt_history_is_capped:
        return
    collection = mqtt_history_collection
    if collection is None:
        return

    prune_every = max(100, _coerce_int(MQTT_HISTORY_CFG.get("prune_check_every_messages"), 2000))
    with mqtt_history_lock:
        mqtt_history_prune_counter += 1
        if not force and mqtt_history_prune_counter < prune_every:
            return
        mqtt_history_prune_counter = 0

    max_bytes = max(1_000_000, _coerce_int(MQTT_HISTORY_CFG.get("max_bytes"), 500 * 1024 * 1024 * 1024))
    target_ratio = min(0.99, max(0.5, float(_coerce_float(MQTT_HISTORY_CFG.get("prune_target_ratio")) or 0.9)))
    target_bytes = int(max_bytes * target_ratio)
    chunk_docs = max(1000, _coerce_int(MQTT_HISTORY_CFG.get("prune_chunk_documents"), 50000))
    sort_asc = ASCENDING if ASCENDING is not None else 1

    try:
        stats = collection.database.command("collStats", collection.name)
        size_bytes = _coerce_int(stats.get("size"), 0)
        if size_bytes <= max_bytes:
            return
        while size_bytes > target_bytes:
            oldest_batch = list(collection.find({}, {"_id": 1}).sort("_id", sort_asc).limit(chunk_docs))
            if not oldest_batch:
                return
            cutoff_id = oldest_batch[-1]["_id"]
            deleted = collection.delete_many({"_id": {"$lte": cutoff_id}}).deleted_count
            if deleted <= 0:
                return
            stats = collection.database.command("collStats", collection.name)
            size_bytes = _coerce_int(stats.get("size"), 0)
            logging.info("Pruned %s MQTT history docs, collection size=%s bytes.", deleted, size_bytes)
    except Exception:
        logging.exception("Failed to prune MQTT history collection.")


def _log_mqtt_history(msg: mqtt.MQTTMessage) -> None:
    global mqtt_history_next_retry_at
    if not mqtt_history_enabled:
        return
    collection = mqtt_history_collection
    if collection is None:
        should_retry = False
        now = time.time()
        with mqtt_history_lock:
            if now >= mqtt_history_next_retry_at:
                mqtt_history_next_retry_at = now + 10.0
                should_retry = True
        if should_retry:
            _init_mqtt_history_logger()
            collection = mqtt_history_collection
        if collection is None:
            return
    if collection is None:
        return

    payload = bytes(msg.payload or b"")
    truncated = False
    if len(payload) > 15_000_000:
        payload = payload[:15_000_000]
        truncated = True

    record = {
        "ts": datetime.datetime.now(datetime.timezone.utc),
        "topic": msg.topic,
        "qos": int(getattr(msg, "qos", 0)),
        "retain": bool(getattr(msg, "retain", False)),
        "payload_size": len(msg.payload or b""),
        "payload": payload,
        "payload_truncated": truncated,
    }

    try:
        collection.insert_one(record)
    except Exception:
        logging.exception("Failed to insert MQTT history record for topic %s", msg.topic)
        return

    _prune_mqtt_history_if_needed()


_init_mqtt_history_logger()


def clamp(value: float, min_value: float, max_value: float) -> float:
    return max(min_value, min(max_value, value))


def _configured_robot_candidates(robot_ref: str) -> list[Dict[str, Any]]:
    if not robot_ref:
        return []
    configured = CONFIG_ROBOT_MAP.get(robot_ref)
    if configured:
        return [configured]
    return [CONFIG_ROBOT_MAP[key] for key in CONFIG_ROBOT_KEYS_BY_ID.get(robot_ref, []) if key in CONFIG_ROBOT_MAP]


def _configured_robot(robot_ref: str) -> Optional[Dict[str, Any]]:
    candidates = _configured_robot_candidates(robot_ref)
    if len(candidates) == 1:
        return candidates[0]
    return None


def _robot_entry_identity(robot: Dict[str, Any]) -> Dict[str, str]:
    component_id = str(robot.get("id") or "").strip()
    component_type = str(robot.get("type") or "robots").strip() or "robots"
    system = str(robot.get("system") or "").strip()
    key = str(robot.get("key") or _component_key(system or "pebble", component_type, component_id))
    return {
        "key": key,
        "system": system,
        "type": component_type,
        "id": component_id,
    }


def _get_robot(robot_ref: str) -> Optional[Dict[str, Any]]:
    with robots_lock:
        exact = discovered_robots.get(robot_ref)
        if exact:
            return exact
        matches = [entry for entry in discovered_robots.values() if entry.get("id") == robot_ref]
        if len(matches) == 1:
            return matches[0]
    return None


def _robot_key(robot_ref: str) -> str:
    robot = _get_robot(robot_ref)
    if robot:
        return _robot_entry_identity(robot)["key"]
    configured = _configured_robot(robot_ref)
    if configured:
        return _robot_entry_identity(configured)["key"]
    return str(robot_ref)


def _config_hint(hints: Dict[str, Dict[str, Any]], robot_ref: str) -> Dict[str, Any]:
    return hints.get(_robot_key(robot_ref), {})


def _component_identity(robot_ref: str) -> Optional[Dict[str, str]]:
    robot = _get_robot(robot_ref)
    if robot:
        return _robot_entry_identity(robot)
    configured = _configured_robot(robot_ref)
    if configured:
        return _robot_entry_identity(configured)
    return None


def _component_type(component_id: str) -> str:
    identity = _component_identity(component_id)
    if identity:
        return identity["type"]
    return "robots"


def _topic(robot_id: str, direction: str, metric: str) -> str:
    identity = _component_identity(robot_id)
    if identity and identity.get("system") and identity.get("id"):
        return _topic_from_identity(identity["system"], identity["type"], identity["id"], direction, metric)
    component_type = _component_type(robot_id)
    return _topic_from_identity("pebble", component_type, str(robot_id), direction, metric)


def _video_topic_for_robot(robot_id: str) -> str:
    robot_key = _robot_key(robot_id)
    for topic, rid in CONFIG_VIDEO_TOPICS.items():
        if rid == robot_key:
            return topic
    return _topic(robot_id, "outgoing", "front-camera")


def _video_overlays_topic_for_robot(robot_id: str) -> str:
    robot = _get_robot(robot_id)
    if robot and robot.get("videoOverlaysTopic"):
        return str(robot.get("videoOverlaysTopic"))
    return _topic(robot_id, "outgoing", "video-overlays")


def _audio_topic_for_robot(robot_id: str) -> str:
    robot_key = _robot_key(robot_id)
    for topic, rid in CONFIG_AUDIO_TOPICS.items():
        if rid == robot_key:
            return topic
    return _topic(robot_id, "outgoing", "audio")


def _audio_uplink_topic_for_robot(robot_id: str) -> str:
    hint = _config_hint(CONFIG_AUDIO_UPLINK_HINTS, robot_id)
    if hint.get("topic"):
        return str(hint["topic"])
    return _topic(robot_id, "incoming", "audio-stream")


def _video_command_topic_for_robot(robot_id: str) -> str:
    robot = _get_robot(robot_id)
    if robot and robot.get("videoCommandTopic"):
        return str(robot.get("videoCommandTopic"))
    return _topic(robot_id, "incoming", "front-camera")


def _audio_command_topic_for_robot(robot_id: str) -> str:
    robot = _get_robot(robot_id)
    if robot and robot.get("audioCommandTopic"):
        return str(robot.get("audioCommandTopic"))
    return _topic(robot_id, "incoming", "audio")


def _video_flag_topic_for_robot(robot_id: str) -> str:
    robot = _get_robot(robot_id)
    if robot and robot.get("videoFlagTopic"):
        return str(robot.get("videoFlagTopic"))
    return _topic(robot_id, "incoming", "flags/mqtt-video")


def _audio_flag_topic_for_robot(robot_id: str) -> str:
    robot = _get_robot(robot_id)
    if robot and robot.get("audioFlagTopic"):
        return str(robot.get("audioFlagTopic"))
    return _topic(robot_id, "incoming", "flags/mqtt-audio")


def _reboot_flag_topic_for_robot(robot_id: str) -> str:
    robot = _get_robot(robot_id)
    if robot and robot.get("rebootFlagTopic"):
        return str(robot.get("rebootFlagTopic"))
    return _topic(robot_id, "incoming", "flags/reboot")


def _service_restart_flag_topic_for_robot(robot_id: str) -> str:
    robot = _get_robot(robot_id)
    if robot and robot.get("serviceRestartFlagTopic"):
        return str(robot.get("serviceRestartFlagTopic"))
    return _topic(robot_id, "incoming", "flags/service-restart")


def _git_pull_flag_topic_for_robot(robot_id: str) -> str:
    robot = _get_robot(robot_id)
    if robot and robot.get("gitPullFlagTopic"):
        return str(robot.get("gitPullFlagTopic"))
    return _topic(robot_id, "incoming", "flags/git-pull")


def _logs_topic_for_robot(robot_id: str) -> str:
    return _topic(robot_id, "outgoing", "logs")


def _soundboard_files_topic_for_robot(robot_id: str) -> str:
    return _topic(robot_id, "outgoing", "soundboard-files")


def _soundboard_status_topic_for_robot(robot_id: str) -> str:
    return _topic(robot_id, "outgoing", "soundboard-status")


def _soundboard_command_topic_for_robot(robot_id: str) -> str:
    return _topic(robot_id, "incoming", "soundboard-command")


def _outlet_status_topic_for_robot(robot_id: str, outlet_id: str) -> str:
    clean_outlet_id = str(outlet_id or "").strip().strip("/")
    return _topic(robot_id, "outgoing", f"outlets/{clean_outlet_id}/status")


def _outlet_power_topic_for_robot(robot_id: str, outlet_id: str) -> str:
    clean_outlet_id = str(outlet_id or "").strip().strip("/")
    return _topic(robot_id, "incoming", f"outlets/{clean_outlet_id}/power")


def _autonomy_files_topic_for_robot(robot_id: str) -> str:
    robot = _get_robot(robot_id)
    if robot and robot.get("autonomyFilesTopic"):
        return str(robot.get("autonomyFilesTopic"))
    return _topic(robot_id, "outgoing", "autonomy-files")


def _autonomy_status_topic_for_robot(robot_id: str) -> str:
    robot = _get_robot(robot_id)
    if robot and robot.get("autonomyStatusTopic"):
        return str(robot.get("autonomyStatusTopic"))
    return _topic(robot_id, "outgoing", "autonomy-status")


def _autonomy_command_topic_for_robot(robot_id: str) -> str:
    robot = _get_robot(robot_id)
    if robot and robot.get("autonomyCommandTopic"):
        return str(robot.get("autonomyCommandTopic"))
    return _topic(robot_id, "incoming", "autonomy-command")


def _ensure_robot(robot_id: str) -> Dict[str, Any]:
    robot = _get_robot(robot_id)
    if not robot:
        abort(404, description=f"Unknown component '{robot_id}'.")
    return robot


def _default_capabilities_enabled(robot: Dict[str, Any]) -> bool:
    component_type = str(robot.get("type") or "robots").strip().lower()
    if component_type and component_type != "robots":
        return False
    return not bool(robot.get("capabilitiesSeen"))


def _robot_has_video(robot_id: str) -> bool:
    robot = _get_robot(robot_id)
    if not robot:
        return False
    if "hasVideo" in robot:
        return bool(robot.get("hasVideo"))
    return _default_capabilities_enabled(robot)


def _robot_has_video_controls(robot_id: str) -> bool:
    robot = _get_robot(robot_id)
    if not robot:
        return False
    if "videoControls" in robot:
        return bool(robot["videoControls"])
    if "hasVideo" in robot:
        return bool(robot.get("hasVideo"))
    return _default_capabilities_enabled(robot)


def _robot_has_audio(robot_id: str) -> bool:
    robot = _get_robot(robot_id)
    if not robot:
        return False
    if "hasAudio" in robot:
        return bool(robot.get("hasAudio"))
    return _default_capabilities_enabled(robot)


def _robot_has_audio_controls(robot_id: str) -> bool:
    robot = _get_robot(robot_id)
    if not robot:
        return False
    if "audioControls" in robot:
        return bool(robot["audioControls"])
    if "hasAudio" in robot:
        return bool(robot.get("hasAudio"))
    return _default_capabilities_enabled(robot)


def _robot_has_soundboard(robot_id: str) -> bool:
    robot = _get_robot(robot_id)
    if not robot:
        return False
    if "hasSoundboard" in robot:
        return bool(robot.get("hasSoundboard"))
    return _default_capabilities_enabled(robot)


def _robot_has_drive(robot_id: str) -> bool:
    robot = _get_robot(robot_id)
    if not robot:
        return False
    return bool(robot.get("hasDrive", False))


def _robot_has_lights(robot_id: str) -> bool:
    robot = _get_robot(robot_id)
    if not robot:
        return False
    return bool(robot.get("hasLights", False))


def _robot_has_imu(robot_id: str) -> bool:
    robot = _get_robot(robot_id)
    if not robot:
        return False
    return bool(robot.get("hasImu", False))


def _robot_has_odometry(robot_id: str) -> bool:
    robot = _get_robot(robot_id)
    if not robot:
        return False
    return bool(robot.get("hasOdometry", False))


def _robot_has_outlets(robot_id: str) -> bool:
    robot = _get_robot(robot_id)
    if not robot:
        return False
    if "hasOutlets" in robot:
        return bool(robot.get("hasOutlets"))
    return False


def _robot_has_outlet_controls(robot_id: str) -> bool:
    robot = _get_robot(robot_id)
    if not robot:
        return False
    if "outletControls" in robot:
        return bool(robot.get("outletControls"))
    if "hasOutlets" in robot:
        return bool(robot.get("hasOutlets"))
    return False


def _robot_has_soundboard_controls(robot_id: str) -> bool:
    robot = _get_robot(robot_id)
    if not robot:
        return False
    if "soundboardControls" in robot:
        return bool(robot.get("soundboardControls"))
    if "hasSoundboard" in robot:
        return bool(robot.get("hasSoundboard"))
    return _default_capabilities_enabled(robot)


def _robot_has_autonomy(robot_id: str) -> bool:
    robot = _get_robot(robot_id)
    if not robot:
        return False
    if "hasAutonomy" in robot:
        return bool(robot.get("hasAutonomy"))
    return _default_capabilities_enabled(robot)


def _robot_has_autonomy_controls(robot_id: str) -> bool:
    robot = _get_robot(robot_id)
    if not robot:
        return False
    if "autonomyControls" in robot:
        return bool(robot.get("autonomyControls"))
    if "hasAutonomy" in robot:
        return bool(robot.get("hasAutonomy"))
    return _default_capabilities_enabled(robot)


def _robot_has_reboot_controls(robot_id: str) -> bool:
    robot = _get_robot(robot_id)
    if not robot:
        return False
    component_type = str(robot.get("type") or "robots").strip().lower()
    if component_type and component_type != "robots":
        return False
    if "rebootControls" in robot:
        return bool(robot.get("rebootControls"))
    return _default_capabilities_enabled(robot)


def _robot_has_service_restart_controls(robot_id: str) -> bool:
    robot = _get_robot(robot_id)
    if not robot:
        return False
    component_type = str(robot.get("type") or "robots").strip().lower()
    if component_type and component_type != "robots":
        return False
    if "serviceRestartControls" in robot:
        return bool(robot.get("serviceRestartControls"))
    if "rebootControls" in robot:
        return bool(robot.get("rebootControls"))
    return _default_capabilities_enabled(robot)


def _robot_has_git_pull_controls(robot_id: str) -> bool:
    robot = _get_robot(robot_id)
    if not robot:
        return False
    component_type = str(robot.get("type") or "robots").strip().lower()
    if component_type and component_type != "robots":
        return False
    if "gitPullControls" in robot:
        return bool(robot.get("gitPullControls"))
    if "rebootControls" in robot:
        return bool(robot.get("rebootControls"))
    return _default_capabilities_enabled(robot)


def _ensure_robot_placeholder(
    robot_id: str,
    component_type: Optional[str] = None,
    *,
    system: Optional[str] = None,
    component_id: Optional[str] = None,
) -> Dict[str, Any]:
    identity = _component_identity(robot_id)
    resolved_system = str(system or (identity or {}).get("system") or "pebble").strip()
    resolved_type = str(component_type or (identity or {}).get("type") or "robots").strip() or "robots"
    resolved_id = str(component_id or (identity or {}).get("id") or robot_id).strip()
    robot_key = _component_key(resolved_system, resolved_type, resolved_id)
    with robots_lock:
        entry = discovered_robots.setdefault(robot_key, {"key": robot_key})
        entry["key"] = robot_key
        entry["system"] = resolved_system
        entry["id"] = resolved_id
        entry.setdefault("name", resolved_id)
        entry["type"] = resolved_type
        if resolved_type == "cameras":
            entry.setdefault("hasVideo", True)
            entry.setdefault("videoControls", True)
        entry["lastSeen"] = time.time()
        _apply_robot_hints(robot_key, entry)
        return entry


def _get_video_condition(robot_id: str) -> threading.Condition:
    robot_key = _robot_key(robot_id)
    with video_cache_lock:
        condition = video_conditions.get(robot_key)
        if condition is None:
            condition = threading.Condition()
            video_conditions[robot_key] = condition
        video_cache.setdefault(robot_key, {"jpeg": None, "timestamp": None, "metadata": {}})
        video_last_frames.setdefault(robot_key, None)
    return condition


def _clear_video_state(robot_id: str) -> None:
    robot_key = _robot_key(robot_id)
    with video_cache_lock:
        video_cache.pop(robot_key, None)
        video_last_frames.pop(robot_key, None)
        condition = video_conditions.pop(robot_key, None)
    if condition:
        with condition:
            condition.notify_all()
    _clear_video_overlay_state(robot_key)


def _mark_robot_video_capable(robot_id: str, controls: Optional[bool] = None) -> None:
    if not VIDEO_SUPPORT:
        return
    entry = _ensure_robot_placeholder(robot_id)
    robot_key = str(entry.get("key") or _robot_key(robot_id))
    _get_video_condition(robot_key)
    with robots_lock:
        current = discovered_robots.get(robot_key)
        if current is None:
            return
        current["hasVideo"] = True
        if controls is not None and "videoControls" not in current:
            current["videoControls"] = bool(controls)


def _clear_video_overlay_state(robot_id: str) -> None:
    robot_key = _robot_key(robot_id)
    with video_overlays_cache_lock:
        video_overlays_cache.pop(robot_key, None)


def _get_audio_condition(robot_id: str) -> threading.Condition:
    robot_key = _robot_key(robot_id)
    with audio_cache_lock:
        condition = audio_conditions.get(robot_key)
        if condition is None:
            condition = threading.Condition()
            audio_conditions[robot_key] = condition
        audio_buffers.setdefault(robot_key, deque(maxlen=AUDIO_STREAM_BUFFER_PACKETS))
        audio_cache.setdefault(robot_key, {"seq": 0})
    return condition


def _clear_audio_state(robot_id: str) -> None:
    robot_key = _robot_key(robot_id)
    with audio_cache_lock:
        audio_cache.pop(robot_key, None)
        audio_buffers.pop(robot_key, None)
        condition = audio_conditions.pop(robot_key, None)
    if condition:
        with condition:
            condition.notify_all()


def _mark_robot_audio_capable(robot_id: str, controls: Optional[bool] = None) -> None:
    entry = _ensure_robot_placeholder(robot_id)
    robot_key = str(entry.get("key") or _robot_key(robot_id))
    with robots_lock:
        current = discovered_robots.get(robot_key)
        if current is None:
            return
        current["hasAudio"] = True
        current["lastSeen"] = time.time()
        if controls is not None and "audioControls" not in current:
            current["audioControls"] = bool(controls)


def _clear_soundboard_state(robot_id: str) -> None:
    robot_key = _robot_key(robot_id)
    with soundboard_cache_lock:
        soundboard_files_cache.pop(robot_key, None)
        soundboard_status_cache.pop(robot_key, None)


def _clear_outlets_state(robot_id: str) -> None:
    robot_key = _robot_key(robot_id)
    with outlets_cache_lock:
        outlets_status_cache.pop(robot_key, None)


def _mark_robot_soundboard_capable(robot_id: str, controls: Optional[bool] = None) -> None:
    entry = _ensure_robot_placeholder(robot_id)
    robot_key = str(entry.get("key") or _robot_key(robot_id))
    with robots_lock:
        current = discovered_robots.get(robot_key)
        if current is None:
            return
        current["hasSoundboard"] = True
        current["lastSeen"] = time.time()
        if controls is not None and "soundboardControls" not in current:
            current["soundboardControls"] = bool(controls)


def _mark_robot_outlets_capable(robot_id: str, controls: Optional[bool] = None) -> None:
    entry = _ensure_robot_placeholder(robot_id)
    robot_key = str(entry.get("key") or _robot_key(robot_id))
    with robots_lock:
        current = discovered_robots.get(robot_key)
        if current is None:
            return
        current["hasOutlets"] = True
        current["lastSeen"] = time.time()
        if controls is not None and "outletControls" not in current:
            current["outletControls"] = bool(controls)


def _clear_autonomy_state(robot_id: str) -> None:
    robot_key = _robot_key(robot_id)
    with autonomy_cache_lock:
        autonomy_files_cache.pop(robot_key, None)
        autonomy_status_cache.pop(robot_key, None)


def _mark_robot_autonomy_capable(robot_id: str, controls: Optional[bool] = None) -> None:
    entry = _ensure_robot_placeholder(robot_id)
    robot_key = str(entry.get("key") or _robot_key(robot_id))
    with robots_lock:
        current = discovered_robots.get(robot_key)
        if current is None:
            return
        current["hasAutonomy"] = True
        current["lastSeen"] = time.time()
        if controls is not None and "autonomyControls" not in current:
            current["autonomyControls"] = bool(controls)


def _publish_command(topic: str, payload: Any, qos: int = 1, retain: bool = False) -> bool:
    client = mqtt_client
    if client is None or not mqtt_connected.is_set():
        logging.warning("MQTT connection not ready; dropping command for %s", topic)
        return False
    try:
        if isinstance(payload, (bytes, bytearray, memoryview)):
            body = bytes(payload)
        elif isinstance(payload, str):
            body = payload.encode("utf-8")
        else:
            body = json.dumps(payload).encode("utf-8")
        client.publish(topic, body, qos=qos, retain=retain)
        return True
    except Exception:
        logging.exception("Failed to publish to %s", topic)
        return False


def _publish_video_control(robot_id: str, enabled: bool) -> bool:
    payload = {"enabled": bool(enabled)}
    topic = _video_command_topic_for_robot(robot_id)
    return _publish_command(topic, payload)


def _publish_audio_control(robot_id: str, enabled: bool) -> bool:
    payload = {"enabled": bool(enabled)}
    topic = _audio_command_topic_for_robot(robot_id)
    return _publish_command(topic, payload)


def _publish_video_flag(robot_id: str, enabled: bool) -> bool:
    payload = {"value": bool(enabled)}
    topic = _video_flag_topic_for_robot(robot_id)
    return _publish_command(topic, payload, qos=1, retain=True)


def _publish_audio_flag(robot_id: str, enabled: bool) -> bool:
    payload = {"value": bool(enabled)}
    topic = _audio_flag_topic_for_robot(robot_id)
    return _publish_command(topic, payload, qos=1, retain=True)


def _publish_reboot_flag(robot_id: str) -> bool:
    payload = {"value": True}
    topic = _reboot_flag_topic_for_robot(robot_id)
    return _publish_command(topic, payload, qos=1, retain=False)


def _publish_service_restart_flag(robot_id: str) -> bool:
    payload = {"value": True}
    topic = _service_restart_flag_topic_for_robot(robot_id)
    return _publish_command(topic, payload, qos=1, retain=False)


def _publish_git_pull_flag(robot_id: str) -> bool:
    payload = {"value": True}
    topic = _git_pull_flag_topic_for_robot(robot_id)
    return _publish_command(topic, payload, qos=1, retain=False)


def _decode_audio_packet(payload: bytes) -> Optional[Dict[str, Any]]:
    if len(payload) < AUDIO_PACKET_HEADER.size:
        return None
    header = payload[: AUDIO_PACKET_HEADER.size]
    body = payload[AUDIO_PACKET_HEADER.size :]
    magic, version, codec, channels, _flags, rate, frame_samples, seq, timestamp_ms = AUDIO_PACKET_HEADER.unpack(header)
    if magic != AUDIO_MAGIC or version != 1 or codec != AUDIO_CODEC_PCM_S16LE:
        return None
    if channels <= 0 or rate <= 0 or frame_samples <= 0:
        return None
    expected_len = frame_samples * channels * 2
    if len(body) < expected_len:
        return None
    return {
        "seq": int(seq),
        "timestamp": float(timestamp_ms) / 1000.0,
        "rate": int(rate),
        "channels": int(channels),
        "frame_samples": int(frame_samples),
        "data": body[:expected_len],
    }


def _encode_audio_packet(seq: int, rate: int, channels: int, pcm_bytes: bytes) -> Optional[bytes]:
    frame_width = channels * 2
    if channels <= 0 or rate <= 0 or len(pcm_bytes) < frame_width:
        return None
    safe_seq = max(0, int(seq)) & 0xFFFFFFFF
    frame_samples = len(pcm_bytes) // frame_width
    payload_len = frame_samples * frame_width
    if payload_len <= 0:
        return None
    payload = pcm_bytes[:payload_len]
    header = AUDIO_PACKET_HEADER.pack(
        AUDIO_MAGIC,
        1,
        AUDIO_CODEC_PCM_S16LE,
        channels,
        0,
        rate,
        frame_samples,
        safe_seq,
        int(time.time() * 1000),
    )
    return header + payload


def _video_frame_generator(robot_id: str):
    robot_key = _robot_key(robot_id)
    condition = _get_video_condition(robot_id)
    last_timestamp = None
    boundary = b"--frame\r\n"
    try:
        while True:
            with video_cache_lock:
                entry = video_cache.get(robot_key) or {}
                frame = entry.get("jpeg")
                timestamp = entry.get("timestamp")
            if frame is not None and timestamp != last_timestamp:
                last_timestamp = timestamp
                yield (
                    boundary
                    + b"Content-Type: image/jpeg\r\n"
                    + f"Content-Length: {len(frame)}\r\n\r\n".encode("ascii")
                    + frame
                    + b"\r\n"
                )
                continue
            if not condition:
                time.sleep(0.2)
                continue
            with condition:
                condition.wait(timeout=1.0)
    except GeneratorExit:
        return


def _audio_stream_generator(robot_id: str):
    robot_key = _robot_key(robot_id)
    condition = _get_audio_condition(robot_id)
    with audio_cache_lock:
        snapshot = audio_cache.get(robot_key) or {}
        last_seq = max(0, _coerce_int(snapshot.get("seq"), 0))
        last_generation = max(0, _coerce_int(snapshot.get("generation"), 0))
    keepalive_at = time.time()
    try:
        while True:
            with audio_cache_lock:
                cache = audio_cache.get(robot_key) or {}
                generation = int(cache.get("generation", 0))
                buffer = list(audio_buffers.get(robot_key, []))
            if generation != last_generation:
                last_generation = generation
                last_seq = 0
            pending = [item for item in buffer if int(item.get("seq", 0)) > last_seq]
            if pending:
                keepalive_at = time.time()
                for item in pending:
                    seq = int(item.get("seq", 0))
                    last_seq = max(last_seq, seq)
                    payload = {
                        "seq": seq,
                        "timestamp": item.get("timestamp"),
                        "rate": item.get("rate"),
                        "channels": item.get("channels"),
                        "frame_samples": item.get("frame_samples"),
                        "data": base64.b64encode(item.get("data") or b"").decode("ascii"),
                    }
                    yield f"data: {json.dumps(payload)}\n\n"
                continue
            now = time.time()
            if now - keepalive_at > 15:
                keepalive_at = now
                yield ": keepalive\n\n"
            with condition:
                condition.wait(timeout=1.0)
    except GeneratorExit:
        return


def _handle_touch_payload(
    robot_id: str,
    payload: Dict[str, Any],
    component_type: Optional[str] = None,
    system: Optional[str] = None,
) -> None:
    robot_key = _robot_key(robot_id)
    values = payload.get("value") if isinstance(payload.get("value"), dict) else payload
    try:
        t0 = int(values.get("a0", values.get("t0")))
        t1 = int(values.get("a1", values.get("t1")))
        t2 = int(values.get("a2", values.get("t2")))
    except (TypeError, ValueError):
        logging.debug("Invalid touch payload for %s: %s", robot_id, payload)
        return
    with telemetry_lock:
        entry = telemetry_cache.setdefault(robot_key, {})
        entry.update({"t0": t0, "t1": t1, "t2": t2})
    _ensure_robot_placeholder(robot_key, component_type, system=system)
    with robots_lock:
        current = discovered_robots.get(robot_key)
        if current is not None:
            current["hasTelemetry"] = True
    _record_heartbeat_sample(
        robot_key,
        component_type=component_type,
        sent_at=_heartbeat_timestamp_seconds(payload),
        explicit=False,
        system=system,
    )


def _handle_charge_payload(
    robot_id: str,
    payload: Dict[str, Any],
    component_type: Optional[str] = None,
    system: Optional[str] = None,
) -> None:
    robot_key = _robot_key(robot_id)
    value = payload.get("value")
    charging = bool(value) if isinstance(value, (bool, int)) else None
    with telemetry_lock:
        entry = telemetry_cache.setdefault(robot_key, {})
        entry.update({"charging": charging})
    _ensure_robot_placeholder(robot_key, component_type, system=system)
    with robots_lock:
        current = discovered_robots.get(robot_key)
        if current is not None:
            current["hasTelemetry"] = True
    _record_heartbeat_sample(
        robot_key,
        component_type=component_type,
        sent_at=_heartbeat_timestamp_seconds(payload),
        explicit=False,
        system=system,
    )


def _handle_charge_level_payload(
    robot_id: str,
    payload: Dict[str, Any],
    component_type: Optional[str] = None,
    system: Optional[str] = None,
) -> None:
    robot_key = _robot_key(robot_id)
    value = payload.get("value")
    if not isinstance(value, (int, float)):
        return
    with telemetry_lock:
        entry = telemetry_cache.setdefault(robot_key, {})
        entry.update({"battery_voltage": float(value)})
    _ensure_robot_placeholder(robot_key, component_type, system=system)
    with robots_lock:
        current = discovered_robots.get(robot_key)
        if current is not None:
            current["hasTelemetry"] = True
    _record_heartbeat_sample(
        robot_key,
        component_type=component_type,
        sent_at=_heartbeat_timestamp_seconds(payload),
        explicit=False,
        system=system,
    )


def _handle_wheel_odometry_payload(
    robot_id: str,
    payload: Dict[str, Any],
    component_type: Optional[str] = None,
    system: Optional[str] = None,
) -> None:
    robot_key = _robot_key(robot_id)
    values = payload.get("value") if isinstance(payload.get("value"), dict) else payload
    if not isinstance(values, dict):
        return
    x_raw = values.get("x_mm")
    y_raw = values.get("y_mm")
    if not isinstance(x_raw, (int, float)) or not isinstance(y_raw, (int, float)):
        return
    heading_deg_raw = values.get("heading_deg")
    heading_rad_raw = values.get("heading_rad")
    heading_deg = float(heading_deg_raw) if isinstance(heading_deg_raw, (int, float)) else None
    heading_rad = float(heading_rad_raw) if isinstance(heading_rad_raw, (int, float)) else None
    if heading_deg is None and heading_rad is not None:
        heading_deg = math.degrees(heading_rad)
    odom = {
        "x_mm": float(x_raw),
        "y_mm": float(y_raw),
        "heading_deg": float(heading_deg) if heading_deg is not None else 0.0,
        "heading_rad": float(heading_rad) if heading_rad is not None else math.radians(float(heading_deg or 0.0)),
        "left_mm": float(values.get("left_mm")) if isinstance(values.get("left_mm"), (int, float)) else None,
        "right_mm": float(values.get("right_mm")) if isinstance(values.get("right_mm"), (int, float)) else None,
        "distance_mm": float(values.get("distance_mm")) if isinstance(values.get("distance_mm"), (int, float)) else None,
        "left_crossings": int(values.get("left_crossings")) if isinstance(values.get("left_crossings"), (int, float)) else None,
        "right_crossings": int(values.get("right_crossings")) if isinstance(values.get("right_crossings"), (int, float)) else None,
        "sample_seq": int(values.get("sample_seq")) if isinstance(values.get("sample_seq"), (int, float)) else None,
        "publish_seq": int(values.get("publish_seq")) if isinstance(values.get("publish_seq"), (int, float)) else None,
        "timestamp": float(payload.get("timestamp")) if isinstance(payload.get("timestamp"), (int, float)) else time.time(),
    }
    with telemetry_lock:
        entry = telemetry_cache.setdefault(robot_key, {})
        entry["odometry"] = odom
    _ensure_robot_placeholder(robot_key, component_type, system=system)
    with robots_lock:
        current = discovered_robots.get(robot_key)
        if current is not None:
            current["hasOdometry"] = True
    _record_heartbeat_sample(
        robot_key,
        component_type=component_type,
        sent_at=_heartbeat_timestamp_seconds(payload),
        explicit=False,
        system=system,
    )


def _vector_dict(value: Any) -> Optional[Dict[str, float]]:
    if not isinstance(value, dict):
        return None
    result: Dict[str, float] = {}
    for axis in ("x", "y", "z"):
        raw = value.get(axis)
        if isinstance(raw, (int, float)) and math.isfinite(float(raw)):
            result[axis] = float(raw)
    return result or None


def _handle_imu_payload(
    robot_id: str,
    payload: Dict[str, Any],
    component_type: Optional[str] = None,
    system: Optional[str] = None,
) -> None:
    values = payload.get("value") if isinstance(payload.get("value"), dict) else payload
    if not isinstance(values, dict):
        return

    imu: Dict[str, Any] = {}
    orientation = values.get("orientation_deg")
    if isinstance(orientation, dict):
        roll = orientation.get("roll")
        pitch = orientation.get("pitch")
        orientation_payload: Dict[str, float] = {}
        if isinstance(roll, (int, float)) and math.isfinite(float(roll)):
            orientation_payload["roll"] = float(roll)
        if isinstance(pitch, (int, float)) and math.isfinite(float(pitch)):
            orientation_payload["pitch"] = float(pitch)
        if orientation_payload:
            imu["orientation_deg"] = orientation_payload

    accel = _vector_dict(values.get("accel_mps2"))
    if accel:
        imu["accel_mps2"] = accel

    gyro = _vector_dict(values.get("gyro_dps"))
    if gyro:
        imu["gyro_dps"] = gyro

    gyro_bias = _vector_dict(values.get("gyro_bias_dps"))
    if gyro_bias:
        imu["gyro_bias_dps"] = gyro_bias

    temp_c = values.get("temp_c")
    if isinstance(temp_c, (int, float)) and math.isfinite(float(temp_c)):
        imu["temp_c"] = float(temp_c)

    accel_norm_g = values.get("accel_norm_g")
    if isinstance(accel_norm_g, (int, float)) and math.isfinite(float(accel_norm_g)):
        imu["accel_norm_g"] = float(accel_norm_g)

    health = values.get("health")
    if isinstance(health, dict):
        health_payload: Dict[str, Any] = {}
        for key in ("device", "protocol", "instance"):
            raw = health.get(key)
            if isinstance(raw, str) and raw:
                health_payload[key] = raw
        for key in ("samples_ok", "samples_error", "calibration_samples"):
            raw = health.get(key)
            if isinstance(raw, (int, float)):
                health_payload[key] = int(raw)
        if health_payload:
            imu["health"] = health_payload

    seq = payload.get("seq")
    if isinstance(seq, (int, float)):
        imu["seq"] = int(seq)
    mcu_ms = payload.get("mcu_ms")
    if isinstance(mcu_ms, (int, float)):
        imu["mcu_ms"] = int(mcu_ms)
    t_ms = payload.get("t")
    if isinstance(t_ms, (int, float)):
        imu["t"] = int(t_ms)

    if not imu:
        return

    robot_key = _robot_key(robot_id)
    with telemetry_lock:
        entry = telemetry_cache.setdefault(robot_key, {})
        merged = dict(entry.get("imu") or {})
        merged.update(imu)
        entry["imu"] = merged
    _ensure_robot_placeholder(robot_key, component_type, system=system)
    with robots_lock:
        current = discovered_robots.get(robot_key)
        if current is not None:
            current["hasImu"] = True
    _record_heartbeat_sample(
        robot_key,
        component_type=component_type,
        sent_at=_heartbeat_timestamp_seconds(payload),
        explicit=False,
        system=system,
    )


def _handle_status_payload(
    component_id: str,
    payload: Dict[str, Any],
    component_type: Optional[str] = None,
    system: Optional[str] = None,
) -> None:
    value = payload.get("value")
    if not isinstance(value, dict):
        return
    webrtc_url = value.get("webrtc_url") or value.get("webrtcUrl") or value.get("url")
    if not isinstance(webrtc_url, str) or not webrtc_url.strip():
        return
    placeholder = _ensure_robot_placeholder(component_id, component_type, system=system)
    robot_key = str(placeholder.get("key") or _robot_key(component_id))
    with robots_lock:
        entry = discovered_robots.get(robot_key)
        if entry is None:
            return
        entry["webrtcUrl"] = webrtc_url.strip()
        entry["lastSeen"] = time.time()


def _handle_capabilities_payload(
    component_id: str,
    payload: Dict[str, Any],
    component_type: Optional[str] = None,
    system: Optional[str] = None,
) -> None:
    value = payload.get("value") if isinstance(payload.get("value"), dict) else payload
    if not isinstance(value, dict):
        return

    clear_video = False
    clear_audio = False
    clear_soundboard = False
    clear_autonomy = False
    now = time.time()
    placeholder = _ensure_robot_placeholder(component_id, component_type, system=system)
    robot_key = str(placeholder.get("key") or _robot_key(component_id))
    with robots_lock:
        entry = discovered_robots.get(robot_key)
        if entry is None:
            return
        entry["lastSeen"] = now
        entry["capabilitiesSeen"] = True
        _apply_robot_hints(robot_key, entry)

        video = value.get("video") if isinstance(value.get("video"), dict) else None
        if video is not None:
            video_available = _coerce_bool(video.get("available"))
            video_controls = _coerce_bool(video.get("controls"))
            video_command_topic = video.get("command_topic")
            video_flag_topic = video.get("flag_topic")
            video_overlays_topic = video.get("overlays_topic")
            if video_available is not None:
                entry["hasVideo"] = video_available
                clear_video = not video_available
            if video_controls is not None:
                entry["videoControls"] = video_controls
            if isinstance(video_command_topic, str) and video_command_topic.strip():
                entry["videoCommandTopic"] = video_command_topic.strip()
            if isinstance(video_flag_topic, str) and video_flag_topic.strip():
                entry["videoFlagTopic"] = video_flag_topic.strip()
            if isinstance(video_overlays_topic, str) and video_overlays_topic.strip():
                entry["videoOverlaysTopic"] = video_overlays_topic.strip()

        audio = value.get("audio") if isinstance(value.get("audio"), dict) else None
        if audio is not None:
            audio_available = _coerce_bool(audio.get("available"))
            audio_controls = _coerce_bool(audio.get("controls"))
            audio_command_topic = audio.get("command_topic")
            audio_flag_topic = audio.get("flag_topic")
            if audio_available is not None:
                entry["hasAudio"] = audio_available
                clear_audio = not audio_available
            if audio_controls is not None:
                entry["audioControls"] = audio_controls
            if isinstance(audio_command_topic, str) and audio_command_topic.strip():
                entry["audioCommandTopic"] = audio_command_topic.strip()
            if isinstance(audio_flag_topic, str) and audio_flag_topic.strip():
                entry["audioFlagTopic"] = audio_flag_topic.strip()

        soundboard = value.get("soundboard") if isinstance(value.get("soundboard"), dict) else None
        if soundboard is not None:
            soundboard_available = _coerce_bool(soundboard.get("available"))
            soundboard_controls = _coerce_bool(soundboard.get("controls"))
            if soundboard_available is not None:
                entry["hasSoundboard"] = soundboard_available
                clear_soundboard = not soundboard_available
            if soundboard_controls is not None:
                entry["soundboardControls"] = soundboard_controls

        autonomy = value.get("autonomy") if isinstance(value.get("autonomy"), dict) else None
        if autonomy is not None:
            autonomy_available = _coerce_bool(autonomy.get("available"))
            autonomy_controls = _coerce_bool(autonomy.get("controls"))
            command_topic = autonomy.get("command_topic")
            files_topic = autonomy.get("files_topic")
            status_topic = autonomy.get("status_topic")
            if autonomy_available is not None:
                entry["hasAutonomy"] = autonomy_available
                clear_autonomy = not autonomy_available
            if autonomy_controls is not None:
                entry["autonomyControls"] = autonomy_controls
            if isinstance(command_topic, str) and command_topic.strip():
                entry["autonomyCommandTopic"] = command_topic.strip()
            if isinstance(files_topic, str) and files_topic.strip():
                entry["autonomyFilesTopic"] = files_topic.strip()
            if isinstance(status_topic, str) and status_topic.strip():
                entry["autonomyStatusTopic"] = status_topic.strip()

        drive = value.get("drive") if isinstance(value.get("drive"), dict) else None
        if drive is not None:
            drive_available = _coerce_bool(drive.get("available"))
            drive_controls = _coerce_bool(drive.get("controls"))
            if drive_available is not None:
                entry["hasDrive"] = bool(drive_available if drive_controls is None else drive_available and drive_controls)

        lights = value.get("lights") if isinstance(value.get("lights"), dict) else None
        if lights is not None:
            solid_available = _coerce_bool(lights.get("solid"))
            flash_available = _coerce_bool(lights.get("flash"))
            if solid_available is not None or flash_available is not None:
                entry["hasLights"] = bool(solid_available or flash_available)

        telemetry = value.get("telemetry") if isinstance(value.get("telemetry"), dict) else None
        if telemetry is not None:
            touch_available = _coerce_bool(telemetry.get("touch_sensors"))
            charging_available = _coerce_bool(telemetry.get("charging_status"))
            odometry_available = _coerce_bool(
                telemetry.get("wheel_odometry")
                if telemetry.get("wheel_odometry") is not None
                else telemetry.get("odometry")
            )
            imu_available = _coerce_bool(telemetry.get("imu"))
            if touch_available is not None or charging_available is not None:
                entry["hasTelemetry"] = bool(touch_available or charging_available)
            if odometry_available is not None:
                entry["hasOdometry"] = bool(odometry_available)
            if imu_available is not None:
                entry["hasImu"] = bool(imu_available)

        system = value.get("system") if isinstance(value.get("system"), dict) else None
        reboot = None
        if system is not None and isinstance(system.get("reboot"), dict):
            reboot = system.get("reboot")
        elif isinstance(value.get("reboot"), dict):
            reboot = value.get("reboot")
        if isinstance(reboot, dict):
            reboot_controls = _coerce_bool(reboot.get("controls"))
            reboot_flag_topic = reboot.get("flag_topic") or reboot.get("topic")
            if reboot_controls is not None:
                entry["rebootControls"] = reboot_controls
            if isinstance(reboot_flag_topic, str) and reboot_flag_topic.strip():
                entry["rebootFlagTopic"] = reboot_flag_topic.strip()

        service_restart = None
        if system is not None and isinstance(system.get("service_restart"), dict):
            service_restart = system.get("service_restart")
        elif system is not None and isinstance(system.get("service-restart"), dict):
            service_restart = system.get("service-restart")
        elif isinstance(value.get("service_restart"), dict):
            service_restart = value.get("service_restart")
        elif isinstance(value.get("service-restart"), dict):
            service_restart = value.get("service-restart")
        if isinstance(service_restart, dict):
            service_restart_controls = _coerce_bool(service_restart.get("controls"))
            service_restart_flag_topic = service_restart.get("flag_topic") or service_restart.get("topic")
            if service_restart_controls is not None:
                entry["serviceRestartControls"] = service_restart_controls
            if isinstance(service_restart_flag_topic, str) and service_restart_flag_topic.strip():
                entry["serviceRestartFlagTopic"] = service_restart_flag_topic.strip()

        git_pull = None
        if system is not None and isinstance(system.get("git_pull"), dict):
            git_pull = system.get("git_pull")
        elif system is not None and isinstance(system.get("git-pull"), dict):
            git_pull = system.get("git-pull")
        elif isinstance(value.get("git_pull"), dict):
            git_pull = value.get("git_pull")
        elif isinstance(value.get("git-pull"), dict):
            git_pull = value.get("git-pull")
        if isinstance(git_pull, dict):
            git_pull_controls = _coerce_bool(git_pull.get("controls"))
            git_pull_flag_topic = git_pull.get("flag_topic") or git_pull.get("topic")
            if git_pull_controls is not None:
                entry["gitPullControls"] = git_pull_controls
            if isinstance(git_pull_flag_topic, str) and git_pull_flag_topic.strip():
                entry["gitPullFlagTopic"] = git_pull_flag_topic.strip()

    if clear_video:
        _clear_video_state(robot_key)
    if clear_audio:
        _clear_audio_state(robot_key)
    if clear_soundboard:
        _clear_soundboard_state(robot_key)
    if clear_autonomy:
        _clear_autonomy_state(robot_key)


def _heartbeat_timestamp_seconds(payload: Dict[str, Any]) -> Optional[float]:
    t_value = _coerce_float(payload.get("t"))
    if t_value is not None:
        if t_value <= 0:
            return None
        if t_value >= 1e11:
            return t_value / 1000.0
        if t_value >= 1e9:
            return t_value
        return None
    timestamp_value = _coerce_float(payload.get("timestamp"))
    if timestamp_value is None or timestamp_value <= 0:
        return None
    if timestamp_value >= 1e11:
        return timestamp_value / 1000.0
    if timestamp_value >= 1e9:
        return timestamp_value
    return None


def _heartbeat_has_nonzero_timestamp(payload: Dict[str, Any]) -> bool:
    t_value = _coerce_float(payload.get("t"))
    if t_value is not None:
        return t_value > 0
    timestamp_value = _coerce_float(payload.get("timestamp"))
    if timestamp_value is not None:
        return timestamp_value > 0
    return False


def _heartbeat_explicit_online(payload: Dict[str, Any]) -> Optional[bool]:
    candidates: list[Dict[str, Any]] = []
    value = payload.get("value")
    if isinstance(value, dict):
        candidates.append(value)
    candidates.append(payload)

    for item in candidates:
        online = _coerce_bool(item.get("online"))
        if online is not None:
            return online
        for key in ("status", "state"):
            raw = item.get(key)
            if not isinstance(raw, str):
                continue
            token = raw.strip().lower()
            if token in {"online", "up", "connected"}:
                return True
            if token in {"offline", "down", "disconnected"}:
                return False
    return None


def _record_heartbeat_explicit_state(
    robot_id: str,
    component_type: Optional[str],
    online: bool,
    system: Optional[str] = None,
) -> None:
    robot_key = _robot_key(robot_id)
    received_at = time.time()
    with telemetry_lock:
        entry = telemetry_cache.setdefault(robot_key, {})
        entry["heartbeat_topic_received_at"] = received_at
        entry["heartbeat_explicit_online"] = online
        entry["heartbeat_explicit_state_received_at"] = received_at

    placeholder = _ensure_robot_placeholder(robot_key, component_type, system=system)
    resolved_key = str(placeholder.get("key") or robot_key)
    with robots_lock:
        entry = discovered_robots.get(resolved_key)
        if entry is None:
            return
        entry["lastSeen"] = received_at
        _apply_robot_hints(resolved_key, entry)


def _record_heartbeat_sample(
    robot_id: str,
    component_type: Optional[str],
    sent_at: Optional[float],
    explicit: bool,
    system: Optional[str] = None,
) -> None:
    robot_key = _robot_key(robot_id)
    received_at = time.time()
    latency_ms: Optional[float] = None
    if sent_at is not None:
        raw_latency = received_at - sent_at
        if -1.0 <= raw_latency <= HEARTBEAT_MAX_LATENCY_SECONDS:
            latency_ms = max(0.0, raw_latency * 1000.0)

    with telemetry_lock:
        entry = telemetry_cache.setdefault(robot_key, {})
        explicit_received_at = _coerce_float(entry.get("heartbeat_topic_received_at"))
        if explicit:
            entry["heartbeat_topic_received_at"] = received_at
            entry["heartbeat_explicit_online"] = True
            entry["heartbeat_explicit_state_received_at"] = received_at
        elif explicit_received_at is not None and (received_at - explicit_received_at) <= HEARTBEAT_TOPIC_FRESH_SECONDS:
            # If explicit heartbeat packets are active, ignore telemetry fallback samples.
            return

        previous_received_at = _coerce_float(entry.get("heartbeat_received_at"))
        entry["heartbeat_received_at"] = received_at
        if sent_at is not None:
            entry["heartbeat_sent_at"] = sent_at
        if previous_received_at is not None:
            interval_s = received_at - previous_received_at
            if HEARTBEAT_MIN_INTERVAL_SAMPLE_SECONDS <= interval_s <= HEARTBEAT_MAX_LATENCY_SECONDS:
                interval_samples_raw = entry.get("heartbeat_interval_samples")
                interval_samples = interval_samples_raw if isinstance(interval_samples_raw, list) else []
                interval_samples.append(interval_s)
                if len(interval_samples) > HEARTBEAT_INTERVAL_SAMPLES:
                    del interval_samples[:-HEARTBEAT_INTERVAL_SAMPLES]
                entry["heartbeat_interval_samples"] = interval_samples
                entry["heartbeat_interval_avg_s"] = sum(interval_samples) / len(interval_samples)
        if latency_ms is not None:
            samples_raw = entry.get("heartbeat_latency_samples")
            samples = samples_raw if isinstance(samples_raw, list) else []
            samples.append(latency_ms)
            if len(samples) > HEARTBEAT_LATENCY_SAMPLES:
                del samples[:-HEARTBEAT_LATENCY_SAMPLES]
            entry["heartbeat_latency_samples"] = samples
            entry["heartbeat_latency_ms"] = latency_ms
            entry["heartbeat_latency_avg_ms"] = sum(samples) / len(samples)

    placeholder = _ensure_robot_placeholder(robot_key, component_type, system=system)
    resolved_key = str(placeholder.get("key") or robot_key)
    with robots_lock:
        entry = discovered_robots.get(resolved_key)
        if entry is None:
            return
        entry["lastSeen"] = received_at
        _apply_robot_hints(resolved_key, entry)


def _heartbeat_window_seconds(avg_latency_ms: Optional[float], avg_interval_s: Optional[float]) -> Optional[float]:
    if avg_interval_s is None or avg_interval_s <= 0:
        return None
    window_s = avg_interval_s * 2.0
    if avg_latency_ms is not None and avg_latency_ms >= 0:
        window_s = max(window_s, (avg_latency_ms / 1000.0) * HEARTBEAT_OFFLINE_MULTIPLIER)
    return window_s


def _heartbeat_connection_state(entry: Dict[str, Any], now: Optional[float] = None) -> Dict[str, Any]:
    now_s = now if now is not None else time.time()
    explicit_online = entry.get("heartbeat_explicit_online")
    explicit_state_received_at = _coerce_float(entry.get("heartbeat_explicit_state_received_at"))
    if explicit_online is False:
        age_s = None if explicit_state_received_at is None else max(0.0, now_s - explicit_state_received_at)
        return {
            "status": "offline",
            "online": False,
            "offline_for_s": age_s,
            "age_s": age_s,
            "window_s": None,
            "interval_s": _coerce_float(entry.get("heartbeat_interval_avg_s")),
            "latency_ms": _coerce_float(entry.get("heartbeat_latency_ms")),
            "latency_avg_ms": _coerce_float(entry.get("heartbeat_latency_avg_ms")),
        }
    received_at = _coerce_float(entry.get("heartbeat_received_at"))
    avg_latency_ms = _coerce_float(entry.get("heartbeat_latency_avg_ms"))
    avg_interval_s = _coerce_float(entry.get("heartbeat_interval_avg_s"))
    latest_latency_ms = _coerce_float(entry.get("heartbeat_latency_ms"))
    window_s = _heartbeat_window_seconds(avg_latency_ms, avg_interval_s)
    if received_at is None:
        return {
            "status": "unknown",
            "online": None,
            "offline_for_s": None,
            "age_s": None,
            "window_s": window_s,
            "interval_s": avg_interval_s,
            "latency_ms": latest_latency_ms,
            "latency_avg_ms": avg_latency_ms,
        }
    age_s = max(0.0, now_s - received_at)
    if window_s is None:
        return {
            "status": "unknown",
            "online": None,
            "offline_for_s": None,
            "age_s": age_s,
            "window_s": None,
            "interval_s": avg_interval_s,
            "latency_ms": latest_latency_ms,
            "latency_avg_ms": avg_latency_ms,
        }
    online = age_s <= window_s
    return {
        "status": "online" if online else "offline",
        "online": online,
        "offline_for_s": 0.0 if online else max(0.0, age_s - window_s),
        "age_s": age_s,
        "window_s": window_s,
        "interval_s": avg_interval_s,
        "latency_ms": latest_latency_ms,
        "latency_avg_ms": avg_latency_ms,
    }


def _handle_heartbeat_payload(
    robot_id: str,
    payload: Dict[str, Any],
    component_type: Optional[str] = None,
    system: Optional[str] = None,
) -> None:
    explicit_online = _heartbeat_explicit_online(payload)
    if explicit_online is False:
        _record_heartbeat_explicit_state(robot_id, component_type, False, system=system)
        return
    # Backward compatibility: accept legacy payloads, but only treat messages
    # with a real timestamp as heartbeats. This ignores old online/offline tags.
    if not _heartbeat_has_nonzero_timestamp(payload):
        return
    sent_at = _heartbeat_timestamp_seconds(payload)
    _record_heartbeat_sample(robot_id, component_type=component_type, sent_at=sent_at, explicit=True, system=system)


def _apply_robot_hints(robot_id: str, entry: Dict[str, Any]) -> None:
    if entry.get("type") and entry.get("type") != "robots":
        return
    entry_key = str(entry.get("key") or _robot_key(robot_id))
    hint = CONFIG_ROBOT_MAP.get(entry_key) or _configured_robot(str(entry.get("id") or robot_id)) or {}
    if hint:
        entry.setdefault("hasDrive", True)
        entry.setdefault("hasLights", True)
    if hint.get("name") and not entry.get("name"):
        entry["name"] = hint["name"]
    if entry_key in CONFIG_VIDEO_HINTS:
        entry.setdefault("hasVideo", True)
        entry.setdefault("videoControls", CONFIG_VIDEO_HINTS[entry_key].get("controls", True))
    if entry_key in CONFIG_AUDIO_HINTS:
        entry.setdefault("hasAudio", True)
        entry.setdefault("audioControls", CONFIG_AUDIO_HINTS[entry_key].get("controls", True))
    if entry_key in CONFIG_AUTONOMY_HINTS:
        entry.setdefault("hasAutonomy", True)
        entry.setdefault("autonomyControls", CONFIG_AUTONOMY_HINTS[entry_key].get("controls", True))
    if entry_key in CONFIG_REBOOT_HINTS:
        entry.setdefault("rebootControls", CONFIG_REBOOT_HINTS[entry_key].get("controls", True))
    if entry_key in CONFIG_SERVICE_RESTART_HINTS:
        entry.setdefault("serviceRestartControls", CONFIG_SERVICE_RESTART_HINTS[entry_key].get("controls", True))
    if entry_key in CONFIG_GIT_PULL_HINTS:
        entry.setdefault("gitPullControls", CONFIG_GIT_PULL_HINTS[entry_key].get("controls", True))


def _seed_configured_robots() -> None:
    if not SEED_CONFIGURED_ROBOTS:
        return
    if not CONFIG_ROBOTS:
        return
    now = time.time()
    with robots_lock:
        for robot in CONFIG_ROBOTS:
            robot_id = robot.get("id")
            robot_key = str(robot.get("key") or "").strip()
            robot_system = str(robot.get("system") or "").strip()
            robot_type = str(robot.get("type") or "robots").strip() or "robots"
            if not robot_id or not robot_key or not robot_system:
                continue
            entry = discovered_robots.setdefault(robot_key, {"key": robot_key})
            entry["key"] = robot_key
            entry["system"] = robot_system
            entry["id"] = robot_id
            entry["type"] = robot_type
            entry.setdefault("name", robot.get("name") or robot_id)
            entry.setdefault("model", robot.get("model"))
            entry.setdefault("lastSeen", now)
            _apply_robot_hints(robot_key, entry)


_seed_configured_robots()


def _handle_infrastructure_payload(system: str, payload_bytes: bytes) -> None:
    global infrastructure_last_update
    try:
        payload = json.loads(payload_bytes.decode("utf-8"))
    except json.JSONDecodeError:
        logging.debug("Malformed infrastructure payload")
        return
    components_payload = {k: v for k, v in payload.items() if isinstance(v, dict)}
    if not components_payload:
        logging.debug("Infrastructure payload missing component dictionaries")
        return

    now = time.time()
    video_capable: list[tuple[str, Optional[bool]]] = []
    with robots_lock:
        seen_by_type: Dict[str, set[str]] = {}
        for component_type, components in components_payload.items():
            if not isinstance(components, dict):
                continue
            seen: set[str] = set()
            for component_id, meta in components.items():
                if not isinstance(meta, dict):
                    meta = {}
                robot_key = _component_key(system, component_type, component_id)
                entry = discovered_robots.get(robot_key, {"key": robot_key})
                entry["key"] = robot_key
                entry["system"] = system
                entry["id"] = component_id
                entry["type"] = component_type
                entry["model"] = meta.get("model")
                entry["name"] = meta.get("name") or CONFIG_ROBOT_MAP.get(robot_key, {}).get("name") or component_id
                entry["lastSeen"] = now
                _apply_robot_hints(robot_key, entry)
                if entry.get("hasVideo"):
                    video_capable.append((robot_key, entry.get("videoControls")))
                discovered_robots[robot_key] = entry
                seen.add(robot_key)
            seen_by_type[component_type] = seen

        for component_type, seen in seen_by_type.items():
            removed = {
                key
                for key, meta in discovered_robots.items()
                if meta.get("system") == system and meta.get("type") == component_type
            } - seen
            if component_type == "robots":
                removed -= {
                    key
                    for key, meta in CONFIG_ROBOT_MAP.items()
                    if meta.get("system") == system and (meta.get("type") or "robots") == component_type
                }
            for robot_key in removed:
                discovered_robots.pop(robot_key, None)
                with telemetry_lock:
                    telemetry_cache.pop(robot_key, None)
                _clear_video_state(robot_key)
                _clear_audio_state(robot_key)
                _clear_soundboard_state(robot_key)
                _clear_outlets_state(robot_key)
                _clear_autonomy_state(robot_key)
                _clear_component_logs(robot_key)

        infrastructure_last_update = now

    for robot_id, controls in video_capable:
        _mark_robot_video_capable(robot_id, controls)


def _ui_robot_snapshot() -> list[Dict[str, Any]]:
    with robots_lock:
        robot_items = sorted(
            discovered_robots.values(),
            key=lambda r: (r.get("system") or "", r.get("name") or r.get("id") or "", r.get("type") or "robots"),
        )
    with telemetry_lock:
        telemetry_snapshot = {robot_id: data.copy() for robot_id, data in telemetry_cache.items()}

    now = time.time()
    snapshot: list[Dict[str, Any]] = []
    for robot in robot_items:
        robot_key = str(robot.get("key") or _robot_key(str(robot.get("id") or "")))
        state = _heartbeat_connection_state(telemetry_snapshot.get(robot_key, {}), now)
        snapshot.append(
            {
                "key": robot_key,
                "id": robot["id"],
                "system": robot.get("system") or "",
                "type": robot.get("type") or "robots",
                "name": robot.get("name") or robot["id"],
                "model": robot.get("model"),
                "hasDrive": _robot_has_drive(robot_key),
                "hasLights": _robot_has_lights(robot_key),
                "hasImu": _robot_has_imu(robot_key),
                "hasOdometry": _robot_has_odometry(robot_key),
                "hasVideo": _robot_has_video(robot_key),
                "hasControls": _robot_has_video_controls(robot_key),
                "hasAudio": _robot_has_audio(robot_key),
                "audioControls": _robot_has_audio_controls(robot_key),
                "hasSoundboard": _robot_has_soundboard(robot_key),
                "soundboardControls": _robot_has_soundboard_controls(robot_key),
                "hasOutlets": _robot_has_outlets(robot_key),
                "outletControls": _robot_has_outlet_controls(robot_key),
                "hasAutonomy": _robot_has_autonomy(robot_key),
                "autonomyControls": _robot_has_autonomy_controls(robot_key),
                "rebootControls": _robot_has_reboot_controls(robot_key),
                "serviceRestartControls": _robot_has_service_restart_controls(robot_key),
                "gitPullControls": _robot_has_git_pull_controls(robot_key),
                "online": state["online"],
                "connectionStatus": state["status"],
                "lastSeen": robot.get("lastSeen"),
                "webrtcUrl": robot.get("webrtcUrl"),
            }
        )
    return snapshot


def _require_video_robot(robot_id: str) -> Dict[str, Any]:
    if not VIDEO_SUPPORT:
        abort(503, description="Video support not available (missing opencv-python or numpy).")
    robot = _get_robot(robot_id)
    if not robot or not _robot_has_video(robot_id):
        abort(404, description=f"No video stream configured for component '{robot_id}'.")
    return robot


def _require_audio_robot(robot_id: str) -> Dict[str, Any]:
    robot = _get_robot(robot_id)
    if not robot or not _robot_has_audio(robot_id):
        abort(404, description=f"No audio stream configured for component '{robot_id}'.")
    return robot


def _normalize_soundboard_file_list(raw_list: Any) -> list[str]:
    if not isinstance(raw_list, list):
        return []
    files: list[str] = []
    for item in raw_list:
        if not isinstance(item, str):
            continue
        clean = item.strip()
        if not clean:
            continue
        if not clean.lower().endswith(".wav"):
            continue
        files.append(clean.replace("\\", "/"))
    # Preserve original case while deduplicating by lowercase path.
    dedup: Dict[str, str] = {}
    for file_name in files:
        dedup[file_name.lower()] = file_name
    return sorted(dedup.values(), key=lambda value: value.lower())


def _handle_soundboard_files_payload(robot_id: str, payload: Any) -> None:
    robot_key = _robot_key(robot_id)
    if isinstance(payload, list):
        payload_dict: Dict[str, Any] = {}
        container: Any = payload
    elif isinstance(payload, dict):
        payload_dict = payload
        value = payload.get("value")
        container = value if isinstance(value, (dict, list)) else payload
    else:
        return
    raw_files = container if isinstance(container, list) else container.get("files")
    files = _normalize_soundboard_file_list(raw_files)
    if not files and raw_files is not None:
        return

    controls_raw = payload_dict.get("controls")
    if isinstance(container, dict) and "controls" in container:
        controls_raw = container.get("controls")
    controls = bool(controls_raw) if isinstance(controls_raw, (bool, int)) else None

    timestamp_raw = payload_dict.get("timestamp", time.time())
    if isinstance(container, dict) and container.get("timestamp") is not None:
        timestamp_raw = container.get("timestamp")
    try:
        timestamp = float(timestamp_raw)
    except (TypeError, ValueError):
        timestamp = time.time()

    with soundboard_cache_lock:
        soundboard_files_cache[robot_key] = {"files": files, "timestamp": timestamp}
    _mark_robot_soundboard_capable(robot_key, controls)


def _handle_soundboard_status_payload(robot_id: str, payload: Any) -> None:
    robot_key = _robot_key(robot_id)
    if not isinstance(payload, dict):
        return
    value = payload.get("value")
    status = value if isinstance(value, dict) else payload

    playing: Optional[bool] = None
    playing_raw = status.get("playing")
    if isinstance(playing_raw, (bool, int)):
        playing = bool(playing_raw)
    elif isinstance(status.get("state"), str):
        state_lower = status["state"].strip().lower()
        if state_lower in {"play", "playing", "running"}:
            playing = True
        elif state_lower in {"stop", "stopped", "idle"}:
            playing = False

    file_name_raw = status.get("file")
    file_name = str(file_name_raw).strip() if isinstance(file_name_raw, str) else None
    if file_name == "":
        file_name = None
    error_raw = status.get("error")
    error_message = str(error_raw).strip() if isinstance(error_raw, str) and str(error_raw).strip() else None

    controls_raw = payload.get("controls")
    if "controls" in status:
        controls_raw = status.get("controls")
    controls = bool(controls_raw) if isinstance(controls_raw, (bool, int)) else None

    timestamp_raw = payload.get("timestamp", time.time())
    if status.get("timestamp") is not None:
        timestamp_raw = status.get("timestamp")
    try:
        timestamp = float(timestamp_raw)
    except (TypeError, ValueError):
        timestamp = time.time()

    with soundboard_cache_lock:
        existing = soundboard_status_cache.get(robot_key, {})
        if playing is None:
            playing = bool(existing.get("playing", False))
        if not file_name:
            file_name = existing.get("file")
        soundboard_status_cache[robot_key] = {
            "playing": bool(playing),
            "file": file_name,
            "error": error_message,
            "timestamp": timestamp,
        }
    _mark_robot_soundboard_capable(robot_key, controls)


def _outlet_sort_key(outlet_id: str) -> tuple[str, int, str]:
    cleaned = str(outlet_id or "").strip()
    prefix = cleaned.rstrip("0123456789")
    suffix = cleaned[len(prefix) :]
    try:
        index = int(suffix) if suffix else -1
    except ValueError:
        index = -1
    return prefix, index, cleaned


def _handle_outlet_status_payload(robot_id: str, outlet_id: str, payload: Any) -> None:
    if not isinstance(payload, dict):
        return
    clean_outlet_id = str(outlet_id or "").strip().strip("/")
    if not clean_outlet_id:
        return
    value = payload.get("value") if isinstance(payload.get("value"), dict) else {}
    if not isinstance(value, dict):
        value = {}
    robot_key = _robot_key(robot_id)
    status = {
        "id": clean_outlet_id,
        "name": str(payload.get("label") or payload.get("name") or clean_outlet_id),
        "profile": payload.get("profile"),
        "interface": payload.get("interface"),
        "device_uid": payload.get("device_uid"),
        "timestamp": payload.get("t") or payload.get("timestamp"),
        "bound": bool(value.get("bound", False)),
        "reachable": bool(value.get("reachable", False)),
        "on": bool(value.get("on", False)),
        "raw": payload,
        "updated_at": time.time(),
    }
    with outlets_cache_lock:
        outlet_map = outlets_status_cache.setdefault(robot_key, {})
        outlet_map[clean_outlet_id] = status
    _mark_robot_outlets_capable(robot_key, True)


def _outlets_payload(robot_id: str) -> Dict[str, Any]:
    robot_key = _robot_key(robot_id)
    with outlets_cache_lock:
        outlet_map = {key: value.copy() for key, value in (outlets_status_cache.get(robot_key) or {}).items()}

    now = time.time()
    outlets = []
    for outlet_id in sorted(outlet_map, key=_outlet_sort_key):
        item = outlet_map[outlet_id]
        updated_at = item.get("updated_at")
        age = (now - float(updated_at)) if updated_at is not None else None
        outlets.append(
            {
                "id": outlet_id,
                "name": item.get("name") or outlet_id,
                "profile": item.get("profile"),
                "interface": item.get("interface"),
                "deviceUid": item.get("device_uid"),
                "topic": _outlet_status_topic_for_robot(robot_id, outlet_id),
                "commandTopic": _outlet_power_topic_for_robot(robot_id, outlet_id),
                "bound": bool(item.get("bound", False)),
                "reachable": bool(item.get("reachable", False)),
                "on": bool(item.get("on", False)),
                "lastUpdateAge": age,
            }
        )

    return {
        "enabled": _robot_has_outlets(robot_id),
        "controls": _robot_has_outlet_controls(robot_id),
        "topicPrefix": _topic(robot_id, "outgoing", "outlets"),
        "commandPrefix": _topic(robot_id, "incoming", "outlets"),
        "outlets": outlets,
    }


def _soundboard_files_payload(robot_id: str) -> Dict[str, Any]:
    robot_key = _robot_key(robot_id)
    with soundboard_cache_lock:
        entry = soundboard_files_cache.get(robot_key) or {}
    timestamp = entry.get("timestamp")
    age = time.time() - float(timestamp) if timestamp else None
    return {
        "enabled": _robot_has_soundboard(robot_id),
        "controls": _robot_has_soundboard_controls(robot_id),
        "topic": _soundboard_files_topic_for_robot(robot_id),
        "commandTopic": _soundboard_command_topic_for_robot(robot_id),
        "files": entry.get("files") or [],
        "lastUpdateAge": age,
    }


def _soundboard_status_payload(robot_id: str) -> Dict[str, Any]:
    robot_key = _robot_key(robot_id)
    with soundboard_cache_lock:
        entry = soundboard_status_cache.get(robot_key) or {}
    timestamp = entry.get("timestamp")
    age = time.time() - float(timestamp) if timestamp else None
    return {
        "enabled": _robot_has_soundboard(robot_id),
        "controls": _robot_has_soundboard_controls(robot_id),
        "topic": _soundboard_status_topic_for_robot(robot_id),
        "commandTopic": _soundboard_command_topic_for_robot(robot_id),
        "playing": bool(entry.get("playing", False)),
        "file": entry.get("file"),
        "error": entry.get("error"),
        "lastUpdateAge": age,
    }


def _normalize_autonomy_configs(raw_list: Any) -> list[dict[str, Any]]:
    if not isinstance(raw_list, list):
        return []
    normalized: list[dict[str, Any]] = []
    for item in raw_list:
        if not isinstance(item, dict):
            continue
        key_raw = item.get("key")
        key = str(key_raw).strip() if isinstance(key_raw, str) else ""
        if not key:
            continue
        field_type = str(item.get("type") or "string").strip().lower()
        if field_type not in {"string", "int", "float", "bool", "enum"}:
            field_type = "string"
        entry: dict[str, Any] = {
            "key": key,
            "label": str(item.get("label") or key),
            "type": field_type,
            "default": item.get("default"),
        }
        if isinstance(item.get("arg"), str) and item.get("arg"):
            entry["arg"] = item.get("arg")
        if isinstance(item.get("required"), bool):
            entry["required"] = bool(item.get("required"))
        if field_type in {"int", "float"}:
            if item.get("min") is not None:
                entry["min"] = item.get("min")
            if item.get("max") is not None:
                entry["max"] = item.get("max")
            if item.get("step") is not None:
                entry["step"] = item.get("step")
        if field_type == "enum":
            options = item.get("options") if isinstance(item.get("options"), list) else []
            normalized_options = [str(option) for option in options if str(option).strip()]
            if normalized_options:
                entry["options"] = normalized_options
        normalized.append(entry)
    return normalized


def _normalize_autonomy_files(raw_list: Any) -> list[dict[str, Any]]:
    if not isinstance(raw_list, list):
        return []
    files: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in raw_list:
        if isinstance(item, str):
            file_name = item.strip()
            if not file_name:
                continue
            key = file_name.lower()
            if key in seen:
                continue
            seen.add(key)
            files.append({"file": file_name, "label": file_name, "configs": []})
            continue
        if not isinstance(item, dict):
            continue
        file_raw = item.get("file")
        file_name = str(file_raw).strip() if isinstance(file_raw, str) else ""
        if not file_name:
            continue
        key = file_name.lower()
        if key in seen:
            continue
        seen.add(key)
        files.append(
            {
                "file": file_name,
                "label": str(item.get("label") or file_name),
                "configs": _normalize_autonomy_configs(item.get("configs")),
            }
        )
    return files


def _handle_autonomy_files_payload(robot_id: str, payload: Any) -> None:
    robot_key = _robot_key(robot_id)
    if isinstance(payload, list):
        payload_dict: Dict[str, Any] = {}
        container: Any = payload
    elif isinstance(payload, dict):
        payload_dict = payload
        value = payload.get("value")
        container = value if isinstance(value, (dict, list)) else payload
    else:
        return

    raw_files = container if isinstance(container, list) else container.get("files")
    files = _normalize_autonomy_files(raw_files)
    if not files and raw_files is not None:
        return

    controls_raw = payload_dict.get("controls")
    if isinstance(container, dict) and "controls" in container:
        controls_raw = container.get("controls")
    controls = bool(controls_raw) if isinstance(controls_raw, (bool, int)) else None

    timestamp_raw = payload_dict.get("timestamp", time.time())
    if isinstance(container, dict) and container.get("timestamp") is not None:
        timestamp_raw = container.get("timestamp")
    try:
        timestamp = float(timestamp_raw)
    except (TypeError, ValueError):
        timestamp = time.time()

    with autonomy_cache_lock:
        autonomy_files_cache[robot_key] = {"files": files, "timestamp": timestamp}
    _mark_robot_autonomy_capable(robot_key, controls)


def _handle_autonomy_status_payload(robot_id: str, payload: Any) -> None:
    robot_key = _robot_key(robot_id)
    if not isinstance(payload, dict):
        return
    value = payload.get("value")
    status = value if isinstance(value, dict) else payload

    running_raw = status.get("running")
    running: Optional[bool] = bool(running_raw) if isinstance(running_raw, (bool, int)) else None
    file_raw = status.get("file")
    file_name = str(file_raw).strip() if isinstance(file_raw, str) and str(file_raw).strip() else None
    pid = int(status.get("pid")) if isinstance(status.get("pid"), (int, float)) else None
    config = status.get("config") if isinstance(status.get("config"), dict) else None
    error_raw = status.get("error")
    error_message = str(error_raw).strip() if isinstance(error_raw, str) and str(error_raw).strip() else None

    controls_raw = payload.get("controls")
    if "controls" in status:
        controls_raw = status.get("controls")
    controls = bool(controls_raw) if isinstance(controls_raw, (bool, int)) else None

    timestamp_raw = payload.get("timestamp", time.time())
    if status.get("timestamp") is not None:
        timestamp_raw = status.get("timestamp")
    try:
        timestamp = float(timestamp_raw)
    except (TypeError, ValueError):
        timestamp = time.time()

    with autonomy_cache_lock:
        existing = autonomy_status_cache.get(robot_key, {})
        if running is None:
            running = bool(existing.get("running", False))
        if file_name is None and running:
            file_name = existing.get("file")
        autonomy_status_cache[robot_key] = {
            "running": bool(running),
            "file": file_name if running else None,
            "pid": pid,
            "error": error_message,
            "config": config if isinstance(config, dict) else existing.get("config"),
            "timestamp": timestamp,
        }
    _mark_robot_autonomy_capable(robot_key, controls)


def _autonomy_files_payload(robot_id: str) -> Dict[str, Any]:
    robot_key = _robot_key(robot_id)
    with autonomy_cache_lock:
        entry = autonomy_files_cache.get(robot_key) or {}
    timestamp = entry.get("timestamp")
    age = time.time() - float(timestamp) if timestamp else None
    return {
        "enabled": _robot_has_autonomy(robot_id),
        "controls": _robot_has_autonomy_controls(robot_id),
        "topic": _autonomy_files_topic_for_robot(robot_id),
        "commandTopic": _autonomy_command_topic_for_robot(robot_id),
        "files": entry.get("files") or [],
        "lastUpdateAge": age,
    }


def _autonomy_status_payload(robot_id: str) -> Dict[str, Any]:
    robot_key = _robot_key(robot_id)
    with autonomy_cache_lock:
        entry = autonomy_status_cache.get(robot_key) or {}
    timestamp = entry.get("timestamp")
    age = time.time() - float(timestamp) if timestamp else None
    return {
        "enabled": _robot_has_autonomy(robot_id),
        "controls": _robot_has_autonomy_controls(robot_id),
        "topic": _autonomy_status_topic_for_robot(robot_id),
        "commandTopic": _autonomy_command_topic_for_robot(robot_id),
        "running": bool(entry.get("running", False)),
        "file": entry.get("file"),
        "pid": entry.get("pid"),
        "error": entry.get("error"),
        "config": entry.get("config") if isinstance(entry.get("config"), dict) else {},
        "lastUpdateAge": age,
    }


def _clear_component_logs(robot_id: str) -> None:
    robot_key = _robot_key(robot_id)
    with component_logs_cache_lock:
        component_logs_cache.pop(robot_key, None)


def _normalize_component_log_timestamp_ms(value: Any) -> int:
    parsed = _coerce_float(value)
    if parsed is None or parsed <= 0:
        return int(time.time() * 1000)
    if parsed >= 1e11:
        return int(parsed)
    if parsed >= 1e9:
        return int(parsed * 1000)
    return int(time.time() * 1000)


def _handle_component_logs_payload(component_id: str, payload: Any) -> None:
    robot_key = _robot_key(component_id)
    if isinstance(payload, dict):
        value = payload.get("value")
        record = value if isinstance(value, dict) else payload
    elif isinstance(payload, str):
        record = {"message": payload}
    else:
        record = {"message": json.dumps(payload, ensure_ascii=True)}

    message_value = record.get("message")
    if not isinstance(message_value, str) or not message_value.strip():
        message_value = record.get("msg")
    if not isinstance(message_value, str) or not message_value.strip():
        message_value = json.dumps(record, ensure_ascii=True)
    message = message_value.strip()
    if not message:
        return

    level_raw = record.get("level")
    level = str(level_raw).strip().upper() if isinstance(level_raw, str) and level_raw.strip() else "INFO"
    service_raw = record.get("service")
    service = str(service_raw).strip() if isinstance(service_raw, str) and service_raw.strip() else "component"
    logger_raw = record.get("logger")
    logger_name = str(logger_raw).strip() if isinstance(logger_raw, str) and logger_raw.strip() else None
    pid = int(record.get("pid")) if isinstance(record.get("pid"), (int, float)) else None
    timestamp_ms = _normalize_component_log_timestamp_ms(record.get("t", record.get("timestamp")))
    timestamp_s = timestamp_ms / 1000.0

    entry = {
        "timestamp": timestamp_s,
        "timestampMs": timestamp_ms,
        "level": level,
        "service": service,
        "message": message,
        "logger": logger_name,
        "pid": pid,
    }
    with component_logs_cache_lock:
        records = component_logs_cache.setdefault(robot_key, deque(maxlen=COMPONENT_LOGS_MAX_ENTRIES))
        records.append(entry)


def _component_logs_payload(robot_id: str, limit: int = 250) -> Dict[str, Any]:
    robot_key = _robot_key(robot_id)
    safe_limit = max(1, min(2000, int(limit)))
    with component_logs_cache_lock:
        records = component_logs_cache.get(robot_key)
        entries = list(records)[-safe_limit:] if records else []
    last_ts = entries[-1]["timestamp"] if entries else None
    age = time.time() - float(last_ts) if isinstance(last_ts, (int, float)) else None
    return {
        "topic": _logs_topic_for_robot(robot_id),
        "entries": entries,
        "lastUpdateAge": age,
        "maxEntries": COMPONENT_LOGS_MAX_ENTRIES,
    }


def _normalize_overlay_shapes(raw_shapes: Any) -> list[list[list[float]]]:
    if not isinstance(raw_shapes, list):
        return []
    shapes: list[list[list[float]]] = []
    for shape in raw_shapes:
        if not isinstance(shape, list):
            continue
        points: list[list[float]] = []
        for point in shape:
            if not isinstance(point, (list, tuple)) or len(point) < 2:
                continue
            x_raw = _coerce_float(point[0])
            y_raw = _coerce_float(point[1])
            if x_raw is None or y_raw is None:
                continue
            points.append([clamp(x_raw, 0.0, 1.0), clamp(y_raw, 0.0, 1.0)])
        if len(points) >= 2:
            shapes.append(points)
    return shapes


def _handle_video_overlays_payload(robot_id: str, payload: Any) -> None:
    robot_key = _robot_key(robot_id)
    if isinstance(payload, list):
        payload_dict: Dict[str, Any] = {}
        container: Any = {"shapes": payload}
    elif isinstance(payload, dict):
        payload_dict = payload
        value = payload.get("value")
        container = value if isinstance(value, dict) else payload
    else:
        return

    shapes_raw = None
    if isinstance(container, dict):
        shapes_raw = container.get("shapes")
        if shapes_raw is None:
            # Backward-compat with apriltag payload shape.
            corners = container.get("corners_norm")
            detected = bool(container.get("detected", bool(corners)))
            if detected and isinstance(corners, list) and corners:
                shapes_raw = [corners]
            else:
                shapes_raw = []
    if shapes_raw is None:
        shapes_raw = []
    shapes = _normalize_overlay_shapes(shapes_raw)

    timestamp_raw = payload_dict.get("timestamp", time.time())
    if isinstance(container, dict) and container.get("timestamp") is not None:
        timestamp_raw = container.get("timestamp")
    try:
        timestamp = float(timestamp_raw)
    except (TypeError, ValueError):
        timestamp = time.time()

    frame_width = None
    frame_height = None
    if isinstance(container, dict):
        fw = _coerce_int(container.get("frame_width"), 0)
        fh = _coerce_int(container.get("frame_height"), 0)
        frame_width = fw if fw > 0 else None
        frame_height = fh if fh > 0 else None

    source = container.get("source") if isinstance(container, dict) else None
    source_str = str(source).strip() if isinstance(source, str) and source.strip() else None

    with video_overlays_cache_lock:
        video_overlays_cache[robot_key] = {
            "shapes": shapes,
            "timestamp": timestamp,
            "frame_width": frame_width,
            "frame_height": frame_height,
            "source": source_str,
        }
    _mark_robot_video_capable(robot_key)


def _video_overlays_payload(robot_id: str) -> Dict[str, Any]:
    robot_key = _robot_key(robot_id)
    with video_overlays_cache_lock:
        entry = video_overlays_cache.get(robot_key) or {}
    timestamp = entry.get("timestamp")
    age = time.time() - float(timestamp) if timestamp else None
    return {
        "enabled": _robot_has_video(robot_id),
        "controls": _robot_has_video_controls(robot_id),
        "topic": _video_overlays_topic_for_robot(robot_id),
        "shapes": entry.get("shapes") or [],
        "frameWidth": entry.get("frame_width"),
        "frameHeight": entry.get("frame_height"),
        "source": entry.get("source"),
        "lastUpdateAge": age,
    }


def _handle_video_payload(robot_id: str, payload_bytes: bytes) -> None:
    if not VIDEO_SUPPORT:
        return
    robot_key = _robot_key(robot_id)

    _mark_robot_video_capable(robot_key)
    condition = _get_video_condition(robot_key)
    frame_bytes: Optional[bytes] = None
    metadata: Dict[str, Any] = {}

    if len(payload_bytes) >= 2 and payload_bytes[0] == 0xFF and payload_bytes[1] == 0xD8:
        frame_bytes = payload_bytes
        metadata = {"keyframe": True, "transport": "binary_jpeg", "timestamp": time.time()}
    else:
        try:
            packet = json.loads(payload_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            logging.debug("Unsupported video payload from %s", robot_id)
            return

        decoded = _decode_video_frame(robot_key, packet)
        if decoded is None:
            return
        frame_bytes, metadata = decoded

    with video_cache_lock:
        entry = video_cache.setdefault(robot_key, {})
        entry.update({"jpeg": frame_bytes, "timestamp": time.time(), "metadata": metadata})
    with condition:
        condition.notify_all()


def _decode_video_frame(robot_id: str, packet: Dict[str, Any]) -> Optional[tuple[bytes, Dict[str, Any]]]:
    if not VIDEO_SUPPORT:
        return None
    robot_key = _robot_key(robot_id)
    encoded = packet.get("data")
    if not encoded:
        return None

    try:
        compressed = base64.b64decode(encoded)
        jpeg_bytes = zlib.decompress(compressed)
    except (base64.binascii.Error, zlib.error) as exc:
        logging.debug("Compressed frame invalid for %s: %s", robot_id, exc)
        return None

    frame_array = np.frombuffer(jpeg_bytes, dtype=np.uint8)
    frame = cv2.imdecode(frame_array, cv2.IMREAD_COLOR)
    if frame is None:
        logging.debug("cv2.imdecode returned None for %s", robot_id)
        return None

    is_keyframe = bool(packet.get("keyframe"))
    last_frame = video_last_frames.get(robot_key)
    if not is_keyframe and last_frame is None:
        logging.debug("Dropping delta frame before keyframe for %s", robot_id)
        return None

    if is_keyframe or last_frame is None:
        reconstructed = frame
    else:
        delta = frame.astype(np.int16) - 128
        base_frame = last_frame.astype(np.int16)
        reconstructed = np.clip(base_frame + delta, 0, 255).astype(np.uint8)

    video_last_frames[robot_key] = reconstructed

    success, encoded_frame = cv2.imencode(".jpg", reconstructed)
    if not success:
        logging.debug("Failed to encode reconstructed frame for %s", robot_id)
        return None

    metadata: Dict[str, Any] = {
        "id": packet.get("id"),
        "keyframe": is_keyframe,
        "timestamp": packet.get("timestamp"),
    }
    if metadata["timestamp"] is not None:
        metadata["latency_ms"] = int((time.time() - float(metadata["timestamp"])) * 1000)

    return encoded_frame.tobytes(), metadata


def _handle_audio_payload(robot_id: str, payload_bytes: bytes) -> None:
    placeholder = _ensure_robot_placeholder(robot_id)
    robot_key = str(placeholder.get("key") or _robot_key(robot_id))
    packet = _decode_audio_packet(payload_bytes)
    if not packet:
        logging.debug("Invalid binary audio payload from %s", robot_id)
        return

    hint = _config_hint(CONFIG_AUDIO_HINTS, robot_key)
    rate = max(1, _coerce_int(packet.get("rate"), int(hint.get("rate", 16000))))
    channels = max(1, _coerce_int(packet.get("channels"), int(hint.get("channels", 1))))
    frame_samples = max(1, _coerce_int(packet.get("frame_samples"), 1))
    audio_bytes = packet.get("data") or b""
    timestamp = float(packet.get("timestamp", time.time()))
    seq = max(0, _coerce_int(packet.get("seq"), 0))

    condition = _get_audio_condition(robot_key)
    with audio_cache_lock:
        cache = audio_cache.setdefault(robot_key, {"seq": 0, "generation": 0})
        previous_seq = max(0, _coerce_int(cache.get("seq"), 0))
        reset_stream = previous_seq > 0 and seq > 0 and (seq + 5) < previous_seq
        if reset_stream:
            buffer = audio_buffers.setdefault(robot_key, deque(maxlen=120))
            buffer.clear()
            cache["generation"] = int(cache.get("generation", 0)) + 1
        cache.update(
            {"seq": seq, "timestamp": timestamp, "rate": rate, "channels": channels, "frame_samples": frame_samples}
        )
        buffer = audio_buffers.setdefault(robot_key, deque(maxlen=AUDIO_STREAM_BUFFER_PACKETS))
        buffer.append(
            {
                "seq": seq,
                "timestamp": timestamp,
                "rate": rate,
                "channels": channels,
                "frame_samples": frame_samples,
                "data": audio_bytes,
            }
        )
    _mark_robot_audio_capable(robot_key, hint.get("controls"))
    with condition:
        condition.notify_all()


def _on_mqtt_connect(client: mqtt.Client, _userdata: Any, _flags: Dict[str, Any], rc: int) -> None:
    global mqtt_connecting, mqtt_last_error
    if rc != 0:
        message = mqtt.connack_string(rc)
        logging.error("MQTT connection failed: %s", message)
        with mqtt_state_lock:
            mqtt_connecting = False
            mqtt_last_error = message
        mqtt_connected.clear()
        try:
            client.disconnect()
        except Exception:
            pass
        return
    if mqtt_history_enabled and mqtt_history_collection is None:
        _init_mqtt_history_logger()

    logging.info("MQTT connected; subscribing to telemetry topics.")
    subscribe_all = (
        mqtt_history_enabled and mqtt_history_collection is not None and bool(MQTT_HISTORY_CFG.get("subscribe_all_topics", True))
    )
    if subscribe_all:
        logging.info("MQTT history logging active; subscribing to all topics (#).")
        client.subscribe("#", qos=0)
    else:
        client.subscribe("+/infrastructure", qos=1)
        client.subscribe("+/+/+/outgoing/touch-sensors", qos=1)
        client.subscribe("+/+/+/outgoing/charging-status", qos=1)
        client.subscribe("+/+/+/outgoing/charging-level", qos=1)
        client.subscribe("+/+/+/outgoing/online", qos=1)
        client.subscribe("+/+/+/outgoing/heartbeat", qos=1)
        client.subscribe("+/+/+/outgoing/capabilities", qos=1)
        client.subscribe("+/+/+/outgoing/status", qos=1)
        client.subscribe("+/+/+/outgoing/logs", qos=1)
        client.subscribe("+/+/+/outgoing/audio", qos=0)
        client.subscribe("+/+/+/outgoing/outlets/#", qos=1)
        client.subscribe("+/+/+/outgoing/soundboard-files", qos=1)
        client.subscribe("+/+/+/outgoing/soundboard-status", qos=1)
        client.subscribe("+/+/+/outgoing/autonomy-files", qos=1)
        client.subscribe("+/+/+/outgoing/autonomy-status", qos=1)
        client.subscribe("+/+/+/outgoing/wheel-odometry", qos=1)
        client.subscribe("+/+/+/outgoing/sensors/imu", qos=1)
        client.subscribe("+/+/+/outgoing/video-overlays", qos=1)
        for topic in CONFIG_AUDIO_TOPICS:
            logging.info("Subscribing to configured audio topic %s", topic)
            client.subscribe(topic, qos=0)
        if VIDEO_SUPPORT:
            client.subscribe("+/+/+/outgoing/front-camera", qos=0)
            for topic in CONFIG_VIDEO_TOPICS:
                logging.info("Subscribing to configured video topic %s", topic)
                client.subscribe(topic, qos=0)
        else:
            logging.info("Video support disabled (missing opencv-python or numpy).")
    with mqtt_state_lock:
        mqtt_connecting = False
        mqtt_last_error = None
    mqtt_connected.set()


def _on_mqtt_disconnect(_client: mqtt.Client, _userdata: Any, rc: int) -> None:
    global mqtt_connecting, mqtt_last_error
    if rc != mqtt.MQTT_ERR_SUCCESS:
        logging.warning("Unexpected MQTT disconnect (rc=%s).", rc)
        with mqtt_state_lock:
            mqtt_connecting = True
            mqtt_last_error = f"Disconnected (rc={rc})"
    else:
        with mqtt_state_lock:
            mqtt_connecting = False
            mqtt_last_error = None
    mqtt_connected.clear()


def _parse_component_topic(topic: str) -> Optional[tuple[str, str, str, str, str]]:
    parts = topic.split("/")
    if len(parts) < 5:
        return None
    return parts[0], parts[1], parts[2], parts[3], "/".join(parts[4:])


def _parse_infrastructure_topic(topic: str) -> Optional[str]:
    parts = topic.split("/")
    if len(parts) == 2 and parts[1] == "infrastructure" and parts[0]:
        return parts[0]
    return None


def _on_mqtt_message(_client: mqtt.Client, _userdata: Any, msg: mqtt.MQTTMessage) -> None:
    _log_mqtt_history(msg)

    system = _parse_infrastructure_topic(msg.topic)
    if system:
        _handle_infrastructure_payload(system, msg.payload)
        return

    audio_robot_id = CONFIG_AUDIO_TOPICS.get(msg.topic)
    audio_system = None
    audio_component_type = None
    audio_component_id = None
    if audio_robot_id is None and msg.topic.endswith("/outgoing/audio"):
        parsed = _parse_component_topic(msg.topic)
        if parsed and parsed[3] == "outgoing" and parsed[4] == "audio":
            audio_system, audio_component_type, audio_component_id = parsed[0], parsed[1], parsed[2]
            audio_robot_id = _component_key(audio_system, audio_component_type, audio_component_id)
    if audio_robot_id:
        _ensure_robot_placeholder(
            audio_robot_id,
            audio_component_type,
            system=audio_system,
            component_id=audio_component_id,
        )
        _handle_audio_payload(audio_robot_id, msg.payload)
        return

    video_robot_id = None
    video_system = None
    video_component_type = None
    video_component_id = None
    if VIDEO_SUPPORT:
        video_robot_id = CONFIG_VIDEO_TOPICS.get(msg.topic)
        if video_robot_id is None and msg.topic.endswith("/outgoing/front-camera"):
            parsed = _parse_component_topic(msg.topic)
            if parsed and parsed[3] == "outgoing" and parsed[4] == "front-camera":
                video_system, video_component_type, video_component_id = parsed[0], parsed[1], parsed[2]
                video_robot_id = _component_key(video_system, video_component_type, video_component_id)
    if video_robot_id:
        _ensure_robot_placeholder(
            video_robot_id,
            video_component_type,
            system=video_system,
            component_id=video_component_id,
        )
        _handle_video_payload(video_robot_id, msg.payload)
        return

    parsed = _parse_component_topic(msg.topic)
    if not parsed:
        return
    system, component_type, component_id, direction, metric = parsed
    robot_key = _component_key(system, component_type, component_id)
    if direction != "outgoing":
        return
    if not _get_robot(robot_key):
        _ensure_robot_placeholder(robot_key, component_type, system=system, component_id=component_id)

    if metric == "logs":
        payload: Any
        try:
            decoded = msg.payload.decode("utf-8")
            payload = json.loads(decoded)
        except (UnicodeDecodeError, json.JSONDecodeError):
            payload = msg.payload.decode("utf-8", errors="replace")
        _handle_component_logs_payload(robot_key, payload)
        return

    try:
        payload = json.loads(msg.payload.decode("utf-8"))
    except json.JSONDecodeError:
        logging.debug("Non-JSON MQTT payload on %s", msg.topic)
        return
    if metric == "touch-sensors":
        _handle_touch_payload(robot_key, payload, component_type, system)
    elif metric == "charging-status":
        _handle_charge_payload(robot_key, payload, component_type, system)
    elif metric == "charging-level":
        _handle_charge_level_payload(robot_key, payload, component_type, system)
    elif metric == "status":
        _handle_status_payload(robot_key, payload, component_type, system)
    elif metric == "capabilities":
        if isinstance(payload, dict):
            _handle_capabilities_payload(robot_key, payload, component_type, system)
    elif metric in ("online", "heartbeat"):
        if isinstance(payload, dict):
            _handle_heartbeat_payload(robot_key, payload, component_type, system)
    elif metric == "soundboard-files":
        _handle_soundboard_files_payload(robot_key, payload)
    elif metric == "soundboard-status":
        _handle_soundboard_status_payload(robot_key, payload)
    elif metric.startswith("outlets/") and metric.endswith("/status"):
        outlet_parts = metric.split("/")
        if len(outlet_parts) >= 3 and outlet_parts[1]:
            _handle_outlet_status_payload(robot_key, outlet_parts[1], payload)
    elif metric == "autonomy-files":
        _handle_autonomy_files_payload(robot_key, payload)
    elif metric == "autonomy-status":
        _handle_autonomy_status_payload(robot_key, payload)
    elif metric == "wheel-odometry":
        _handle_wheel_odometry_payload(robot_key, payload, component_type, system)
    elif metric == "sensors/imu":
        _handle_imu_payload(robot_key, payload, component_type, system)
    elif metric in ("video-overlays", "apriltag-data"):
        _handle_video_overlays_payload(robot_key, payload)


def _disconnect_mqtt(clear_error: bool = False) -> None:
    global mqtt_client, mqtt_connecting, mqtt_last_error
    client: Optional[mqtt.Client] = None
    with mqtt_state_lock:
        client = mqtt_client
        mqtt_client = None
        mqtt_connected.clear()
        mqtt_connecting = False
        if clear_error:
            mqtt_last_error = None
    if client:
        try:
            client.loop_stop()
        finally:
            try:
                client.disconnect()
            except Exception:
                logging.exception("Failed to cleanly disconnect MQTT client.")


def _connect_mqtt(settings: Dict[str, Any]) -> tuple[bool, Optional[str]]:
    global mqtt_client, mqtt_current_settings, mqtt_connecting, mqtt_last_error
    host = str(settings.get("host", "") or "").strip()
    if not host:
        return False, "Host is required."
    port = _coerce_int(settings.get("port"), 1883)
    keepalive = _coerce_int(settings.get("keepalive"), 60)
    username_raw = settings.get("username")
    username = str(username_raw).strip() if isinstance(username_raw, str) else username_raw
    password_raw = settings.get("password")
    password = str(password_raw) if isinstance(password_raw, str) else password_raw
    if password and not username:
        return False, "Username is required when providing a password."
    tls_cfg = _normalize_tls_config(settings.get("tls") or settings.get("mqtt_tls"))
    try:
        tls_cfg = _resolve_tls_paths(tls_cfg, CONFIG_PATH.parent)
    except ValueError as exc:
        return False, str(exc)

    _disconnect_mqtt()
    client = _create_mqtt_client(client_id="pebble-web")
    if username:
        client.username_pw_set(username, password or None)
    _configure_tls(client, tls_cfg)
    client.on_connect = _on_mqtt_connect
    client.on_disconnect = _on_mqtt_disconnect
    client.on_message = _on_mqtt_message

    logging.info("Connecting to MQTT broker %s:%s...", host, port)
    try:
        client.connect(host, port, keepalive)
    except Exception as exc:  # pragma: no cover - network failure
        error_msg = str(exc)
        logging.error("Failed to connect to MQTT broker: %s", error_msg)
        with mqtt_state_lock:
            mqtt_last_error = error_msg
            mqtt_connecting = False
            mqtt_current_settings = {
                "host": host,
                "port": port,
                "username": username,
                "keepalive": keepalive,
                "tls": tls_cfg,
            }
        return False, error_msg

    client.loop_start()
    with mqtt_state_lock:
        mqtt_client = client
        mqtt_connecting = True
        mqtt_last_error = None
        mqtt_current_settings = {
            "host": host,
            "port": port,
            "username": username,
            "keepalive": keepalive,
            "tls": tls_cfg,
        }
    return True, None


def _mqtt_status() -> Dict[str, Any]:
    with mqtt_state_lock:
        status = {
            "connected": mqtt_connected.is_set(),
            "connecting": mqtt_connecting,
            "host": mqtt_current_settings.get("host"),
            "port": mqtt_current_settings.get("port"),
            "username": mqtt_current_settings.get("username"),
            "keepalive": mqtt_current_settings.get("keepalive"),
            "tls": mqtt_current_settings.get("tls"),
            "error": mqtt_last_error,
        }
    return status


def _robot_options_html() -> str:
    robots_json = json.dumps(_ui_robot_snapshot())
    mqtt_defaults_json = json.dumps(MQTT_DEFAULTS)
    template = """
<!doctype html>
<meta charset="utf-8">
<title>Component Control + Telemetry</title>
<style>
  body { font-family: system-ui, sans-serif; margin:0; }
  .grid { display:grid; grid-template-columns: 1fr 1fr 1fr; gap:18px; padding:18px; }
  @media (max-width: 1200px){ .grid{ grid-template-columns: 1fr 1fr; } }
  @media (max-width: 800px){ .grid{ grid-template-columns: 1fr; } }
  .card { border:1px solid #ccc; border-radius:12px; padding:16px; box-shadow:0 1px 2px rgba(0,0,0,.04); background:#fff; }
  h2 { margin:0 0 10px; font-size:18px; }
  .card-heading { display:flex; align-items:center; justify-content:space-between; gap:10px; }
  .card-collapse-toggle { margin:0; padding:4px 10px; font:600 12px/1.2 ui-monospace,monospace; }
  .card-collapse-body { margin-top:4px; }
  .card.card-collapsed .card-collapse-body { display:none; }
  kbd { border:1px solid #aaa; border-bottom-width:3px; padding:6px 10px; border-radius:6px; margin:4px; display:inline-block; min-width:2ch; }
  .on { background:#dff5ff; }
  #status, #lightStatus { margin-top:10px; font:600 14px/1.2 ui-monospace,monospace; }
  label { display:inline-flex; align-items:center; gap:8px; margin-right:12px; }
  input[type="range"], input[type="number"], select { vertical-align:middle; }
  button { margin:6px 8px 0 0; padding:8px 12px; border-radius:8px; border:1px solid #777; background:#fafafa; cursor:pointer; }
  .row { margin-top:8px; }
  .meter { height: 10px; background:#eee; border-radius:6px; overflow:hidden; }
  .bar { height: 100%; width:0%; background:#88c; transition: width .15s linear; }
  .mono { font:600 14px/1.2 ui-monospace,monospace; }
  .pill { display:inline-block; padding:4px 8px; border-radius:999px; border:1px solid #999; }
  .ok { background:#e8ffe8; }
  .warn { background:#fff5e0; }
  .robot-picker { display:flex; align-items:center; gap:12px; margin-bottom:12px; flex-wrap:wrap; }
  .robot-picker select { min-width:10rem; }
  .component-card { grid-column: span 3; }
  @media (max-width: 1200px){ .component-card{ grid-column: span 2; } }
  @media (max-width: 800px){ .component-card{ grid-column: span 1; } }
  .component-actions { display:flex; flex-wrap:wrap; align-items:center; gap:10px; margin-top:8px; }
  .component-actions button { min-width:7rem; }
  .section-picker { display:flex; justify-content:flex-end; margin:-2px 0 12px; }
  .section-menu { position:relative; }
  .section-menu summary { list-style:none; cursor:pointer; padding:8px 12px; border-radius:999px; border:1px solid #bbb; background:#f8f8f8; font:600 12px/1.2 ui-monospace,monospace; }
  .section-menu summary::-webkit-details-marker { display:none; }
  .section-menu[open] summary { border-bottom-left-radius:12px; border-bottom-right-radius:12px; }
  .section-menu-panel { position:absolute; right:0; top:calc(100% + 8px); min-width:260px; z-index:20; border:1px solid #d6d6d6; border-radius:14px; background:#fff; box-shadow:0 12px 30px rgba(0,0,0,.12); padding:10px; }
  .section-menu-title { margin:0 0 8px; font:600 12px/1.2 ui-monospace,monospace; opacity:.75; }
  .section-toggle-list { display:grid; gap:6px; }
  .section-toggle { display:flex; align-items:flex-start; justify-content:space-between; gap:10px; margin:0; padding:6px 8px; border-radius:10px; background:#fafafa; }
  .section-toggle input { margin-top:2px; }
  .section-toggle-copy { display:grid; gap:2px; }
  .section-toggle-name { font:600 13px/1.2 system-ui,sans-serif; }
  .section-toggle-mode { font:500 11px/1.2 ui-monospace,monospace; opacity:.65; }
  .odometry-card { grid-column: span 2; }
  @media (max-width: 1200px){ .odometry-card{ grid-column: span 1; } }
  .odometry-meta { margin-bottom:8px; }
  .odometry-wrap { width:100%; aspect-ratio:16/9; border:1px solid #d6d6d6; border-radius:10px; overflow:hidden; background:#f8fafc; }
  .odometry-wrap canvas { width:100%; height:100%; display:block; }
  .imu-wrap { width:100%; aspect-ratio:1 / 1; max-height:260px; border:1px solid #d6d6d6; border-radius:10px; overflow:hidden; background:#f8fafc; margin-bottom:10px; }
  .imu-wrap canvas { width:100%; height:100%; display:block; }
  .imu-meta { display:grid; grid-template-columns: 1fr; gap:6px; }
  .video-card { grid-column: span 2; }
  @media (max-width: 1200px){ .video-card{ grid-column: span 1; } }
  .video-wrap { position:relative; width:100%; padding-top:56.25%; background:#000; border-radius:12px; overflow:hidden; }
  .video-wrap img { position:absolute; inset:0; width:100%; height:100%; object-fit:cover; }
  .video-wrap canvas { position:absolute; inset:0; width:100%; height:100%; pointer-events:none; }
  .video-placeholder { position:absolute; inset:0; display:flex; align-items:center; justify-content:center; text-align:center; color:#fff; background:rgba(0,0,0,.5); padding:12px; transition:opacity .2s ease-in-out; }
  .webrtc-wrap { display:none; margin-top:10px; border-radius:18px; overflow:hidden; border:1px solid rgba(255,255,255,.08); background:#0c0f19; }
  .webrtc-wrap iframe { width:100%; aspect-ratio:16/9; border:0; background:#000; }
  .webrtc-controls { display:none; gap:8px; margin-top:10px; align-items:center; }
  .webrtc-controls input { flex:1; min-width:0; padding:8px 10px; border-radius:10px; border:1px solid rgba(255,255,255,.12); background:#0f1117; color:#e7edf3; }
  .webrtc-controls a { color:#bcd7ff; text-decoration:none; font-weight:600; }
  .video-controls { display:flex; flex-wrap:wrap; align-items:center; gap:10px; margin-top:12px; }
  .video-controls button { flex:1 1 auto; min-width:7rem; }
  .video-overlay-toggle { margin-left:auto; font:600 13px/1.2 ui-monospace,monospace; }
  .audio-card { grid-column: span 3; }
  @media (max-width: 1200px){ .audio-card{ grid-column: span 2; } }
  @media (max-width: 800px){ .audio-card{ grid-column: span 1; } }
  .audio-controls { display:flex; flex-wrap:wrap; align-items:center; gap:10px; margin-top:8px; }
  .audio-controls button { flex:1 1 auto; min-width:6.5rem; }
  .audio-indicators { display:flex; flex-wrap:wrap; gap:8px; margin-top:8px; align-items:center; }
  .audio-indicator { display:inline-flex; align-items:center; gap:6px; padding:4px 10px; border-radius:999px; border:1px solid #ccc; background:#f7f7f7; font:600 13px/1.2 ui-monospace,monospace; }
  .audio-indicator .dot { width:10px; height:10px; border-radius:50%; background:#bbb; display:inline-block; }
  .audio-indicator.active { border-color:#0a6; background:#e8ffef; color:#064; }
  .audio-indicator.active .dot { background:#0a6; }
  .outlets-card { grid-column: span 1; }
  .outlet-list { display:grid; gap:10px; }
  .outlet-item { border:1px solid #d6d6d6; border-radius:12px; padding:10px 12px; background:#fafafa; }
  .outlet-head { display:flex; align-items:center; justify-content:space-between; gap:10px; margin-bottom:8px; }
  .outlet-name { font:700 14px/1.2 ui-monospace,monospace; }
  .outlet-state { display:flex; gap:6px; flex-wrap:wrap; }
  .outlet-pill { display:inline-flex; align-items:center; gap:6px; padding:4px 8px; border-radius:999px; border:1px solid #ccc; background:#fff; font:600 12px/1.2 ui-monospace,monospace; }
  .outlet-pill.on { border-color:#1f8f57; background:#e9fff2; color:#145c39; }
  .outlet-pill.off { border-color:#b64b4b; background:#fff0f0; color:#7d2525; }
  .outlet-pill.warn { border-color:#d2a100; background:#fff9df; color:#725800; }
  .outlet-actions { display:flex; align-items:center; gap:8px; flex-wrap:wrap; }
  .outlet-actions button { min-width:7rem; }
  .outlet-empty { padding:10px; color:#666; font:500 13px/1.3 ui-monospace,monospace; border:1px dashed #d6d6d6; border-radius:10px; background:#fafafa; }
  .soundboard-card { grid-column: span 1; }
  .soundboard-controls { display:flex; align-items:center; gap:10px; margin:8px 0; }
  .soundboard-controls button { min-width:6rem; }
  .sound-list { max-height:220px; overflow-y:auto; border:1px solid #d6d6d6; border-radius:10px; background:#fafafa; padding:4px; }
  .sound-item { width:100%; display:block; margin:0; padding:8px 10px; border:0; border-radius:8px; text-align:left; background:transparent; font:500 13px/1.3 ui-monospace,monospace; cursor:pointer; }
  .sound-item:hover { background:#ececec; }
  .sound-item.active { background:#dff5ff; border:1px solid #95d6ff; }
  .sound-empty { padding:10px; color:#666; font:500 13px/1.3 ui-monospace,monospace; }
  .autonomy-card { grid-column: span 2; }
  @media (max-width: 1200px){ .autonomy-card{ grid-column: span 2; } }
  @media (max-width: 800px){ .autonomy-card{ grid-column: span 1; } }
  .autonomy-layout { display:grid; grid-template-columns: 1fr 1fr; gap:12px; }
  @media (max-width: 900px){ .autonomy-layout{ grid-template-columns: 1fr; } }
  .autonomy-controls { display:flex; align-items:center; gap:10px; margin:8px 0; }
  .autonomy-controls button { min-width:7rem; }
  .autonomy-list { max-height:220px; overflow-y:auto; border:1px solid #d6d6d6; border-radius:10px; background:#fafafa; padding:4px; }
  .autonomy-item { width:100%; display:block; margin:0; padding:8px 10px; border:0; border-radius:8px; text-align:left; background:transparent; font:500 13px/1.3 ui-monospace,monospace; cursor:pointer; }
  .autonomy-item:hover { background:#ececec; }
  .autonomy-item.active { background:#dff5ff; border:1px solid #95d6ff; }
  .autonomy-config { border:1px solid #d6d6d6; border-radius:10px; background:#fafafa; padding:10px; min-height:120px; }
  .autonomy-field { display:flex; align-items:center; justify-content:space-between; gap:8px; margin-bottom:8px; }
  .autonomy-field label { margin:0; display:block; font:600 13px/1.2 ui-monospace,monospace; }
  .autonomy-field input, .autonomy-field select { flex:1; min-width:0; }
  .autonomy-empty { color:#666; font:500 13px/1.3 ui-monospace,monospace; }
  .logs-card { grid-column: span 3; }
  @media (max-width: 1200px){ .logs-card{ grid-column: span 2; } }
  @media (max-width: 800px){ .logs-card{ grid-column: span 1; } }
  .logs-controls { display:flex; flex-wrap:wrap; align-items:center; gap:10px; margin:8px 0; }
  .logs-controls button { min-width:6rem; }
  .logs-output {
    max-height:260px;
    overflow:auto;
    border:1px solid #d6d6d6;
    border-radius:10px;
    background:#fbfbfb;
    margin:0;
    padding:10px;
    font:500 12px/1.35 ui-monospace,monospace;
    white-space:pre-wrap;
    word-break:break-word;
  }
  .mono-dim { font:600 13px/1.2 ui-monospace,monospace; opacity:.7; }
  .hidden { display:none !important; }
  .fullscreen-active { position:fixed !important; top:0; left:0; right:0; bottom:0; z-index:1000; padding:0; margin:0; border-radius:0; }
  .fullscreen-active .video-wrap { height:100%; padding-top:0; border-radius:0; }
  .fullscreen-active .video-wrap img { object-fit:contain; }
  .connection-card { grid-column: span 3; }
  @media (max-width: 1200px){ .connection-card{ grid-column: span 2; } }
  @media (max-width: 800px){ .connection-card{ grid-column: span 1; } }
  .desktop-only { display:inline; }
  .mobile-only { display:none; }
  .mobile-layout { display:none; min-height:100vh; flex-direction:column; }
  .mobile-video { height:50vh; min-height:240px; padding:12px; box-sizing:border-box; }
  .mobile-joystick { flex:1; min-height:240px; display:flex; align-items:center; justify-content:center; padding:12px; box-sizing:border-box; }
  .mobile-joystick .joystick-area { width:100%; display:flex; flex-direction:column; align-items:center; gap:12px; }
  .joystick-base { width:min(70vw, 280px); aspect-ratio:1; border-radius:50%; background:radial-gradient(circle at 30% 30%, #f3f3f3, #d6d6d6); border:2px solid #999; position:relative; touch-action:none; }
  .joystick-thumb { width:36%; height:36%; border-radius:50%; background:radial-gradient(circle at 30% 30%, #fff, #b9b9b9); border:2px solid #666; position:absolute; top:50%; left:50%; transform:translate(-50%, -50%); transition:transform .06s ease-out; }
  .joystick-label { font:600 13px/1.2 ui-monospace,monospace; opacity:.8; }
  .mobile-options { position:fixed; top:12px; right:12px; z-index:1002; }
  .mobile-options summary { list-style:none; cursor:pointer; padding:10px 14px; border-radius:999px; border:1px solid #777; background:#fff; font:600 13px/1.2 system-ui, sans-serif; box-shadow:0 1px 2px rgba(0,0,0,.08); }
  .mobile-options summary::-webkit-details-marker { display:none; }
  .mobile-options[open] summary { border-bottom-left-radius:12px; border-bottom-right-radius:12px; }
  .mobile-options .options-panel { margin-top:8px; width:min(92vw, 540px); max-height:80vh; overflow:auto; background:#fff; border:1px solid #ccc; border-radius:14px; padding:12px; box-shadow:0 10px 30px rgba(0,0,0,.2); }
  .mobile-options .card { margin:10px 0; }
  .is-mobile .grid { display:none; }
  .is-mobile .mobile-layout { display:flex; }
  .is-mobile .desktop-only { display:none; }
  .is-mobile .mobile-only { display:inline; }
  .is-mobile .video-card { height:100%; display:flex; flex-direction:column; }
  .is-mobile #videoCard .card-collapse-toggle { display:none; }
  .is-mobile #videoCard.card-collapsed .card-collapse-body { display:block; }
  .is-mobile .video-wrap { flex:1; height:auto; padding-top:0; }
  .is-mobile .video-wrap img { object-fit:cover; }
  .is-mobile .video-controls { margin-top:8px; }
</style>

<div class="mobile-layout" id="mobileLayout">
  <div class="mobile-video" id="mobileVideoSlot"></div>
  <div class="mobile-joystick">
    <div class="joystick-area">
      <div class="joystick-base" id="joystickBase">
        <div class="joystick-thumb" id="joystickThumb"></div>
      </div>
      <div class="joystick-label" id="joystickStatus">Drag to drive</div>
    </div>
  </div>
  <details class="mobile-options" id="mobileOptions">
    <summary>Options</summary>
    <div class="options-panel" id="mobileOptionsContent"></div>
  </details>
</div>

<div class="grid" id="desktopGrid">
  <div class="card connection-card" id="connectionCard">
    <h2>MQTT Connection</h2>
    <div class="row">
      <label>Host <input id="mqttHost" type="text" placeholder="mqtt.example.com"></label>
      <label>Port <input id="mqttPort" type="number" min="1" step="1" value="1883" style="width:6rem"></label>
      <label>Keepalive <input id="mqttKeepalive" type="number" min="10" step="5" value="60" style="width:6rem"></label>
    </div>
    <div class="row">
      <label>Username <input id="mqttUsername" type="text" autocomplete="username"></label>
      <label>Password <input id="mqttPassword" type="password" autocomplete="current-password"></label>
    </div>
    <div class="row">
      <label><input id="mqttTlsEnabled" type="checkbox"> Use TLS</label>
      <label>CA cert <input id="mqttTlsCa" type="text" placeholder="path/to/ca.crt" style="width:18rem"></label>
    </div>
    <div class="row">
      <label>Client cert <input id="mqttTlsCert" type="text" placeholder="path/to/client.crt" style="width:18rem"></label>
      <label>Client key <input id="mqttTlsKey" type="text" placeholder="path/to/client.key" style="width:18rem"></label>
    </div>
    <div class="row">
      <label><input id="mqttTlsInsecure" type="checkbox"> Allow insecure certs</label>
      <label>Ciphers <input id="mqttTlsCiphers" type="text" placeholder="optional" style="width:18rem"></label>
    </div>
    <div class="row">
      <button id="mqttConnect">Connect</button>
      <button id="mqttDisconnect">Disconnect</button>
      <span id="mqttStatus" class="mono"></span>
    </div>
    <div class="section-picker">
      <details class="section-menu" id="sectionMenu">
        <summary>Sections</summary>
        <div class="section-menu-panel">
          <div class="section-menu-title">Visible for selected component</div>
          <div class="section-toggle-list" id="sectionToggleList"></div>
        </div>
      </details>
    </div>
  </div>

  <div class="card component-card" id="componentCard">
    <h2>Component</h2>
    <div class="robot-picker">
      <label for="systemSelect">System:</label>
      <select id="systemSelect"></select>
      <label for="robotSelect">Component:</label>
      <select id="robotSelect"></select>
      <button type="button" id="replayNavBtn" onclick="window.location.href='/replay'">Replay</button>
      <span id="activeRobot" class="mono"></span>
    </div>
    <div class="component-actions">
      <button id="rebootBtn">Reboot Robot</button>
      <button id="serviceRestartBtn">Restart Service</button>
      <button id="gitPullBtn">Git Pull</button>
      <span id="rebootStatus" class="mono"></span>
      <span id="serviceRestartStatus" class="mono"></span>
      <span id="gitPullStatus" class="mono"></span>
    </div>
  </div>

  <div class="card" id="driveCard">
    <h2><span class="desktop-only">Drive (WASD + Q/E grousers, Space=Stop)</span><span class="mobile-only">Drive Settings</span></h2>
    <div style="text-align:center" class="desktop-only">
      <div><kbd id="kW">W</kbd></div>
      <div><kbd id="kQ">Q</kbd></div>
      <div><kbd id="kA">A</kbd><kbd id="kS">S</kbd><kbd id="kD">D</kbd></div>
      <div><kbd id="kE">E</kbd></div>
    </div>
    <div class="row">
      <label>Speed <input id="spd" type="range" min="0" max="100" value="100"></label>
      <label>Turn <input id="trn" type="range" min="0" max="100" value="100"></label>
    </div>
    <div class="row">
      <label>Balance <input id="bal" type="range" min="-50" max="50" value="0"></label>
      <span id="balStatus" class="mono">Centered</span>
    </div>
    <div class="row">
      <button id="stopBtn">STOP</button>
    </div>
    <div id="status" class="mono">Select a component to begin.</div>
  </div>

  <div class="card" id="lightsCard">
    <h2>Lights (B,G,R)</h2>
    <div class="row">
      <label>Color <input id="color" type="color" value="#ff0000"></label>
      <label>Flash period (s) <input id="flashT" type="number" min="0.05" step="0.05" value="2.00" style="width:6rem"></label>
    </div>
    <div class="row">
      <button id="btnSolid">Set Solid (L)</button>
      <button id="btnFlash">Flash (F)</button>
      <button id="btnOff">Off</button>
    </div>
    <div id="lightStatus" class="mono">Awaiting command...</div>
  </div>

  <div class="card" id="touchCard">
    <h2>Telemetry</h2>
    <div class="row mono">Charging: <span id="chg" class="pill">–</span></div>
    <div class="row mono">Battery: <span id="bat">–</span> V</div>
    <div class="row mono">Connection: <span id="conn" class="pill">Unknown</span></div>
    <div class="row mono">Avg latency: <span id="latAvg">–</span></div>
    <div class="row mono">Offline for: <span id="offlineFor">–</span></div>
  </div>

  <div class="card" id="imuCard">
    <h2>IMU</h2>
    <div class="imu-wrap">
      <canvas id="imuCanvas"></canvas>
    </div>
    <div class="imu-meta mono">
      <div id="imuOrientation">Roll: – | Pitch: –</div>
      <div id="imuAccel">Accel: –</div>
      <div id="imuGyro">Gyro: –</div>
      <div id="imuHealth">Status: Awaiting data...</div>
    </div>
  </div>

  <div class="card odometry-card" id="odometryCard">
    <h2>Map & Location</h2>
    <div class="odometry-meta mono" id="odomMeta">Awaiting data...</div>
    <div class="odometry-wrap">
      <canvas id="odomCanvas"></canvas>
    </div>
  </div>

  <div class="card video-card" id="videoCard">
    <h2>Front Camera</h2>
    <div class="video-wrap">
      <img id="videoImg" alt="Live video stream">
      <canvas id="videoOverlayCanvas"></canvas>
      <div id="videoPlaceholder" class="video-placeholder">Select a component with video support.</div>
    </div>
    <div class="webrtc-wrap" id="webrtcWrap">
      <iframe id="webrtcFrame" title="WebRTC Stream" allow="camera; microphone; autoplay; fullscreen"></iframe>
    </div>
    <div class="webrtc-controls" id="webrtcControls">
      <input id="webrtcUrlInput" type="text" placeholder="https://cam.yourdomain/stream" spellcheck="false">
      <button id="webrtcSet">Set</button>
      <button id="webrtcClear">Clear</button>
      <a id="webrtcOpen" href="#" target="_blank" rel="noreferrer">Open</a>
    </div>
    <div class="video-controls">
      <button id="videoStart">Start Stream</button>
      <button id="videoStop">Stop Stream</button>
      <button id="videoFullscreen">Fullscreen</button>
      <label id="videoOverlayLabel" class="video-overlay-toggle"><input id="videoOverlayToggle" type="checkbox"> Overlays</label>
      <span id="videoStatus" class="mono"></span>
    </div>
  </div>

  <div class="card audio-card" id="audioCard">
    <h2>Component Audio</h2>
    <div class="audio-controls">
      <button id="audioStart">Start Audio</button>
      <button id="audioTalk">Talk: Muted</button>
      <button id="audioStop">Stop Audio</button>
      <span id="audioStatus" class="mono"></span>
    </div>
    <div class="audio-indicators">
      <span class="audio-indicator" id="audioInIndicator"><span class="dot"></span>From component</span>
      <span class="audio-indicator" id="audioOutIndicator"><span class="dot"></span>To component</span>
      <span class="audio-indicator" id="audioModeIndicator"><span class="dot"></span>Listening</span>
    </div>
    <div class="mono-dim" id="audioMeta"></div>
  </div>

  <div class="card outlets-card" id="outletsCard">
    <h2>Outlets</h2>
    <div class="mono-dim" id="outletsStatus">Waiting for outlet state from component...</div>
    <div class="outlet-list" id="outletsList"></div>
  </div>

  <div class="card soundboard-card" id="soundboardCard">
    <h2>Soundboard</h2>
    <div class="mono-dim" id="soundSelection">Select a sound file.</div>
    <div class="soundboard-controls hidden" id="soundboardControls">
      <button id="soundPlayStop">Play</button>
      <span id="soundboardStatus" class="mono"></span>
    </div>
    <div class="sound-list" id="soundList"></div>
  </div>

  <div class="card autonomy-card" id="autonomyCard">
    <h2>Autonomy</h2>
    <div class="mono-dim" id="autonomySelection">Select an autonomy script.</div>
    <div class="autonomy-controls" id="autonomyControls">
      <button id="autonomyStartStop">Start</button>
      <span id="autonomyStatus" class="mono"></span>
    </div>
    <div class="autonomy-layout">
      <div class="autonomy-list" id="autonomyList"></div>
      <div class="autonomy-config" id="autonomyConfig"></div>
    </div>
  </div>

  <div class="card logs-card" id="logsCard">
    <h2>Incoming Logs</h2>
    <div class="logs-controls">
      <label><input id="logsAutoscroll" type="checkbox" checked> Auto-scroll</label>
      <button id="logsClear">Clear View</button>
      <span id="logsStatus" class="mono"></span>
    </div>
    <pre id="logsOutput" class="logs-output">Select a component to begin.</pre>
  </div>
</div>

<script>
let robots = __ROBOTS__ || [];
const MQTT_DEFAULTS = __MQTT_DEFAULTS__;
const mqttHostEl = document.getElementById('mqttHost');
const mqttPortEl = document.getElementById('mqttPort');
const mqttUserEl = document.getElementById('mqttUsername');
const mqttPassEl = document.getElementById('mqttPassword');
const mqttKeepaliveEl = document.getElementById('mqttKeepalive');
const mqttTlsEnabledEl = document.getElementById('mqttTlsEnabled');
const mqttTlsCaEl = document.getElementById('mqttTlsCa');
const mqttTlsCertEl = document.getElementById('mqttTlsCert');
const mqttTlsKeyEl = document.getElementById('mqttTlsKey');
const mqttTlsInsecureEl = document.getElementById('mqttTlsInsecure');
const mqttTlsCiphersEl = document.getElementById('mqttTlsCiphers');
const mqttConnectBtn = document.getElementById('mqttConnect');
const mqttDisconnectBtn = document.getElementById('mqttDisconnect');
const mqttStatusEl = document.getElementById('mqttStatus');
const desktopGrid = document.getElementById('desktopGrid');
const mobileLayout = document.getElementById('mobileLayout');
const mobileVideoSlot = document.getElementById('mobileVideoSlot');
const mobileOptionsContent = document.getElementById('mobileOptionsContent');
const connectionCard = document.getElementById('connectionCard');
const componentCard = document.getElementById('componentCard');
const driveCard = document.getElementById('driveCard');
const lightsCard = document.getElementById('lightsCard');
const touchCard = document.getElementById('touchCard');
const imuCard = document.getElementById('imuCard');
const odometryCard = document.getElementById('odometryCard');
const outletsCard = document.getElementById('outletsCard');
const soundboardCard = document.getElementById('soundboardCard');
const autonomyCard = document.getElementById('autonomyCard');
const logsCard = document.getElementById('logsCard');
const sectionMenu = document.getElementById('sectionMenu');
const sectionToggleList = document.getElementById('sectionToggleList');
const joystickBase = document.getElementById('joystickBase');
const joystickThumb = document.getElementById('joystickThumb');
const joystickStatus = document.getElementById('joystickStatus');
const imuCanvas = document.getElementById('imuCanvas');
const imuOrientationEl = document.getElementById('imuOrientation');
const imuAccelEl = document.getElementById('imuAccel');
const imuGyroEl = document.getElementById('imuGyro');
const imuHealthEl = document.getElementById('imuHealth');
const odomMeta = document.getElementById('odomMeta');
const odomCanvas = document.getElementById('odomCanvas');
const ODOM_TRAIL_MAX_POINTS = 320;
let imuLatest = null;
let imuAccelBaseline = null;
let imuAccelBaselineAt = 0;
let odomTrail = [];
let odomLastToken = null;
let odomLatest = null;

function makeCardCollapsible(cardId) {
  const card = document.getElementById(cardId);
  if (!card || card.dataset.collapsibleInit === "1") return;
  const heading = card.querySelector("h2");
  if (!heading) return;

  card.dataset.collapsibleInit = "1";
  heading.classList.add("card-heading");

  const body = document.createElement("div");
  body.className = "card-collapse-body";
  let node = heading.nextSibling;
  while (node) {
    const next = node.nextSibling;
    body.appendChild(node);
    node = next;
  }
  card.appendChild(body);

  const toggle = document.createElement("button");
  toggle.type = "button";
  toggle.className = "card-collapse-toggle";
  heading.appendChild(toggle);

  const storageKey = `collapsed:${cardId}`;
  function setCollapsed(collapsed) {
    card.classList.toggle("card-collapsed", collapsed);
    toggle.textContent = collapsed ? "Expand" : "Collapse";
    toggle.setAttribute("aria-expanded", String(!collapsed));
    try {
      localStorage.setItem(storageKey, collapsed ? "1" : "0");
    } catch (_err) {
      // Ignore localStorage failures.
    }
    if (cardId === "videoCard") drawVideoOverlays();
    if (cardId === "imuCard") drawImuPlot();
    if (cardId === "odometryCard") drawOdometryPlot();
  }

  toggle.addEventListener("click", () => {
    setCollapsed(!card.classList.contains("card-collapsed"));
  });

  let initialCollapsed = false;
  try {
    initialCollapsed = localStorage.getItem(storageKey) === "1";
  } catch (_err) {
    initialCollapsed = false;
  }
  setCollapsed(initialCollapsed);
}

function initCollapsibleCards() {
  [
    "connectionCard",
    "componentCard",
    "driveCard",
    "lightsCard",
    "touchCard",
    "imuCard",
    "odometryCard",
    "videoCard",
    "audioCard",
    "outletsCard",
    "soundboardCard",
    "autonomyCard",
    "logsCard",
  ].forEach(makeCardCollapsible);
}

function applyDefaultMqttFields() {
  if (MQTT_DEFAULTS.host) mqttHostEl.value = MQTT_DEFAULTS.host;
  if (MQTT_DEFAULTS.port) mqttPortEl.value = MQTT_DEFAULTS.port;
  if (MQTT_DEFAULTS.keepalive) mqttKeepaliveEl.value = MQTT_DEFAULTS.keepalive;
  if (MQTT_DEFAULTS.username) mqttUserEl.value = MQTT_DEFAULTS.username;
  if (MQTT_DEFAULTS.password) mqttPassEl.value = MQTT_DEFAULTS.password;
  if (MQTT_DEFAULTS.tls) {
    mqttTlsEnabledEl.checked = !!MQTT_DEFAULTS.tls.enabled;
    if (MQTT_DEFAULTS.tls.ca_cert) mqttTlsCaEl.value = MQTT_DEFAULTS.tls.ca_cert;
    if (MQTT_DEFAULTS.tls.client_cert) mqttTlsCertEl.value = MQTT_DEFAULTS.tls.client_cert;
    if (MQTT_DEFAULTS.tls.client_key) mqttTlsKeyEl.value = MQTT_DEFAULTS.tls.client_key;
    mqttTlsInsecureEl.checked = !!MQTT_DEFAULTS.tls.insecure;
    if (MQTT_DEFAULTS.tls.ciphers) mqttTlsCiphersEl.value = MQTT_DEFAULTS.tls.ciphers;
  }
}
applyDefaultMqttFields();

async function fetchJson(url, options) {
  const res = await fetch(url, options);
  const data = await res.json().catch(() => ({}));
  return { ok: res.ok, data };
}

async function refreshMqttStatus(){
  try {
    const { ok, data } = await fetchJson("/mqtt/status");
    if (!ok) throw new Error("status");
    let message = "Disconnected";
    if (data.connected) {
      const host = data.host || "broker";
      const port = data.port || "";
      message = `Connected to ${host}${port ? `:${port}` : ""}`;
    } else if (data.connecting) {
      const host = data.host || "broker";
      message = `Connecting to ${host}...`;
    }
    if (data.error) {
      message += ` — ${data.error}`;
    }
    mqttStatusEl.textContent = message;
    mqttConnectBtn.disabled = Boolean(data.connected || data.connecting);
    mqttDisconnectBtn.disabled = !data.connected && !data.connecting;
  } catch(err) {
    mqttStatusEl.textContent = "Status unavailable";
    mqttConnectBtn.disabled = false;
    mqttDisconnectBtn.disabled = true;
  }
}

async function connectMqtt(){
  const payload = {
    host: mqttHostEl.value.trim(),
    port: Number(mqttPortEl.value),
    username: mqttUserEl.value.trim(),
    password: mqttPassEl.value,
    keepalive: Number(mqttKeepaliveEl.value),
    tls: {
      enabled: mqttTlsEnabledEl.checked,
      ca_cert: mqttTlsCaEl.value.trim(),
      client_cert: mqttTlsCertEl.value.trim(),
      client_key: mqttTlsKeyEl.value.trim(),
      insecure: mqttTlsInsecureEl.checked,
      ciphers: mqttTlsCiphersEl.value.trim(),
    },
  };
  if (!payload.host) {
    mqttStatusEl.textContent = "Host is required.";
    return;
  }
  if (!Number.isFinite(payload.port)) delete payload.port;
  if (!payload.username) delete payload.username;
  if (!payload.password) delete payload.password;
  if (!Number.isFinite(payload.keepalive)) delete payload.keepalive;
  const { ok, data } = await fetchJson("/mqtt/connect", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });
  if (!ok) {
    mqttStatusEl.textContent = data.error || "Failed to connect.";
  } else {
    mqttStatusEl.textContent = "Connecting...";
    refreshMqttStatus();
  }
}

async function disconnectMqtt(){
  await fetch("/mqtt/disconnect", { method:"POST" });
  refreshMqttStatus();
}

mqttConnectBtn.addEventListener("click", () => { connectMqtt().catch(console.error); });
mqttDisconnectBtn.addEventListener("click", () => { disconnectMqtt().catch(console.error); });
refreshMqttStatus();
setInterval(refreshMqttStatus, 5000);

let currentSystem = robots.length ? String(robots[0].system || "") : null;
let currentRobot = robots.length ? String(robots[0].key || robots[0].id || "") : null;

function robotRef(robot) {
  if (!robot || typeof robot !== "object") return null;
  return robot.key || robot.id || null;
}

function findRobot(id) {
  return robots.find(r => robotRef(r) === id) || null;
}

function availableSystems() {
  return Array.from(new Set(
    robots
      .map(r => String(r.system || ""))
      .filter(Boolean)
  )).sort((a, b) => a.localeCompare(b));
}

function visibleRobots() {
  return robots.filter((robot) => !currentSystem || String(robot.system || "") === currentSystem);
}

function robotName(id) {
  const found = findRobot(id);
  return found ? (found.name || found.id) : (id || "–");
}

function componentType(id) {
  const found = findRobot(id);
  return found && found.type ? found.type : "robots";
}

function getWebrtcUrl(id) {
  if (!id) return "";
  const stored = localStorage.getItem(`webrtcUrl:${id}`);
  if (stored) return stored;
  const found = findRobot(id);
  return found && found.webrtcUrl ? found.webrtcUrl : "";
}

function setWebrtcUrl(id, url) {
  if (!id) return;
  const clean = (url || "").trim();
  if (clean) {
    localStorage.setItem(`webrtcUrl:${id}`, clean);
  } else {
    localStorage.removeItem(`webrtcUrl:${id}`);
  }
  const found = findRobot(id);
  if (found) {
    found.webrtcUrl = clean || null;
  }
}

function robotLabel(id) {
  const robot = findRobot(id);
  if (!robot) return robotName(id);
  const base = robot.name || robot.id || "–";
  const typeSuffix = robot.type && robot.type !== "robots" ? ` (${robot.type})` : "";
  let onlineSuffix = "";
  if (robot.online === true) onlineSuffix = " [online]";
  if (robot.online === false) onlineSuffix = " [offline]";
  return `${base}${typeSuffix}${onlineSuffix}`;
}

function robotHasVideo(id) {
  const robot = findRobot(id);
  return robot ? Boolean(robot.hasVideo) : false;
}

function usesWebrtc(id) {
  return componentType(id) === "cameras";
}

function robotHasControls(id) {
  const robot = findRobot(id);
  return robot ? Boolean(robot.hasControls) : false;
}

function robotHasAudio(id) {
  const robot = findRobot(id);
  return robot ? Boolean(robot.hasAudio) : false;
}

function robotHasAudioControls(id) {
  const robot = findRobot(id);
  return robot ? Boolean(robot.audioControls) : false;
}

function robotHasDrive(id) {
  const robot = findRobot(id);
  return robot ? Boolean(robot.hasDrive) : false;
}

function robotHasLights(id) {
  const robot = findRobot(id);
  return robot ? Boolean(robot.hasLights) : false;
}

function robotHasImu(id) {
  const robot = findRobot(id);
  return robot ? Boolean(robot.hasImu) : false;
}

function robotHasOdometry(id) {
  const robot = findRobot(id);
  return robot ? Boolean(robot.hasOdometry) : false;
}

function robotHasOutlets(id) {
  const robot = findRobot(id);
  return robot ? Boolean(robot.hasOutlets) : false;
}

function robotHasOutletControls(id) {
  const robot = findRobot(id);
  return robot ? Boolean(robot.outletControls) : false;
}

function robotHasSoundboard(id) {
  const robot = findRobot(id);
  if (!robot) return false;
  if (typeof robot.hasSoundboard === "boolean") return robot.hasSoundboard;
  return true;
}

function robotHasSoundboardControls(id) {
  const robot = findRobot(id);
  if (!robot) return false;
  if (typeof robot.soundboardControls === "boolean") return robot.soundboardControls;
  return true;
}

function robotHasAutonomy(id) {
  const robot = findRobot(id);
  if (!robot) return false;
  if (typeof robot.hasAutonomy === "boolean") return robot.hasAutonomy;
  return true;
}

function robotHasAutonomyControls(id) {
  const robot = findRobot(id);
  if (!robot) return false;
  if (typeof robot.autonomyControls === "boolean") return robot.autonomyControls;
  return true;
}

function robotHasRebootControls(id) {
  const robot = findRobot(id);
  if (!robot) return false;
  if (robot.type && robot.type !== "robots") return false;
  if (typeof robot.rebootControls === "boolean") return robot.rebootControls;
  return true;
}

function robotHasServiceRestartControls(id) {
  const robot = findRobot(id);
  if (!robot) return false;
  if (robot.type && robot.type !== "robots") return false;
  if (typeof robot.serviceRestartControls === "boolean") return robot.serviceRestartControls;
  if (typeof robot.rebootControls === "boolean") return robot.rebootControls;
  return true;
}

function robotHasGitPullControls(id) {
  const robot = findRobot(id);
  if (!robot) return false;
  if (robot.type && robot.type !== "robots") return false;
  if (typeof robot.gitPullControls === "boolean") return robot.gitPullControls;
  if (typeof robot.rebootControls === "boolean") return robot.rebootControls;
  return true;
}

const SECTION_DEFS = [
  { id: "drive", label: "Drive", cardId: "driveCard" },
  { id: "lights", label: "Lights", cardId: "lightsCard" },
  { id: "telemetry", label: "Telemetry", cardId: "touchCard" },
  { id: "imu", label: "IMU", cardId: "imuCard" },
  { id: "odometry", label: "Map & Location", cardId: "odometryCard" },
  { id: "video", label: "Front Camera", cardId: "videoCard" },
  { id: "audio", label: "Component Audio", cardId: "audioCard" },
  { id: "outlets", label: "Outlets", cardId: "outletsCard" },
  { id: "soundboard", label: "Soundboard", cardId: "soundboardCard" },
  { id: "autonomy", label: "Autonomy", cardId: "autonomyCard" },
  { id: "logs", label: "Incoming Logs", cardId: "logsCard" },
];

function sectionStorageKey(sectionId, robotId) {
  return `sectionOverride:${robotId || "__none__"}:${sectionId}`;
}

function getSectionOverride(sectionId, robotId) {
  try {
    const value = localStorage.getItem(sectionStorageKey(sectionId, robotId));
    return value === "show" || value === "hide" ? value : null;
  } catch (_err) {
    return null;
  }
}

function setSectionOverride(sectionId, robotId, value) {
  try {
    const key = sectionStorageKey(sectionId, robotId);
    if (value === "show" || value === "hide") {
      localStorage.setItem(key, value);
    } else {
      localStorage.removeItem(key);
    }
  } catch (_err) {
    // Ignore localStorage failures.
  }
}

function sectionDefaultVisible(sectionId, robotId) {
  if (!robotId) {
    return ["telemetry", "logs"].includes(sectionId);
  }
  if (sectionId === "telemetry" || sectionId === "logs") {
    return true;
  }
  if (sectionId === "drive") return robotHasDrive(robotId);
  if (sectionId === "lights") return robotHasLights(robotId);
  if (sectionId === "imu") return robotHasImu(robotId);
  if (sectionId === "odometry") return robotHasOdometry(robotId);
  if (sectionId === "video") return robotHasVideo(robotId);
  if (sectionId === "audio") return robotHasAudio(robotId);
  if (sectionId === "outlets") return robotHasOutlets(robotId);
  if (sectionId === "soundboard") return robotHasSoundboard(robotId);
  if (sectionId === "autonomy") return robotHasAutonomy(robotId);
  return false;
}

function sectionVisible(sectionId, robotId) {
  const override = getSectionOverride(sectionId, robotId);
  if (override === "show") return true;
  if (override === "hide") return false;
  return sectionDefaultVisible(sectionId, robotId);
}

function sectionVisibilityMode(sectionId, robotId) {
  if (getSectionOverride(sectionId, robotId) === "show") return "manual";
  if (getSectionOverride(sectionId, robotId) === "hide") return "hidden";
  return sectionDefaultVisible(sectionId, robotId) ? "auto" : "manual-hidden";
}

function renderSectionMenu() {
  if (!sectionToggleList) return;
  sectionToggleList.innerHTML = "";
  SECTION_DEFS.forEach((section) => {
    const row = document.createElement("label");
    row.className = "section-toggle";

    const copy = document.createElement("span");
    copy.className = "section-toggle-copy";
    const name = document.createElement("span");
    name.className = "section-toggle-name";
    name.textContent = section.label;
    const mode = document.createElement("span");
    mode.className = "section-toggle-mode";
    const modeKey = sectionVisibilityMode(section.id, currentRobot);
    mode.textContent = modeKey === "auto"
      ? "auto"
      : modeKey === "hidden"
        ? "manually hidden"
        : modeKey === "manual"
          ? "manually shown"
          : "hidden by capability";
    copy.appendChild(name);
    copy.appendChild(mode);

    const input = document.createElement("input");
    input.type = "checkbox";
    input.checked = sectionVisible(section.id, currentRobot);
    input.addEventListener("change", () => {
      const defaultVisible = sectionDefaultVisible(section.id, currentRobot);
      if (input.checked === defaultVisible) {
        setSectionOverride(section.id, currentRobot, null);
      } else {
        setSectionOverride(section.id, currentRobot, input.checked ? "show" : "hide");
      }
      syncSectionVisibility();
    });

    row.appendChild(copy);
    row.appendChild(input);
    sectionToggleList.appendChild(row);
  });
}

function syncSectionVisibility() {
  SECTION_DEFS.forEach((section) => {
    const card = document.getElementById(section.cardId);
    if (!card) return;
    card.classList.toggle("hidden", !sectionVisible(section.id, currentRobot));
  });
  renderSectionMenu();
}

const systemSelect = document.getElementById('systemSelect');
const robotSelect = document.getElementById('robotSelect');
const activeRobot = document.getElementById('activeRobot');

function updateActiveRobotText() {
  const robot = currentRobot ? findRobot(currentRobot) : null;
  if (robot) {
    const systemPrefix = robot.system ? `${robot.system} / ` : "";
    activeRobot.textContent = `${systemPrefix}${robotLabel(currentRobot)}`;
    return;
  }
  if (currentSystem) {
    activeRobot.textContent = `No components detected on ${currentSystem}.`;
    return;
  }
  activeRobot.textContent = "No systems detected.";
}

function renderRobotOptions(newRobots) {
  robots = Array.isArray(newRobots) ? newRobots : [];
  const systems = availableSystems();
  if (currentSystem && !systems.includes(currentSystem)) {
    currentSystem = null;
  }
  if (!currentSystem) {
    currentSystem = systems.length ? systems[0] : null;
  }

  systemSelect.innerHTML = "";
  systems.forEach((system) => {
    const opt = document.createElement("option");
    opt.value = system;
    opt.textContent = system;
    systemSelect.appendChild(opt);
  });
  if (currentSystem) {
    systemSelect.value = currentSystem;
  }
  systemSelect.disabled = systems.length === 0;

  const selectableRobots = visibleRobots();
  if (!selectableRobots.some(robot => robotRef(robot) === currentRobot)) {
    currentRobot = selectableRobots.length ? robotRef(selectableRobots[0]) : null;
  }

  robotSelect.innerHTML = "";
  selectableRobots.forEach(robot => {
    const ref = robotRef(robot);
    if (!robot || !ref) return;
    const opt = document.createElement('option');
    opt.value = ref;
    opt.textContent = robotLabel(ref);
    robotSelect.appendChild(opt);
  });
  if (currentRobot) {
    robotSelect.value = currentRobot;
  }
  robotSelect.disabled = selectableRobots.length === 0;
  updateActiveRobotText();
  syncRebootControls();
  syncServiceRestartControls();
  syncGitPullControls();
  syncSectionVisibility();
}

function handleRobotSelectionChanged(previousRobot) {
  updateActiveRobotText();
  syncRebootControls();
  syncServiceRestartControls();
  syncGitPullControls();
  syncSectionVisibility();
  refreshVideoVisibility();
  refreshAudioVisibility();
  if (previousRobot !== currentRobot) {
    resetLightsShortcutState();
    clearImuUI();
    clearOdometryUI();
    resetOutletsState();
    renderOutlets();
    refreshOutlets();
    resetSoundboardState();
    renderSoundList();
    refreshSoundFiles();
    refreshSoundboardStatus();
    resetAutonomyState();
    renderAutonomyList();
    refreshAutonomyFiles();
    refreshAutonomyStatus();
    resetLogsState();
    refreshComponentLogs(true);
  }
  if (!currentRobot && typeof clearTelemetryUI === "function") {
    clearTelemetryUI();
  }
  updateSoundboardUi();
  updateAutonomyUi();
}

async function refreshRobotList(){
  try {
    const { ok, data } = await fetchJson("/robots");
    if (!ok) return;
    if (Array.isArray(data.robots)) {
      const beforeRobot = currentRobot;
      renderRobotOptions(data.robots);
      handleRobotSelectionChanged(beforeRobot);
    }
  } catch (err) {
    console.error(err);
  }
}

function refreshWebrtcVisibility() {
  if (!currentRobot || !usesWebrtc(currentRobot)) {
    webrtcWrap.style.display = "none";
    webrtcControls.style.display = "none";
    webrtcFrame.removeAttribute("src");
    return false;
  }
  const url = getWebrtcUrl(currentRobot);
  webrtcWrap.style.display = "block";
  webrtcControls.style.display = "flex";
  if (webrtcUrlInput.value !== url) {
    webrtcUrlInput.value = url;
  }
  if (url) {
    webrtcFrame.src = url;
    webrtcOpenLink.href = url;
    webrtcOpenLink.style.pointerEvents = "auto";
    webrtcOpenLink.style.opacity = "1";
  } else {
    webrtcFrame.removeAttribute("src");
    webrtcOpenLink.href = "#";
    webrtcOpenLink.style.pointerEvents = "none";
    webrtcOpenLink.style.opacity = "0.5";
  }
  return true;
}

const videoCard = document.getElementById('videoCard');
const videoWrap = videoCard ? videoCard.querySelector('.video-wrap') : null;
const videoImg = document.getElementById('videoImg');
const videoPlaceholder = document.getElementById('videoPlaceholder');
const videoOverlayCanvas = document.getElementById('videoOverlayCanvas');
const videoOverlayLabel = document.getElementById('videoOverlayLabel');
const videoOverlayToggle = document.getElementById('videoOverlayToggle');
const webrtcWrap = document.getElementById('webrtcWrap');
const webrtcFrame = document.getElementById('webrtcFrame');
const webrtcUrlInput = document.getElementById('webrtcUrlInput');
const webrtcSetBtn = document.getElementById('webrtcSet');
const webrtcClearBtn = document.getElementById('webrtcClear');
const webrtcOpenLink = document.getElementById('webrtcOpen');
const webrtcControls = document.getElementById('webrtcControls');
const videoStartBtn = document.getElementById('videoStart');
const videoStopBtn = document.getElementById('videoStop');
const videoFullscreenBtn = document.getElementById('videoFullscreen');
const videoStatusEl = document.getElementById('videoStatus');
let videoStatusTimer = null;
let streamingRobot = null;
let awaitingFirstFrame = false;
let videoStreamSuppressed = false;
let lastVideoRobot = null;
let videoOverlayTimer = null;
let videoOverlayData = null;
let videoOverlayFetchInFlight = false;
const VIDEO_STALE_SECONDS = 5;
const VIDEO_OVERLAY_STALE_SECONDS = 2;

const audioCard = document.getElementById('audioCard');
const audioStartBtn = document.getElementById('audioStart');
const audioStopBtn = document.getElementById('audioStop');
const audioTalkBtn = document.getElementById('audioTalk');
const audioInIndicator = document.getElementById('audioInIndicator');
const audioOutIndicator = document.getElementById('audioOutIndicator');
const audioModeIndicator = document.getElementById('audioModeIndicator');
const audioStatusEl = document.getElementById('audioStatus');
const audioMetaEl = document.getElementById('audioMeta');
let audioSource = null;
let audioRobot = null;
let audioCtx = null;
let audioGain = null;
let audioQueueTime = 0;
let audioIncomingMuted = false;
let audioHadChunk = false;
let audioDrops = 0;
let audioOutgoingActive = false;
let audioOutgoingStream = null;
let audioCaptureCtx = null;
let audioOutgoingSource = null;
let audioOutgoingProcessor = null;
let audioOutgoingQueue = [];
let audioOutgoingSending = false;
let audioOutgoingHadChunk = false;
let audioOutgoingRobot = null;
let audioTalkUnmuted = false;
let audioExpectedSeq = null;
let audioLastChunkPcm = null;
let audioLastChunkRate = 16000;
let audioLastChunkChannels = 1;
let audioChunkMs = 20;
let audioConcealmentMs = 100;
let audioConcealedMs = 0;
let audioConcealmentTimer = null;

const outletsListEl = document.getElementById('outletsList');
const outletsStatusEl = document.getElementById('outletsStatus');
let outlets = [];
let outletControlsEnabled = true;
let outletBusyId = "";

const soundboardControls = document.getElementById('soundboardControls');
const soundPlayStopBtn = document.getElementById('soundPlayStop');
const soundboardStatusEl = document.getElementById('soundboardStatus');
const soundSelectionEl = document.getElementById('soundSelection');
const soundListEl = document.getElementById('soundList');
let soundFiles = [];
let selectedSoundFile = "";
let playingSoundFile = "";
let soundIsPlaying = false;
let soundboardBusy = false;
let soundboardControlsEnabled = true;

const autonomyControlsEl = document.getElementById('autonomyControls');
const autonomyStartStopBtn = document.getElementById('autonomyStartStop');
const autonomyStatusEl = document.getElementById('autonomyStatus');
const autonomySelectionEl = document.getElementById('autonomySelection');
const autonomyListEl = document.getElementById('autonomyList');
const autonomyConfigEl = document.getElementById('autonomyConfig');
let autonomyFiles = [];
let selectedAutonomyFile = "";
let runningAutonomyFile = "";
let autonomyRunning = false;
let autonomyBusy = false;
let autonomyControlsEnabled = true;
let selectedAutonomyConfigValues = {};

const logsOutputEl = document.getElementById('logsOutput');
const logsStatusEl = document.getElementById('logsStatus');
const logsClearBtn = document.getElementById('logsClear');
const logsAutoscrollEl = document.getElementById('logsAutoscroll');
let logsRenderSignature = "";
let logsMinTimestampMs = 0;
let logsNewestTimestampMs = 0;

let mobileMode = false;
function prefersMobileLayout() {
  const coarsePointer = window.matchMedia ? window.matchMedia("(pointer: coarse)").matches : false;
  const noHover = window.matchMedia ? window.matchMedia("(hover: none)").matches : false;
  const touchPoints = Number(navigator.maxTouchPoints || 0);
  const userAgent = typeof navigator.userAgent === "string" ? navigator.userAgent : "";
  const mobileUa = /Android|iPhone|iPad|iPod|Mobile/i.test(userAgent);
  return mobileUa || (coarsePointer && noHover && touchPoints > 0);
}

function moveCard(card, target) {
  if (!card || !target) return;
  if (card.parentNode !== target) {
    target.appendChild(card);
  }
}

function applyLayoutMode(isMobile) {
  document.body.classList.toggle('is-mobile', isMobile);
  if (isMobile) {
    moveCard(videoCard, mobileVideoSlot);
    moveCard(connectionCard, mobileOptionsContent);
    moveCard(componentCard, mobileOptionsContent);
    moveCard(driveCard, mobileOptionsContent);
    moveCard(lightsCard, mobileOptionsContent);
    moveCard(touchCard, mobileOptionsContent);
    moveCard(imuCard, mobileOptionsContent);
    moveCard(odometryCard, mobileOptionsContent);
    moveCard(audioCard, mobileOptionsContent);
    moveCard(outletsCard, mobileOptionsContent);
    moveCard(soundboardCard, mobileOptionsContent);
    moveCard(autonomyCard, mobileOptionsContent);
    moveCard(logsCard, mobileOptionsContent);
  } else {
    moveCard(connectionCard, desktopGrid);
    moveCard(componentCard, desktopGrid);
    moveCard(driveCard, desktopGrid);
    moveCard(lightsCard, desktopGrid);
    moveCard(touchCard, desktopGrid);
    moveCard(imuCard, desktopGrid);
    moveCard(odometryCard, desktopGrid);
    moveCard(videoCard, desktopGrid);
    moveCard(audioCard, desktopGrid);
    moveCard(outletsCard, desktopGrid);
    moveCard(soundboardCard, desktopGrid);
    moveCard(autonomyCard, desktopGrid);
    moveCard(logsCard, desktopGrid);
  }
}

function updateLayoutMode() {
  const shouldMobile = prefersMobileLayout();
  if (shouldMobile === mobileMode) {
    drawVideoOverlays();
    drawImuPlot();
    drawOdometryPlot();
    return;
  }
  mobileMode = shouldMobile;
  applyLayoutMode(mobileMode);
  drawVideoOverlays();
  drawImuPlot();
  drawOdometryPlot();
  if (mobileMode) {
    setTimeout(updateJoystickMetrics, 0);
  }
}

function resetLogsState(message = "Select a component to begin.") {
  logsRenderSignature = "";
  logsMinTimestampMs = 0;
  logsNewestTimestampMs = 0;
  if (logsOutputEl) logsOutputEl.textContent = message;
  if (logsStatusEl) logsStatusEl.textContent = "";
}

function formatLogTimestamp(timestampMs) {
  const d = new Date(Number(timestampMs) || 0);
  if (Number.isNaN(d.getTime())) return "---- --:--:--.---";
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  const hh = String(d.getHours()).padStart(2, "0");
  const mi = String(d.getMinutes()).padStart(2, "0");
  const ss = String(d.getSeconds()).padStart(2, "0");
  const ms = String(d.getMilliseconds()).padStart(3, "0");
  return `${yyyy}-${mm}-${dd} ${hh}:${mi}:${ss}.${ms}`;
}

function formatLogLine(entry) {
  const ts = formatLogTimestamp(entry.timestampMs || (Number(entry.timestamp) * 1000));
  const level = String(entry.level || "INFO").toUpperCase().padEnd(5, " ");
  const service = String(entry.service || "component");
  const message = String(entry.message || "");
  return `${ts} [${level}] [${service}] ${message}`;
}

async function refreshComponentLogs(force = false) {
  if (!logsOutputEl || !logsStatusEl) return;
  if (!currentRobot) {
    if (force) resetLogsState();
    return;
  }
  try {
    const { ok, data } = await fetchJson(`/robots/${currentRobot}/logs?limit=300`);
    if (!ok) throw new Error(data.error || data.status || "Request failed.");
    const entries = Array.isArray(data.entries) ? data.entries : [];
    if (entries.length) {
      const newest = Number(entries[entries.length - 1].timestampMs || 0);
      if (Number.isFinite(newest) && newest > logsNewestTimestampMs) {
        logsNewestTimestampMs = newest;
      }
    }
    const filtered = entries.filter(entry => Number(entry.timestampMs || 0) > logsMinTimestampMs);
    const last = filtered.length ? filtered[filtered.length - 1] : null;
    const signature = `${filtered.length}|${last ? Number(last.timestampMs || 0) : 0}|${last ? String(last.message || "") : ""}`;
    if (!force && signature === logsRenderSignature) {
      return;
    }
    logsRenderSignature = signature;
    if (!filtered.length) {
      logsOutputEl.textContent = "No logs received yet.";
    } else {
      logsOutputEl.textContent = filtered.map(formatLogLine).join("\\n");
      if (logsAutoscrollEl && logsAutoscrollEl.checked) {
        logsOutputEl.scrollTop = logsOutputEl.scrollHeight;
      }
    }
    logsStatusEl.textContent = data.topic ? `Topic: ${data.topic}` : "";
  } catch (err) {
    console.error(err);
    logsStatusEl.textContent = `Logs unavailable: ${err.message || err}`;
  }
}

window.addEventListener("resize", updateLayoutMode);
if (window.matchMedia) {
  const media = window.matchMedia("(pointer: coarse)");
  if (typeof media.addEventListener === "function") {
    media.addEventListener("change", updateLayoutMode);
  } else if (typeof media.addListener === "function") {
    media.addListener(updateLayoutMode);
  }
}

function cleanupVideoStream() {
  streamingRobot = null;
  awaitingFirstFrame = false;
  videoImg.removeAttribute('src');
  videoImg.removeAttribute('data-stream');
}

function suppressVideoStream(message) {
  if (!videoStreamSuppressed) {
    cleanupVideoStream();
    videoStreamSuppressed = true;
  }
  videoPlaceholder.textContent = message || "Video not running.";
  videoPlaceholder.style.opacity = 1;
}

function ensureVideoStream() {
  if (videoStreamSuppressed) {
    videoStreamSuppressed = false;
  }
  attachVideoStream();
}

function stopVideoLoops(message, keepControls = false) {
  if (videoStatusTimer) {
    clearInterval(videoStatusTimer);
    videoStatusTimer = null;
  }
  stopVideoOverlayLoop(true);
  videoStreamSuppressed = false;
  lastVideoRobot = null;
  cleanupVideoStream();
  videoImg.removeAttribute('src');
  videoPlaceholder.textContent = message || "Select a component with video support.";
  videoPlaceholder.style.opacity = 1;
  videoStatusEl.textContent = "";
  if (!keepControls) {
    videoStartBtn.disabled = true;
    videoStopBtn.disabled = true;
  }
}

function clearVideoOverlayCanvas() {
  if (!videoOverlayCanvas) return;
  const ctx = videoOverlayCanvas.getContext('2d');
  if (!ctx) return;
  ctx.setTransform(1, 0, 0, 1, 0, 0);
  ctx.clearRect(0, 0, videoOverlayCanvas.width, videoOverlayCanvas.height);
}

function drawVideoOverlays() {
  try {
    if (!videoOverlayCanvas || !videoWrap) return;
    const ctx = videoOverlayCanvas.getContext('2d');
    if (!ctx) return;

    const width = Math.max(1, Math.floor(videoWrap.clientWidth));
    const height = Math.max(1, Math.floor(videoWrap.clientHeight));
    const dpr = window.devicePixelRatio || 1;
    videoOverlayCanvas.width = Math.max(1, Math.floor(width * dpr));
    videoOverlayCanvas.height = Math.max(1, Math.floor(height * dpr));
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, width, height);

    if (!videoOverlayToggle || !videoOverlayToggle.checked) return;
    if (!currentRobot || !robotHasVideo(currentRobot) || usesWebrtc(currentRobot)) return;
    if (!videoOverlayData || !Array.isArray(videoOverlayData.shapes) || !videoOverlayData.shapes.length) return;
    if (typeof videoOverlayData.lastUpdateAge === "number" && videoOverlayData.lastUpdateAge > VIDEO_OVERLAY_STALE_SECONDS) return;

    const naturalWidth = videoImg ? Number(videoImg.naturalWidth) : 0;
    const naturalHeight = videoImg ? Number(videoImg.naturalHeight) : 0;
    const sourceWidth = Math.max(
      1,
      Number(videoOverlayData.frameWidth) || naturalWidth || width,
    );
    const sourceHeight = Math.max(
      1,
      Number(videoOverlayData.frameHeight) || naturalHeight || height,
    );

    // Match `object-fit: cover` mapping used by the image stream.
    const scale = Math.max(width / sourceWidth, height / sourceHeight);
    const displayWidth = sourceWidth * scale;
    const displayHeight = sourceHeight * scale;
    const offsetX = (width - displayWidth) / 2;
    const offsetY = (height - displayHeight) / 2;

    ctx.strokeStyle = "#00ff5e";
    ctx.lineWidth = 2;
    ctx.lineJoin = "round";
    ctx.lineCap = "round";

    for (const shape of videoOverlayData.shapes) {
      if (!Array.isArray(shape) || shape.length < 2) continue;
      let started = false;
      ctx.beginPath();
      for (const point of shape) {
        if (!Array.isArray(point) || point.length < 2) continue;
        const px = Number(point[0]);
        const py = Number(point[1]);
        if (!Number.isFinite(px) || !Number.isFinite(py)) continue;
        const x = offsetX + (Math.max(0, Math.min(1, px)) * displayWidth);
        const y = offsetY + (Math.max(0, Math.min(1, py)) * displayHeight);
        if (!started) {
          ctx.moveTo(x, y);
          started = true;
        } else {
          ctx.lineTo(x, y);
        }
      }
      if (!started) continue;
      ctx.closePath();
      ctx.stroke();
    }
  } catch (err) {
    console.error("overlay draw error", err);
  }
}

function stopVideoOverlayLoop(clearState = false) {
  if (videoOverlayTimer) {
    clearInterval(videoOverlayTimer);
    videoOverlayTimer = null;
  }
  if (clearState) {
    videoOverlayData = null;
  }
  clearVideoOverlayCanvas();
}

async function fetchVideoOverlays() {
  if (videoOverlayFetchInFlight) return;
  if (!currentRobot || !robotHasVideo(currentRobot) || usesWebrtc(currentRobot)) {
    videoOverlayData = null;
    clearVideoOverlayCanvas();
    return;
  }
  if (!videoOverlayToggle || !videoOverlayToggle.checked) {
    clearVideoOverlayCanvas();
    return;
  }
  try {
    videoOverlayFetchInFlight = true;
    const { ok, data } = await fetchJson(`/robots/${currentRobot}/video/overlays?ts=${Date.now()}`);
    if (!ok) throw new Error("overlays");
    videoOverlayData = data;
    drawVideoOverlays();
  } catch (err) {
    console.error(err);
    videoOverlayData = null;
    clearVideoOverlayCanvas();
  } finally {
    videoOverlayFetchInFlight = false;
  }
}

function startVideoOverlayLoop() {
  if (!currentRobot || !robotHasVideo(currentRobot) || usesWebrtc(currentRobot)) {
    stopVideoOverlayLoop(true);
    return;
  }
  if (!videoOverlayToggle || !videoOverlayToggle.checked) {
    stopVideoOverlayLoop(true);
    return;
  }
  if (videoOverlayTimer) return;
  fetchVideoOverlays().catch(console.error);
  videoOverlayTimer = setInterval(() => { fetchVideoOverlays().catch(console.error); }, 500);
}

async function fetchVideoStatus() {
  if (!currentRobot || !robotHasVideo(currentRobot)) return;
  if (usesWebrtc(currentRobot)) return;
  try {
    const res = await fetch(`/robots/${currentRobot}/video/status?ts=${Date.now()}`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    if (!data.enabled) {
      videoStatusEl.textContent = "Video not configured.";
      videoStartBtn.disabled = true;
      videoStopBtn.disabled = true;
      return;
    }
    const ageValue = typeof data.lastFrameAge === "number" ? Math.max(0, data.lastFrameAge) : null;
    const hasFrame = Boolean(data.hasFrame);
    const stale = hasFrame && ageValue !== null && ageValue > VIDEO_STALE_SECONDS;
    const running = hasFrame && !stale;
    const age = ageValue !== null ? `${ageValue.toFixed(1)}s ago` : "no frame";
    const prefix = running ? "Streaming" : (hasFrame ? "Stopped" : "Idle");
    videoStatusEl.textContent = `${prefix} • ${data.hasFrame ? age : "no frame yet"}`;
    const hasControls = Boolean(data.controls);
    videoStartBtn.disabled = !hasControls;
    videoStopBtn.disabled = !hasControls;
    if (!hasControls) {
      videoStatusEl.textContent += " • no remote controls";
    }
    if (stale) {
      suppressVideoStream("Video not running.");
    } else if (hasFrame) {
      ensureVideoStream();
    }
  } catch (err) {
    console.error(err);
    videoStatusEl.textContent = "Video status unavailable.";
  }
}

function startVideoLoops() {
  if (!currentRobot || !robotHasVideo(currentRobot)) return;
  if (usesWebrtc(currentRobot)) return;
  if (videoStreamSuppressed) return;
  attachVideoStream();
  startVideoOverlayLoop();
  if (!videoStatusTimer) {
    fetchVideoStatus();
    videoStatusTimer = setInterval(fetchVideoStatus, 2000);
  }
}

function attachVideoStream() {
  if (!currentRobot || !robotHasVideo(currentRobot)) return;
  if (usesWebrtc(currentRobot)) return;
  if (streamingRobot === currentRobot) return;
  streamingRobot = currentRobot;
  awaitingFirstFrame = true;
  videoPlaceholder.textContent = "Connecting to video stream...";
  videoPlaceholder.style.opacity = 1;
  const streamUrl = `/robots/${currentRobot}/video/stream?ts=${Date.now()}`;
  videoImg.dataset.stream = currentRobot;
  videoImg.src = streamUrl;
}

function refreshVideoVisibility() {
  if (videoOverlayLabel) {
    videoOverlayLabel.classList.remove('hidden');
  }
  if (!currentRobot) {
    stopVideoLoops("Select a component to begin.");
    videoStartBtn.disabled = true;
    videoStopBtn.disabled = true;
    if (videoOverlayLabel) videoOverlayLabel.classList.add('hidden');
    refreshWebrtcVisibility();
    return;
  }
  if (robotHasVideo(currentRobot)) {
    const usingWebrtc = refreshWebrtcVisibility();
    if (usingWebrtc) {
      cleanupVideoStream();
      stopVideoOverlayLoop(true);
      if (videoStatusTimer) {
        clearInterval(videoStatusTimer);
        videoStatusTimer = null;
      }
      if (videoOverlayLabel) videoOverlayLabel.classList.add('hidden');
      const url = getWebrtcUrl(currentRobot);
      videoImg.style.display = "none";
      if (url) {
        videoPlaceholder.style.display = "none";
        videoStatusEl.textContent = "WebRTC viewer active.";
      } else {
        videoPlaceholder.style.display = "flex";
        videoPlaceholder.textContent = "Set WebRTC URL to view stream.";
        videoStatusEl.textContent = "Waiting for WebRTC URL.";
      }
      const controllable = robotHasControls(currentRobot);
      videoStartBtn.disabled = !controllable;
      videoStopBtn.disabled = !controllable;
      return;
    }
    videoImg.style.display = "block";
    videoPlaceholder.style.display = "flex";
    if (videoOverlayLabel) videoOverlayLabel.classList.remove('hidden');
    if (currentRobot !== lastVideoRobot) {
      videoStreamSuppressed = false;
      lastVideoRobot = currentRobot;
      videoOverlayData = null;
      clearVideoOverlayCanvas();
    }
    if (streamingRobot && streamingRobot !== currentRobot) {
      cleanupVideoStream();
    }
    startVideoLoops();
    const controllable = robotHasControls(currentRobot);
    videoStartBtn.disabled = !controllable;
    videoStopBtn.disabled = !controllable;
  } else {
    stopVideoLoops("No video stream configured for this component.");
    videoStartBtn.disabled = true;
    videoStopBtn.disabled = true;
    if (videoOverlayLabel) videoOverlayLabel.classList.add('hidden');
  }
}

async function sendAudioCommand(action) {
  if (!currentRobot || !robotHasAudio(currentRobot) || !robotHasAudioControls(currentRobot)) return;
  const targetBtn = action === "start" ? audioStartBtn : audioStopBtn;
  targetBtn.disabled = true;
  try {
    const res = await fetch(`/robots/${currentRobot}/audio/${action}`, { method: "POST" });
    if (!res.ok) {
      const text = await res.text();
      throw new Error(text || `HTTP ${res.status}`);
    }
  } catch (err) {
    console.error(err);
    audioStatusEl.textContent = `Failed to ${action} audio: ${err.message}`;
  } finally {
    targetBtn.disabled = false;
  }
}

function updateIncomingGain() {
  if (audioGain) {
    audioGain.gain.value = audioIncomingMuted ? 0 : 1;
  }
}

function clearAudioConcealmentTimer() {
  if (!audioConcealmentTimer) return;
  clearInterval(audioConcealmentTimer);
  audioConcealmentTimer = null;
}

function startAudioConcealmentTimer() {
  clearAudioConcealmentTimer();
  audioConcealmentTimer = setInterval(() => {
    if (!audioSource || !audioCtx || !audioLastChunkPcm) return;
    if (audioIncomingMuted) return;
    if (audioConcealedMs >= audioConcealmentMs) return;
    if (audioCtx.currentTime + 0.01 < audioQueueTime) return;
    if (playPcmChunk(audioLastChunkPcm, audioLastChunkRate, audioLastChunkChannels)) {
      audioConcealedMs += audioChunkMs;
      updateAudioIndicators();
      audioStatusEl.textContent = "Concealing packet loss...";
    }
  }, Math.max(10, Math.floor(audioChunkMs / 2)));
}

function closeAudioEventSource() {
  if (audioSource) {
    audioSource.close();
    audioSource = null;
  }
}

function resetIncomingAudioDecoderState() {
  audioQueueTime = audioCtx ? audioCtx.currentTime : 0;
  audioHadChunk = false;
  audioDrops = 0;
  audioExpectedSeq = null;
  audioConcealedMs = 0;
  audioLastChunkPcm = null;
}

function attachIncomingAudioStream(robotId, statusMessage = "Connecting...") {
  closeAudioEventSource();
  resetIncomingAudioDecoderState();
  audioRobot = robotId;
  audioStatusEl.textContent = statusMessage;
  const url = `/robots/${robotId}/audio/stream?ts=${Date.now()}`;
  const es = new EventSource(url);
  audioSource = es;
  es.onmessage = (event) => {
    try {
      const payload = JSON.parse(event.data || "{}");
      handleAudioChunk(payload);
    } catch (err) {
      console.error(err);
    }
  };
  es.onerror = () => {
    audioStatusEl.textContent = "Audio stream error.";
  };
}

function cleanupAudioStream() {
  clearAudioConcealmentTimer();
  closeAudioEventSource();
  audioRobot = null;
  syncAudioButtons();
}

function stopAudio(message, stopOutgoing = false) {
  cleanupAudioStream();
  audioQueueTime = 0;
  audioHadChunk = false;
  audioDrops = 0;
  audioExpectedSeq = null;
  audioLastChunkPcm = null;
  audioConcealedMs = 0;
  audioTalkUnmuted = false;
  audioIncomingMuted = false;
  updateIncomingGain();
  audioStatusEl.textContent = message || "Audio stopped.";
  audioMetaEl.textContent = "";
  if (stopOutgoing) {
    stopAudioUplink();
  }
  syncAudioButtons();
  updateAudioIndicators();
}

function ensureAudioContext() {
  if (!audioCtx) {
    const Ctx = window.AudioContext || window.webkitAudioContext;
    if (!Ctx) {
      audioStatusEl.textContent = "Browser audio unsupported.";
      return;
    }
    audioCtx = new Ctx();
    audioGain = audioCtx.createGain();
    audioGain.gain.value = audioIncomingMuted ? 0 : 1;
    audioGain.connect(audioCtx.destination);
  }
  updateIncomingGain();
}

function base64ToBytes(b64) {
  try {
    const bin = atob(b64);
    const out = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
    return out;
  } catch (err) {
    console.error("Failed to decode audio chunk", err);
    return null;
  }
}

function bytesToBase64(bytes) {
  let binary = "";
  const chunkSize = 0x8000;
  for (let i = 0; i < bytes.length; i += chunkSize) {
    const slice = bytes.subarray(i, i + chunkSize);
    binary += String.fromCharCode.apply(null, slice);
  }
  return btoa(binary);
}

function updateAudioIndicators() {
  if (audioInIndicator) {
    audioInIndicator.classList.toggle('active', audioHadChunk && !audioIncomingMuted && !!audioSource);
  }
  if (audioOutIndicator) {
    audioOutIndicator.classList.toggle('active', audioOutgoingHadChunk && audioOutgoingActive && audioTalkUnmuted);
  }
  if (audioModeIndicator) {
    const label = audioTalkUnmuted ? "Talking" : "Listening";
    audioModeIndicator.innerHTML = `<span class="dot"></span>${label}`;
    audioModeIndicator.classList.toggle('active', true);
  }
}

function syncAudioButtons() {
  const hasAudio = currentRobot && robotHasAudio(currentRobot);
  audioStartBtn.disabled = !hasAudio || (!!audioSource && audioRobot === currentRobot);
  audioTalkBtn.disabled = !hasAudio || !audioSource;
  audioStopBtn.disabled = !audioSource && !audioOutgoingActive;
  audioTalkBtn.textContent = audioTalkUnmuted ? "Talk: Unmuted" : "Talk: Muted";
}

function stopAudioUplink() {
  audioOutgoingActive = false;
  audioOutgoingHadChunk = false;
  audioOutgoingQueue = [];
  audioOutgoingSending = false;
  audioOutgoingRobot = null;
  if (audioOutgoingProcessor) {
    audioOutgoingProcessor.disconnect();
    audioOutgoingProcessor.onaudioprocess = null;
    audioOutgoingProcessor = null;
  }
  if (audioOutgoingSource) {
    audioOutgoingSource.disconnect();
    audioOutgoingSource = null;
  }
  if (audioCaptureCtx) {
    audioCaptureCtx.close().catch(() => {});
    audioCaptureCtx = null;
  }
  if (audioOutgoingStream) {
    audioOutgoingStream.getTracks().forEach(t => t.stop());
    audioOutgoingStream = null;
  }
  syncAudioButtons();
  updateAudioIndicators();
}

function enqueueOutgoingChunk(pcm, rate, channels) {
  if (!currentRobot || !audioOutgoingActive || !audioTalkUnmuted) return;
  const bytes = new Uint8Array(pcm.buffer, pcm.byteOffset, pcm.byteLength);
  if (audioOutgoingQueue.length > 12) {
    audioOutgoingQueue.shift();
  }
  audioOutgoingQueue.push({ bytes, rate, channels });
  flushOutgoingQueue().catch(console.error);
}

async function flushOutgoingQueue() {
  if (audioOutgoingSending || !audioTalkUnmuted || !audioOutgoingActive) return;
  const targetRobot = audioOutgoingRobot;
  if (!targetRobot) {
    audioOutgoingQueue = [];
    return;
  }
  audioOutgoingSending = true;
  try {
    while (audioOutgoingQueue.length && audioOutgoingActive && audioTalkUnmuted) {
      const { bytes, rate, channels } = audioOutgoingQueue.shift();
      const data = bytesToBase64(bytes);
      const res = await fetch(`/robots/${targetRobot}/audio/uplink`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ data, rate, channels })
      });
      if (!res.ok) {
        audioStatusEl.textContent = "Audio uplink error.";
        audioOutgoingQueue = [];
        break;
      }
      audioOutgoingHadChunk = true;
      audioStatusEl.textContent = "Talking to component...";
      updateAudioIndicators();
    }
  } finally {
    audioOutgoingSending = false;
  }
}

async function startAudioUplink() {
  if (!currentRobot || !robotHasAudio(currentRobot)) return;
  if (audioOutgoingActive) return;
  try {
    const hasGum = navigator.mediaDevices && typeof navigator.mediaDevices.getUserMedia === "function";
    if (!hasGum) {
      audioStatusEl.textContent = "Mic capture blocked; use HTTPS/localhost and allow mic access.";
      return;
    }
    const stream = await navigator.mediaDevices.getUserMedia({ audio: true, video: false });
    const Ctx = window.AudioContext || window.webkitAudioContext;
    if (!Ctx) {
      audioStatusEl.textContent = "Browser audio unsupported.";
      return;
    }
    try {
    audioCaptureCtx = new Ctx({ sampleRate: 16000 });
  } catch (err) {
    audioCaptureCtx = new Ctx();
  }
  audioOutgoingStream = stream;
  audioOutgoingHadChunk = false;
  audioOutgoingSource = audioCaptureCtx.createMediaStreamSource(stream);
    const channels = audioOutgoingSource.channelCount || 1;
    audioOutgoingProcessor = audioCaptureCtx.createScriptProcessor(4096, channels, channels);
    const nullGain = audioCaptureCtx.createGain();
    nullGain.gain.value = 0;
    audioOutgoingSource.connect(audioOutgoingProcessor);
    audioOutgoingProcessor.connect(nullGain);
    nullGain.connect(audioCaptureCtx.destination);
    audioOutgoingProcessor.onaudioprocess = (event) => {
      if (!audioOutgoingActive || !audioTalkUnmuted) return;
      const input = event.inputBuffer;
      const frames = input.length;
      const chCount = input.numberOfChannels || 1;
      const pcm = new Int16Array(frames * chCount);
      for (let ch = 0; ch < chCount; ch++) {
        const channelData = input.getChannelData(ch);
        for (let i = 0; i < frames; i++) {
          const sample = Math.max(-1, Math.min(1, channelData[i] || 0));
          pcm[i * chCount + ch] = sample < 0 ? sample * 32768 : sample * 32767;
        }
      }
      enqueueOutgoingChunk(pcm, audioCaptureCtx.sampleRate, chCount);
    };
    audioOutgoingActive = true;
    audioOutgoingRobot = currentRobot;
    audioStatusEl.textContent = "Mic ready.";
  } catch (err) {
    console.error(err);
    const hint = window.isSecureContext ? "" : " (enable mic permissions and use HTTPS/localhost)";
    audioStatusEl.textContent = `Mic error: ${err.message || err}${hint}`;
    audioTalkUnmuted = false;
    audioIncomingMuted = false;
    updateIncomingGain();
    stopAudioUplink();
  }
  syncAudioButtons();
  updateAudioIndicators();
}

function playPcmChunk(pcm, rate, channels) {
  if (!audioCtx || !audioGain || !pcm || !pcm.length) return false;
  const frames = Math.floor(pcm.length / channels);
  if (!frames) return false;
  const buffer = audioCtx.createBuffer(channels, frames, rate);
  for (let ch = 0; ch < channels; ch++) {
    const channelData = buffer.getChannelData(ch);
    for (let i = 0; i < frames; i++) {
      channelData[i] = (pcm[i * channels + ch] || 0) / 32768;
    }
  }
  const source = audioCtx.createBufferSource();
  source.buffer = buffer;
  source.connect(audioGain);
  let startAt = Math.max(audioQueueTime, audioCtx.currentTime + 0.02);
  const lead = startAt - audioCtx.currentTime;
  if (lead > 0.75) {
    audioDrops += 1;
    startAt = audioCtx.currentTime + 0.02;
    audioQueueTime = startAt;
  }
  source.start(startAt);
  audioQueueTime = startAt + buffer.duration;
  return true;
}

function decodePcmBytes(bytes, channels) {
  const aligned = bytes.byteLength - (bytes.byteLength % (channels * 2));
  if (!aligned) return null;
  const view = new DataView(bytes.buffer, bytes.byteOffset, aligned);
  const pcm = new Int16Array(aligned / 2);
  for (let i = 0; i < pcm.length; i++) {
    pcm[i] = view.getInt16(i * 2, true);
  }
  return pcm;
}

function handleAudioChunk(payload) {
  if (!payload || !payload.data) return;
  if (audioTalkUnmuted) return;
  ensureAudioContext();
  if (!audioCtx || !audioGain) return;
  const bytes = base64ToBytes(payload.data);
  if (!bytes) return;
  const channels = Math.max(1, Number(payload.channels) || 1);
  const rate = Math.max(1, Number(payload.rate) || 16000);
  const frameSamples = Math.max(1, Number(payload.frame_samples) || Math.floor(bytes.byteLength / (channels * 2)));
  const seq = Number(payload.seq);

  if (Number.isFinite(seq) && audioExpectedSeq !== null) {
    if (seq < audioExpectedSeq) {
      return;
    }
    if (seq > audioExpectedSeq && audioLastChunkPcm) {
      const missing = seq - audioExpectedSeq;
      const packetMs = Math.max(1, frameSamples * 1000 / rate);
      const remainingMs = Math.max(0, audioConcealmentMs - audioConcealedMs);
      const maxConceal = Math.floor(remainingMs / packetMs);
      const toInject = Math.min(missing, maxConceal);
      for (let i = 0; i < toInject; i++) {
        if (!playPcmChunk(audioLastChunkPcm, audioLastChunkRate, audioLastChunkChannels)) break;
        audioConcealedMs += packetMs;
      }
    }
  }

  const pcm = decodePcmBytes(bytes, channels);
  if (!pcm) return;

  audioChunkMs = Math.max(1, frameSamples * 1000 / rate);
  audioConcealedMs = 0;
  audioExpectedSeq = Number.isFinite(seq) ? (seq + 1) : null;
  if (!playPcmChunk(pcm, rate, channels)) return;
  audioLastChunkPcm = pcm;
  audioLastChunkRate = rate;
  audioLastChunkChannels = channels;
  audioHadChunk = true;
  audioMetaEl.textContent = `${rate} Hz • ${channels}ch • ${Math.round(audioChunkMs)} ms • conceal ${audioConcealmentMs} ms`;
  audioStatusEl.textContent = audioTalkUnmuted ? "Talking to component..." : "Listening...";
  updateAudioIndicators();
}

async function startAudioStream() {
  if (!currentRobot || !robotHasAudio(currentRobot)) return;
  stopAudio("", true);
  if (robotHasAudioControls(currentRobot)) {
    await sendAudioCommand("start");
  }
  ensureAudioContext();
  if (audioCtx && audioCtx.state === "suspended") {
    audioCtx.resume().catch(err => console.error(err));
  }
  try {
    const res = await fetch(`/robots/${currentRobot}/audio/status?ts=${Date.now()}`);
    if (res.ok) {
      const status = await res.json();
      if (Number.isFinite(Number(status.concealmentMs))) {
        audioConcealmentMs = Math.max(0, Number(status.concealmentMs));
      }
      if (Number.isFinite(Number(status.chunkMs))) {
        audioChunkMs = Math.max(1, Number(status.chunkMs));
      }
    }
  } catch (err) {
    console.error(err);
  }
  attachIncomingAudioStream(currentRobot, "Connecting...");
  startAudioConcealmentTimer();
  syncAudioButtons();
  updateAudioIndicators();
}

async function toggleTalkMode() {
  if (!currentRobot || !robotHasAudio(currentRobot) || !audioSource) return;
  audioTalkUnmuted = !audioTalkUnmuted;
  audioIncomingMuted = audioTalkUnmuted;
  updateIncomingGain();
  if (audioTalkUnmuted) {
    resetIncomingAudioDecoderState();
    if (robotHasAudioControls(currentRobot)) {
      sendAudioCommand("start").catch(console.error);
    }
    if (!audioOutgoingActive) {
      await startAudioUplink();
    }
    if (!audioOutgoingActive) {
      audioTalkUnmuted = false;
      audioIncomingMuted = false;
      updateIncomingGain();
      syncAudioButtons();
      updateAudioIndicators();
      return;
    }
    audioStatusEl.textContent = "Talking to component...";
  } else {
    stopAudioUplink();
    attachIncomingAudioStream(currentRobot, "Listening (re-syncing)...");
    audioStatusEl.textContent = "Listening (re-syncing)...";
  }
  syncAudioButtons();
  updateAudioIndicators();
}

function refreshAudioVisibility() {
  if (!currentRobot) {
    stopAudio("Select a component to begin.", true);
    syncAudioButtons();
    return;
  }
  const hasAudio = robotHasAudio(currentRobot);
  if (hasAudio && !robotHasAudioControls(currentRobot)) {
    audioStatusEl.textContent = "Mic must be started manually on component.";
  }
  if (!hasAudio) {
    stopAudio("Audio not available for this component.", true);
  } else if (audioRobot && audioRobot !== currentRobot) {
    stopAudio("Component changed; restart audio.", true);
  } else if (!audioSource && !audioOutgoingActive) {
    audioStatusEl.textContent = "Audio ready. Press Start Audio.";
  }
  syncAudioButtons();
}

function resetOutletsState() {
  outlets = [];
  outletControlsEnabled = currentRobot ? robotHasOutletControls(currentRobot) : false;
  outletBusyId = "";
  if (outletsStatusEl) {
    if (!currentRobot) {
      outletsStatusEl.textContent = "Select a component to begin.";
    } else {
      outletsStatusEl.textContent = "Waiting for outlet state from component...";
    }
  }
}

function renderOutlets() {
  if (!outletsListEl) return;
  outletsListEl.innerHTML = "";
  if (!currentRobot) {
    const empty = document.createElement("div");
    empty.className = "outlet-empty";
    empty.textContent = "Select a component to view outlets.";
    outletsListEl.appendChild(empty);
    return;
  }
  if (!robotHasOutlets(currentRobot) && !outlets.length) {
    const empty = document.createElement("div");
    empty.className = "outlet-empty";
    empty.textContent = "Outlets unavailable for this component.";
    outletsListEl.appendChild(empty);
    return;
  }
  if (!outlets.length) {
    const empty = document.createElement("div");
    empty.className = "outlet-empty";
    empty.textContent = "Waiting for outlet state from component...";
    outletsListEl.appendChild(empty);
    return;
  }

  outlets.forEach((outlet) => {
    const item = document.createElement("div");
    item.className = "outlet-item";

    const head = document.createElement("div");
    head.className = "outlet-head";
    const name = document.createElement("div");
    name.className = "outlet-name";
    name.textContent = outlet.name || outlet.id;
    head.appendChild(name);

    const state = document.createElement("div");
    state.className = "outlet-state";
    const power = document.createElement("span");
    power.className = `outlet-pill ${outlet.on ? "on" : "off"}`;
    power.textContent = outlet.on ? "On" : "Off";
    state.appendChild(power);
    const reach = document.createElement("span");
    reach.className = `outlet-pill ${outlet.reachable ? "on" : "warn"}`;
    reach.textContent = outlet.reachable ? "Reachable" : "Unreachable";
    state.appendChild(reach);
    const bound = document.createElement("span");
    bound.className = `outlet-pill ${outlet.bound ? "on" : "warn"}`;
    bound.textContent = outlet.bound ? "Bound" : "Unbound";
    state.appendChild(bound);
    head.appendChild(state);
    item.appendChild(head);

    const actions = document.createElement("div");
    actions.className = "outlet-actions";
    const toggleBtn = document.createElement("button");
    toggleBtn.type = "button";
    toggleBtn.textContent = outlet.on ? "Turn Off" : "Turn On";
    toggleBtn.disabled = !outletControlsEnabled || outletBusyId === outlet.id;
    toggleBtn.addEventListener("click", () => {
      toggleOutlet(outlet.id, !outlet.on).catch(console.error);
    });
    actions.appendChild(toggleBtn);

    const meta = document.createElement("span");
    meta.className = "mono";
    if (typeof outlet.lastUpdateAge === "number") {
      meta.textContent = `${outlet.lastUpdateAge.toFixed(1)}s ago`;
    } else {
      meta.textContent = outlet.commandTopic || "";
    }
    actions.appendChild(meta);
    item.appendChild(actions);
    outletsListEl.appendChild(item);
  });
}

async function refreshOutlets() {
  if (!currentRobot) {
    resetOutletsState();
    renderOutlets();
    return;
  }
  try {
    const { ok, data } = await fetchJson(`/robots/${currentRobot}/outlets?ts=${Date.now()}`);
    if (!ok) throw new Error(data.error || "outlets");
    outletControlsEnabled = typeof data.controls === "boolean"
      ? data.controls
      : robotHasOutletControls(currentRobot);
    const items = Array.isArray(data.outlets) ? data.outlets.filter(item => item && typeof item.id === "string") : [];
    outlets = items;
    if (findRobot(currentRobot)) {
      const robot = findRobot(currentRobot);
      if (robot) {
        robot.hasOutlets = Boolean(data.enabled || items.length);
        robot.outletControls = outletControlsEnabled;
      }
    }
    if (!items.length) {
      outletsStatusEl.textContent = robotHasOutlets(currentRobot)
        ? "Waiting for outlet state from component..."
        : "Outlets unavailable for this component.";
    } else if (!outletControlsEnabled) {
      outletsStatusEl.textContent = "Outlet state available; remote toggles disabled.";
    } else {
      outletsStatusEl.textContent = `${items.length} outlet${items.length === 1 ? "" : "s"} available.`;
    }
    syncSectionVisibility();
    renderOutlets();
  } catch (err) {
    console.error(err);
    if (outletsStatusEl) {
      outletsStatusEl.textContent = "Outlet status unavailable.";
    }
  }
}

async function toggleOutlet(outletId, on) {
  if (!currentRobot || !outletId || outletBusyId) return;
  outletBusyId = outletId;
  if (outletsStatusEl) {
    outletsStatusEl.textContent = `${on ? "Turning on" : "Turning off"} ${outletId}...`;
  }
  renderOutlets();
  try {
    const { ok, data } = await fetchJson(`/robots/${currentRobot}/outlets/${encodeURIComponent(outletId)}/power`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ on }),
    });
    if (!ok) {
      throw new Error(data.error || "Failed to toggle outlet.");
    }
  } catch (err) {
    console.error(err);
    if (outletsStatusEl) {
      outletsStatusEl.textContent = `${err.message || err}`;
    }
  } finally {
    outletBusyId = "";
    await refreshOutlets();
  }
}

function selectedSoundIsPlaying() {
  return Boolean(selectedSoundFile && soundIsPlaying && playingSoundFile === selectedSoundFile);
}

function resetSoundboardState() {
  soundFiles = [];
  selectedSoundFile = "";
  playingSoundFile = "";
  soundIsPlaying = false;
  soundboardControlsEnabled = currentRobot ? robotHasSoundboardControls(currentRobot) : false;
}

function updateSoundboardUi() {
  const hasRobot = Boolean(currentRobot);
  const hasFeature = hasRobot && robotHasSoundboard(currentRobot);
  const hasSelection = Boolean(selectedSoundFile);
  soundboardControls.classList.toggle('hidden', !hasSelection || !hasFeature);
  if (!hasRobot) {
    soundSelectionEl.textContent = "Select a component to begin.";
  } else if (!hasFeature) {
    soundSelectionEl.textContent = "Soundboard unavailable for this component.";
  } else if (hasSelection) {
    soundSelectionEl.textContent = `Selected: ${selectedSoundFile}`;
  } else if (soundFiles.length) {
    soundSelectionEl.textContent = "Select a sound file.";
  } else {
    soundSelectionEl.textContent = "Waiting for sound list from component...";
  }
  soundPlayStopBtn.textContent = selectedSoundIsPlaying() ? "Stop" : "Play";
  soundPlayStopBtn.disabled = !hasSelection || soundboardBusy || !soundboardControlsEnabled;
  if (soundIsPlaying && playingSoundFile) {
    soundboardStatusEl.textContent = `Playing: ${playingSoundFile}`;
  } else if (hasRobot && !soundboardControlsEnabled) {
    soundboardStatusEl.textContent = "No remote soundboard controls.";
  } else if (!soundboardBusy) {
    soundboardStatusEl.textContent = hasRobot ? "Idle" : "";
  }
}

function renderSoundList() {
  soundListEl.innerHTML = "";
  if (!soundFiles.length) {
    const empty = document.createElement("div");
    empty.className = "sound-empty";
    if (!currentRobot) {
      empty.textContent = "Select a component to view sounds.";
    } else if (!robotHasSoundboard(currentRobot)) {
      empty.textContent = "Soundboard unavailable for this component.";
    } else {
      empty.textContent = "No .wav files reported yet.";
    }
    soundListEl.appendChild(empty);
    return;
  }
  soundFiles.forEach((file) => {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "sound-item";
    if (file === selectedSoundFile) {
      btn.classList.add("active");
    }
    btn.textContent = file;
    btn.addEventListener("click", () => {
      selectedSoundFile = file;
      renderSoundList();
      updateSoundboardUi();
    });
    soundListEl.appendChild(btn);
  });
}

async function refreshSoundFiles() {
  if (!currentRobot) {
    resetSoundboardState();
    renderSoundList();
    updateSoundboardUi();
    return;
  }
  try {
    const { ok, data } = await fetchJson(`/robots/${currentRobot}/soundboard/files?ts=${Date.now()}`);
    if (!ok) throw new Error("files");
    soundboardControlsEnabled = typeof data.controls === "boolean"
      ? data.controls
      : robotHasSoundboardControls(currentRobot);
    const items = Array.isArray(data.files) ? data.files.filter(item => typeof item === "string") : [];
    soundFiles = items;
    if (selectedSoundFile && !soundFiles.includes(selectedSoundFile)) {
      selectedSoundFile = "";
    }
    if (!selectedSoundFile && playingSoundFile && soundFiles.includes(playingSoundFile)) {
      selectedSoundFile = playingSoundFile;
    }
    renderSoundList();
    updateSoundboardUi();
  } catch (err) {
    console.error(err);
    if (!soundboardBusy) {
      soundboardStatusEl.textContent = "Sound list unavailable.";
    }
  }
}

async function refreshSoundboardStatus() {
  if (!currentRobot) return;
  try {
    const { ok, data } = await fetchJson(`/robots/${currentRobot}/soundboard/status?ts=${Date.now()}`);
    if (!ok) throw new Error("status");
    soundboardControlsEnabled = typeof data.controls === "boolean"
      ? data.controls
      : robotHasSoundboardControls(currentRobot);
    soundIsPlaying = Boolean(data.playing);
    playingSoundFile = soundIsPlaying && typeof data.file === "string" ? data.file : "";
    if (!selectedSoundFile && playingSoundFile) {
      selectedSoundFile = playingSoundFile;
    }
    if (!soundIsPlaying && data.error && !soundboardBusy) {
      soundboardStatusEl.textContent = data.error;
    }
    renderSoundList();
    updateSoundboardUi();
  } catch (err) {
    console.error(err);
    if (!soundboardBusy) {
      soundboardStatusEl.textContent = "Sound status unavailable.";
    }
  }
}

async function playSelectedSound() {
  if (!currentRobot || !selectedSoundFile || soundboardBusy) return;
  soundboardBusy = true;
  soundboardStatusEl.textContent = "Starting...";
  updateSoundboardUi();
  try {
    const { ok, data } = await fetchJson(`/robots/${currentRobot}/soundboard/play`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ file: selectedSoundFile })
    });
    if (!ok) {
      throw new Error(data.error || "Failed to play sound.");
    }
  } catch (err) {
    console.error(err);
    soundboardStatusEl.textContent = `${err.message || err}`;
  } finally {
    soundboardBusy = false;
    await refreshSoundboardStatus();
  }
}

async function stopSelectedSound() {
  if (!currentRobot || soundboardBusy) return;
  soundboardBusy = true;
  soundboardStatusEl.textContent = "Stopping...";
  updateSoundboardUi();
  try {
    await fetch(`/robots/${currentRobot}/soundboard/stop`, { method: "POST" });
  } catch (err) {
    console.error(err);
    soundboardStatusEl.textContent = "Failed to stop sound.";
  } finally {
    soundboardBusy = false;
    await refreshSoundboardStatus();
  }
}

function selectedAutonomyDescriptor() {
  return autonomyFiles.find(item => item && item.file === selectedAutonomyFile) || null;
}

function resetAutonomyState() {
  autonomyFiles = [];
  selectedAutonomyFile = "";
  runningAutonomyFile = "";
  autonomyRunning = false;
  autonomyBusy = false;
  autonomyControlsEnabled = currentRobot ? robotHasAutonomyControls(currentRobot) : false;
  selectedAutonomyConfigValues = {};
}

function selectedAutonomyIsRunning() {
  return Boolean(selectedAutonomyFile && autonomyRunning && runningAutonomyFile === selectedAutonomyFile);
}

function renderAutonomyConfigFields() {
  autonomyConfigEl.innerHTML = "";
  const descriptor = selectedAutonomyDescriptor();
  if (!descriptor) {
    const empty = document.createElement("div");
    empty.className = "autonomy-empty";
    empty.textContent = currentRobot ? "Select an autonomy script to edit runtime config." : "Select a component first.";
    autonomyConfigEl.appendChild(empty);
    return;
  }
  const configs = Array.isArray(descriptor.configs) ? descriptor.configs : [];
  if (!configs.length) {
    const empty = document.createElement("div");
    empty.className = "autonomy-empty";
    empty.textContent = "No runtime config fields for this script.";
    autonomyConfigEl.appendChild(empty);
    return;
  }
  configs.forEach((cfg) => {
    if (!cfg || typeof cfg.key !== "string") return;
    const key = cfg.key;
    const row = document.createElement("div");
    row.className = "autonomy-field";
    const label = document.createElement("label");
    label.textContent = cfg.label || key;
    label.setAttribute("for", `autonomyCfg_${key}`);
    row.appendChild(label);

    let input = null;
    const type = String(cfg.type || "string").toLowerCase();
    if (type === "bool") {
      input = document.createElement("input");
      input.type = "checkbox";
      const value = selectedAutonomyConfigValues[key];
      input.checked = Boolean(value);
      input.addEventListener("change", () => {
        selectedAutonomyConfigValues[key] = input.checked;
      });
    } else if (type === "enum") {
      input = document.createElement("select");
      const options = Array.isArray(cfg.options) ? cfg.options : [];
      options.forEach((opt) => {
        const option = document.createElement("option");
        option.value = String(opt);
        option.textContent = String(opt);
        input.appendChild(option);
      });
      const value = selectedAutonomyConfigValues[key];
      if (value !== undefined && value !== null) {
        input.value = String(value);
      }
      input.addEventListener("change", () => {
        selectedAutonomyConfigValues[key] = input.value;
      });
    } else {
      input = document.createElement("input");
      input.type = (type === "int" || type === "float") ? "number" : "text";
      if (type === "int") input.step = String(cfg.step ?? 1);
      if (type === "float") input.step = String(cfg.step ?? "any");
      if (cfg.min !== undefined) input.min = String(cfg.min);
      if (cfg.max !== undefined) input.max = String(cfg.max);
      const value = selectedAutonomyConfigValues[key];
      if (value !== undefined && value !== null) {
        input.value = String(value);
      } else if (cfg.default !== undefined && cfg.default !== null) {
        input.value = String(cfg.default);
      }
      input.addEventListener("input", () => {
        if (type === "int") {
          const parsed = Number.parseInt(input.value, 10);
          selectedAutonomyConfigValues[key] = Number.isFinite(parsed) ? parsed : input.value;
        } else if (type === "float") {
          const parsed = Number.parseFloat(input.value);
          selectedAutonomyConfigValues[key] = Number.isFinite(parsed) ? parsed : input.value;
        } else {
          selectedAutonomyConfigValues[key] = input.value;
        }
      });
    }
    input.id = `autonomyCfg_${key}`;
    row.appendChild(input);
    autonomyConfigEl.appendChild(row);
  });
}

function renderAutonomyList() {
  autonomyListEl.innerHTML = "";
  if (!autonomyFiles.length) {
    const empty = document.createElement("div");
    empty.className = "sound-empty";
    if (!currentRobot) {
      empty.textContent = "Select a component to view autonomy scripts.";
    } else if (!robotHasAutonomy(currentRobot)) {
      empty.textContent = "Autonomy unavailable for this component.";
    } else {
      empty.textContent = "No autonomy scripts reported yet.";
    }
    autonomyListEl.appendChild(empty);
    renderAutonomyConfigFields();
    return;
  }
  autonomyFiles.forEach((entry) => {
    if (!entry || !entry.file) return;
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "autonomy-item";
    if (entry.file === selectedAutonomyFile) {
      btn.classList.add("active");
    }
    btn.textContent = entry.label || entry.file;
    btn.addEventListener("click", () => {
      selectedAutonomyFile = entry.file;
      const cfgs = Array.isArray(entry.configs) ? entry.configs : [];
      const nextValues = {};
      cfgs.forEach((cfg) => {
        if (!cfg || typeof cfg.key !== "string") return;
        nextValues[cfg.key] = cfg.default;
      });
      selectedAutonomyConfigValues = nextValues;
      renderAutonomyList();
      updateAutonomyUi();
      renderAutonomyConfigFields();
    });
    autonomyListEl.appendChild(btn);
  });
  renderAutonomyConfigFields();
}

function updateAutonomyUi() {
  const hasRobot = Boolean(currentRobot);
  const hasFeature = hasRobot && robotHasAutonomy(currentRobot);
  const hasSelection = Boolean(selectedAutonomyFile);
  if (!hasRobot) {
    autonomySelectionEl.textContent = "Select a component to begin.";
  } else if (!hasFeature) {
    autonomySelectionEl.textContent = "Autonomy unavailable for this component.";
  } else if (hasSelection) {
    autonomySelectionEl.textContent = `Selected: ${selectedAutonomyFile}`;
  } else if (autonomyFiles.length) {
    autonomySelectionEl.textContent = "Select an autonomy script.";
  } else {
    autonomySelectionEl.textContent = "Waiting for autonomy list from component...";
  }

  const shouldStop = selectedAutonomyIsRunning();
  autonomyStartStopBtn.textContent = shouldStop ? "Stop" : "Start";
  autonomyStartStopBtn.disabled = !hasSelection || autonomyBusy || !autonomyControlsEnabled;
  autonomyControlsEl.classList.toggle("hidden", !hasFeature);

  if (autonomyRunning && runningAutonomyFile) {
    autonomyStatusEl.textContent = `Running: ${runningAutonomyFile}`;
  } else if (hasRobot && !autonomyControlsEnabled) {
    autonomyStatusEl.textContent = "No remote autonomy controls.";
  } else if (!autonomyBusy) {
    autonomyStatusEl.textContent = hasRobot ? "Idle" : "";
  }
}

async function refreshAutonomyFiles() {
  if (!currentRobot) {
    resetAutonomyState();
    renderAutonomyList();
    updateAutonomyUi();
    return;
  }
  try {
    const { ok, data } = await fetchJson(`/robots/${currentRobot}/autonomy/files?ts=${Date.now()}`);
    if (!ok) throw new Error("files");
    autonomyControlsEnabled = typeof data.controls === "boolean"
      ? data.controls
      : robotHasAutonomyControls(currentRobot);
    const items = Array.isArray(data.files) ? data.files.filter(item => item && typeof item.file === "string") : [];
    autonomyFiles = items;
    if (selectedAutonomyFile && !autonomyFiles.some(item => item.file === selectedAutonomyFile)) {
      selectedAutonomyFile = "";
      selectedAutonomyConfigValues = {};
    }
    if (!selectedAutonomyFile && runningAutonomyFile && autonomyFiles.some(item => item.file === runningAutonomyFile)) {
      selectedAutonomyFile = runningAutonomyFile;
    }
    if (selectedAutonomyFile && Object.keys(selectedAutonomyConfigValues).length === 0) {
      const descriptor = selectedAutonomyDescriptor();
      if (descriptor && Array.isArray(descriptor.configs)) {
        descriptor.configs.forEach((cfg) => {
          if (!cfg || typeof cfg.key !== "string") return;
          selectedAutonomyConfigValues[cfg.key] = cfg.default;
        });
      }
    }
    renderAutonomyList();
    updateAutonomyUi();
  } catch (err) {
    console.error(err);
    if (!autonomyBusy) {
      autonomyStatusEl.textContent = "Autonomy list unavailable.";
    }
  }
}

async function refreshAutonomyStatus() {
  if (!currentRobot) return;
  try {
    const { ok, data } = await fetchJson(`/robots/${currentRobot}/autonomy/status?ts=${Date.now()}`);
    if (!ok) throw new Error("status");
    autonomyControlsEnabled = typeof data.controls === "boolean"
      ? data.controls
      : robotHasAutonomyControls(currentRobot);
    autonomyRunning = Boolean(data.running);
    runningAutonomyFile = autonomyRunning && typeof data.file === "string" ? data.file : "";
    if (!selectedAutonomyFile && runningAutonomyFile) {
      selectedAutonomyFile = runningAutonomyFile;
    }
    if (!autonomyRunning && data.error && !autonomyBusy) {
      autonomyStatusEl.textContent = data.error;
    }
    updateAutonomyUi();
  } catch (err) {
    console.error(err);
    if (!autonomyBusy) {
      autonomyStatusEl.textContent = "Autonomy status unavailable.";
    }
  }
}

function buildSelectedAutonomyConfig() {
  const descriptor = selectedAutonomyDescriptor();
  if (!descriptor || !Array.isArray(descriptor.configs)) {
    return {};
  }
  const payload = {};
  descriptor.configs.forEach((cfg) => {
    if (!cfg || typeof cfg.key !== "string") return;
    const key = cfg.key;
    const type = String(cfg.type || "string").toLowerCase();
    let value = selectedAutonomyConfigValues[key];
    if (value === undefined) value = cfg.default;
    if (value === undefined) return;
    if (type === "int") {
      const parsed = Number.parseInt(value, 10);
      if (Number.isFinite(parsed)) payload[key] = parsed;
    } else if (type === "float") {
      const parsed = Number.parseFloat(value);
      if (Number.isFinite(parsed)) payload[key] = parsed;
    } else if (type === "bool") {
      payload[key] = Boolean(value);
    } else {
      payload[key] = String(value);
    }
  });
  return payload;
}

async function startSelectedAutonomy() {
  if (!currentRobot || !selectedAutonomyFile || autonomyBusy) return;
  autonomyBusy = true;
  autonomyStatusEl.textContent = "Starting...";
  updateAutonomyUi();
  try {
    const cfg = buildSelectedAutonomyConfig();
    const { ok, data } = await fetchJson(`/robots/${currentRobot}/autonomy/start`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ file: selectedAutonomyFile, config: cfg }),
    });
    if (!ok) {
      throw new Error(data.error || "Failed to start autonomy.");
    }
  } catch (err) {
    console.error(err);
    autonomyStatusEl.textContent = `${err.message || err}`;
  } finally {
    autonomyBusy = false;
    await refreshAutonomyStatus();
  }
}

async function stopAutonomy() {
  if (!currentRobot || autonomyBusy) return;
  autonomyBusy = true;
  autonomyStatusEl.textContent = "Stopping...";
  updateAutonomyUi();
  try {
    await fetch(`/robots/${currentRobot}/autonomy/stop`, { method: "POST" });
  } catch (err) {
    console.error(err);
    autonomyStatusEl.textContent = "Failed to stop autonomy.";
  } finally {
    autonomyBusy = false;
    await refreshAutonomyStatus();
  }
}

async function sendVideoCommand(action) {
  if (!currentRobot || !robotHasVideo(currentRobot) || !robotHasControls(currentRobot)) return;
  const targetBtn = action === "start" ? videoStartBtn : videoStopBtn;
  targetBtn.disabled = true;
  try {
    const res = await fetch(`/robots/${currentRobot}/video/${action}`, { method: "POST" });
    if (!res.ok) {
      const text = await res.text();
      throw new Error(text || `HTTP ${res.status}`);
    }
  } catch (err) {
    console.error(err);
    videoStatusEl.textContent = `Failed to ${action} video: ${err.message}`;
  } finally {
    targetBtn.disabled = false;
    fetchVideoStatus();
  }
}

systemSelect.addEventListener('change', () => {
  const beforeRobot = currentRobot;
  currentSystem = systemSelect.value || null;
  renderRobotOptions(robots);
  handleRobotSelectionChanged(beforeRobot);
});

robotSelect.addEventListener('change', () => {
  const beforeRobot = currentRobot;
  currentRobot = robotSelect.value || null;
  handleRobotSelectionChanged(beforeRobot);
});

videoStartBtn.addEventListener('click', () => { sendVideoCommand("start").catch(console.error); });
videoStopBtn.addEventListener('click', () => { sendVideoCommand("stop").catch(console.error); });
webrtcSetBtn.addEventListener('click', () => {
  if (!currentRobot) return;
  setWebrtcUrl(currentRobot, webrtcUrlInput.value);
  refreshVideoVisibility();
});
webrtcClearBtn.addEventListener('click', () => {
  if (!currentRobot) return;
  setWebrtcUrl(currentRobot, "");
  webrtcUrlInput.value = "";
  refreshVideoVisibility();
});
webrtcUrlInput.addEventListener('keydown', (event) => {
  if (event.key !== "Enter") return;
  if (!currentRobot) return;
  setWebrtcUrl(currentRobot, webrtcUrlInput.value);
  refreshVideoVisibility();
});
webrtcOpenLink.addEventListener('click', (event) => {
  if (!webrtcOpenLink.href || webrtcOpenLink.href.endsWith("#")) {
    event.preventDefault();
  }
});
function toggleVideoFullscreen() {
  if (!videoCard) return;
  if (document.fullscreenElement === videoCard) {
    document.exitFullscreen().catch(() => {});
  } else {
    videoCard.requestFullscreen().catch(err => console.error(err));
  }
}
videoFullscreenBtn.addEventListener('click', () => { toggleVideoFullscreen(); });
document.addEventListener('fullscreenchange', () => {
  if (document.fullscreenElement === videoCard) {
    videoCard.classList.add('fullscreen-active');
    videoFullscreenBtn.textContent = "Exit Fullscreen";
  } else {
    videoCard.classList.remove('fullscreen-active');
    videoFullscreenBtn.textContent = "Fullscreen";
  }
});
videoImg.addEventListener('load', () => {
  if (awaitingFirstFrame) {
    awaitingFirstFrame = false;
    videoPlaceholder.style.opacity = 0;
  }
  drawVideoOverlays();
});
videoImg.addEventListener('error', () => {
  videoPlaceholder.textContent = "Video stream error.";
  videoPlaceholder.style.opacity = 1;
  clearVideoOverlayCanvas();
});
if (videoOverlayToggle) {
  videoOverlayToggle.addEventListener('change', () => {
    if (videoOverlayToggle.checked) {
      startVideoOverlayLoop();
    } else {
      stopVideoOverlayLoop(true);
    }
  });
}
window.addEventListener('resize', drawVideoOverlays);
window.addEventListener('resize', drawImuPlot);
window.addEventListener('beforeunload', () => {
  cleanupVideoStream();
  stopVideoOverlayLoop(true);
});

audioStartBtn.addEventListener('click', () => { startAudioStream().catch(console.error); });
audioStopBtn.addEventListener('click', () => {
  if (robotHasAudioControls(currentRobot)) {
    sendAudioCommand("stop").catch(console.error);
  }
  stopAudio("Audio stopped.", true);
});
audioTalkBtn.addEventListener('click', () => {
  toggleTalkMode().catch(console.error);
});
soundPlayStopBtn.addEventListener('click', () => {
  if (!selectedSoundFile) return;
  if (selectedSoundIsPlaying()) {
    stopSelectedSound().catch(console.error);
  } else {
    playSelectedSound().catch(console.error);
  }
});
autonomyStartStopBtn.addEventListener('click', () => {
  if (!selectedAutonomyFile) return;
  if (selectedAutonomyIsRunning()) {
    stopAutonomy().catch(console.error);
  } else {
    startSelectedAutonomy().catch(console.error);
  }
});
if (logsClearBtn) {
  logsClearBtn.addEventListener('click', () => {
    logsMinTimestampMs = logsNewestTimestampMs;
    logsRenderSignature = "";
    if (logsOutputEl) logsOutputEl.textContent = "Log view cleared.";
    refreshComponentLogs(true).catch(console.error);
  });
}
if (logsAutoscrollEl && logsOutputEl) {
  logsAutoscrollEl.addEventListener('change', () => {
    if (logsAutoscrollEl.checked) {
      logsOutputEl.scrollTop = logsOutputEl.scrollHeight;
    }
  });
}
window.addEventListener('beforeunload', () => { cleanupAudioStream(); stopAudioUplink(); });

initCollapsibleCards();
stopAudio("Select a component to begin.", true);
updateLayoutMode();

const pressed = new Set();
const k = {
  W:document.getElementById('kW'),
  A:document.getElementById('kA'),
  S:document.getElementById('kS'),
  D:document.getElementById('kD'),
  Q:document.getElementById('kQ'),
  E:document.getElementById('kE')
};
const statusEl = document.getElementById('status');
const spdEl = document.getElementById('spd');
const trnEl = document.getElementById('trn');
const balEl = document.getElementById('bal');
const balStatusEl = document.getElementById('balStatus');
const stopBtn = document.getElementById('stopBtn');
const rebootBtn = document.getElementById('rebootBtn');
const rebootStatusEl = document.getElementById('rebootStatus');
const serviceRestartBtn = document.getElementById('serviceRestartBtn');
const serviceRestartStatusEl = document.getElementById('serviceRestartStatus');
const gitPullBtn = document.getElementById('gitPullBtn');
const gitPullStatusEl = document.getElementById('gitPullStatus');
let joystickActive = false;
let joystickPointerId = null;
let joystickCenter = { x: 0, y: 0 };
let joystickRadius = 1;
let joystickVector = { x: 0, y: 0 };
let joystickTimer = null;
const GAMEPAD_DEADZONE = 0.1;
const GAMEPAD_POLL_MS = 50;
let gamepadIndex = null;
let gamepadTimer = null;
let lastGamepadState = "";
let lastGamepadSendMs = 0;
const DRIVE_MIN_SEND_MS = 80;
const DRIVE_KEEPALIVE_MS = 250;
let driveLastSentMs = 0;
let driveLastSentState = "";
let driveQueued = null;
let driveFlushTimer = null;
let drivePublishBusy = false;

function clampValue(value, minValue, maxValue) {
  return Math.max(minValue, Math.min(maxValue, value));
}

function applyDeadzone(value, deadzone) {
  return Math.abs(value) < deadzone ? 0 : value;
}

function driveBalanceBias() {
  return clampValue(Number(balEl?.value || 0) / 100, -0.5, 0.5);
}

function updateDriveBalanceStatus() {
  if (!balStatusEl) return;
  const raw = Number(balEl?.value || 0);
  if (!Number.isFinite(raw) || Math.abs(raw) < 0.5) {
    balStatusEl.textContent = "Centered";
    return;
  }
  const side = raw > 0 ? "Right" : "Left";
  balStatusEl.textContent = `${side} +${Math.abs(raw).toFixed(0)}%`;
}

function applyDriveBalance(l, r) {
  const bias = driveBalanceBias();
  if (!Number.isFinite(bias) || Math.abs(bias) < 0.001) {
    return [l, r];
  }
  let leftScale = 1;
  let rightScale = 1;
  if (bias > 0) {
    leftScale = Math.max(0, 1 - bias);
    rightScale = 1 + bias;
  } else {
    leftScale = 1 - bias;
    rightScale = Math.max(0, 1 + bias);
  }
  const adjustedLeft = l * leftScale;
  const adjustedRight = r * rightScale;
  const maxMag = Math.max(1, Math.abs(adjustedLeft), Math.abs(adjustedRight));
  return [adjustedLeft / maxMag, adjustedRight / maxMag];
}

function updateIndicators(){
  ["W","A","S","D","Q","E"].forEach(c => k[c]?.classList.toggle('on', pressed.has(c)));
}

function mix() {
  const speed = Number(spdEl.value)/100;
  const turnG = Number(trnEl.value)/100;
  let throttle = 0, turn = 0;
  if (pressed.has("W")) throttle += speed;
  if (pressed.has("S")) throttle -= speed;
  if (pressed.has("A")) turn     -= turnG;
  if (pressed.has("D")) turn     += turnG;
  let grouser = 0;
  if (pressed.has("Q")) grouser += speed;
  if (pressed.has("E")) grouser -= speed;
  let L = throttle + turn;
  let R = throttle - turn;
  const m = Math.max(1, Math.abs(L), Math.abs(R));
  return [L/m, R/m, clampValue(grouser, -1, 1)];
}

let lastSent = "";
function driveState(l, r, y) {
  return `${l.toFixed(3)}|${r.toFixed(3)}|${y.toFixed(3)}`;
}

async function publishDriveCommand(cmd) {
  if (!currentRobot) return;
  drivePublishBusy = true;
  const { l, r, y, state } = cmd;
  const robotId = currentRobot;
  const msg = `M ${l.toFixed(3)} ${r.toFixed(3)} ${y.toFixed(3)}`;
  lastSent = msg;
  statusEl.textContent = `${robotName(robotId)} → ${msg}`;
  try {
    await fetch(`/robots/${robotId}/m`, {
      method:"POST",
      headers:{ "Content-Type":"application/json" },
      body: JSON.stringify({ l, r, y })
    });
  } catch(e) { console.error(e); }
  finally {
    driveLastSentMs = Date.now();
    driveLastSentState = state;
    drivePublishBusy = false;
    if (driveQueued) {
      const next = driveQueued;
      driveQueued = null;
      const elapsed = Date.now() - driveLastSentMs;
      const waitMs = next.immediate ? 0 : Math.max(0, DRIVE_MIN_SEND_MS - elapsed);
      if (waitMs === 0) {
        publishDriveCommand(next);
      } else if (!driveFlushTimer) {
        driveFlushTimer = setTimeout(() => {
          driveFlushTimer = null;
          if (!driveQueued || drivePublishBusy) return;
          const queued = driveQueued;
          driveQueued = null;
          publishDriveCommand(queued);
        }, waitMs);
      } else {
        driveQueued = next;
      }
    }
  }
}

function enqueueDriveCommand(l, r, y = 0, options = {}) {
  if (!currentRobot) return;
  const force = Boolean(options && options.force);
  const immediate = Boolean(options && options.immediate);
  const state = driveState(l, r, y);
  const now = Date.now();

  if (!force && state === driveLastSentState && (now - driveLastSentMs) < DRIVE_KEEPALIVE_MS) {
    return;
  }

  const cmd = { l, r, y, state, immediate };
  if (drivePublishBusy) {
    driveQueued = cmd;
    return;
  }

  const elapsed = now - driveLastSentMs;
  if (immediate || elapsed >= DRIVE_MIN_SEND_MS) {
    publishDriveCommand(cmd);
    return;
  }

  driveQueued = cmd;
  if (!driveFlushTimer) {
    driveFlushTimer = setTimeout(() => {
      driveFlushTimer = null;
      if (!driveQueued || drivePublishBusy) return;
      const queued = driveQueued;
      driveQueued = null;
      publishDriveCommand(queued);
    }, DRIVE_MIN_SEND_MS - elapsed);
  }
}

function sendM(l, r, y = 0, options = {}){
  const [balancedL, balancedR] = applyDriveBalance(l, r);
  enqueueDriveCommand(balancedL, balancedR, y, options);
}

function sendStopNow() {
  driveQueued = null;
  if (driveFlushTimer) {
    clearTimeout(driveFlushTimer);
    driveFlushTimer = null;
  }
  sendM(0, 0, 0, { force: true, immediate: true });
}

function updateJoystickMetrics() {
  if (!joystickBase) return;
  const rect = joystickBase.getBoundingClientRect();
  joystickCenter = { x: rect.left + rect.width / 2, y: rect.top + rect.height / 2 };
  joystickRadius = Math.min(rect.width, rect.height) / 2;
}

function applyJoystickVector(dx, dy) {
  if (!joystickThumb) return;
  const max = Math.max(20, joystickRadius * 0.85);
  const dist = Math.hypot(dx, dy);
  const scale = dist > max ? max / dist : 1;
  const x = dx * scale;
  const y = dy * scale;
  joystickThumb.style.transform = `translate(calc(-50% + ${x}px), calc(-50% + ${y}px))`;
  joystickVector = { x: x / max, y: y / max };
  updateJoystickStatus();
  sendJoystickCommand();
  if (!joystickTimer) {
    joystickTimer = setInterval(sendJoystickCommand, 500);
  }
}

function resetJoystick() {
  if (joystickThumb) {
    joystickThumb.style.transform = "translate(-50%, -50%)";
  }
  joystickVector = { x: 0, y: 0 };
  updateJoystickStatus();
  if (joystickTimer) {
    clearInterval(joystickTimer);
    joystickTimer = null;
  }
  sendStopNow();
}

function updateJoystickStatus() {
  if (!joystickStatus) return;
  const throttle = -joystickVector.y;
  const turn = joystickVector.x;
  if (Math.abs(throttle) < 0.05 && Math.abs(turn) < 0.05) {
    joystickStatus.textContent = "Drag to drive";
  } else {
    joystickStatus.textContent = `Throttle ${(throttle * 100).toFixed(0)}% • Turn ${(turn * 100).toFixed(0)}%`;
  }
}

function sendJoystickCommand() {
  if (!currentRobot) return;
  const speed = Number(spdEl.value) / 100;
  const turnG = Number(trnEl.value) / 100;
  const throttle = clampValue(-joystickVector.y, -1, 1) * speed;
  const turn = clampValue(joystickVector.x, -1, 1) * turnG;
  let L = throttle + turn;
  let R = throttle - turn;
  const m = Math.max(1, Math.abs(L), Math.abs(R));
  sendM(L / m, R / m, 0);
}

if (joystickBase) {
  joystickBase.addEventListener("pointerdown", (event) => {
    joystickActive = true;
    joystickPointerId = event.pointerId;
    joystickBase.setPointerCapture(event.pointerId);
    updateJoystickMetrics();
    applyJoystickVector(event.clientX - joystickCenter.x, event.clientY - joystickCenter.y);
  });
  joystickBase.addEventListener("pointermove", (event) => {
    if (!joystickActive || event.pointerId !== joystickPointerId) return;
    applyJoystickVector(event.clientX - joystickCenter.x, event.clientY - joystickCenter.y);
  });
  const stopJoystick = (event) => {
    if (event.pointerId && event.pointerId !== joystickPointerId) return;
    joystickActive = false;
    joystickPointerId = null;
    resetJoystick();
  };
  joystickBase.addEventListener("pointerup", stopJoystick);
  joystickBase.addEventListener("pointercancel", stopJoystick);
  joystickBase.addEventListener("pointerleave", stopJoystick);
}

function pickGamepadIndex() {
  if (!navigator.getGamepads) return null;
  const pads = Array.from(navigator.getGamepads());
  const preferred = pads.find(pad => pad && /Logitech Attack 3/i.test(pad.id));
  if (preferred) return preferred.index;
  const fallback = pads.find(pad => pad);
  return fallback ? fallback.index : null;
}

function sendGamepadCommand() {
  if (!currentRobot || !navigator.getGamepads) return;
  if (gamepadIndex === null) {
    gamepadIndex = pickGamepadIndex();
  }
  if (gamepadIndex === null) return;
  const pad = navigator.getGamepads()[gamepadIndex];
  if (!pad) return;
  const axis0 = applyDeadzone(pad.axes?.[0] ?? 0, GAMEPAD_DEADZONE);
  const axis1 = applyDeadzone(pad.axes?.[1] ?? 0, GAMEPAD_DEADZONE);
  const b2Pressed = !!pad.buttons?.[1]?.pressed;
  const speed = Number(spdEl.value) / 100;
  const turnG = Number(trnEl.value) / 100;
  const throttle = clampValue(-axis1, -1, 1) * speed;
  let turn = 0;
  let grouser = 0;
  if (b2Pressed) {
    grouser = clampValue(axis0, -1, 1) * speed;
  } else {
    turn = clampValue(axis0, -1, 1) * turnG;
  }
  let L = throttle + turn;
  let R = throttle - turn;
  const m = Math.max(1, Math.abs(L), Math.abs(R));
  L /= m;
  R /= m;
  const state = `${L.toFixed(3)}|${R.toFixed(3)}|${grouser.toFixed(3)}`;
  const now = Date.now();
  if (state !== lastGamepadState || now - lastGamepadSendMs > 250) {
    lastGamepadState = state;
    lastGamepadSendMs = now;
    sendM(L, R, grouser);
  }
}

function startGamepadPolling() {
  if (gamepadTimer) return;
  gamepadTimer = setInterval(sendGamepadCommand, GAMEPAD_POLL_MS);
}

window.addEventListener("gamepadconnected", (event) => {
  if (gamepadIndex === null || /Logitech Attack 3/i.test(event.gamepad.id)) {
    gamepadIndex = event.gamepad.index;
  }
  startGamepadPolling();
});

window.addEventListener("gamepaddisconnected", (event) => {
  if (event.gamepad.index === gamepadIndex) {
    gamepadIndex = pickGamepadIndex();
  }
  if (gamepadIndex === null && gamepadTimer) {
    clearInterval(gamepadTimer);
    gamepadTimer = null;
    lastGamepadState = "";
  }
});

startGamepadPolling();

async function playLastSelectedSoundShortcut() {
  if (!currentRobot || !robotHasSoundboard(currentRobot)) return;
  if (soundboardBusy || !soundboardControlsEnabled) return;
  const file = selectedSoundFile || playingSoundFile;
  if (!file) return;
  if (selectedSoundFile !== file) {
    selectedSoundFile = file;
    renderSoundList();
    updateSoundboardUi();
  }
  await playSelectedSound();
}

function toggleAudioMuteShortcut() {
  if (!audioSource || !audioGain) return;
  audioIncomingMuted = !audioIncomingMuted;
  updateIncomingGain();
  audioStatusEl.textContent = audioIncomingMuted ? "Listening muted." : "Listening...";
  updateAudioIndicators();
}

function sendFromPressedNow(){
  if (!pressed.size) { sendStopNow(); return; }
  const [L, R, Y] = mix();
  sendM(L, R, Y);
}

function handleKey(e, down){
  const target = e.target;
  if (target && (target.isContentEditable || ["INPUT", "TEXTAREA", "SELECT"].includes(target.tagName))) {
    return;
  }
  const key = e.key.toUpperCase();
  if (["W","A","S","D","Q","E","P","M","F"," "].includes(key)) e.preventDefault();
  if (down && !e.repeat) {
    if (key === "P") {
      playLastSelectedSoundShortcut().catch(console.error);
      return;
    }
    if (key === "M") {
      toggleAudioMuteShortcut();
      return;
    }
    if (key === "F") {
      if (currentRobot && robotHasLights(currentRobot)) {
        (lightsShortcutEnabled ? sendLightsOff() : sendF_fromColor()).catch(console.error);
      }
      return;
    }
  }
  if (key === " ") { pressed.clear(); updateIndicators(); sendStopNow(); return; }
  if (!["W","A","S","D","Q","E"].includes(key)) return;
  if (down && e.repeat) return;
  if (down) pressed.add(key); else pressed.delete(key);
  updateIndicators();
  sendFromPressedNow();
}

window.addEventListener("keydown", e => handleKey(e, true));
window.addEventListener("keyup",   e => handleKey(e, false));
stopBtn.addEventListener("click", () => { pressed.clear(); updateIndicators(); sendStopNow(); });
if (balEl) {
  balEl.addEventListener("input", updateDriveBalanceStatus);
  updateDriveBalanceStatus();
}
if (rebootBtn) {
  rebootBtn.addEventListener("click", () => { sendRebootCommand().catch(console.error); });
}
if (serviceRestartBtn) {
  serviceRestartBtn.addEventListener("click", () => { sendServiceRestartCommand().catch(console.error); });
}
if (gitPullBtn) {
  gitPullBtn.addEventListener("click", () => { sendGitPullCommand().catch(console.error); });
}
setInterval(() => { if (pressed.size) { const [L,R,Y] = mix(); sendM(L,R,Y); } }, 500);

// Lights
const colorEl = document.getElementById('color');
const flashTEl = document.getElementById('flashT');
const lightStatusEl = document.getElementById('lightStatus');
let lightsShortcutEnabled = false;

function resetLightsShortcutState() {
  lightsShortcutEnabled = false;
}

function hexToRgb01(hex) {
  const m = /^#?([0-9a-f]{2})([0-9a-f]{2})([0-9a-f]{2})$/i.exec(hex);
  if (!m) return {r:0,g:0,b:0};
  const r = parseInt(m[1],16)/255, g = parseInt(m[2],16)/255, b = parseInt(m[3],16)/255;
  return {r,g,b};
}

async function commandRobot(path, payload) {
  if (!currentRobot) return;
  await fetch(`/robots/${currentRobot}/${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });
}

function syncRebootControls() {
  if (!rebootBtn || !rebootStatusEl) return;
  if (!currentRobot) {
    rebootBtn.disabled = true;
    rebootStatusEl.textContent = "";
    return;
  }
  const isRobot = componentType(currentRobot) === "robots";
  const hasControls = isRobot && robotHasRebootControls(currentRobot);
  rebootBtn.disabled = !hasControls;
  if (!isRobot) {
    rebootStatusEl.textContent = "Reboot unavailable for this component type.";
  } else if (!hasControls) {
    rebootStatusEl.textContent = "No remote reboot controls.";
  } else if (
    rebootStatusEl.textContent === "Reboot unavailable for this component type."
    || rebootStatusEl.textContent === "No remote reboot controls."
  ) {
    rebootStatusEl.textContent = "";
  }
}

function syncServiceRestartControls() {
  if (!serviceRestartBtn || !serviceRestartStatusEl) return;
  if (!currentRobot) {
    serviceRestartBtn.disabled = true;
    serviceRestartStatusEl.textContent = "";
    return;
  }
  const isRobot = componentType(currentRobot) === "robots";
  const hasControls = isRobot && robotHasServiceRestartControls(currentRobot);
  serviceRestartBtn.disabled = !hasControls;
  if (!isRobot) {
    serviceRestartStatusEl.textContent = "Service restart unavailable for this component type.";
  } else if (!hasControls) {
    serviceRestartStatusEl.textContent = "No remote service restart controls.";
  } else if (
    serviceRestartStatusEl.textContent === "Service restart unavailable for this component type."
    || serviceRestartStatusEl.textContent === "No remote service restart controls."
  ) {
    serviceRestartStatusEl.textContent = "";
  }
}

function syncGitPullControls() {
  if (!gitPullBtn || !gitPullStatusEl) return;
  if (!currentRobot) {
    gitPullBtn.disabled = true;
    gitPullStatusEl.textContent = "";
    return;
  }
  const isRobot = componentType(currentRobot) === "robots";
  const hasControls = isRobot && robotHasGitPullControls(currentRobot);
  gitPullBtn.disabled = !hasControls;
  if (!isRobot) {
    gitPullStatusEl.textContent = "Git pull unavailable for this component type.";
  } else if (!hasControls) {
    gitPullStatusEl.textContent = "No remote git pull controls.";
  } else if (
    gitPullStatusEl.textContent === "Git pull unavailable for this component type."
    || gitPullStatusEl.textContent === "No remote git pull controls."
  ) {
    gitPullStatusEl.textContent = "";
  }
}

async function sendRebootCommand() {
  if (!currentRobot) return;
  if (componentType(currentRobot) !== "robots") return;
  if (!robotHasRebootControls(currentRobot)) return;
  const confirmed = window.confirm(`Reboot ${robotName(currentRobot)} now?`);
  if (!confirmed) return;
  rebootBtn.disabled = true;
  rebootStatusEl.textContent = "Sending reboot request...";
  try {
    const { ok, data } = await fetchJson(`/robots/${currentRobot}/reboot`, { method: "POST" });
    if (!ok) {
      throw new Error(data.error || data.status || "Request failed.");
    }
    rebootStatusEl.textContent = "Reboot requested.";
  } catch (err) {
    console.error(err);
    rebootStatusEl.textContent = `Reboot failed: ${err.message || err}`;
  } finally {
    syncRebootControls();
  }
}

async function sendServiceRestartCommand() {
  if (!currentRobot) return;
  if (componentType(currentRobot) !== "robots") return;
  if (!robotHasServiceRestartControls(currentRobot)) return;
  serviceRestartBtn.disabled = true;
  serviceRestartStatusEl.textContent = "Sending service restart request...";
  try {
    const { ok, data } = await fetchJson(`/robots/${currentRobot}/service-restart`, { method: "POST" });
    if (!ok) {
      throw new Error(data.error || data.status || "Request failed.");
    }
    serviceRestartStatusEl.textContent = "Service restart requested.";
  } catch (err) {
    console.error(err);
    serviceRestartStatusEl.textContent = `Service restart failed: ${err.message || err}`;
  } finally {
    syncServiceRestartControls();
  }
}

async function sendGitPullCommand() {
  if (!currentRobot) return;
  if (componentType(currentRobot) !== "robots") return;
  if (!robotHasGitPullControls(currentRobot)) return;
  gitPullBtn.disabled = true;
  gitPullStatusEl.textContent = "Sending git pull request...";
  try {
    const { ok, data } = await fetchJson(`/robots/${currentRobot}/git-pull`, { method: "POST" });
    if (!ok) {
      throw new Error(data.error || data.status || "Request failed.");
    }
    gitPullStatusEl.textContent = "Git pull requested.";
  } catch (err) {
    console.error(err);
    gitPullStatusEl.textContent = `Git pull failed: ${err.message || err}`;
  } finally {
    syncGitPullControls();
  }
}

async function sendL_fromColor() {
  const {r,g,b} = hexToRgb01(colorEl.value);
  await commandRobot("l", { b, g, r });
  lightsShortcutEnabled = true;
  lightStatusEl.textContent = `${robotName(currentRobot)} → L ${b.toFixed(3)} ${g.toFixed(3)} ${r.toFixed(3)}`;
}

async function sendF_fromColor() {
  const {r,g,b} = hexToRgb01(colorEl.value);
  let period = Number(flashTEl.value);
  if (!isFinite(period) || period <= 0) period = 2.0;
  await commandRobot("f", { b, g, r, period });
  lightsShortcutEnabled = true;
  lightStatusEl.textContent = `${robotName(currentRobot)} → F ${b.toFixed(3)} ${g.toFixed(3)} ${r.toFixed(3)} ${period.toFixed(2)}`;
}

async function sendLightsOff() {
  await commandRobot("l", { b:0, g:0, r:0 });
  lightsShortcutEnabled = false;
  lightStatusEl.textContent = `${robotName(currentRobot)} → L 0.000 0.000 0.000`;
}

document.getElementById('btnSolid').addEventListener("click", () => { sendL_fromColor().catch(console.error); });
document.getElementById('btnFlash').addEventListener("click", () => { sendF_fromColor().catch(console.error); });
document.getElementById('btnOff').addEventListener("click", () => { sendLightsOff().catch(console.error); });

// Telemetry
const chg = document.getElementById('chg');
const bat = document.getElementById('bat');
const conn = document.getElementById('conn');
const latAvg = document.getElementById('latAvg');
const offlineFor = document.getElementById('offlineFor');

function formatDuration(seconds){
  const total = Math.max(0, Number(seconds) || 0);
  const hours = Math.floor(total / 3600);
  const minutes = Math.floor((total % 3600) / 60);
  const secs = (total % 60).toFixed(1).padStart(4, '0');
  if (hours > 0) return `${hours}:${String(minutes).padStart(2, '0')}:${secs}`;
  return `${minutes}:${secs}`;
}

function clearOdometryUI() {
  odomTrail = [];
  odomLastToken = null;
  odomLatest = null;
  if (odomMeta) {
    odomMeta.textContent = "Awaiting data...";
  }
  drawOdometryPlot();
}

function formatVectorLine(label, value, unit) {
  if (!value || typeof value !== "object") {
    return `${label}: –`;
  }
  const x = Number(value.x);
  const y = Number(value.y);
  const z = Number(value.z);
  if (![x, y, z].every(Number.isFinite)) {
    return `${label}: –`;
  }
  return `${label}: x=${x.toFixed(2)} ${unit}, y=${y.toFixed(2)} ${unit}, z=${z.toFixed(2)} ${unit}`;
}

function clearImuUI() {
  imuLatest = null;
  imuAccelBaseline = null;
  imuAccelBaselineAt = 0;
  if (imuOrientationEl) imuOrientationEl.textContent = "Roll: – | Pitch: –";
  if (imuAccelEl) imuAccelEl.textContent = "Accel: –";
  if (imuGyroEl) imuGyroEl.textContent = "Gyro: –";
  if (imuHealthEl) imuHealthEl.textContent = "Status: Awaiting data...";
  drawImuPlot();
}

function imuFilteredAccel() {
  const accel = imuLatest?.accel_mps2;
  const ax = Number(accel?.x);
  const ay = Number(accel?.y);
  const az = Number(accel?.z);
  if (!Number.isFinite(ax) || !Number.isFinite(ay) || !Number.isFinite(az)) {
    return null;
  }

  const now = Date.now() / 1000;
  const sample = { x: ax, y: ay, z: az };
  if (!imuAccelBaseline) {
    imuAccelBaseline = sample;
    imuAccelBaselineAt = now;
    return { raw: sample, baseline: imuAccelBaseline, delta: { x: 0, y: 0, z: 0 } };
  }

  const dt = Math.max(0.001, Math.min(1.0, now - (imuAccelBaselineAt || now)));
  imuAccelBaselineAt = now;
  const tauSeconds = 12.0;
  const alpha = clampValue(dt / tauSeconds, 0.0025, 0.08);
  imuAccelBaseline = {
    x: imuAccelBaseline.x + (sample.x - imuAccelBaseline.x) * alpha,
    y: imuAccelBaseline.y + (sample.y - imuAccelBaseline.y) * alpha,
    z: imuAccelBaseline.z + (sample.z - imuAccelBaseline.z) * alpha,
  };

  const deadband = 0.06;
  function subtractNoise(value) {
    if (!Number.isFinite(value)) return 0;
    if (Math.abs(value) <= deadband) return 0;
    return Math.sign(value) * (Math.abs(value) - deadband);
  }

  return {
    raw: sample,
    baseline: imuAccelBaseline,
    delta: {
      x: subtractNoise(sample.x - imuAccelBaseline.x),
      y: subtractNoise(sample.y - imuAccelBaseline.y),
      z: subtractNoise(sample.z - imuAccelBaseline.z),
    },
  };
}

function drawImuPlot() {
  if (!imuCanvas) return;
  const ctx = imuCanvas.getContext("2d");
  if (!ctx) return;

  const rect = imuCanvas.getBoundingClientRect();
  const width = Math.max(1, Math.floor(rect.width));
  const height = Math.max(1, Math.floor(rect.height));
  const dpr = window.devicePixelRatio || 1;
  imuCanvas.width = Math.max(1, Math.floor(width * dpr));
  imuCanvas.height = Math.max(1, Math.floor(height * dpr));
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, width, height);

  ctx.fillStyle = "#f8fafc";
  ctx.fillRect(0, 0, width, height);

  const cx = width * 0.5;
  const cy = height * 0.5;
  const radius = Math.max(24, Math.min(width, height) * 0.34);

  ctx.save();
  ctx.beginPath();
  ctx.arc(cx, cy, radius, 0, Math.PI * 2);
  ctx.clip();

  const rollDeg = Number(imuLatest?.orientation_deg?.roll);
  const pitchDeg = Number(imuLatest?.orientation_deg?.pitch);
  const rollRad = Number.isFinite(rollDeg) ? (rollDeg * Math.PI / 180.0) : 0;
  const pitchOffset = Number.isFinite(pitchDeg) ? clampValue((pitchDeg / 45.0) * radius, -radius * 0.85, radius * 0.85) : 0;

  ctx.translate(cx, cy);
  ctx.rotate(-rollRad);
  ctx.fillStyle = "#9dd2ff";
  ctx.fillRect(-radius * 2, -radius * 2 + pitchOffset, radius * 4, radius * 2 - pitchOffset);
  ctx.fillStyle = "#d8c6a2";
  ctx.fillRect(-radius * 2, pitchOffset, radius * 4, radius * 2);
  ctx.strokeStyle = "#ffffff";
  ctx.lineWidth = 3;
  ctx.beginPath();
  ctx.moveTo(-radius * 1.6, pitchOffset);
  ctx.lineTo(radius * 1.6, pitchOffset);
  ctx.stroke();
  ctx.restore();

  ctx.strokeStyle = "#334155";
  ctx.lineWidth = 2;
  ctx.beginPath();
  ctx.arc(cx, cy, radius, 0, Math.PI * 2);
  ctx.stroke();

  ctx.strokeStyle = "#64748b";
  ctx.lineWidth = 1;
  ctx.beginPath();
  ctx.moveTo(cx - radius, cy);
  ctx.lineTo(cx + radius, cy);
  ctx.moveTo(cx, cy - radius);
  ctx.lineTo(cx, cy + radius);
  ctx.stroke();

  const accelView = imuFilteredAccel();
  if (accelView) {
    const dx = clampValue(accelView.delta.y * (radius * 0.18), -radius * 0.8, radius * 0.8);
    const dy = clampValue(accelView.delta.z * (radius * 0.18), -radius * 0.8, radius * 0.8);
    const depth = accelView.delta.x;
    const symbolRadius = clampValue(5 + (Math.abs(depth) * radius * 0.04), 5, 12);
    const tipX = cx + dx;
    const tipY = cy + dy;
    ctx.strokeStyle = "#dc2626";
    ctx.lineWidth = 3;
    ctx.beginPath();
    ctx.moveTo(cx, cy);
    ctx.lineTo(tipX, tipY);
    ctx.stroke();

    if (depth >= 0) {
      ctx.strokeStyle = "#dc2626";
      ctx.lineWidth = 3;
      ctx.beginPath();
      ctx.moveTo(tipX - symbolRadius, tipY - symbolRadius);
      ctx.lineTo(tipX + symbolRadius, tipY + symbolRadius);
      ctx.moveTo(tipX - symbolRadius, tipY + symbolRadius);
      ctx.lineTo(tipX + symbolRadius, tipY - symbolRadius);
      ctx.stroke();
    } else {
      ctx.fillStyle = "#dc2626";
      ctx.beginPath();
      ctx.arc(tipX, tipY, symbolRadius, 0, Math.PI * 2);
      ctx.fill();
      ctx.fillStyle = "#f8fafc";
      ctx.beginPath();
      ctx.arc(tipX, tipY, Math.max(2, symbolRadius * 0.38), 0, Math.PI * 2);
      ctx.fill();
    }
  }

  ctx.fillStyle = "#0f172a";
  ctx.font = "600 12px ui-monospace,monospace";
  ctx.fillText("Roll / Pitch", 12, 20);
  ctx.fillStyle = "#475569";
  ctx.fillText("Accel residual: Y/Z plane, X=cross/dot", 12, 38);
}

function updateImuFromTelemetry(imu) {
  if (!imu || typeof imu !== "object") {
    clearImuUI();
    return;
  }
  imuLatest = imu;

  const roll = Number(imu.orientation_deg?.roll);
  const pitch = Number(imu.orientation_deg?.pitch);
  const temp = Number(imu.temp_c);
  const accelNormG = Number(imu.accel_norm_g);
  const samplesOk = Number(imu.health?.samples_ok);
  const samplesErr = Number(imu.health?.samples_error);
  const cal = Number(imu.health?.calibration_samples);

  if (imuOrientationEl) {
    const parts = [
      `Roll: ${Number.isFinite(roll) ? `${roll.toFixed(1)}°` : "–"}`,
      `Pitch: ${Number.isFinite(pitch) ? `${pitch.toFixed(1)}°` : "–"}`,
    ];
    if (Number.isFinite(temp)) parts.push(`Temp: ${temp.toFixed(1)} C`);
    imuOrientationEl.textContent = parts.join(" | ");
  }
  if (imuAccelEl) {
    let line = formatVectorLine("Accel", imu.accel_mps2, "m/s^2");
    if (Number.isFinite(accelNormG)) {
      line += ` | |a|=${accelNormG.toFixed(3)} g`;
    }
    const accelView = imuFilteredAccel();
    if (accelView) {
      line += ` | Δx=${accelView.delta.x.toFixed(2)}, Δy=${accelView.delta.y.toFixed(2)}, Δz=${accelView.delta.z.toFixed(2)}`;
    }
    imuAccelEl.textContent = line;
  }
  if (imuGyroEl) {
    imuGyroEl.textContent = formatVectorLine("Gyro", imu.gyro_dps, "dps");
  }
  if (imuHealthEl) {
    const parts = [];
    if (Number.isFinite(samplesOk)) parts.push(`ok=${samplesOk.toFixed(0)}`);
    if (Number.isFinite(samplesErr)) parts.push(`err=${samplesErr.toFixed(0)}`);
    if (Number.isFinite(cal)) parts.push(`cal=${cal.toFixed(0)}`);
    if (Number.isFinite(Number(imu.seq))) parts.push(`seq=${Number(imu.seq).toFixed(0)}`);
    imuHealthEl.textContent = parts.length ? `Status: ${parts.join(" | ")}` : "Status: Live IMU sample";
  }
  drawImuPlot();
}

function drawOdometryPlot() {
  if (!odomCanvas) return;
  const ctx = odomCanvas.getContext("2d");
  if (!ctx) return;

  const rect = odomCanvas.getBoundingClientRect();
  const width = Math.max(1, Math.floor(rect.width));
  const height = Math.max(1, Math.floor(rect.height));
  const dpr = window.devicePixelRatio || 1;
  odomCanvas.width = Math.max(1, Math.floor(width * dpr));
  odomCanvas.height = Math.max(1, Math.floor(height * dpr));
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, width, height);

  ctx.fillStyle = "#f8fafc";
  ctx.fillRect(0, 0, width, height);

  const cx = width * 0.5;
  const cy = height * 0.5;
  let maxAbs = 250;
  for (const pt of odomTrail) {
    const px = Math.abs(Number(pt.x_mm) || 0);
    const py = Math.abs(Number(pt.y_mm) || 0);
    maxAbs = Math.max(maxAbs, px, py);
  }
  const worldHalf = Math.max(200, maxAbs * 1.15);
  const pxPerMm = Math.max(0.05, (Math.min(width, height) * 0.46) / worldHalf);

  ctx.strokeStyle = "#d7dee6";
  ctx.lineWidth = 1;
  for (let i = 1; i <= 4; i++) {
    const step = (worldHalf / 4) * i;
    const off = step * pxPerMm;
    ctx.beginPath();
    ctx.moveTo(cx - off, 0);
    ctx.lineTo(cx - off, height);
    ctx.moveTo(cx + off, 0);
    ctx.lineTo(cx + off, height);
    ctx.moveTo(0, cy - off);
    ctx.lineTo(width, cy - off);
    ctx.moveTo(0, cy + off);
    ctx.lineTo(width, cy + off);
    ctx.stroke();
  }

  ctx.strokeStyle = "#7b8794";
  ctx.lineWidth = 1.5;
  ctx.beginPath();
  ctx.moveTo(0, cy);
  ctx.lineTo(width, cy);
  ctx.moveTo(cx, 0);
  ctx.lineTo(cx, height);
  ctx.stroke();

  const toScreen = (xMm, yMm) => ({
    x: cx + (xMm * pxPerMm),
    y: cy - (yMm * pxPerMm),
  });

  if (odomTrail.length > 1) {
    const maxIdx = odomTrail.length - 1;
    for (let i = 1; i < odomTrail.length; i++) {
      const p0 = odomTrail[i - 1];
      const p1 = odomTrail[i];
      const s0 = toScreen(Number(p0.x_mm) || 0, Number(p0.y_mm) || 0);
      const s1 = toScreen(Number(p1.x_mm) || 0, Number(p1.y_mm) || 0);
      const t = i / Math.max(1, maxIdx);
      const alpha = 0.12 + (0.78 * t);
      ctx.strokeStyle = `rgba(26, 103, 179, ${alpha.toFixed(3)})`;
      ctx.lineWidth = 2;
      ctx.beginPath();
      ctx.moveTo(s0.x, s0.y);
      ctx.lineTo(s1.x, s1.y);
      ctx.stroke();
    }
  }

  if (!odomLatest) return;
  const rx = Number(odomLatest.x_mm) || 0;
  const ry = Number(odomLatest.y_mm) || 0;
  const headingRad = Number(odomLatest.heading_rad);
  const theta = Number.isFinite(headingRad) ? headingRad : 0;
  const p = toScreen(rx, ry);

  const bodyPx = 10;
  ctx.save();
  ctx.translate(p.x, p.y);
  ctx.rotate(-theta);
  ctx.fillStyle = "#0f172a";
  ctx.beginPath();
  ctx.moveTo(bodyPx * 1.8, 0);
  ctx.lineTo(-bodyPx * 1.0, bodyPx * 0.8);
  ctx.lineTo(-bodyPx * 0.55, 0);
  ctx.lineTo(-bodyPx * 1.0, -bodyPx * 0.8);
  ctx.closePath();
  ctx.fill();
  ctx.restore();

  ctx.fillStyle = "#0f172a";
  ctx.font = "600 12px ui-monospace,monospace";
  ctx.fillText("0,0", cx + 6, cy - 6);
}

function updateOdometryFromTelemetry(odometry) {
  if (!odometry || typeof odometry !== "object") {
    if (odomMeta) odomMeta.textContent = "Awaiting data...";
    return;
  }
  const x = Number(odometry.x_mm);
  const y = Number(odometry.y_mm);
  if (!Number.isFinite(x) || !Number.isFinite(y)) {
    return;
  }
  const headingDeg = Number.isFinite(Number(odometry.heading_deg)) ? Number(odometry.heading_deg) : 0;
  const headingRadRaw = Number(odometry.heading_rad);
  const headingRad = Number.isFinite(headingRadRaw) ? headingRadRaw : (headingDeg * Math.PI / 180.0);
  const tokenSource = odometry.publish_seq ?? odometry.sample_seq ?? odometry.timestamp ?? `${x}:${y}:${headingRad}`;
  const token = String(tokenSource);

  odomLatest = {
    x_mm: x,
    y_mm: y,
    heading_deg: headingDeg,
    heading_rad: headingRad,
  };

  if (token !== odomLastToken) {
    odomTrail.push(odomLatest);
    if (odomTrail.length > ODOM_TRAIL_MAX_POINTS) {
      odomTrail.shift();
    }
    odomLastToken = token;
  }

  const distance = Number(odometry.distance_mm);
  const leftMm = Number(odometry.left_mm);
  const rightMm = Number(odometry.right_mm);
  const lines = [
    `x=${x.toFixed(1)} mm, y=${y.toFixed(1)} mm, heading=${headingDeg.toFixed(1)}°`,
    `${Number.isFinite(distance) ? `dist=${distance.toFixed(1)} mm` : "dist=–"}, ${Number.isFinite(leftMm) ? `left=${leftMm.toFixed(1)} mm` : "left=–"}, ${Number.isFinite(rightMm) ? `right=${rightMm.toFixed(1)} mm` : "right=–"}`,
  ];
  if (odomMeta) odomMeta.textContent = lines.join(" | ");
  drawOdometryPlot();
}

function clearTelemetryUI(){
  chg.textContent = "–";
  chg.classList.remove("ok", "warn");
  bat.textContent = "–";
  conn.textContent = "Unknown";
  conn.classList.remove("ok", "warn");
  latAvg.textContent = "–";
  offlineFor.textContent = "–";
  statusEl.textContent = "Select a component to begin.";
  lightStatusEl.textContent = "Awaiting command...";
  if (rebootStatusEl) rebootStatusEl.textContent = "";
  if (serviceRestartStatusEl) serviceRestartStatusEl.textContent = "";
  if (gitPullStatusEl) gitPullStatusEl.textContent = "";
  clearImuUI();
  clearOdometryUI();
}

renderRobotOptions(robots);
handleRobotSelectionChanged(null);
refreshRobotList();
setInterval(refreshRobotList, 2000);
setInterval(refreshOutlets, 1500);
setInterval(refreshSoundboardStatus, 1000);
setInterval(refreshSoundFiles, 5000);
setInterval(refreshAutonomyStatus, 1000);
setInterval(refreshAutonomyFiles, 5000);
setInterval(refreshComponentLogs, 1000);

async function pollTelemetry(){
  if (!currentRobot) return;
  try {
    const res = await fetch(`/robots/${currentRobot}/telemetry`);
    if (!res.ok) return;
    const t = await res.json();
    if (typeof t.charging === "boolean") {
      chg.textContent = t.charging ? "Charging" : "Not charging";
      chg.classList.toggle("ok", t.charging);
      chg.classList.toggle("warn", !t.charging);
    } else {
      chg.textContent = "–";
      chg.classList.remove("ok","warn");
    }
    if (typeof t.battery_voltage === "number") {
      bat.textContent = t.battery_voltage.toFixed(2);
    } else {
      bat.textContent = "–";
    }

    if (t.connection_status === "online") {
      conn.textContent = "Online";
      conn.classList.add("ok");
      conn.classList.remove("warn");
    } else if (t.connection_status === "offline") {
      conn.textContent = "Offline";
      conn.classList.add("warn");
      conn.classList.remove("ok");
    } else {
      conn.textContent = "Unknown";
      conn.classList.remove("ok", "warn");
    }

    if (typeof t.heartbeat_latency_avg_ms === "number") {
      latAvg.textContent = `${t.heartbeat_latency_avg_ms.toFixed(1)} ms`;
    } else {
      latAvg.textContent = "–";
    }

    if (typeof t.offline_for_s === "number") {
      offlineFor.textContent = formatDuration(t.offline_for_s);
    } else {
      offlineFor.textContent = "–";
    }

    updateOdometryFromTelemetry(t.odometry);
    updateImuFromTelemetry(t.imu);
  } catch(e) { console.error(e); }
}
setInterval(pollTelemetry, 200);
pollTelemetry();
</script>
"""
    return template.replace("__ROBOTS__", robots_json).replace("__MQTT_DEFAULTS__", mqtt_defaults_json)


@app.route("/")
def index() -> Response:
    return Response(_robot_options_html(), mimetype="text/html")


@app.route("/replay")
def replay_index() -> Response:
    if REPLAY_ARCHIVE is None or not REPLAY_ARCHIVE.is_available():
        return Response("Replay archive unavailable. Enable MongoDB MQTT history and install pymongo.", mimetype="text/plain", status=503)
    REPLAY_ARCHIVE.start_discovery()
    return Response(REPLAY_ARCHIVE.render_page(), mimetype="text/html")


@app.route("/replay/api/discovery/start", methods=["POST"])
def replay_discovery_start() -> Response:
    if REPLAY_ARCHIVE is None or not REPLAY_ARCHIVE.is_available():
        return jsonify({"error": "Replay archive unavailable"}), 503
    started = REPLAY_ARCHIVE.start_discovery()
    return jsonify({"started": started})


@app.route("/replay/api/discovery/status")
def replay_discovery_status() -> Response:
    if REPLAY_ARCHIVE is None or not REPLAY_ARCHIVE.is_available():
        return jsonify({"error": "Replay archive unavailable"}), 503
    return jsonify(REPLAY_ARCHIVE.discovery_status())


@app.route("/replay/api/sessions")
def replay_sessions() -> Response:
    if REPLAY_ARCHIVE is None or not REPLAY_ARCHIVE.is_available():
        return jsonify({"error": "Replay archive unavailable"}), 503
    limit = max(1, min(_coerce_int(request.args.get("limit"), 20), 100))
    before_raw = request.args.get("before", "").strip()
    before_value = None
    if before_raw:
        try:
            before_value = datetime.datetime.fromisoformat(before_raw.replace("Z", "+00:00"))
        except ValueError:
            abort(400, description="Invalid 'before' timestamp.")
    return jsonify({"sessions": REPLAY_ARCHIVE.list_sessions(limit=limit, before=before_value)})


@app.route("/replay/api/session/<session_id>/snapshot")
def replay_session_snapshot(session_id: str) -> Response:
    if REPLAY_ARCHIVE is None or not REPLAY_ARCHIVE.is_available():
        return jsonify({"error": "Replay archive unavailable"}), 503
    at_raw = request.args.get("at", "").strip()
    if not at_raw:
        abort(400, description="Missing 'at' timestamp.")
    try:
        at_value = datetime.datetime.fromisoformat(at_raw.replace("Z", "+00:00"))
    except ValueError:
        abort(400, description="Invalid 'at' timestamp.")
    components = [value.strip() for value in request.args.getlist("component") if value.strip()]
    try:
        payload = REPLAY_ARCHIVE.snapshot(session_id, at_value.timestamp(), components or None)
    except KeyError:
        abort(404, description="Unknown session.")
    return jsonify(payload)


@app.route("/replay/api/session/<session_id>/frame-step")
def replay_frame_step(session_id: str) -> Response:
    if REPLAY_ARCHIVE is None or not REPLAY_ARCHIVE.is_available():
        return jsonify({"error": "Replay archive unavailable"}), 503
    at_raw = request.args.get("at", "").strip()
    direction = request.args.get("direction", "next").strip().lower()
    if direction not in {"next", "prev"}:
        abort(400, description="Invalid direction.")
    try:
        at_value = datetime.datetime.fromisoformat(at_raw.replace("Z", "+00:00"))
    except ValueError:
        abort(400, description="Invalid 'at' timestamp.")
    components = [value.strip() for value in request.args.getlist("component") if value.strip()]
    result = REPLAY_ARCHIVE.frame_step_time(session_id, at_value.timestamp(), direction, components)
    return jsonify({"at": result and datetime.datetime.fromtimestamp(result, tz=datetime.timezone.utc).isoformat()})


@app.route("/replay/api/session/<session_id>/video/preload", methods=["POST"])
def replay_video_preload_start(session_id: str) -> Response:
    if REPLAY_ARCHIVE is None or not REPLAY_ARCHIVE.is_available():
        return jsonify({"error": "Replay archive unavailable"}), 503
    components = [value.strip() for value in request.args.getlist("component") if value.strip()]
    started = REPLAY_ARCHIVE.start_video_preload(session_id, components or None)
    return jsonify({"started": started, "status": REPLAY_ARCHIVE.video_preload_status(session_id)})


@app.route("/replay/api/session/<session_id>/video/preload")
def replay_video_preload_status(session_id: str) -> Response:
    if REPLAY_ARCHIVE is None or not REPLAY_ARCHIVE.is_available():
        return jsonify({"error": "Replay archive unavailable"}), 503
    return jsonify(REPLAY_ARCHIVE.video_preload_status(session_id))


@app.route("/replay/api/video/frame/<component_id>")
def replay_video_frame(component_id: str) -> Response:
    if REPLAY_ARCHIVE is None or not REPLAY_ARCHIVE.is_available():
        return jsonify({"error": "Replay archive unavailable"}), 503
    at_raw = request.args.get("at", "").strip()
    session_id = request.args.get("session", "").strip() or None
    try:
        at_value = datetime.datetime.fromisoformat(at_raw.replace("Z", "+00:00"))
    except ValueError:
        abort(400, description="Invalid 'at' timestamp.")
    result = REPLAY_ARCHIVE.reconstruct_video_frame(component_id, at_value.timestamp(), session_id=session_id)
    if result is None:
        abort(404, description="No replay frame available.")
    resp = Response(result["jpeg"], mimetype="image/jpeg")
    metadata = result.get("metadata") or {}
    if metadata.get("sourceTs"):
        resp.headers["X-Replay-Source-Timestamp"] = str(metadata["sourceTs"])
    return resp


@app.route("/replay/api/video/stream/<component_id>")
def replay_video_stream(component_id: str) -> Response:
    if REPLAY_ARCHIVE is None or not REPLAY_ARCHIVE.is_available():
        return jsonify({"error": "Replay archive unavailable"}), 503
    session_id = request.args.get("session", "").strip()
    start_raw = request.args.get("start", "").strip()
    token = request.args.get("token", "").strip()
    if not session_id or not start_raw or not token:
        abort(400, description="Missing session, start timestamp, or stream token.")
    try:
        start_value = datetime.datetime.fromisoformat(start_raw.replace("Z", "+00:00"))
    except ValueError:
        abort(400, description="Invalid start timestamp.")
    generator = stream_with_context(REPLAY_ARCHIVE.replay_video_stream(session_id, component_id, start_value.timestamp(), token))
    return Response(generator, mimetype="multipart/x-mixed-replace; boundary=frame")


@app.route("/replay/api/audio/chunks/<component_id>")
def replay_audio_chunks(component_id: str) -> Response:
    if REPLAY_ARCHIVE is None or not REPLAY_ARCHIVE.is_available():
        return jsonify({"error": "Replay archive unavailable"}), 503
    start_raw = request.args.get("start", "").strip()
    end_raw = request.args.get("end", "").strip()
    try:
        start_value = datetime.datetime.fromisoformat(start_raw.replace("Z", "+00:00"))
        end_value = datetime.datetime.fromisoformat(end_raw.replace("Z", "+00:00"))
    except ValueError:
        abort(400, description="Invalid start or end timestamp.")
    return jsonify({"chunks": REPLAY_ARCHIVE.audio_chunks(component_id, start_value.timestamp(), end_value.timestamp())})


@app.route("/mqtt/status")
def mqtt_status_route():
    return jsonify(_mqtt_status())


@app.route("/mqtt/connect", methods=["POST"])
def mqtt_connect_route():
    data = request.get_json(force=True, silent=True) or {}
    ok, error = _connect_mqtt(data)
    body = {"status": "connecting" if ok else "error"}
    if error:
        body["error"] = error
    return jsonify(body), (200 if ok else 400)


@app.route("/mqtt/disconnect", methods=["POST"])
def mqtt_disconnect_route():
    _disconnect_mqtt(clear_error=True)
    return jsonify({"status": "disconnected"})


@app.route("/robots/<robot_id>/soundboard/files")
def get_soundboard_files(robot_id: str):
    _ensure_robot(robot_id)
    return jsonify(_soundboard_files_payload(robot_id))


@app.route("/robots/<robot_id>/soundboard/status")
def get_soundboard_status(robot_id: str):
    _ensure_robot(robot_id)
    return jsonify(_soundboard_status_payload(robot_id))


@app.route("/robots/<robot_id>/soundboard/play", methods=["POST"])
def play_soundboard_file(robot_id: str):
    _ensure_robot(robot_id)
    payload = request.get_json(force=True, silent=True) or {}
    requested_file = payload.get("file")
    if not isinstance(requested_file, str) or not requested_file.strip():
        abort(400, description="Missing 'file'.")
    file_name = requested_file.strip().replace("\\", "/")
    message = {"action": "play", "file": file_name, "timestamp": time.time()}
    ok = _publish_command(_soundboard_command_topic_for_robot(robot_id), message, qos=1)
    if not ok:
        return jsonify({"status": "mqtt-unavailable"}), 503
    return jsonify({"status": "ok", "file": file_name, "topic": _soundboard_command_topic_for_robot(robot_id)})


@app.route("/robots/<robot_id>/soundboard/stop", methods=["POST"])
def stop_soundboard_file(robot_id: str):
    _ensure_robot(robot_id)
    message = {"action": "stop", "timestamp": time.time()}
    ok = _publish_command(_soundboard_command_topic_for_robot(robot_id), message, qos=1)
    if not ok:
        return jsonify({"status": "mqtt-unavailable"}), 503
    return jsonify({"status": "ok", "topic": _soundboard_command_topic_for_robot(robot_id)})


@app.route("/robots/<robot_id>/outlets")
def get_outlets(robot_id: str):
    _ensure_robot(robot_id)
    return jsonify(_outlets_payload(robot_id))


@app.route("/robots/<robot_id>/outlets/<outlet_id>/power", methods=["POST"])
def set_outlet_power(robot_id: str, outlet_id: str):
    _ensure_robot(robot_id)
    if not _robot_has_outlet_controls(robot_id):
        return jsonify({"status": "unsupported"}), 403
    clean_outlet_id = str(outlet_id or "").strip().strip("/")
    if not clean_outlet_id:
        abort(400, description="Outlet id is required.")

    payload = request.get_json(force=True, silent=True) or {}
    desired = _coerce_bool(payload.get("on"))
    if desired is None:
        with outlets_cache_lock:
            current = (outlets_status_cache.get(_robot_key(robot_id)) or {}).get(clean_outlet_id) or {}
        desired = not bool(current.get("on", False))

    ok = _publish_command(_outlet_power_topic_for_robot(robot_id, clean_outlet_id), {"on": desired}, qos=1)
    if not ok:
        return jsonify({"status": "mqtt-unavailable"}), 503
    return jsonify(
        {
            "status": "ok",
            "outlet": clean_outlet_id,
            "on": desired,
            "topic": _outlet_power_topic_for_robot(robot_id, clean_outlet_id),
        }
    )


@app.route("/robots/<robot_id>/autonomy/files")
def get_autonomy_files(robot_id: str):
    _ensure_robot(robot_id)
    return jsonify(_autonomy_files_payload(robot_id))


@app.route("/robots/<robot_id>/autonomy/status")
def get_autonomy_status(robot_id: str):
    _ensure_robot(robot_id)
    return jsonify(_autonomy_status_payload(robot_id))


@app.route("/robots/<robot_id>/autonomy/start", methods=["POST"])
def start_autonomy(robot_id: str):
    _ensure_robot(robot_id)
    if not _robot_has_autonomy(robot_id):
        return jsonify({"status": "unsupported"}), 403
    if not _robot_has_autonomy_controls(robot_id):
        return jsonify({"status": "unsupported"}), 403

    payload = request.get_json(force=True, silent=True) or {}
    requested_file = payload.get("file")
    if not isinstance(requested_file, str) or not requested_file.strip():
        abort(400, description="Missing 'file'.")
    file_name = requested_file.strip()

    config_payload = payload.get("config")
    if config_payload is not None and not isinstance(config_payload, dict):
        abort(400, description="'config' must be an object when provided.")

    message = {
        "action": "start",
        "file": file_name,
        "config": config_payload if isinstance(config_payload, dict) else {},
        "timestamp": time.time(),
    }
    ok = _publish_command(_autonomy_command_topic_for_robot(robot_id), message, qos=1)
    if not ok:
        return jsonify({"status": "mqtt-unavailable"}), 503
    return jsonify({"status": "ok", "file": file_name, "topic": _autonomy_command_topic_for_robot(robot_id)})


@app.route("/robots/<robot_id>/autonomy/stop", methods=["POST"])
def stop_autonomy(robot_id: str):
    _ensure_robot(robot_id)
    if not _robot_has_autonomy(robot_id):
        return jsonify({"status": "unsupported"}), 403
    if not _robot_has_autonomy_controls(robot_id):
        return jsonify({"status": "unsupported"}), 403

    message = {"action": "stop", "timestamp": time.time()}
    ok = _publish_command(_autonomy_command_topic_for_robot(robot_id), message, qos=1)
    if not ok:
        return jsonify({"status": "mqtt-unavailable"}), 503
    return jsonify({"status": "ok", "topic": _autonomy_command_topic_for_robot(robot_id)})


@app.route("/robots/<robot_id>/reboot", methods=["POST"])
def reboot_robot(robot_id: str):
    robot = _ensure_robot(robot_id)
    component_type = str(robot.get("type") or "robots").strip().lower()
    if component_type != "robots":
        return jsonify({"status": "unsupported"}), 403
    if not _robot_has_reboot_controls(robot_id):
        return jsonify({"status": "unsupported"}), 403
    ok = _publish_reboot_flag(robot_id)
    if not ok:
        return jsonify({"status": "mqtt-unavailable"}), 503
    return jsonify({"status": "ok", "topic": _reboot_flag_topic_for_robot(robot_id), "retain": False})


@app.route("/robots/<robot_id>/service-restart", methods=["POST"])
def service_restart_robot(robot_id: str):
    robot = _ensure_robot(robot_id)
    component_type = str(robot.get("type") or "robots").strip().lower()
    if component_type != "robots":
        return jsonify({"status": "unsupported"}), 403
    if not _robot_has_service_restart_controls(robot_id):
        return jsonify({"status": "unsupported"}), 403
    ok = _publish_service_restart_flag(robot_id)
    if not ok:
        return jsonify({"status": "mqtt-unavailable"}), 503
    return jsonify({"status": "ok", "topic": _service_restart_flag_topic_for_robot(robot_id), "retain": False})


@app.route("/robots/<robot_id>/git-pull", methods=["POST"])
def git_pull_robot(robot_id: str):
    robot = _ensure_robot(robot_id)
    component_type = str(robot.get("type") or "robots").strip().lower()
    if component_type != "robots":
        return jsonify({"status": "unsupported"}), 403
    if not _robot_has_git_pull_controls(robot_id):
        return jsonify({"status": "unsupported"}), 403
    ok = _publish_git_pull_flag(robot_id)
    if not ok:
        return jsonify({"status": "mqtt-unavailable"}), 503
    return jsonify({"status": "ok", "topic": _git_pull_flag_topic_for_robot(robot_id), "retain": False})


@app.route("/robots")
def list_robots():
    return jsonify({"robots": _ui_robot_snapshot(), "updated": infrastructure_last_update})


@app.route("/robots/<robot_id>/telemetry")
def get_telemetry(robot_id: str):
    _ensure_robot(robot_id)
    robot_key = _robot_key(robot_id)
    with telemetry_lock:
        data = telemetry_cache.get(robot_key, {}).copy()
    state = _heartbeat_connection_state(data)
    data["connection_status"] = state["status"]
    data["online"] = state["online"]
    data["offline_for_s"] = state["offline_for_s"]
    data["heartbeat_age_s"] = state["age_s"]
    data["heartbeat_window_s"] = state["window_s"]
    data["heartbeat_interval_s"] = state["interval_s"]
    data["heartbeat_latency_ms"] = state["latency_ms"]
    data["heartbeat_latency_avg_ms"] = state["latency_avg_ms"]
    return jsonify(data)


@app.route("/robots/<robot_id>/logs")
def get_component_logs(robot_id: str):
    _ensure_robot(robot_id)
    limit_raw = request.args.get("limit", "250")
    try:
        limit = int(limit_raw)
    except (TypeError, ValueError):
        limit = 250
    return jsonify(_component_logs_payload(robot_id, limit))


@app.route("/robots/<robot_id>/audio/status")
def get_audio_status(robot_id: str):
    _ensure_robot(robot_id)
    robot_key = _robot_key(robot_id)
    if not _robot_has_audio(robot_id):
        return jsonify({"enabled": False})
    with audio_cache_lock:
        entry = audio_cache.get(robot_key) or {}
    with audio_uplink_lock:
        uplink = audio_uplink_state.get(robot_key) or {}
    timestamp = entry.get("timestamp")
    age = time.time() - timestamp if timestamp else None
    uplink_age = None
    if uplink.get("timestamp") is not None:
        uplink_age = time.time() - float(uplink.get("timestamp"))
    status = {
        "enabled": True,
        "topic": _audio_topic_for_robot(robot_id),
        "lastChunkAge": age,
        "hasChunk": bool(entry.get("seq")),
        "rate": entry.get("rate"),
        "channels": entry.get("channels"),
        "frameSamples": entry.get("frame_samples"),
        "chunkMs": _coerce_int(_config_hint(CONFIG_AUDIO_HINTS, robot_key).get("chunk_ms"), 20),
        "concealmentMs": _coerce_int(_config_hint(CONFIG_AUDIO_HINTS, robot_key).get("concealment_ms"), 100),
        "controls": _robot_has_audio_controls(robot_id),
        "uplinkTopic": _audio_uplink_topic_for_robot(robot_id),
        "uplinkSeq": uplink.get("seq"),
        "uplinkLastAge": uplink_age,
    }
    return jsonify(status)


@app.route("/robots/<robot_id>/audio/uplink", methods=["POST"])
def publish_audio_uplink(robot_id: str):
    _ensure_robot(robot_id)
    _require_audio_robot(robot_id)
    robot_key = _robot_key(robot_id)
    payload = request.get_json(force=True, silent=True) or {}
    encoded = payload.get("data")
    if not encoded:
        abort(400, description="Missing base64 'data'.")
    try:
        pcm_bytes = base64.b64decode(encoded)
    except base64.binascii.Error:
        abort(400, description="Invalid base64 payload.")

    hint = _config_hint(CONFIG_AUDIO_UPLINK_HINTS, robot_key)
    rate = max(1, _coerce_int(payload.get("rate"), int(hint.get("rate", 16000))))
    channels = max(1, _coerce_int(payload.get("channels"), int(hint.get("channels", 1))))
    qos = max(0, min(1, _coerce_int(payload.get("qos"), int(hint.get("qos", 0)))))

    with audio_uplink_lock:
        state = audio_uplink_state.setdefault(robot_key, {"seq": 0})
        seq = int(state.get("seq", 0)) + 1
        state.update({"seq": seq, "timestamp": time.time(), "last_bytes": len(pcm_bytes)})

    mqtt_payload = _encode_audio_packet(seq, rate, channels, pcm_bytes)
    if not mqtt_payload:
        abort(400, description="Invalid PCM payload for configured format.")
    ok = _publish_command(_audio_uplink_topic_for_robot(robot_id), mqtt_payload, qos=qos)
    if not ok:
        abort(503, description="MQTT publish failed.")
    return jsonify({"ok": True, "seq": seq})


@app.route("/robots/<robot_id>/audio/stream")
def stream_audio(robot_id: str):
    _ensure_robot(robot_id)
    _require_audio_robot(robot_id)
    generator = stream_with_context(_audio_stream_generator(robot_id))
    resp = Response(generator, mimetype="text/event-stream")
    resp.headers["Cache-Control"] = "no-store"
    return resp


@app.route("/robots/<robot_id>/audio/start", methods=["POST"])
def start_audio(robot_id: str):
    _ensure_robot(robot_id)
    _require_audio_robot(robot_id)
    if not _robot_has_audio_controls(robot_id):
        return jsonify({"status": "unsupported"}), 403
    _clear_audio_state(robot_id)
    flag_ok = _publish_audio_flag(robot_id, True)
    if not flag_ok:
        return jsonify({"status": "mqtt-unavailable"}), 503
    ok = _publish_audio_control(robot_id, True)
    if ok:
        return jsonify({"status": "ok"}), 200
    return jsonify({"status": "mqtt-unavailable"}), 503


@app.route("/robots/<robot_id>/audio/stop", methods=["POST"])
def stop_audio_remote(robot_id: str):
    _ensure_robot(robot_id)
    _require_audio_robot(robot_id)
    if not _robot_has_audio_controls(robot_id):
        return jsonify({"status": "unsupported"}), 403
    ok = _publish_audio_control(robot_id, False)
    _clear_audio_state(robot_id)
    if ok:
        return jsonify({"status": "ok"}), 200
    return jsonify({"status": "mqtt-unavailable"}), 503


@app.route("/robots/<robot_id>/video/frame")
def get_video_frame(robot_id: str):
    _ensure_robot(robot_id)
    _require_video_robot(robot_id)
    robot_key = _robot_key(robot_id)
    with video_cache_lock:
        entry = video_cache.get(robot_key) or {}
        frame_bytes = entry.get("jpeg")
        timestamp = entry.get("timestamp")
    if not frame_bytes:
        abort(404, description="No video frame available yet.")
    resp = Response(frame_bytes, mimetype="image/jpeg")
    resp.headers["Cache-Control"] = "no-store"
    if timestamp:
        resp.headers["X-Frame-Timestamp"] = str(timestamp)
    return resp


@app.route("/robots/<robot_id>/video/stream")
def stream_video(robot_id: str):
    _ensure_robot(robot_id)
    _require_video_robot(robot_id)
    generator = stream_with_context(_video_frame_generator(robot_id))
    return Response(generator, mimetype="multipart/x-mixed-replace; boundary=frame")


@app.route("/robots/<robot_id>/video/status")
def get_video_status(robot_id: str):
    _ensure_robot(robot_id)
    robot_key = _robot_key(robot_id)
    if not _robot_has_video(robot_id):
        return jsonify({"enabled": False})
    if not VIDEO_SUPPORT:
        return jsonify({"enabled": False, "error": "Video support unavailable"})

    with video_cache_lock:
        entry = video_cache.get(robot_key) or {}
        timestamp = entry.get("timestamp")
        has_frame = bool(entry.get("jpeg"))

    age = time.time() - timestamp if timestamp else None
    status = {
        "enabled": True,
        "topic": _video_topic_for_robot(robot_id),
        "hasFrame": has_frame,
        "lastFrameAge": age,
        "controls": _robot_has_video_controls(robot_id),
    }
    return jsonify(status)


@app.route("/robots/<robot_id>/video/overlays")
def get_video_overlays(robot_id: str):
    _ensure_robot(robot_id)
    if not _robot_has_video(robot_id):
        return jsonify({"enabled": False, "shapes": []})
    return jsonify(_video_overlays_payload(robot_id))


@app.route("/robots/<robot_id>/video/start", methods=["POST"])
def start_video(robot_id: str):
    _ensure_robot(robot_id)
    _require_video_robot(robot_id)
    flag_ok = _publish_video_flag(robot_id, True)
    if not flag_ok:
        return jsonify({"status": "mqtt-unavailable"}), 503
    ok = _publish_video_control(robot_id, True)
    if ok:
        return jsonify({"status": "ok"}), 200
    return jsonify({"status": "mqtt-unavailable"}), 503


@app.route("/robots/<robot_id>/video/stop", methods=["POST"])
def stop_video(robot_id: str):
    _ensure_robot(robot_id)
    _require_video_robot(robot_id)
    flag_ok = _publish_video_flag(robot_id, False)
    if not flag_ok:
        return jsonify({"status": "mqtt-unavailable"}), 503
    ok = _publish_video_control(robot_id, False)
    if ok:
        return jsonify({"status": "ok"}), 200
    return jsonify({"status": "mqtt-unavailable"}), 503


@app.route("/robots/<robot_id>/m", methods=["POST"])
def post_m(robot_id: str):
    _ensure_robot(robot_id)
    data = request.get_json(force=True, silent=True) or {}
    try:
        left = clamp(float(data.get("l", 0.0)), -1.0, 1.0)
        right = clamp(float(data.get("r", 0.0)), -1.0, 1.0)
        grouser = clamp(float(data.get("y", 0.0)), -1.0, 1.0)
    except (TypeError, ValueError):
        return ("bad args", 400)
    z = clamp((left + right) / 2.0, -1.0, 1.0)
    x = clamp((left - right) / 2.0, -1.0, 1.0)
    payload = {"x": x, "z": z, "y": grouser}
    topic = _topic(robot_id, "incoming", "drive-values")
    # Drive commands are high-rate freshness data; QoS 0 avoids stale queue buildup.
    ok = _publish_command(topic, payload, qos=0)
    return ("ok", 200) if ok else ("mqtt unavailable", 503)


@app.route("/robots/<robot_id>/l", methods=["POST"])
def post_l(robot_id: str):
    _ensure_robot(robot_id)
    data = request.get_json(force=True, silent=True) or {}
    try:
        b = clamp(float(data.get("b", 0.0)), 0.0, 1.0)
        g = clamp(float(data.get("g", 0.0)), 0.0, 1.0)
        r = clamp(float(data.get("r", 0.0)), 0.0, 1.0)
    except (TypeError, ValueError):
        return ("bad args", 400)
    payload = {"b": b, "g": g, "r": r}
    topic = _topic(robot_id, "incoming", "lights-solid")
    ok = _publish_command(topic, payload)
    return ("ok", 200) if ok else ("mqtt unavailable", 503)


@app.route("/robots/<robot_id>/f", methods=["POST"])
def post_f(robot_id: str):
    _ensure_robot(robot_id)
    data = request.get_json(force=True, silent=True) or {}
    try:
        b = clamp(float(data.get("b", 0.0)), 0.0, 1.0)
        g = clamp(float(data.get("g", 0.0)), 0.0, 1.0)
        r = clamp(float(data.get("r", 0.0)), 0.0, 1.0)
        period = clamp(float(data.get("period", 2.0)), 0.05, 30.0)
    except (TypeError, ValueError):
        return ("bad args", 400)
    payload = {"b": b, "g": g, "r": r, "period": period}
    topic = _topic(robot_id, "incoming", "lights-flash")
    ok = _publish_command(topic, payload)
    return ("ok", 200) if ok else ("mqtt unavailable", 503)


def main() -> None:
    if mqtt_history_enabled:
        ok, error = _connect_mqtt(MQTT_CFG)
        if not ok:
            logging.error("MQTT history startup connect failed: %s", error)
    app.run(host=WEB_HOST, port=WEB_PORT, debug=False)


if __name__ == "__main__":
    main()
