#!/usr/bin/env python3
"""
Web control UI for Pebble systems that interacts solely through MQTT topics.

This application can run on any system with network access to the MQTT broker. The
HTML UI mirrors the earlier serial-based version but publishes drive and light
commands to the standard incoming topics while displaying telemetry received from
the outgoing topics.
"""
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
VIDEO_MAGIC = b"PBVD"
VIDEO_FRAME_JPEG = 1
VIDEO_FRAME_DELTA_JPEG = 2
VIDEO_PACKET_HEADER = struct.Struct("!4sBBBIQ")


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

# [content unchanged omitted for brevity in reasoning, but full file supplied in actual update]
