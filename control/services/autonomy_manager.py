#!/usr/bin/env python3
"""Manage autonomy script discovery and one-at-a-time execution via local MQTT."""

from __future__ import annotations

import argparse
import json
import logging
import os
import queue
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, Optional

import paho.mqtt.client as mqtt

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parents[2]))

from control.common.config import default_retained_publish_interval_seconds, load_config, log_level, service_cfg
from control.common.mqtt import mqtt_auth_and_tls
from control.common.topics import identity_from_config

REPO_ROOT = Path(__file__).resolve().parents[2]


def _parse_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "on", "enabled", "enable"}:
            return True
        if lowered in {"0", "false", "no", "off", "disabled", "disable"}:
            return False
    return default


class AutonomyManager:
    def __init__(self, config: dict[str, Any], config_path: Path) -> None:
        self.config = config
        self.config_path = config_path
        self.identity = identity_from_config(config)
        self.service_cfg = service_cfg(config, "autonomy_manager")
        if not self.service_cfg.get("enabled", True):
            raise SystemExit("autonomy_manager is disabled in config.")

        topics = self.service_cfg.get("topics") if isinstance(self.service_cfg.get("topics"), dict) else {}
        self.command_topic = str(topics.get("command") or self.identity.topic("incoming", "autonomy-command"))
        self.files_topic = str(topics.get("files") or self.identity.topic("outgoing", "autonomy-files"))
        self.status_topic = str(topics.get("status") or self.identity.topic("outgoing", "autonomy-status"))

        autonomy_root_raw = str(self.service_cfg.get("autonomy_root") or "autonomy")
        autonomy_root = Path(autonomy_root_raw).expanduser()
        if not autonomy_root.is_absolute():
            autonomy_root = (REPO_ROOT / autonomy_root).resolve()
        self.autonomy_root = autonomy_root

        self.scan_interval_seconds = max(1.0, _parse_float(self.service_cfg.get("scan_interval_seconds"), 5.0))
        self.stop_timeout_seconds = max(0.5, _parse_float(self.service_cfg.get("stop_timeout_seconds"), 8.0))

        default_retain_interval = default_retained_publish_interval_seconds(config, default=3600.0)
        interval_raw = self.service_cfg.get("retained_publish_interval_seconds")
        try:
            interval_value = float(interval_raw if interval_raw is not None else default_retain_interval)
        except (TypeError, ValueError):
            interval_value = default_retain_interval
        self.retained_publish_interval_seconds = interval_value if interval_value > 0 else default_retain_interval

        self.global_env = self.service_cfg.get("env") if isinstance(self.service_cfg.get("env"), dict) else {}
        self.scripts_cfg = self.service_cfg.get("scripts") if isinstance(self.service_cfg.get("scripts"), dict) else {}

        self.local_mqtt_cfg = config.get("local_mqtt") if isinstance(config.get("local_mqtt"), dict) else {}
        self.local_host = str(self.local_mqtt_cfg.get("host") or "127.0.0.1")
        self.local_port = int(self.local_mqtt_cfg.get("port") or 1883)
        self.local_keepalive = int(self.local_mqtt_cfg.get("keepalive") or 60)

        self.stop_event = threading.Event()
        self.reconnect_lock = threading.Lock()
        self.reconnect_thread: Optional[threading.Thread] = None

        self.client: Optional[mqtt.Client] = None
        self.command_queue: queue.SimpleQueue[Any] = queue.SimpleQueue()

        self.available_scripts: dict[str, dict[str, Any]] = {}
        self.available_files_payload: list[dict[str, Any]] = []

        self.process: Optional[subprocess.Popen] = None
        self.dependency_processes: list[dict[str, Any]] = []
        self.running_file: Optional[str] = None
        self.last_error: Optional[str] = None
        self.last_config: Optional[dict[str, Any]] = None

        self.next_scan_at = 0.0
        self.next_retained_republish_at = 0.0

    def start(self) -> None:
        self._connect_local_mqtt()
        self._refresh_scripts(force=True)
        while not self.stop_event.is_set():
            self._drain_command_queue()
            self._poll_process()
            self._refresh_scripts()
            self._republish_retained_if_due()
            self.stop_event.wait(0.05)

    def stop(self) -> None:
        self.stop_event.set()
        self._stop_process(publish_status=False, clear_error=False)
        if self.client:
            try:
                self.client.loop_stop()
                self.client.disconnect()
            except Exception:
                logging.debug("MQTT disconnect failed", exc_info=True)
            self.client = None

    def _connect_local_mqtt(self) -> None:
        client = mqtt.Client()
        mqtt_auth_and_tls(client, self.local_mqtt_cfg, self.config_path.parent)
        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_message = self._on_message
        self.client = client

        delay = 2.0
        while not self.stop_event.is_set():
            try:
                client.connect(self.local_host, self.local_port, self.local_keepalive)
                client.loop_start()
                return
            except OSError as exc:
                logging.warning("Local MQTT connection failed: %s", exc)
                self.stop_event.wait(delay)
                delay = min(delay * 2, 60.0)

    def _on_connect(self, client: mqtt.Client, _userdata: Any, _flags: dict[str, Any], rc: int) -> None:
        if rc != 0:
            logging.error("Local MQTT connect failed rc=%s", rc)
            return
        logging.info("Local MQTT connected; subscribing %s", self.command_topic)
        client.subscribe(self.command_topic, qos=1)
        self._refresh_scripts(force=True)
        self._publish_status()
        self.next_retained_republish_at = time.time() + self.retained_publish_interval_seconds

    def _on_disconnect(self, _client: mqtt.Client, _userdata: Any, rc: int) -> None:
        if rc != mqtt.MQTT_ERR_SUCCESS:
            logging.warning("Unexpected local MQTT disconnect rc=%s", rc)
            self._start_reconnect_thread()

    def _start_reconnect_thread(self) -> None:
        if self.stop_event.is_set():
            return
        with self.reconnect_lock:
            if self.reconnect_thread and self.reconnect_thread.is_alive():
                return
            thread = threading.Thread(target=self._reconnect_loop, name="autonomy-mqtt-reconnect", daemon=True)
            thread.start()
            self.reconnect_thread = thread

    def _reconnect_loop(self) -> None:
        delay = 2.0
        while not self.stop_event.is_set():
            client = self.client
            if client is None:
                return
            is_connected = getattr(client, "is_connected", None)
            if callable(is_connected) and is_connected():
                return
            try:
                client.connect(self.local_host, self.local_port, self.local_keepalive)
                return
            except OSError as exc:
                logging.warning("MQTT reconnect failed: %s", exc)
                self.stop_event.wait(delay)
                delay = min(delay * 2, 60.0)

    def _on_message(self, _client: mqtt.Client, _userdata: Any, msg: mqtt.MQTTMessage) -> None:
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
        except json.JSONDecodeError:
            logging.warning("Non-JSON payload for autonomy command")
            return
        self.command_queue.put(payload)

    def _drain_command_queue(self) -> None:
        while True:
            try:
                payload = self.command_queue.get_nowait()
            except queue.Empty:
                return
            self._handle_command(payload)

    def _publish_json(self, topic: str, payload: dict[str, Any], qos: int = 1, retain: bool = False) -> None:
        client = self.client
        if client is None:
            return
        is_connected = getattr(client, "is_connected", None)
        if callable(is_connected) and not is_connected():
            return
        client.publish(topic, json.dumps(payload), qos=qos, retain=retain)

    def _scan_scripts(self) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
        scripts: dict[str, dict[str, Any]] = {}
        files_payload: list[dict[str, Any]] = []

        if not self.autonomy_root.exists() or not self.autonomy_root.is_dir():
            return scripts, files_payload

        for script_dir in sorted(self.autonomy_root.iterdir(), key=lambda p: p.name.lower()):
            if not script_dir.is_dir():
                continue
            name = script_dir.name
            script_cfg = self.scripts_cfg.get(name) if isinstance(self.scripts_cfg.get(name), dict) else {}
            if script_cfg and script_cfg.get("enabled") is False:
                continue

            entrypoint = str(script_cfg.get("entrypoint") or "run.py").strip() or "run.py"
            entrypoint_path = script_dir / entrypoint
            if not entrypoint_path.exists() or not entrypoint_path.is_file():
                continue

            args_schema = self._normalize_args_schema(script_cfg.get("args"))
            args_schema = self._apply_schema_defaults(name, args_schema)
            default_cfg = self._default_config_map(args_schema)

            cwd_value = script_cfg.get("cwd")
            if isinstance(cwd_value, str) and cwd_value.strip():
                cwd = Path(cwd_value).expanduser()
                if not cwd.is_absolute():
                    cwd = (REPO_ROOT / cwd).resolve()
            else:
                cwd = script_dir

            script_env = script_cfg.get("env") if isinstance(script_cfg.get("env"), dict) else {}
            dependencies = self._normalize_dependencies(script_cfg.get("dependencies"))
            dependencies = self._apply_dependency_defaults(name, dependencies)

            scripts[name] = {
                "name": name,
                "display_name": str(script_cfg.get("display_name") or name),
                "dir": script_dir,
                "entrypoint": entrypoint_path,
                "cwd": cwd,
                "env": script_env,
                "args": args_schema,
                "defaults": default_cfg,
                "dependencies": dependencies,
            }

            files_payload.append(
                {
                    "file": name,
                    "label": str(script_cfg.get("display_name") or name),
                    "configs": args_schema,
                }
            )

        return scripts, files_payload

    @staticmethod
    def _apply_schema_defaults(script_name: str, args_schema: list[dict[str, Any]]) -> list[dict[str, Any]]:
        keys = {str(item.get("key")) for item in args_schema if isinstance(item, dict)}

        out = list(args_schema)
        if script_name == "wheel-odometry":
            # Keep wheel-odometry tune-state toggle available to remote web controls,
            # even when local config lacks this newer schema field.
            if "use_tune_state" not in keys:
                out.append(
                    {
                        "key": "use_tune_state",
                        "label": "Use Tune State",
                        "type": "bool",
                        "default": False,
                        "arg": "--use-tune-state",
                        "mode": "flag",
                        "arg_false": "--no-use-tune-state",
                    }
                )
            return out

        if script_name == "apriltag-odom-follow":
            defaults: list[dict[str, Any]] = [
                {"key": "tag_id", "label": "Tag ID", "type": "int", "default": 13, "arg": "--marker-id", "min": 0},
                {
                    "key": "tag_size_m",
                    "label": "Tag Size (m)",
                    "type": "float",
                    "default": 0.25,
                    "arg": "--marker-size",
                    "min": 0.01,
                    "required": True,
                },
                {
                    "key": "target_distance_m",
                    "label": "Target Distance (m)",
                    "type": "float",
                    "default": 0.6,
                    "arg": "--target-distance",
                    "min": 0.05,
                },
                {
                    "key": "distance_threshold_m",
                    "label": "Distance Threshold (m)",
                    "type": "float",
                    "default": 0.05,
                    "arg": "--distance-threshold",
                    "min": 0.0,
                },
                {
                    "key": "standoff_hysteresis_m",
                    "label": "Standoff Hysteresis (m)",
                    "type": "float",
                    "default": 0.03,
                    "arg": "--standoff-hysteresis-m",
                    "min": 0.0,
                },
                {
                    "key": "vision_forward_kp",
                    "label": "Vision Forward Kp",
                    "type": "float",
                    "default": 2.0,
                    "arg": "--vision-forward-kp",
                    "min": 0.0,
                },
                {
                    "key": "vision_reverse_kp",
                    "label": "Vision Reverse Kp",
                    "type": "float",
                    "default": 2.0,
                    "arg": "--vision-reverse-kp",
                    "min": 0.0,
                },
                {
                    "key": "vision_forward_floor_distance_m",
                    "label": "Vision Floor Dist (m)",
                    "type": "float",
                    "default": 0.2,
                    "arg": "--vision-forward-floor-distance-m",
                    "min": 0.0,
                },
                {
                    "key": "forward_cone_deg",
                    "label": "Forward Cone (deg)",
                    "type": "float",
                    "default": 18.0,
                    "arg": "--forward-cone-deg",
                    "min": 5.0,
                    "max": 75.0,
                },
                {
                    "key": "forward_min_power",
                    "label": "Forward Min Power",
                    "type": "float",
                    "default": 0.8,
                    "arg": "--forward-min-power",
                    "min": 0.0,
                    "max": 1.0,
                },
                {
                    "key": "forward_max_power",
                    "label": "Forward Max Power",
                    "type": "float",
                    "default": 1.0,
                    "arg": "--forward-max-power",
                    "min": 0.0,
                    "max": 1.0,
                },
                {
                    "key": "tags_topic",
                    "label": "Tags Topic",
                    "type": "string",
                    "default": "",
                    "arg": "--tags-topic",
                },
                {
                    "key": "odom_topic",
                    "label": "Odom Topic",
                    "type": "string",
                    "default": "",
                    "arg": "--odom-topic",
                },
                {
                    "key": "control_rate_hz",
                    "label": "Control Rate (Hz)",
                    "type": "float",
                    "default": 10.0,
                    "arg": "--rate",
                    "min": 1.0,
                },
                {
                    "key": "turn_power",
                    "label": "Turn Power",
                    "type": "float",
                    "default": 0.5,
                    "arg": "--turn-power",
                    "min": 0.05,
                    "max": 1.0,
                },
                {
                    "key": "turn_pulse_s",
                    "label": "Turn Pulse (s)",
                    "type": "float",
                    "default": 0.08,
                    "arg": "--turn-pulse",
                    "min": 0.02,
                },
                {
                    "key": "turn_cooldown_s",
                    "label": "Turn Cooldown (s)",
                    "type": "float",
                    "default": 0.18,
                    "arg": "--turn-cooldown",
                    "min": 0.0,
                },
                {
                    "key": "odom_goal_tolerance_m",
                    "label": "Odom Goal Tolerance (m)",
                    "type": "float",
                    "default": 0.06,
                    "arg": "--odom-goal-tolerance-m",
                    "min": 0.01,
                },
                {
                    "key": "map_pose_topic",
                    "label": "Map Pose Topic",
                    "type": "string",
                    "default": "",
                    "arg": "--map-pose-topic",
                },
                {
                    "key": "min_wheel_pwm",
                    "label": "Min Wheel PWM",
                    "type": "float",
                    "default": 0.3,
                    "arg": "--min-wheel-pwm",
                    "min": 0.0,
                    "max": 1.0,
                },
                {
                    "key": "odom_heading_kp",
                    "label": "Odom Heading Kp",
                    "type": "float",
                    "default": 1.5,
                    "arg": "--odom-heading-kp",
                    "min": 0.1,
                },
                {
                    "key": "odom_distance_kp",
                    "label": "Odom Distance Kp",
                    "type": "float",
                    "default": 1.4,
                    "arg": "--odom-distance-kp",
                    "min": 0.1,
                },
                {
                    "key": "odom_min_drive_power",
                    "label": "Odom Min Drive Power",
                    "type": "float",
                    "default": 0.35,
                    "arg": "--odom-min-drive-power",
                    "min": 0.0,
                    "max": 1.0,
                },
                {
                    "key": "odom_max_drive_power",
                    "label": "Odom Max Drive Power",
                    "type": "float",
                    "default": 0.9,
                    "arg": "--odom-max-drive-power",
                    "min": 0.0,
                    "max": 1.0,
                },
                {
                    "key": "odom_brake_distance_m",
                    "label": "Odom Brake Distance (m)",
                    "type": "float",
                    "default": 0.25,
                    "arg": "--odom-brake-distance-m",
                    "min": 0.0,
                },
                {
                    "key": "search_pause_s",
                    "label": "Search Pause (s)",
                    "type": "float",
                    "default": 0.35,
                    "arg": "--search-pause",
                    "min": 0.0,
                },
                {
                    "key": "search_reverse_after_s",
                    "label": "Search Reverse After (s)",
                    "type": "float",
                    "default": 0.0,
                    "arg": "--search-reverse-after",
                    "min": 0.0,
                },
                {
                    "key": "lost_timeout_s",
                    "label": "Lost Timeout (s)",
                    "type": "float",
                    "default": 0.7,
                    "arg": "--lost-timeout",
                    "min": 0.0,
                },
                {
                    "key": "tag_timeout_s",
                    "label": "Tag Timeout (s)",
                    "type": "float",
                    "default": 0.35,
                    "arg": "--tag-timeout",
                    "min": 0.05,
                },
                {
                    "key": "min_goal_advance_m",
                    "label": "Min Goal Advance (m)",
                    "type": "float",
                    "default": 0.1,
                    "arg": "--min-goal-advance-m",
                    "min": 0.0,
                },
                {
                    "key": "goal_hold_s",
                    "label": "Goal Hold (s)",
                    "type": "float",
                    "default": 0.4,
                    "arg": "--goal-hold-s",
                    "min": 0.0,
                },
                {
                    "key": "log_file",
                    "label": "Log File",
                    "type": "string",
                    "default": "",
                    "arg": "--log-file",
                },
                {
                    "key": "preview",
                    "label": "Preview Window",
                    "type": "bool",
                    "default": False,
                    "arg": "--preview",
                    "mode": "flag",
                },
            ]
            for item in defaults:
                key = str(item.get("key") or "")
                if key and key not in keys:
                    out.append(item)
            return out

        if script_name == "apriltag-locations":
            defaults = [
                {
                    "key": "width",
                    "label": "Frame Width",
                    "type": "int",
                    "default": 1280,
                    "arg": "--width",
                    "min": 160,
                },
                {
                    "key": "height",
                    "label": "Frame Height",
                    "type": "int",
                    "default": 720,
                    "arg": "--height",
                    "min": 120,
                },
                {
                    "key": "fps",
                    "label": "Frame FPS",
                    "type": "float",
                    "default": 15.0,
                    "arg": "--fps",
                    "min": 1.0,
                },
                {
                    "key": "control_rate_hz",
                    "label": "Publish Rate (Hz)",
                    "type": "float",
                    "default": 12.0,
                    "arg": "--rate",
                    "min": 1.0,
                },
                {
                    "key": "marker_dictionary",
                    "label": "Tag Dictionary",
                    "type": "string",
                    "default": "APRILTAG_25H9",
                    "arg": "--marker-dictionary",
                },
                {
                    "key": "locations_topic",
                    "label": "Locations Topic",
                    "type": "string",
                    "default": "",
                    "arg": "--locations-topic",
                },
                {
                    "key": "overlays_topic",
                    "label": "Overlays Topic",
                    "type": "string",
                    "default": "",
                    "arg": "--overlays-topic",
                },
                {
                    "key": "preview",
                    "label": "Preview Window",
                    "type": "bool",
                    "default": False,
                    "arg": "--preview",
                    "mode": "flag",
                },
            ]
            for item in defaults:
                key = str(item.get("key") or "")
                if key and key not in keys:
                    out.append(item)
            return out

        return out

    @staticmethod
    def _normalize_dependencies(raw: Any) -> list[dict[str, Any]]:
        if not isinstance(raw, list):
            return []
        dependencies: list[dict[str, Any]] = []
        for item in raw:
            if not isinstance(item, dict):
                continue
            if item.get("enabled") is False:
                continue
            file_name = str(item.get("file") or "").strip()
            if not file_name:
                continue
            cfg = item.get("config")
            cfg_map = dict(cfg) if isinstance(cfg, dict) else {}
            dependencies.append({"file": file_name, "config": cfg_map})
        return dependencies

    @staticmethod
    def _apply_dependency_defaults(script_name: str, dependencies: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if script_name != "apriltag-odom-follow":
            return dependencies
        if dependencies:
            return dependencies
        return [
            {
                "file": "apriltag-locations",
                "config": {
                    "width": 1280,
                    "height": 720,
                    "fps": 15.0,
                },
            },
            {"file": "wheel-odometry", "config": {"use_tune_state": False}},
        ]

    @staticmethod
    def _normalize_args_schema(raw: Any) -> list[dict[str, Any]]:
        if not isinstance(raw, list):
            return []
        result: list[dict[str, Any]] = []
        for item in raw:
            if not isinstance(item, dict):
                continue
            key = item.get("key")
            if not isinstance(key, str) or not key.strip():
                continue
            entry: dict[str, Any] = {
                "key": key.strip(),
                "label": str(item.get("label") or key.strip()),
                "type": str(item.get("type") or "string").strip().lower(),
                "default": item.get("default"),
                "arg": str(item.get("arg") or "").strip(),
            }
            if entry["type"] not in {"string", "int", "float", "bool", "enum"}:
                entry["type"] = "string"
            if isinstance(item.get("required"), bool):
                entry["required"] = bool(item.get("required"))
            if entry["type"] in {"int", "float"}:
                if item.get("min") is not None:
                    entry["min"] = item.get("min")
                if item.get("max") is not None:
                    entry["max"] = item.get("max")
                if item.get("step") is not None:
                    entry["step"] = item.get("step")
            if entry["type"] == "enum":
                options = item.get("options") if isinstance(item.get("options"), list) else []
                normalized_options = [str(option) for option in options if str(option).strip()]
                if normalized_options:
                    entry["options"] = normalized_options
            mode = item.get("mode")
            if isinstance(mode, str) and mode.strip().lower() in {"flag", "value"}:
                entry["mode"] = mode.strip().lower()
            arg_false = item.get("arg_false")
            if isinstance(arg_false, str) and arg_false.strip():
                entry["arg_false"] = arg_false.strip()
            result.append(entry)
        return result

    @staticmethod
    def _default_config_map(args_schema: list[dict[str, Any]]) -> dict[str, Any]:
        defaults: dict[str, Any] = {}
        for item in args_schema:
            key = item.get("key")
            if not isinstance(key, str) or not key:
                continue
            if "default" in item:
                defaults[key] = item.get("default")
        return defaults

    def _refresh_scripts(self, force: bool = False) -> None:
        now = time.time()
        if not force and now < self.next_scan_at:
            return
        self.next_scan_at = now + self.scan_interval_seconds

        scanned_scripts, scanned_files_payload = self._scan_scripts()
        changed = scanned_files_payload != self.available_files_payload
        self.available_scripts = scanned_scripts
        self.available_files_payload = scanned_files_payload

        if force or changed:
            self._publish_files()

    def _publish_files(self) -> None:
        payload = {
            "files": self.available_files_payload,
            "controls": True,
            "timestamp": time.time(),
        }
        self._publish_json(self.files_topic, payload, qos=1, retain=True)

    def _status_payload(self) -> dict[str, Any]:
        proc = self.process
        running = bool(proc and proc.poll() is None)
        file_name = self.running_file if running else None
        pid = proc.pid if running and proc is not None else None
        return {
            "running": running,
            "file": file_name,
            "pid": pid,
            "dependencies": [
                {
                    "file": str(entry.get("file") or ""),
                    "pid": int(entry["process"].pid),
                    "running": bool(entry["process"].poll() is None),
                }
                for entry in self.dependency_processes
                if isinstance(entry, dict) and isinstance(entry.get("process"), subprocess.Popen)
            ],
            "error": self.last_error,
            "config": self.last_config,
            "controls": True,
            "timestamp": time.time(),
        }

    def _publish_status(self) -> None:
        self._publish_json(self.status_topic, self._status_payload(), qos=1, retain=True)

    def _republish_retained_if_due(self) -> None:
        now = time.time()
        if now < self.next_retained_republish_at:
            return
        self.next_retained_republish_at = now + self.retained_publish_interval_seconds
        self._publish_files()
        self._publish_status()

    @staticmethod
    def _parse_action(payload: Any) -> tuple[Optional[str], Optional[str], dict[str, Any]]:
        if not isinstance(payload, dict):
            return None, None, {}

        value = payload.get("value") if isinstance(payload.get("value"), dict) else payload
        action_raw = value.get("action")
        action_norm = str(action_raw).strip().lower() if isinstance(action_raw, str) else ""
        action: Optional[str]
        if action_norm in {"start", "run", "play", "enable", "enabled", "on"}:
            action = "start"
        elif action_norm in {"stop", "disable", "disabled", "off"}:
            action = "stop"
        else:
            enabled = value.get("enabled")
            if not isinstance(enabled, bool):
                enabled = payload.get("enabled")
            if isinstance(enabled, bool):
                action = "start" if enabled else "stop"
            else:
                action = None

        file_raw = value.get("file")
        if not isinstance(file_raw, str):
            file_raw = payload.get("file")
        file_name = file_raw.strip() if isinstance(file_raw, str) and file_raw.strip() else None

        cfg_raw = value.get("config")
        if not isinstance(cfg_raw, dict):
            cfg_raw = payload.get("config") if isinstance(payload.get("config"), dict) else {}
        config_values = dict(cfg_raw) if isinstance(cfg_raw, dict) else {}

        return action, file_name, config_values

    @staticmethod
    def _coerce_field_value(item: dict[str, Any], raw: Any) -> Any:
        field_type = str(item.get("type") or "string")
        if field_type == "bool":
            return _parse_bool(raw, default=False)
        if field_type == "int":
            return int(raw)
        if field_type == "float":
            return float(raw)
        if field_type == "enum":
            value = str(raw)
            options = item.get("options") if isinstance(item.get("options"), list) else []
            if options and value not in options:
                raise ValueError(f"Invalid option '{value}' for {item.get('key')}")
            return value
        return str(raw)

    @staticmethod
    def _validate_numeric_bounds(item: dict[str, Any], value: Any) -> None:
        if not isinstance(value, (int, float)):
            return
        if item.get("min") is not None and value < float(item.get("min")):
            raise ValueError(f"{item.get('key')} below minimum {item.get('min')}")
        if item.get("max") is not None and value > float(item.get("max")):
            raise ValueError(f"{item.get('key')} above maximum {item.get('max')}")

    def _build_command(
        self,
        script: dict[str, Any],
        overrides: dict[str, Any],
    ) -> tuple[list[str], dict[str, Any], Optional[str], dict[str, str]]:
        args_schema = script.get("args") if isinstance(script.get("args"), list) else []
        effective: dict[str, Any] = dict(script.get("defaults") if isinstance(script.get("defaults"), dict) else {})
        argv: list[str] = []

        for item in args_schema:
            key = item.get("key")
            if not isinstance(key, str) or not key:
                continue
            has_override = key in overrides
            raw_value = overrides.get(key) if has_override else effective.get(key)
            required = bool(item.get("required", False))
            if raw_value is None:
                if required:
                    raise ValueError(f"Missing required config field '{key}'")
                continue

            coerced = self._coerce_field_value(item, raw_value)
            self._validate_numeric_bounds(item, coerced)
            effective[key] = coerced

            arg = item.get("arg")
            if not isinstance(arg, str) or not arg:
                continue
            field_type = str(item.get("type") or "string")
            if field_type == "bool":
                mode = str(item.get("mode") or "flag").lower()
                if mode == "value":
                    argv.extend([arg, "true" if coerced else "false"])
                else:
                    if bool(coerced):
                        argv.append(arg)
                    else:
                        arg_false = item.get("arg_false")
                        if isinstance(arg_false, str) and arg_false:
                            argv.append(arg_false)
            else:
                argv.extend([arg, str(coerced)])

        cmd = [sys.executable, str(script["entrypoint"]), "--config", str(self.config_path)]
        cmd.extend(argv)

        cwd = str(script["cwd"]) if isinstance(script.get("cwd"), Path) else str(script.get("cwd") or "")
        if not cwd:
            cwd = str(script.get("dir") or REPO_ROOT)

        merged_env = os.environ.copy()
        merged_env.update({str(k): str(v) for k, v in self.global_env.items()})
        script_env = script.get("env") if isinstance(script.get("env"), dict) else {}
        merged_env.update({str(k): str(v) for k, v in script_env.items()})

        return cmd, effective, cwd, merged_env

    def _start_dependency_processes(self, script: dict[str, Any]) -> None:
        dependencies = script.get("dependencies") if isinstance(script.get("dependencies"), list) else []
        started: list[dict[str, Any]] = []
        try:
            for dep in dependencies:
                if not isinstance(dep, dict):
                    continue
                dep_file = str(dep.get("file") or "").strip()
                if not dep_file:
                    continue
                dep_script = self.available_scripts.get(dep_file)
                if dep_script is None:
                    raise RuntimeError(f"Dependency script not found: {dep_file}")
                dep_overrides = dep.get("config")
                dep_overrides_map = dict(dep_overrides) if isinstance(dep_overrides, dict) else {}
                cmd, effective, cwd, merged_env = self._build_command(dep_script, dep_overrides_map)
                proc = subprocess.Popen(cmd, cwd=cwd, env=merged_env)
                started.append(
                    {
                        "file": dep_file,
                        "process": proc,
                        "config": effective,
                    }
                )
                logging.info("Started dependency %s (pid=%s)", dep_file, proc.pid)
        except Exception:
            for entry in reversed(started):
                proc = entry.get("process")
                if isinstance(proc, subprocess.Popen) and proc.poll() is None:
                    try:
                        proc.terminate()
                        proc.wait(timeout=self.stop_timeout_seconds)
                    except Exception:
                        try:
                            proc.kill()
                        except Exception:
                            pass
            raise
        self.dependency_processes = started

    def _stop_dependency_processes(self) -> bool:
        had_running = False
        for entry in reversed(self.dependency_processes):
            proc = entry.get("process")
            dep_file = str(entry.get("file") or "<dependency>")
            if not isinstance(proc, subprocess.Popen):
                continue
            running = proc.poll() is None
            had_running = had_running or running
            if running:
                proc.terminate()
                try:
                    proc.wait(timeout=self.stop_timeout_seconds)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    try:
                        proc.wait(timeout=self.stop_timeout_seconds)
                    except subprocess.TimeoutExpired:
                        logging.warning("Timed out waiting for dependency %s kill", dep_file)
        self.dependency_processes = []
        return had_running

    def _start_process(self, file_name: str, overrides: dict[str, Any]) -> None:
        script = self.available_scripts.get(file_name)
        if script is None:
            self.last_error = f"Unknown autonomy file: {file_name}"
            self._publish_status()
            return

        try:
            cmd, effective, cwd, merged_env = self._build_command(script, overrides)
        except Exception as exc:
            self.last_error = f"Invalid config for {file_name}: {exc}"
            self._publish_status()
            return

        self._stop_process(publish_status=False, clear_error=True)
        try:
            self._start_dependency_processes(script)
            self.process = subprocess.Popen(cmd, cwd=cwd, env=merged_env)
        except Exception as exc:
            self._stop_dependency_processes()
            self.process = None
            self.running_file = None
            self.last_config = None
            self.last_error = f"Failed to start {file_name}: {exc}"
            self._publish_status()
            return

        self.running_file = file_name
        self.last_config = effective
        self.last_error = None
        logging.info("Started autonomy %s (pid=%s)", file_name, self.process.pid)
        self._publish_status()

    def _stop_process(self, publish_status: bool = False, clear_error: bool = True) -> bool:
        proc = self.process
        deps_running = bool(self.dependency_processes)
        if proc is None:
            deps_running = self._stop_dependency_processes() or deps_running
            self.running_file = None
            self.last_config = None
            if clear_error:
                self.last_error = None
            if publish_status:
                self._publish_status()
            return deps_running

        running = proc.poll() is None
        if running:
            proc.terminate()
            try:
                proc.wait(timeout=self.stop_timeout_seconds)
            except subprocess.TimeoutExpired:
                proc.kill()
                try:
                    proc.wait(timeout=self.stop_timeout_seconds)
                except subprocess.TimeoutExpired:
                    logging.warning("Timed out waiting for autonomy process kill")

        deps_running = self._stop_dependency_processes() or deps_running
        self.process = None
        self.running_file = None
        self.last_config = None
        if clear_error:
            self.last_error = None
        if publish_status:
            self._publish_status()
        return running or deps_running

    def _poll_process(self) -> None:
        for entry in list(self.dependency_processes):
            proc = entry.get("process")
            dep_file = str(entry.get("file") or "<dependency>")
            if not isinstance(proc, subprocess.Popen):
                continue
            rc = proc.poll()
            if rc is None:
                continue
            if self.process is not None and self.process.poll() is None:
                self.last_error = f"dependency {dep_file} exited with code {rc}"
                logging.warning("Dependency %s exited rc=%s; stopping autonomy", dep_file, rc)
                self._stop_process(publish_status=True, clear_error=False)
                return

        proc = self.process
        if proc is None:
            return
        rc = proc.poll()
        if rc is None:
            return
        self._stop_dependency_processes()
        file_name = self.running_file or "<unknown>"
        self.process = None
        self.running_file = None
        self.last_config = None
        self.last_error = None if rc == 0 else f"{file_name} exited with code {rc}"
        logging.info("Autonomy %s exited rc=%s", file_name, rc)
        self._publish_status()

    def _handle_command(self, payload: Any) -> None:
        action, file_name, config_values = self._parse_action(payload)
        if action is None:
            logging.warning("Invalid autonomy command payload: %s", payload)
            return
        if action == "stop":
            self._stop_process(publish_status=True, clear_error=True)
            return
        if not file_name:
            self.last_error = "Start command missing file"
            self._publish_status()
            return
        self._start_process(file_name, config_values)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Manage autonomy scripts via local MQTT.")
    parser.add_argument("--config", help="Path to runtime config JSON.")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    config, config_path = load_config(args.config)
    svc_cfg = service_cfg(config, "autonomy_manager")
    logging.basicConfig(
        level=getattr(logging, log_level(config, svc_cfg), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    manager = AutonomyManager(config, config_path)

    def _shutdown(_signum: int, _frame: Any) -> None:
        manager.stop()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    try:
        manager.start()
    finally:
        manager.stop()


if __name__ == "__main__":
    main()
