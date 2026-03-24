#!/usr/bin/env python3
"""Handle soundboard playback commands over local MQTT."""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import shlex
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, Optional, Tuple

import paho.mqtt.client as mqtt

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parents[2]))

from control.common.config import default_retained_publish_interval_seconds, load_config, log_level, service_cfg
from control.common.mqtt import mqtt_auth_and_tls
from control.common.topics import identity_from_config


def _parse_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


class SoundboardHandler:
    def __init__(self, config: dict[str, Any], config_path: Path) -> None:
        self.config = config
        self.config_path = config_path
        self.identity = identity_from_config(config)
        self.service_cfg = service_cfg(config, "soundboard_handler")
        if not self.service_cfg.get("enabled", True):
            raise SystemExit("soundboard_handler is disabled in config.")

        topics = self.service_cfg.get("topics") if isinstance(self.service_cfg.get("topics"), dict) else {}
        self.command_topic = str(topics.get("command") or self.identity.topic("incoming", "soundboard-command"))
        self.files_topic = str(topics.get("files") or self.identity.topic("outgoing", "soundboard-files"))
        self.status_topic = str(topics.get("status") or self.identity.topic("outgoing", "soundboard-status"))

        playback = self.service_cfg.get("playback") if isinstance(self.service_cfg.get("playback"), dict) else {}
        directory_raw = str(playback.get("directory") or "~/sounds")
        directory = Path(directory_raw).expanduser()
        if not directory.is_absolute():
            directory = (config_path.parent / directory).resolve()
        self.soundboard_dir = directory
        self.player_command = playback.get("player_command") or ["aplay"]
        self.playback_cwd = playback.get("cwd")
        self.stop_timeout = max(0.5, _parse_float(playback.get("stop_timeout"), 5.0))
        self.scan_interval = max(1.0, _parse_float(playback.get("scan_interval"), 10.0))
        self.playback_env = playback.get("env") if isinstance(playback.get("env"), dict) else {}
        default_retain_interval = default_retained_publish_interval_seconds(config, default=3600.0)
        interval_raw = self.service_cfg.get("retained_publish_interval_seconds")
        try:
            interval_value = float(interval_raw if interval_raw is not None else default_retain_interval)
        except (TypeError, ValueError):
            interval_value = default_retain_interval
        self.retained_publish_interval_seconds = interval_value if interval_value > 0 else default_retain_interval

        self.local_mqtt_cfg = config.get("local_mqtt") if isinstance(config.get("local_mqtt"), dict) else {}
        self.local_host = str(self.local_mqtt_cfg.get("host") or "127.0.0.1")
        self.local_port = int(self.local_mqtt_cfg.get("port") or 1883)
        self.local_keepalive = int(self.local_mqtt_cfg.get("keepalive") or 60)

        self.stop_event = threading.Event()
        self.reconnect_lock = threading.Lock()
        self.soundboard_lock = threading.Lock()

        self.client: Optional[mqtt.Client] = None
        self.reconnect_thread: Optional[threading.Thread] = None
        self.process: Optional[subprocess.Popen] = None
        self.current_file: Optional[str] = None
        self.last_error: Optional[str] = None
        self.last_files: list[str] = []
        self.next_scan_at = 0.0
        self.next_retained_republish_at = 0.0

    def start(self) -> None:
        self._connect_local_mqtt()
        while not self.stop_event.is_set():
            self._poll_process()
            self._refresh_files()
            self._republish_retained_if_due()
            self.stop_event.wait(0.05)

    def stop(self) -> None:
        self.stop_event.set()
        self._stop_process(publish_status=True, clear_error=False)
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
        self._refresh_files(force=True)
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
            thread = threading.Thread(target=self._reconnect_loop, name="soundboard-reconnect", daemon=True)
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
            logging.warning("Non-JSON payload for soundboard command")
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

    def _scan_files(self) -> list[str]:
        if not self.soundboard_dir.exists() or not self.soundboard_dir.is_dir():
            return []
        files: list[str] = []
        for path in self.soundboard_dir.rglob("*"):
            if not path.is_file() or path.suffix.lower() != ".wav":
                continue
            files.append(path.relative_to(self.soundboard_dir).as_posix())
        return sorted(files, key=lambda value: value.lower())

    def _refresh_files(self, force: bool = False) -> None:
        now = time.time()
        if not force and now < self.next_scan_at:
            return
        self.next_scan_at = now + self.scan_interval
        scanned = self._scan_files()
        should_publish = force
        with self.soundboard_lock:
            if scanned != self.last_files:
                self.last_files = scanned
                should_publish = True
            current_files = list(self.last_files)
        if should_publish:
            self._publish_json(
                self.files_topic,
                {"files": current_files, "controls": True, "timestamp": now},
                qos=1,
                retain=True,
            )

    def _status_payload(self) -> dict[str, Any]:
        with self.soundboard_lock:
            proc = self.process
            playing = bool(proc and proc.poll() is None)
            current_file = self.current_file if playing else None
            error = self.last_error
        return {
            "playing": playing,
            "file": current_file,
            "error": error,
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
        self._refresh_files(force=True)
        self._publish_status()

    def _resolve_file(self, file_name: str) -> Optional[Tuple[str, Path]]:
        clean = str(file_name or "").strip().replace("\\", "/")
        if not clean:
            return None
        requested = Path(clean).expanduser()
        candidate = requested.resolve() if requested.is_absolute() else (self.soundboard_dir / requested).resolve()
        try:
            candidate.relative_to(self.soundboard_dir)
        except ValueError:
            return None
        if not candidate.exists() or not candidate.is_file() or candidate.suffix.lower() != ".wav":
            return None
        return candidate.relative_to(self.soundboard_dir).as_posix(), candidate

    @staticmethod
    def _parse_action(payload: Any) -> Tuple[Optional[str], Optional[str]]:
        if isinstance(payload, bool):
            return ("play" if payload else "stop"), None
        if not isinstance(payload, dict):
            return None, None
        value = payload.get("value")
        if isinstance(value, bool):
            return ("play" if value else "stop"), None
        command_payload = value if isinstance(value, dict) else payload

        action_raw = command_payload.get("action")
        action_norm = str(action_raw).strip().lower() if isinstance(action_raw, str) else None
        if action_norm in {"play", "start", "on", "enable", "enabled"}:
            action = "play"
        elif action_norm in {"stop", "off", "disable", "disabled"}:
            action = "stop"
        else:
            enabled = command_payload.get("enabled")
            if not isinstance(enabled, bool):
                enabled = payload.get("enabled")
            action = "play" if enabled is True else "stop" if enabled is False else None

        file_raw = command_payload.get("file")
        if not isinstance(file_raw, str):
            file_raw = payload.get("file")
        file_name = file_raw.strip() if isinstance(file_raw, str) and file_raw.strip() else None
        return action, file_name

    def _resolve_cwd(self) -> Optional[str]:
        if not self.playback_cwd:
            return None
        cwd = Path(str(self.playback_cwd)).expanduser()
        if not cwd.is_absolute():
            cwd = (self.config_path.parent / cwd).resolve()
        return str(cwd)

    @staticmethod
    def _normalize_command(command: Any) -> list[str]:
        if isinstance(command, str):
            return shlex.split(command)
        if isinstance(command, list):
            parts = [str(part) for part in command if part is not None]
            if parts:
                return parts
        return ["aplay"]

    def _build_command(self, sound_path: Path) -> list[str]:
        cmd = self._normalize_command(self.player_command)
        if any("{file}" in part for part in cmd):
            return [part.replace("{file}", str(sound_path)) for part in cmd]
        return cmd + [str(sound_path)]

    def _start_process(self, relative_file: str, sound_path: Path) -> None:
        with self.soundboard_lock:
            self._stop_process_locked()
            merged_env = os.environ.copy()
            merged_env.update({str(k): str(v) for k, v in self.playback_env.items()})
            try:
                cmd = self._build_command(sound_path)
                self.process = subprocess.Popen(cmd, cwd=self._resolve_cwd(), env=merged_env)
            except OSError as exc:
                self.process = None
                self.current_file = None
                self.last_error = f"Failed to start playback: {exc}"
                logging.error(self.last_error)
            else:
                self.current_file = relative_file
                self.last_error = None
                logging.info("Started soundboard playback %s (pid=%s)", relative_file, self.process.pid)
        self._publish_status()

    def _stop_process_locked(self) -> bool:
        proc = self.process
        if not proc:
            return False
        running = proc.poll() is None
        if running:
            proc.terminate()
            try:
                proc.wait(timeout=self.stop_timeout)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=self.stop_timeout)
        self.process = None
        self.current_file = None
        return running

    def _stop_process(self, publish_status: bool = False, clear_error: bool = True) -> bool:
        with self.soundboard_lock:
            running = self._stop_process_locked()
            if clear_error:
                self.last_error = None
        if publish_status:
            self._publish_status()
        return running

    def _poll_process(self) -> None:
        should_publish = False
        with self.soundboard_lock:
            proc = self.process
            if proc is None:
                return
            rc = proc.poll()
            if rc is None:
                return
            self.process = None
            self.current_file = None
            self.last_error = None if rc == 0 else f"player exited with code {rc}"
            should_publish = True
        if should_publish:
            self._publish_status()

    def _handle_command(self, payload: Any) -> None:
        action, requested_file = self._parse_action(payload)
        if action is None:
            logging.warning("Invalid soundboard command payload: %s", payload)
            return
        if action == "stop":
            self._stop_process(publish_status=True, clear_error=True)
            return
        if not requested_file:
            with self.soundboard_lock:
                self.last_error = "Play command missing file"
            self._publish_status()
            return
        resolved = self._resolve_file(requested_file)
        if not resolved:
            with self.soundboard_lock:
                self.last_error = f"Sound file not found: {requested_file}"
            self._publish_status()
            return
        relative_file, sound_path = resolved
        self._start_process(relative_file, sound_path)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Handle local MQTT soundboard commands.")
    parser.add_argument("--config", help="Path to runtime config JSON.")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    config, config_path = load_config(args.config)
    svc_cfg = service_cfg(config, "soundboard_handler")
    logging.basicConfig(
        level=getattr(logging, log_level(config, svc_cfg), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    handler = SoundboardHandler(config, config_path)

    def _shutdown(_signum: int, _frame: Any) -> None:
        handler.stop()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    try:
        handler.start()
    finally:
        handler.stop()


if __name__ == "__main__":
    main()
