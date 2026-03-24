#!/usr/bin/env python3
"""Launch and supervise Pebble control services."""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

import paho.mqtt.client as mqtt

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from control.common.capabilities import CAPABILITIES_SCHEMA, build_capabilities_value, capabilities_topic
from control.common.config import default_retained_publish_interval_seconds, load_config, log_level, service_cfg
from control.common.mqtt import mqtt_auth_and_tls
from control.common.topics import identity_from_config


SERVICE_MODULES = {
    "av_daemon": "control.services.av_daemon",
    "mqtt_bridge": "control.services.mqtt_bridge",
    "serial_mcu_bridge": "control.services.serial_mcu_bridge",
    "soundboard_handler": "control.services.soundboard_handler",
    "autonomy_manager": "control.services.autonomy_manager",
}

CHILD_LOG_PATTERN = re.compile(r"^\d{2}:\d{2}:\d{2}\s+\[(DEBUG|INFO|WARNING|ERROR|CRITICAL)\]\s*(.*)$")


class LauncherLogsPublisher:
    def __init__(self, config: dict[str, Any], config_path: Path) -> None:
        self.config = config
        self.config_path = config_path
        launcher_cfg = service_cfg(config, "launcher")
        logs_cfg = launcher_cfg.get("logs") if isinstance(launcher_cfg.get("logs"), dict) else {}

        self.identity = identity_from_config(config)
        self.enabled = bool(logs_cfg.get("enabled", True))
        self.topic = str(logs_cfg.get("topic") or self.identity.topic("outgoing", "logs"))
        self.qos = int(logs_cfg.get("qos") or 0)
        self.retain = bool(logs_cfg.get("retain", False))
        self.max_message_chars = max(64, int(logs_cfg.get("max_message_chars") or 2048))
        level_name = str(logs_cfg.get("min_level") or "INFO").strip().upper()
        self.min_level = getattr(logging, level_name, logging.INFO)

        local_cfg = config.get("local_mqtt") if isinstance(config.get("local_mqtt"), dict) else {}
        self.local_cfg = local_cfg
        self.local_host = str(local_cfg.get("host") or "127.0.0.1")
        self.local_port = int(local_cfg.get("port") or 1883)
        self.local_keepalive = int(local_cfg.get("keepalive") or 60)

        self.client: mqtt.Client | None = None

    def start(self) -> None:
        if not self.enabled:
            return
        client = mqtt.Client(client_id=f"{self.identity.base}-launcher-logs")
        mqtt_auth_and_tls(client, self.local_cfg, self.config_path.parent)
        client.reconnect_delay_set(min_delay=1, max_delay=30)
        self.client = client
        client.connect_async(self.local_host, self.local_port, self.local_keepalive)
        client.loop_start()

    def stop(self) -> None:
        client = self.client
        self.client = None
        if client is None:
            return
        try:
            client.loop_stop()
            client.disconnect()
        except Exception:
            logging.debug("Failed to stop launcher logs publisher", exc_info=True)

    def publish_record(self, record: logging.LogRecord) -> None:
        if not self.enabled:
            return
        if record.levelno < self.min_level:
            return
        client = self.client
        if client is None:
            return
        is_connected = getattr(client, "is_connected", None)
        if callable(is_connected) and not is_connected():
            return

        message = record.getMessage()
        if len(message) > self.max_message_chars:
            message = message[: self.max_message_chars - 3] + "..."
        service = str(getattr(record, "pb_service", "launcher") or "launcher")

        payload: dict[str, Any] = {
            "t": int(record.created * 1000),
            "level": str(record.levelname or "INFO").upper(),
            "service": service,
            "message": message,
            "logger": record.name,
        }

        pid_raw = getattr(record, "pb_pid", None)
        try:
            pid = int(pid_raw if pid_raw is not None else os.getpid())
        except (TypeError, ValueError):
            pid = os.getpid()
        payload["pid"] = pid

        client.publish(self.topic, json.dumps(payload), qos=self.qos, retain=self.retain)


class LauncherLogMqttHandler(logging.Handler):
    def __init__(self, publisher: LauncherLogsPublisher) -> None:
        super().__init__(level=logging.NOTSET)
        self.publisher = publisher

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self.publisher.publish_record(record)
        except Exception:
            self.handleError(record)


class LauncherCapabilitiesPublisher:
    def __init__(self, config: dict[str, Any], config_path: Path) -> None:
        self.config = config
        self.config_path = config_path
        launcher_cfg = service_cfg(config, "launcher")
        caps_cfg = launcher_cfg.get("capabilities") if isinstance(launcher_cfg.get("capabilities"), dict) else {}
        self.enabled = bool(caps_cfg.get("enabled", True))
        self.qos = int(caps_cfg.get("qos") or 1)
        self.retain = bool(caps_cfg.get("retain", True))
        default_retain_interval = default_retained_publish_interval_seconds(config, default=3600.0)
        interval_raw = caps_cfg.get("publish_interval_seconds")
        if interval_raw is None:
            interval_raw = caps_cfg.get("interval_seconds")
        try:
            interval_value = float(interval_raw if interval_raw is not None else default_retain_interval)
        except (TypeError, ValueError):
            interval_value = default_retain_interval
        self.publish_interval_seconds = interval_value if interval_value > 0 else default_retain_interval

        self.identity = identity_from_config(config)
        self.topic = capabilities_topic(self.identity, launcher_cfg)
        self.capabilities_value = build_capabilities_value(config)

        local_cfg = config.get("local_mqtt") if isinstance(config.get("local_mqtt"), dict) else {}
        self.local_cfg = local_cfg
        self.local_host = str(local_cfg.get("host") or "127.0.0.1")
        self.local_port = int(local_cfg.get("port") or 1883)
        self.local_keepalive = int(local_cfg.get("keepalive") or 60)

        self.client: mqtt.Client | None = None
        self._stop_event = threading.Event()
        self._periodic_thread: threading.Thread | None = None

    def start(self) -> None:
        if not self.enabled:
            return
        client = mqtt.Client(client_id=f"{self.identity.base}-launcher-caps")
        mqtt_auth_and_tls(client, self.local_cfg, self.config_path.parent)
        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.reconnect_delay_set(min_delay=1, max_delay=30)
        self.client = client
        client.connect_async(self.local_host, self.local_port, self.local_keepalive)
        client.loop_start()
        if self.publish_interval_seconds > 0:
            self._periodic_thread = threading.Thread(
                target=self._periodic_publish_loop,
                name="launcher-capabilities-publisher",
                daemon=True,
            )
            self._periodic_thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        periodic_thread = self._periodic_thread
        self._periodic_thread = None
        if periodic_thread is not None:
            periodic_thread.join(timeout=2.0)
        client = self.client
        self.client = None
        if client is None:
            return
        try:
            client.loop_stop()
            client.disconnect()
        except Exception:
            logging.debug("Failed to stop launcher capabilities publisher", exc_info=True)

    def _publish(self) -> None:
        client = self.client
        if client is None:
            return
        payload = {
            "schema": CAPABILITIES_SCHEMA,
            "t": int(time.time() * 1000),
            "value": self.capabilities_value,
        }
        client.publish(self.topic, json.dumps(payload), qos=self.qos, retain=self.retain)
        logging.info("Published capabilities topic %s", self.topic)

    def _periodic_publish_loop(self) -> None:
        interval = self.publish_interval_seconds
        if interval <= 0:
            return
        while not self._stop_event.wait(interval):
            client = self.client
            if client is None:
                continue
            if not client.is_connected():
                continue
            self._publish()

    def _on_connect(self, _client: mqtt.Client, _userdata: Any, _flags: dict[str, Any], rc: int) -> None:
        if rc != 0:
            logging.warning("Launcher capabilities MQTT connect failed rc=%s", rc)
            return
        self._publish()

    def _on_disconnect(self, _client: mqtt.Client, _userdata: Any, rc: int) -> None:
        if self._stop_event.is_set():
            return
        if rc != mqtt.MQTT_ERR_SUCCESS:
            logging.warning("Launcher capabilities MQTT disconnected rc=%s", rc)


class Launcher:
    def __init__(self, config: dict[str, Any], config_path: Path) -> None:
        self.config = config
        self.config_path = config_path
        self.launcher_cfg = service_cfg(config, "launcher")
        self.restart_delay = float(self.launcher_cfg.get("restart_delay_seconds") or 2.0)
        self.shutdown_timeout = float(self.launcher_cfg.get("shutdown_timeout_seconds") or 8.0)

        self.stop_requested = False
        self.children: dict[str, subprocess.Popen] = {}
        self.child_log_threads: dict[str, threading.Thread] = {}
        self.child_threads_lock = threading.Lock()
        self.next_restart: dict[str, float] = {}
        self.logs_publisher = LauncherLogsPublisher(config, config_path)
        self.logs_handler: LauncherLogMqttHandler | None = None
        self.capabilities_publisher = LauncherCapabilitiesPublisher(config, config_path)

    def _service_enabled(self, name: str) -> bool:
        return bool(service_cfg(self.config, name).get("enabled", False))

    def _attach_logs_handler(self) -> None:
        if self.logs_handler is not None:
            return
        handler = LauncherLogMqttHandler(self.logs_publisher)
        logging.getLogger().addHandler(handler)
        self.logs_handler = handler

    def _detach_logs_handler(self) -> None:
        handler = self.logs_handler
        self.logs_handler = None
        if handler is None:
            return
        root = logging.getLogger()
        try:
            root.removeHandler(handler)
        except ValueError:
            pass
        handler.close()

    @staticmethod
    def _parse_child_line(line: str) -> tuple[int, str]:
        match = CHILD_LOG_PATTERN.match(line)
        if not match:
            return logging.INFO, line
        level_name, message = match.groups()
        level = getattr(logging, level_name, logging.INFO)
        text = message.strip() or line
        return level, text

    def _pump_child_output(self, name: str, proc: subprocess.Popen) -> None:
        stream = proc.stdout
        if stream is None:
            return
        try:
            for raw in stream:
                line = raw.rstrip("\r\n")
                if not line:
                    continue
                level, message = self._parse_child_line(line)
                logging.log(
                    level,
                    "[%s] %s",
                    name,
                    message,
                    extra={"pb_service": name, "pb_pid": proc.pid},
                )
        finally:
            try:
                stream.close()
            except Exception:
                pass
            with self.child_threads_lock:
                current = self.child_log_threads.get(name)
                if current is threading.current_thread():
                    self.child_log_threads.pop(name, None)

    def _start_child(self, name: str) -> None:
        module = SERVICE_MODULES[name]
        cmd = [sys.executable, "-m", module, "--config", str(self.config_path)]
        env = os.environ.copy()
        env.setdefault("PYTHONUNBUFFERED", "1")
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
            env=env,
        )
        self.children[name] = proc
        self.next_restart.pop(name, None)
        logging.info("Started %s (pid=%s)", name, proc.pid)
        thread = threading.Thread(
            target=self._pump_child_output,
            args=(name, proc),
            name=f"{name}-log-pump",
            daemon=True,
        )
        with self.child_threads_lock:
            self.child_log_threads[name] = thread
        thread.start()

    def _stop_child(self, name: str, proc: subprocess.Popen) -> None:
        if proc.poll() is not None:
            return
        proc.terminate()

    def start(self) -> None:
        enabled = [name for name in SERVICE_MODULES if self._service_enabled(name)]
        if not enabled:
            raise SystemExit("No enabled services in config.services")
        self.logs_publisher.start()
        self._attach_logs_handler()
        for name in enabled:
            self._start_child(name)
        self.capabilities_publisher.start()

        while not self.stop_requested:
            now = time.monotonic()
            for name in enabled:
                proc = self.children.get(name)
                if proc is None:
                    if now >= self.next_restart.get(name, 0.0):
                        self._start_child(name)
                    continue
                rc = proc.poll()
                if rc is None:
                    continue
                logging.warning("%s exited rc=%s; scheduling restart", name, rc)
                self.children.pop(name, None)
                self.next_restart[name] = now + self.restart_delay
            time.sleep(0.2)

    def stop(self) -> None:
        self.stop_requested = True
        self.capabilities_publisher.stop()
        for name, proc in list(self.children.items()):
            self._stop_child(name, proc)

        deadline = time.monotonic() + self.shutdown_timeout
        while time.monotonic() < deadline:
            if all(proc.poll() is not None for proc in self.children.values()):
                break
            time.sleep(0.1)

        for name, proc in list(self.children.items()):
            if proc.poll() is None:
                logging.warning("Force-killing %s (pid=%s)", name, proc.pid)
                proc.kill()
            self.children.pop(name, None)

        with self.child_threads_lock:
            threads = list(self.child_log_threads.values())
        for thread in threads:
            thread.join(timeout=1.0)

        self._detach_logs_handler()
        self.logs_publisher.stop()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Launch Pebble control services.")
    parser.add_argument("--config", help="Path to runtime config JSON.")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    config, config_path = load_config(args.config)
    launcher_cfg = service_cfg(config, "launcher")
    logging.basicConfig(
        level=getattr(logging, log_level(config, launcher_cfg), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    launcher = Launcher(config, config_path)

    def _shutdown(_signum: int, _frame: Any) -> None:
        launcher.stop()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    try:
        launcher.start()
    finally:
        launcher.stop()


if __name__ == "__main__":
    main()
