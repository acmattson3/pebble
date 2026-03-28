#!/usr/bin/env python3
"""On-robot smoke tests for the Pebble stack.

This script is intended to run on the robot itself. It can start the launcher,
connect to local/remote MQTT, and verify core runtime behavior:

- launcher stays up (no immediate pipeline crash loops)
- local retained status topics appear (soundboard, serial telemetry where enabled)
- remote heartbeat is visible (when remote broker checks are enabled)
- remote-mirror flag behavior for incoming topic forwarding
"""

from __future__ import annotations

import argparse
import json
import queue
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional

import paho.mqtt.client as mqtt

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from control.common.config import enabled_service_instances, load_config, service_cfg
from control.common.capabilities import capabilities_topic
from control.common.mqtt import mqtt_auth_and_tls
from control.common.topics import identity_from_config


DEFAULT_CONFIG = Path(__file__).resolve().parents[1] / "control" / "configs" / "config.json"


@dataclass
class Event:
    topic: str
    payload: bytes
    qos: int
    retain: bool
    received_at: float


class MQTTWatcher:
    def __init__(self, name: str, mqtt_cfg: dict[str, Any], config_dir: Path) -> None:
        self.name = name
        self.mqtt_cfg = mqtt_cfg
        self.config_dir = config_dir

        self.host = str(mqtt_cfg.get("host") or "127.0.0.1")
        self.port = int(mqtt_cfg.get("port") or 1883)
        self.keepalive = int(mqtt_cfg.get("keepalive") or 60)

        self.client = mqtt.Client(client_id=f"pb-smoke-{name}-{int(time.time() * 1000)}")
        mqtt_auth_and_tls(self.client, mqtt_cfg, config_dir)

        self.connected = threading.Event()
        self.disconnected = threading.Event()
        self.subscriptions: list[str] = []

        self._events_lock = threading.Lock()
        self._events: list[Event] = []
        self._event_q: "queue.Queue[Event]" = queue.Queue()

        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message

    def _on_connect(self, client: mqtt.Client, _userdata: Any, _flags: dict[str, Any], rc: int) -> None:
        if rc != 0:
            return
        for topic in self.subscriptions:
            client.subscribe(topic, qos=1)
        self.connected.set()

    def _on_disconnect(self, _client: mqtt.Client, _userdata: Any, _rc: int) -> None:
        self.disconnected.set()
        self.connected.clear()

    def _on_message(self, _client: mqtt.Client, _userdata: Any, msg: mqtt.MQTTMessage) -> None:
        evt = Event(
            topic=msg.topic,
            payload=bytes(msg.payload),
            qos=int(msg.qos),
            retain=bool(msg.retain),
            received_at=time.time(),
        )
        with self._events_lock:
            self._events.append(evt)
        self._event_q.put(evt)

    def connect(self, subscriptions: list[str], timeout: float) -> None:
        self.subscriptions = subscriptions
        self.client.connect(self.host, self.port, self.keepalive)
        self.client.loop_start()
        if not self.connected.wait(timeout=timeout):
            raise RuntimeError(f"{self.name}: MQTT connect timeout to {self.host}:{self.port}")

    def disconnect(self) -> None:
        try:
            self.client.loop_stop()
            self.client.disconnect()
        except Exception:
            pass

    def publish(self, topic: str, payload: Any, qos: int = 1, retain: bool = False) -> None:
        if isinstance(payload, (bytes, bytearray, memoryview)):
            body = bytes(payload)
        elif isinstance(payload, str):
            body = payload.encode("utf-8")
        else:
            body = json.dumps(payload).encode("utf-8")
        self.client.publish(topic, body, qos=qos, retain=retain)

    def wait_for(self, predicate: Callable[[Event], bool], timeout: float, label: str) -> Event:
        deadline = time.monotonic() + timeout
        seen = 0
        while time.monotonic() < deadline:
            with self._events_lock:
                events = self._events[seen:]
                seen = len(self._events)
            for evt in events:
                if predicate(evt):
                    return evt
            try:
                evt = self._event_q.get(timeout=min(0.2, max(0.01, deadline - time.monotonic())))
            except queue.Empty:
                continue
            if predicate(evt):
                return evt
        raise TimeoutError(f"Timed out waiting for {label}")


class LauncherRunner:
    def __init__(self, config_path: Path, repo_root: Path) -> None:
        self.config_path = config_path
        self.repo_root = repo_root
        self.proc: Optional[subprocess.Popen[str]] = None
        self._lines_lock = threading.Lock()
        self._lines: list[str] = []
        self._reader: Optional[threading.Thread] = None

    def start(self) -> None:
        cmd = [sys.executable, "control/launcher.py", "--config", str(self.config_path)]
        self.proc = subprocess.Popen(
            cmd,
            cwd=str(self.repo_root),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        self._reader = threading.Thread(target=self._read_output, name="launcher-log-reader", daemon=True)
        self._reader.start()

    def _read_output(self) -> None:
        proc = self.proc
        if proc is None or proc.stdout is None:
            return
        for line in proc.stdout:
            text = line.rstrip("\n")
            with self._lines_lock:
                self._lines.append(text)
            print(text)

    def stop(self) -> None:
        proc = self.proc
        self.proc = None
        if proc is None:
            return
        if proc.poll() is None:
            proc.send_signal(signal.SIGINT)
            try:
                proc.wait(timeout=10.0)
            except subprocess.TimeoutExpired:
                proc.terminate()
                try:
                    proc.wait(timeout=5.0)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=5.0)

    def running(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    def lines(self) -> list[str]:
        with self._lines_lock:
            return list(self._lines)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run on-robot smoke tests for Pebble.")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG), help="Path to runtime config JSON.")
    parser.add_argument("--startup-seconds", type=float, default=8.0, help="Seconds to wait after launcher start.")
    parser.add_argument("--timeout", type=float, default=12.0, help="Per-check timeout in seconds.")
    parser.add_argument(
        "--no-launch",
        action="store_true",
        help="Do not start launcher; assume stack already running.",
    )
    parser.add_argument(
        "--skip-remote",
        action="store_true",
        help="Skip remote broker checks (heartbeat + mirror behavior).",
    )
    return parser.parse_args()


def _json_payload(event: Event) -> Any:
    try:
        return json.loads(event.payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None


def _main() -> int:
    args = _parse_args()
    config, config_path = load_config(args.config)
    config_dir = config_path.parent
    repo_root = Path(__file__).resolve().parents[1]

    identity = identity_from_config(config)
    base = identity.base

    launcher_cfg = service_cfg(config, "launcher")
    launcher_caps_cfg = launcher_cfg.get("capabilities") if isinstance(launcher_cfg.get("capabilities"), dict) else {}
    launcher_caps_enabled = bool(launcher_caps_cfg.get("enabled", True))
    launcher_caps_topic = capabilities_topic(identity, launcher_cfg)
    soundboard_cfg = service_cfg(config, "soundboard_handler")
    autonomy_cfg = service_cfg(config, "autonomy_manager")
    serial_cfg = service_cfg(config, "serial_mcu_bridge")
    serial_instances = enabled_service_instances(config, "serial_mcu_bridge")
    mqtt_bridge_cfg = service_cfg(config, "mqtt_bridge")
    heartbeat_cfg = mqtt_bridge_cfg.get("heartbeat") if isinstance(mqtt_bridge_cfg.get("heartbeat"), dict) else {}

    launcher = LauncherRunner(config_path=config_path, repo_root=repo_root)
    local_cfg = config.get("local_mqtt") if isinstance(config.get("local_mqtt"), dict) else {}
    remote_cfg = mqtt_bridge_cfg.get("remote_mqtt") if isinstance(mqtt_bridge_cfg.get("remote_mqtt"), dict) else {}

    checks: list[tuple[str, str, Optional[str]]] = []

    def record(name: str, status: str, detail: Optional[str] = None) -> None:
        checks.append((name, status, detail))
        detail_text = f": {detail}" if detail else ""
        print(f"[{status}] {name}{detail_text}")

    local = MQTTWatcher("local", local_cfg, config_dir)
    remote: Optional[MQTTWatcher] = None
    run_remote = bool(mqtt_bridge_cfg.get("enabled", True)) and not args.skip_remote and bool(remote_cfg.get("host"))
    if run_remote:
        remote = MQTTWatcher("remote", remote_cfg, config_dir)

    try:
        if not args.no_launch:
            launcher.start()
            time.sleep(max(0.5, args.startup_seconds))
            if not launcher.running():
                raise RuntimeError("launcher exited during startup window")

            bad_patterns = ("erroneous pipeline:", "pipeline exited rc=")
            lines = launcher.lines()
            for pattern in bad_patterns:
                if any(pattern in line for line in lines):
                    raise RuntimeError(f"launcher logs contain failure pattern: {pattern}")
            record("launcher startup", "PASS")
        else:
            record("launcher startup", "SKIP", "no-launch requested")

        local.connect([f"{base}/#"], timeout=args.timeout)
        record("local mqtt connect", "PASS", f"{local.host}:{local.port}")

        if launcher_caps_enabled:
            local.wait_for(
                lambda e: e.topic == launcher_caps_topic and isinstance(_json_payload(e), dict),
                timeout=args.timeout,
                label="local capabilities topic",
            )
            record("local capabilities", "PASS")
        else:
            record("local capabilities", "SKIP", "launcher capabilities disabled")

        if bool(soundboard_cfg.get("enabled", True)):
            local.wait_for(
                lambda e: e.topic == f"{base}/outgoing/soundboard-files" and _json_payload(e) is not None,
                timeout=args.timeout,
                label="soundboard-files",
            )
            local.wait_for(
                lambda e: e.topic == f"{base}/outgoing/soundboard-status" and _json_payload(e) is not None,
                timeout=args.timeout,
                label="soundboard-status",
            )
            record("soundboard retained topics", "PASS")
        else:
            record("soundboard retained topics", "SKIP", "soundboard_handler disabled")

        if bool(autonomy_cfg.get("enabled", True)):
            local.wait_for(
                lambda e: e.topic == f"{base}/outgoing/autonomy-files" and _json_payload(e) is not None,
                timeout=args.timeout,
                label="autonomy-files",
            )
            local.wait_for(
                lambda e: e.topic == f"{base}/outgoing/autonomy-status" and _json_payload(e) is not None,
                timeout=args.timeout,
                label="autonomy-status",
            )
            record("autonomy retained topics", "PASS")
        else:
            record("autonomy retained topics", "SKIP", "autonomy_manager disabled")

        if bool(serial_cfg.get("enabled", True)) or bool(serial_instances):
            local.wait_for(
                lambda e: e.topic in {
                    f"{base}/outgoing/touch-sensors",
                    f"{base}/outgoing/charging-status",
                },
                timeout=args.timeout,
                label="serial telemetry topic",
            )
            record("serial telemetry", "PASS")
        else:
            record("serial telemetry", "SKIP", "serial_mcu_bridge disabled")

        if remote is None:
            record("remote broker checks", "SKIP", "remote disabled or not configured")
        else:
            remote.connect([f"{base}/#"], timeout=args.timeout)
            record("remote mqtt connect", "PASS", f"{remote.host}:{remote.port}")

            heartbeat_topic = str(heartbeat_cfg.get("topic") or f"{base}/outgoing/online")
            if bool(heartbeat_cfg.get("enabled", True)):
                remote.wait_for(
                    lambda e: e.topic == heartbeat_topic and isinstance(_json_payload(e), dict),
                    timeout=args.timeout,
                    label="remote heartbeat",
                )
                record("remote heartbeat", "PASS")
            else:
                record("remote heartbeat", "SKIP", "heartbeat disabled")

            if launcher_caps_enabled:
                remote.wait_for(
                    lambda e: e.topic == launcher_caps_topic and isinstance(_json_payload(e), dict),
                    timeout=args.timeout,
                    label="remote capabilities",
                )
                record("remote capabilities", "PASS")
            else:
                record("remote capabilities", "SKIP", "launcher capabilities disabled")

            if bool(autonomy_cfg.get("enabled", True)):
                remote.wait_for(
                    lambda e: e.topic == f"{base}/outgoing/autonomy-files" and _json_payload(e) is not None,
                    timeout=args.timeout,
                    label="remote autonomy-files",
                )
                remote.wait_for(
                    lambda e: e.topic == f"{base}/outgoing/autonomy-status" and _json_payload(e) is not None,
                    timeout=args.timeout,
                    label="remote autonomy-status",
                )
                record("remote autonomy retained topics", "PASS")
            else:
                record("remote autonomy retained topics", "SKIP", "autonomy_manager disabled")

            mirror_flag_topic = f"{base}/incoming/flags/remote-mirror"
            probe_topic = f"{base}/incoming/diagnostics-confirm"

            local.publish(mirror_flag_topic, {"value": False}, qos=1, retain=True)
            remote.wait_for(
                lambda e: e.topic == mirror_flag_topic and isinstance(_json_payload(e), dict),
                timeout=args.timeout,
                label="remote-mirror=false flag mirror",
            )

            nonce_false = f"smoke-false-{int(time.time() * 1000)}"
            local.publish(probe_topic, {"value": nonce_false}, qos=1, retain=False)
            try:
                remote.wait_for(
                    lambda e: e.topic == probe_topic and _json_payload(e) == {"value": nonce_false},
                    timeout=2.0,
                    label="probe with mirror=false",
                )
                raise RuntimeError("non-flag incoming probe unexpectedly mirrored while remote-mirror=false")
            except TimeoutError:
                pass

            local.publish(mirror_flag_topic, {"value": True}, qos=1, retain=True)
            remote.wait_for(
                lambda e: e.topic == mirror_flag_topic and _json_payload(e) == {"value": True},
                timeout=args.timeout,
                label="remote-mirror=true flag mirror",
            )

            nonce_true = f"smoke-true-{int(time.time() * 1000)}"
            local.publish(probe_topic, {"value": nonce_true}, qos=1, retain=False)
            remote.wait_for(
                lambda e: e.topic == probe_topic and _json_payload(e) == {"value": nonce_true},
                timeout=args.timeout,
                label="probe with mirror=true",
            )
            record("remote mirror behavior", "PASS")

            # Cleanup to preserve expected default behavior.
            local.publish(mirror_flag_topic, {"value": False}, qos=1, retain=True)

    except Exception as exc:
        record("smoke test run", "FAIL", str(exc))
    finally:
        if remote is not None:
            remote.disconnect()
        local.disconnect()
        if not args.no_launch:
            launcher.stop()

    failures = [item for item in checks if item[1] == "FAIL"]
    print("\nSmoke Test Summary")
    for name, status, detail in checks:
        detail_text = f" - {detail}" if detail else ""
        print(f"- {status}: {name}{detail_text}")

    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(_main())
