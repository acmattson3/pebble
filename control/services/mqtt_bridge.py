#!/usr/bin/env python3
"""Bridge local MQTT and remote MQTT for Pebble robots."""

from __future__ import annotations

import argparse
import json
import logging
import os
import shlex
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

from control.common.config import enabled_service_instances, load_config, log_level, service_cfg
from control.common.mqtt import create_client, mqtt_auth_and_tls, parse_bool_payload
from control.common.topics import identity_from_config


class MqttBridge:
    def __init__(self, config: dict[str, Any], config_path: Path) -> None:
        self.config = config
        self.config_path = config_path
        self.identity = identity_from_config(config)
        self.service_cfg = service_cfg(config, "mqtt_bridge")
        if not self.service_cfg.get("enabled", True):
            raise SystemExit("mqtt_bridge is disabled in config.")

        self.local_mqtt_cfg = config.get("local_mqtt") if isinstance(config.get("local_mqtt"), dict) else {}
        self.local_host = str(self.local_mqtt_cfg.get("host") or "127.0.0.1")
        self.local_port = int(self.local_mqtt_cfg.get("port") or 1883)
        self.local_keepalive = int(self.local_mqtt_cfg.get("keepalive") or 60)

        remote_cfg = self.service_cfg.get("remote_mqtt") if isinstance(self.service_cfg.get("remote_mqtt"), dict) else {}
        self.remote_mqtt_cfg = remote_cfg
        self.remote_host = str(remote_cfg.get("host") or "").strip()
        if not self.remote_host:
            raise SystemExit("services.mqtt_bridge.remote_mqtt.host is required")
        self.remote_port = int(remote_cfg.get("port") or 1883)
        self.remote_keepalive = int(remote_cfg.get("keepalive") or 60)
        self.same_broker_mode = self._brokers_match()

        self.base = self.identity.base
        self.incoming_prefix = f"{self.base}/incoming/"
        self.outgoing_prefix = f"{self.base}/outgoing/"
        self.flags_prefix = f"{self.base}/incoming/flags/"
        ignored_outgoing_topics: set[str] = set()
        for _instance_name, instance_cfg in enabled_service_instances(config, "serial_mcu_bridge"):
            protocol_name = str(instance_cfg.get("protocol") or "goob_base_v1").strip().lower()
            if protocol_name != "imu_mpu6050_v1":
                continue
            instance_topics_cfg = instance_cfg.get("topics") if isinstance(instance_cfg.get("topics"), dict) else {}
            ignored_outgoing_topics.add(
                str(instance_topics_cfg.get("high_rate") or f"{self.base}/outgoing/sensors/imu-fast")
            )
        self.ignored_outgoing_topics = ignored_outgoing_topics

        topics_cfg = self.service_cfg.get("topics") if isinstance(self.service_cfg.get("topics"), dict) else {}
        self.remote_mirror_topic = str(topics_cfg.get("remote_mirror") or f"{self.flags_prefix}remote-mirror")
        self.mqtt_audio_flag_topic = str(topics_cfg.get("mqtt_audio") or f"{self.flags_prefix}mqtt-audio")
        self.mqtt_video_flag_topic = str(topics_cfg.get("mqtt_video") or f"{self.flags_prefix}mqtt-video")
        self.reboot_flag_topic = str(topics_cfg.get("reboot") or f"{self.flags_prefix}reboot")
        self.service_restart_flag_topic = str(
            topics_cfg.get("service_restart") or f"{self.flags_prefix}service-restart"
        )
        self.git_pull_flag_topic = str(topics_cfg.get("git_pull") or f"{self.flags_prefix}git-pull")
        self.audio_control_topic = str(topics_cfg.get("audio_control") or f"{self.base}/incoming/audio")
        self.video_control_topic = str(topics_cfg.get("video_control") or f"{self.base}/incoming/front-camera")

        heartbeat_cfg = self.service_cfg.get("heartbeat") if isinstance(self.service_cfg.get("heartbeat"), dict) else {}
        self.heartbeat_enabled = bool(heartbeat_cfg.get("enabled", True))
        self.heartbeat_topic = str(heartbeat_cfg.get("topic") or f"{self.base}/outgoing/online")
        self.heartbeat_interval = max(0.05, float(heartbeat_cfg.get("interval_seconds") or 0.25))
        self.heartbeat_qos = int(heartbeat_cfg.get("qos") or 1)
        self.heartbeat_retain = bool(heartbeat_cfg.get("retain", True))
        self.next_heartbeat_at = 0.0

        self.video_control_cfg = self.service_cfg.get("video_control") if isinstance(self.service_cfg.get("video_control"), dict) else {}
        self.audio_control_cfg = self.service_cfg.get("audio_control") if isinstance(self.service_cfg.get("audio_control"), dict) else {}
        self.reboot_control_cfg = self.service_cfg.get("reboot_control") if isinstance(self.service_cfg.get("reboot_control"), dict) else {}
        self.reboot_cooldown_seconds = max(0.0, float(self.reboot_control_cfg.get("cooldown_seconds") or 30.0))
        self.reboot_ignore_retained = bool(self.reboot_control_cfg.get("ignore_retained", True))
        self.service_restart_control_cfg = (
            self.service_cfg.get("service_restart_control")
            if isinstance(self.service_cfg.get("service_restart_control"), dict)
            else {}
        )
        self.service_restart_cooldown_seconds = max(
            0.0,
            float(self.service_restart_control_cfg.get("cooldown_seconds") or 10.0),
        )
        self.service_restart_ignore_retained = bool(self.service_restart_control_cfg.get("ignore_retained", True))
        self.git_pull_control_cfg = self.service_cfg.get("git_pull_control") if isinstance(self.service_cfg.get("git_pull_control"), dict) else {}
        self.git_pull_cooldown_seconds = max(0.0, float(self.git_pull_control_cfg.get("cooldown_seconds") or 60.0))
        self.git_pull_ignore_retained = bool(self.git_pull_control_cfg.get("ignore_retained", True))
        media_cfg = self.service_cfg.get("media") if isinstance(self.service_cfg.get("media"), dict) else {}
        audio_rx_cfg = media_cfg.get("audio_receiver") if isinstance(media_cfg.get("audio_receiver"), dict) else {}
        self.audio_stream_topic = str(audio_rx_cfg.get("topic") or f"{self.base}/incoming/audio-stream")

        self.stop_event = threading.Event()
        self.state_lock = threading.Lock()
        self.media_lock = threading.Lock()
        self.reconnect_lock = threading.Lock()

        self.local_client: Optional[mqtt.Client] = None
        self.remote_client: Optional[mqtt.Client] = None
        self.reconnect_threads: dict[str, threading.Thread] = {}

        self.remote_mirror_enabled = False
        self.mqtt_audio_enabled = False
        self.mqtt_video_enabled = False
        self.mqtt_audio_requested = False
        self.mqtt_video_requested = False
        self.last_reboot_request_at = 0.0
        self.last_service_restart_request_at = 0.0
        self.last_git_pull_request_at = 0.0
        self.mirror_subscription: Optional[str] = None

        self.video_process: Optional[subprocess.Popen] = None
        self.git_pull_process: Optional[subprocess.Popen] = None
        self.audio_processes: dict[str, Optional[subprocess.Popen]] = {"publisher": None, "receiver": None}

        self.dynamic_local_only_topics_by_device: dict[str, set[str]] = {}
        self.dynamic_ignored_outgoing_topics: set[str] = set()
        self.recent_remote_to_local: dict[str, tuple[bytes, float]] = {}
        self.retained_local_outgoing: dict[str, tuple[bytes, int, bool]] = {}

    def _brokers_match(self) -> bool:
        if self.local_host != self.remote_host:
            return False
        if self.local_port != self.remote_port:
            return False
        if str(self.local_mqtt_cfg.get("username") or "") != str(self.remote_mqtt_cfg.get("username") or ""):
            return False
        local_password = self.local_mqtt_cfg.get("password")
        remote_password = self.remote_mqtt_cfg.get("password")
        if ("" if local_password is None else str(local_password)) != ("" if remote_password is None else str(remote_password)):
            return False
        local_tls = self.local_mqtt_cfg.get("tls") if isinstance(self.local_mqtt_cfg.get("tls"), dict) else {}
        remote_tls = self.remote_mqtt_cfg.get("tls") if isinstance(self.remote_mqtt_cfg.get("tls"), dict) else {}
        return local_tls == remote_tls

    def start(self) -> None:
        self._connect_local()
        self._connect_remote()
        if self.same_broker_mode:
            logging.info("mqtt_bridge same-broker mode enabled; mirroring disabled, heartbeat/media controls remain active.")
        while not self.stop_event.is_set():
            self._publish_heartbeat_if_due()
            self.stop_event.wait(0.05)

    def stop(self) -> None:
        self.stop_event.set()
        self._stop_video_process()
        self._stop_audio_stack()
        self._disconnect_client(self.local_client)
        self._disconnect_client(self.remote_client)
        self.local_client = None
        self.remote_client = None

    @staticmethod
    def _disconnect_client(client: Optional[mqtt.Client]) -> None:
        if not client:
            return
        try:
            client.loop_stop()
            client.disconnect()
        except Exception:
            logging.debug("MQTT disconnect failed", exc_info=True)

    def _connect_local(self) -> None:
        client = create_client(client_id=f"{self.base}-local-bridge")
        mqtt_auth_and_tls(client, self.local_mqtt_cfg, self.config_path.parent)
        client.on_connect = self._on_local_connect
        client.on_disconnect = self._on_local_disconnect
        client.on_message = self._on_local_message
        self.local_client = client

        delay = 2.0
        while not self.stop_event.is_set():
            try:
                client.connect(self.local_host, self.local_port, self.local_keepalive)
                client.loop_start()
                return
            except OSError as exc:
                logging.warning("Local MQTT connect failed: %s", exc)
                self.stop_event.wait(delay)
                delay = min(delay * 2, 60.0)

    def _connect_remote(self) -> None:
        client = create_client(client_id=f"{self.base}-remote-bridge")
        mqtt_auth_and_tls(client, self.remote_mqtt_cfg, self.config_path.parent)
        client.on_connect = self._on_remote_connect
        client.on_disconnect = self._on_remote_disconnect
        client.on_message = self._on_remote_message
        self.remote_client = client

        delay = 2.0
        while not self.stop_event.is_set():
            try:
                client.connect(self.remote_host, self.remote_port, self.remote_keepalive)
                client.loop_start()
                return
            except OSError as exc:
                logging.warning("Remote MQTT connect failed: %s", exc)
                self.stop_event.wait(delay)
                delay = min(delay * 2, 60.0)

    def _on_local_connect(self, client: mqtt.Client, _userdata: Any, _flags: dict[str, Any], rc: int) -> None:
        if rc != 0:
            logging.error("Local MQTT connect failed rc=%s", rc)
            return
        logging.info("Local MQTT connected; subscribing bridge topics.")
        client.subscribe(self.audio_control_topic, qos=1)
        client.subscribe(self.video_control_topic, qos=1)
        client.subscribe(self.reboot_flag_topic, qos=1)
        client.subscribe(self.service_restart_flag_topic, qos=1)
        client.subscribe(self.git_pull_flag_topic, qos=1)
        if self.same_broker_mode:
            return
        client.subscribe(f"{self.outgoing_prefix}#", qos=1)
        self._refresh_mirror_subscription(force=True)

    def _on_remote_connect(self, client: mqtt.Client, _userdata: Any, _flags: dict[str, Any], rc: int) -> None:
        if rc != 0:
            logging.error("Remote MQTT connect failed rc=%s", rc)
            return
        if self.same_broker_mode:
            logging.info("Remote MQTT connected in same-broker mode; skipping mirror subscriptions.")
        else:
            logging.info("Remote MQTT connected; subscribing %s#", self.incoming_prefix)
            client.subscribe(f"{self.incoming_prefix}#", qos=1)
        self.next_heartbeat_at = 0.0
        self._publish_heartbeat()
        if not self.same_broker_mode:
            self._replay_retained_outgoing_to_remote()

    def _on_local_disconnect(self, _client: mqtt.Client, _userdata: Any, rc: int) -> None:
        if rc != mqtt.MQTT_ERR_SUCCESS:
            logging.warning("Unexpected local disconnect rc=%s", rc)
            self._start_reconnect("local")

    def _on_remote_disconnect(self, _client: mqtt.Client, _userdata: Any, rc: int) -> None:
        if rc != mqtt.MQTT_ERR_SUCCESS:
            logging.warning("Unexpected remote disconnect rc=%s", rc)
            self._start_reconnect("remote")

    def _start_reconnect(self, which: str) -> None:
        if self.stop_event.is_set():
            return
        with self.reconnect_lock:
            existing = self.reconnect_threads.get(which)
            if existing and existing.is_alive():
                return
            thread = threading.Thread(target=self._reconnect_loop, args=(which,), name=f"{which}-reconnect", daemon=True)
            thread.start()
            self.reconnect_threads[which] = thread

    def _reconnect_loop(self, which: str) -> None:
        delay = 2.0
        while not self.stop_event.is_set():
            if which == "local":
                client = self.local_client
                host, port, keepalive = self.local_host, self.local_port, self.local_keepalive
            else:
                client = self.remote_client
                host, port, keepalive = self.remote_host, self.remote_port, self.remote_keepalive
            if client is None:
                return
            is_connected = getattr(client, "is_connected", None)
            if callable(is_connected) and is_connected():
                return
            try:
                client.connect(host, port, keepalive)
                return
            except OSError as exc:
                logging.warning("%s reconnect failed: %s", which, exc)
                self.stop_event.wait(delay)
                delay = min(delay * 2, 60.0)

    def _decode_json(self, payload_bytes: bytes) -> Any:
        try:
            return json.loads(payload_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return None

    def _on_local_message(self, _client: mqtt.Client, _userdata: Any, msg: mqtt.MQTTMessage) -> None:
        topic = msg.topic
        payload_bytes = bytes(msg.payload)
        parsed = self._decode_json(payload_bytes)

        self._handle_flag_topics(topic, parsed, retained=bool(msg.retain))
        self._handle_media_topics(topic, parsed)
        if self.same_broker_mode:
            return
        self._handle_standard_discovery(topic, parsed)

        if topic.startswith(self.outgoing_prefix):
            if self._is_ignored_outgoing_topic(topic):
                logging.debug("Ignoring local-only outgoing topic for remote mirror: %s", topic)
                return
            if bool(msg.retain):
                with self.state_lock:
                    self.retained_local_outgoing[topic] = (payload_bytes, int(msg.qos), True)
            self._publish_remote_raw(topic, payload_bytes, msg.qos, msg.retain)
            return

        if not topic.startswith(self.incoming_prefix):
            return

        with self.state_lock:
            remote_mirror_enabled = self.remote_mirror_enabled

        should_mirror = False
        if remote_mirror_enabled:
            should_mirror = True
        elif topic.startswith(self.flags_prefix):
            should_mirror = True

        if not should_mirror:
            return

        if self._was_forwarded_from_remote(topic, payload_bytes):
            return
        self._publish_remote_raw(topic, payload_bytes, msg.qos, msg.retain)

    def _handle_flag_topics(self, topic: str, parsed: Any, retained: bool = False) -> None:
        changed_mirror = False
        start_audio = False
        start_video = False
        request_reboot = False
        request_service_restart = False
        request_git_pull = False
        with self.state_lock:
            if topic == self.remote_mirror_topic:
                value = parse_bool_payload(parsed)
                if value is not None and value != self.remote_mirror_enabled:
                    self.remote_mirror_enabled = value
                    changed_mirror = True
            elif topic == self.mqtt_audio_flag_topic:
                value = parse_bool_payload(parsed)
                if value is not None:
                    previous = self.mqtt_audio_enabled
                    self.mqtt_audio_enabled = value
                    if previous and not value:
                        self._stop_audio_stack()
                    elif value and not previous and self.mqtt_audio_requested:
                        start_audio = True
            elif topic == self.mqtt_video_flag_topic:
                value = parse_bool_payload(parsed)
                if value is not None:
                    previous = self.mqtt_video_enabled
                    self.mqtt_video_enabled = value
                    if previous and not value:
                        self._stop_video_process()
                    elif value and not previous and self.mqtt_video_requested:
                        start_video = True
            elif topic == self.reboot_flag_topic:
                value = parse_bool_payload(parsed)
                if value is None:
                    logging.warning("reboot flag payload missing boolean: %s", parsed)
                elif value:
                    if retained and self.reboot_ignore_retained:
                        logging.warning("Ignoring retained reboot request on %s", topic)
                    else:
                        now = time.monotonic()
                        elapsed = now - self.last_reboot_request_at
                        if elapsed < self.reboot_cooldown_seconds:
                            logging.warning(
                                "Ignoring reboot request on %s during cooldown (%.1fs remaining)",
                                topic,
                                self.reboot_cooldown_seconds - elapsed,
                            )
                        else:
                            self.last_reboot_request_at = now
                            request_reboot = True
            elif topic == self.service_restart_flag_topic:
                value = parse_bool_payload(parsed)
                if value is None:
                    logging.warning("service-restart flag payload missing boolean: %s", parsed)
                elif value:
                    if retained and self.service_restart_ignore_retained:
                        logging.warning("Ignoring retained service-restart request on %s", topic)
                    else:
                        now = time.monotonic()
                        elapsed = now - self.last_service_restart_request_at
                        if elapsed < self.service_restart_cooldown_seconds:
                            logging.warning(
                                "Ignoring service-restart request on %s during cooldown (%.1fs remaining)",
                                topic,
                                self.service_restart_cooldown_seconds - elapsed,
                            )
                        else:
                            self.last_service_restart_request_at = now
                            request_service_restart = True
            elif topic == self.git_pull_flag_topic:
                value = parse_bool_payload(parsed)
                if value is None:
                    logging.warning("git-pull flag payload missing boolean: %s", parsed)
                elif value:
                    if retained and self.git_pull_ignore_retained:
                        logging.warning("Ignoring retained git-pull request on %s", topic)
                    else:
                        now = time.monotonic()
                        elapsed = now - self.last_git_pull_request_at
                        if elapsed < self.git_pull_cooldown_seconds:
                            logging.warning(
                                "Ignoring git-pull request on %s during cooldown (%.1fs remaining)",
                                topic,
                                self.git_pull_cooldown_seconds - elapsed,
                            )
                        else:
                            self.last_git_pull_request_at = now
                            request_git_pull = True

        if changed_mirror:
            self._refresh_mirror_subscription(force=False)
        if start_audio:
            self._start_audio_stack()
        if start_video:
            self._start_video_process()
        if request_reboot:
            self._start_reboot_command()
        if request_service_restart:
            self._start_service_restart_command()
        if request_git_pull:
            self._start_git_pull_command()

    def _handle_media_topics(self, topic: str, parsed: Any) -> None:
        if topic == self.audio_control_topic:
            desired = parse_bool_payload(parsed)
            if desired is None:
                logging.warning("audio control payload missing boolean: %s", parsed)
                return
            with self.state_lock:
                self.mqtt_audio_requested = desired
                enabled = self.mqtt_audio_enabled
            if desired:
                if enabled:
                    self._start_audio_stack()
            else:
                self._stop_audio_stack()
            return

        if topic == self.video_control_topic:
            desired = parse_bool_payload(parsed)
            if desired is None:
                logging.warning("front-camera control payload missing boolean: %s", parsed)
                return
            with self.state_lock:
                self.mqtt_video_requested = desired
                enabled = self.mqtt_video_enabled
            if desired:
                if enabled:
                    self._start_video_process()
            else:
                self._stop_video_process()

    def _desired_mirror_subscription(self) -> str:
        with self.state_lock:
            mirror_enabled = self.remote_mirror_enabled
        return f"{self.incoming_prefix}#" if mirror_enabled else f"{self.flags_prefix}#"

    def _refresh_mirror_subscription(self, force: bool) -> None:
        if self.same_broker_mode:
            return
        client = self.local_client
        if client is None:
            return
        desired = self._desired_mirror_subscription()
        with self.state_lock:
            current = self.mirror_subscription
        if not force and desired == current:
            return
        if current and current != desired:
            client.unsubscribe(current)
        client.subscribe(desired, qos=1)
        with self.state_lock:
            self.mirror_subscription = desired

    def _on_remote_message(self, _client: mqtt.Client, _userdata: Any, msg: mqtt.MQTTMessage) -> None:
        if self.same_broker_mode:
            return
        topic = msg.topic
        if not topic.startswith(self.incoming_prefix):
            return
        if topic == self.audio_stream_topic:
            return
        payload = bytes(msg.payload)
        self._mark_remote_forward(topic, payload)
        self._publish_local_raw(topic, payload, msg.qos, msg.retain)

    def _mark_remote_forward(self, topic: str, payload: bytes) -> None:
        expires = time.monotonic() + 8.0
        with self.state_lock:
            self.recent_remote_to_local[topic] = (payload, expires)

    def _was_forwarded_from_remote(self, topic: str, payload: bytes) -> bool:
        now = time.monotonic()
        with self.state_lock:
            stale = [name for name, (_, exp) in self.recent_remote_to_local.items() if exp < now]
            for name in stale:
                self.recent_remote_to_local.pop(name, None)
            record = self.recent_remote_to_local.get(topic)
            if not record:
                return False
            data, exp = record
            if exp < now:
                self.recent_remote_to_local.pop(topic, None)
                return False
            if data == payload:
                self.recent_remote_to_local.pop(topic, None)
                return True
            return False

    def _publish_local_raw(self, topic: str, payload: bytes, qos: int, retain: bool) -> None:
        client = self.local_client
        if not client:
            return
        is_connected = getattr(client, "is_connected", None)
        if callable(is_connected) and not is_connected():
            return
        client.publish(topic, payload, qos=qos, retain=retain)

    def _publish_remote_raw(self, topic: str, payload: bytes, qos: int, retain: bool) -> None:
        client = self.remote_client
        if not client:
            return
        is_connected = getattr(client, "is_connected", None)
        if callable(is_connected) and not is_connected():
            return
        client.publish(topic, payload, qos=qos, retain=retain)

    def _is_ignored_outgoing_topic(self, topic: str) -> bool:
        with self.state_lock:
            return topic in self.ignored_outgoing_topics or topic in self.dynamic_ignored_outgoing_topics

    def _handle_standard_discovery(self, topic: str, parsed: Any) -> None:
        if not topic.startswith(f"{self.outgoing_prefix}mcu/") or not topic.endswith("/describe"):
            return
        if not isinstance(parsed, dict):
            return
        interfaces = parsed.get("interfaces")
        if not isinstance(interfaces, list):
            return
        protocol_name = str(parsed.get("protocol") or "").strip().lower()
        if protocol_name and protocol_name != "pebble_serial_v1":
            return

        suffix = topic[len(self.outgoing_prefix) :]
        parts = suffix.split("/")
        if len(parts) < 3 or parts[0] != "mcu":
            return
        device_uid = str(parsed.get("device_uid") or parts[1]).strip().strip("/") or parts[1]

        local_only_topics: set[str] = set()
        for interface in interfaces:
            if not isinstance(interface, dict):
                continue
            if not bool(interface.get("local_only", False)):
                continue
            name = str(interface.get("name") or "").strip().strip("/")
            if name:
                local_only_topics.add(self.identity.topic("outgoing", f"mcu/{device_uid}/{name}"))
            channel = str(interface.get("channel") or "").strip().strip("/")
            if channel:
                local_only_topics.add(self.identity.topic("outgoing", channel))

        with self.state_lock:
            self.dynamic_local_only_topics_by_device[device_uid] = local_only_topics
            merged: set[str] = set()
            for topics in self.dynamic_local_only_topics_by_device.values():
                merged.update(topics)
            self.dynamic_ignored_outgoing_topics = merged
            for ignored_topic in local_only_topics:
                self.retained_local_outgoing.pop(ignored_topic, None)

    def _replay_retained_outgoing_to_remote(self) -> None:
        with self.state_lock:
            items = list(self.retained_local_outgoing.items())
        if not items:
            return
        logging.info("Replaying %d retained outgoing topics to remote broker", len(items))
        for topic, (payload, qos, retain) in items:
            if self._is_ignored_outgoing_topic(topic):
                continue
            self._publish_remote_raw(topic, payload, qos=qos, retain=retain)

    @staticmethod
    def _normalize_command(command: Any) -> list[str]:
        if isinstance(command, str):
            parts = shlex.split(command)
        elif isinstance(command, list):
            parts = [str(item) for item in command if item is not None]
        else:
            raise ValueError("command must be string or list")
        if not parts:
            raise ValueError("command cannot be empty")
        return parts

    def _resolve_cwd(self, cwd_value: Any) -> Optional[str]:
        if not cwd_value:
            return None
        cwd = Path(str(cwd_value)).expanduser()
        if not cwd.is_absolute():
            cwd = (self.config_path.parent / cwd).resolve()
        return str(cwd)

    def _reboot_command(self) -> Any:
        return self.reboot_control_cfg.get("command")

    def _reboot_env(self) -> dict[str, str]:
        env = os.environ.copy()
        env_overrides = self.reboot_control_cfg.get("env")
        if isinstance(env_overrides, dict):
            env.update({str(k): str(v) for k, v in env_overrides.items()})
        return env

    def _start_reboot_command(self) -> None:
        command = self._reboot_command()
        if not command:
            logging.warning("reboot flag received but services.mqtt_bridge.reboot_control.command is missing")
            return
        try:
            proc = subprocess.Popen(
                self._normalize_command(command),
                cwd=self._resolve_cwd(self.reboot_control_cfg.get("cwd")),
                env=self._reboot_env(),
            )
            logging.warning("Issued reboot command pid=%s", proc.pid)
        except (OSError, ValueError) as exc:
            logging.error("Failed to execute reboot command: %s", exc)

    def _service_restart_command(self) -> Any:
        return self.service_restart_control_cfg.get("command")

    def _service_restart_env(self) -> dict[str, str]:
        env = os.environ.copy()
        env_overrides = self.service_restart_control_cfg.get("env")
        if isinstance(env_overrides, dict):
            env.update({str(k): str(v) for k, v in env_overrides.items()})
        return env

    def _start_service_restart_command(self) -> None:
        command = self._service_restart_command()
        if not command:
            logging.warning(
                "service-restart flag received but services.mqtt_bridge.service_restart_control.command is missing"
            )
            return
        try:
            proc = subprocess.Popen(
                self._normalize_command(command),
                cwd=self._resolve_cwd(self.service_restart_control_cfg.get("cwd")),
                env=self._service_restart_env(),
            )
            logging.warning("Issued service restart command pid=%s", proc.pid)
        except (OSError, ValueError) as exc:
            logging.error("Failed to execute service restart command: %s", exc)

    def _git_pull_command(self) -> Any:
        return self.git_pull_control_cfg.get("command")

    def _git_pull_env(self) -> dict[str, str]:
        env = os.environ.copy()
        env_overrides = self.git_pull_control_cfg.get("env")
        if isinstance(env_overrides, dict):
            env.update({str(k): str(v) for k, v in env_overrides.items()})
        return env

    def _start_git_pull_command(self) -> None:
        command = self._git_pull_command()
        if not command:
            logging.warning("git-pull flag received but services.mqtt_bridge.git_pull_control.command is missing")
            return
        existing = self.git_pull_process
        if existing and existing.poll() is None:
            logging.warning("Ignoring git-pull request while previous git pull command is still running")
            return
        try:
            proc = subprocess.Popen(
                self._normalize_command(command),
                cwd=self._resolve_cwd(self.git_pull_control_cfg.get("cwd")),
                env=self._git_pull_env(),
            )
            self.git_pull_process = proc
            logging.warning("Issued git pull command pid=%s", proc.pid)
        except (OSError, ValueError) as exc:
            logging.error("Failed to execute git pull command: %s", exc)

    def _start_video_process(self) -> None:
        command = self.video_control_cfg.get("command")
        if not command:
            logging.warning("mqtt_video enabled but services.mqtt_bridge.video_control.command is missing")
            return
        with self.media_lock:
            if self.video_process and self.video_process.poll() is None:
                return
            self.video_process = None
            env = os.environ.copy()
            env_overrides = self.video_control_cfg.get("env")
            if isinstance(env_overrides, dict):
                env.update({str(k): str(v) for k, v in env_overrides.items()})
            try:
                self.video_process = subprocess.Popen(
                    self._normalize_command(command),
                    cwd=self._resolve_cwd(self.video_control_cfg.get("cwd")),
                    env=env,
                )
                logging.info("Started MQTT video process pid=%s", self.video_process.pid)
            except OSError as exc:
                logging.error("Failed to start MQTT video process: %s", exc)
                self.video_process = None

    def _stop_video_process(self) -> None:
        with self.media_lock:
            proc = self.video_process
            if not proc:
                return
            timeout = float(self.video_control_cfg.get("stop_timeout") or 10.0)
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=timeout)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=timeout)
            self.video_process = None

    def _audio_command(self, role: str) -> Any:
        if role == "publisher":
            return self.audio_control_cfg.get("publisher_command") or self.audio_control_cfg.get("command")
        if role == "receiver":
            return self.audio_control_cfg.get("receiver_command")
        return None

    def _audio_cwd(self, role: str) -> Any:
        if role == "publisher":
            return self.audio_control_cfg.get("publisher_cwd") or self.audio_control_cfg.get("cwd")
        if role == "receiver":
            return self.audio_control_cfg.get("receiver_cwd") or self.audio_control_cfg.get("cwd")
        return None

    def _audio_env(self, role: str) -> dict[str, str]:
        env = os.environ.copy()
        global_env = self.audio_control_cfg.get("env")
        if isinstance(global_env, dict):
            env.update({str(k): str(v) for k, v in global_env.items()})
        role_env = self.audio_control_cfg.get(f"{role}_env")
        if isinstance(role_env, dict):
            env.update({str(k): str(v) for k, v in role_env.items()})
        return env

    def _start_audio_stack(self) -> None:
        with self.media_lock:
            started_any = False
            for role, label in (("publisher", "MQTT audio publisher"), ("receiver", "MQTT audio receiver")):
                command = self._audio_command(role)
                if not command:
                    continue
                started_any = True
                proc = self.audio_processes.get(role)
                if proc and proc.poll() is None:
                    continue
                self.audio_processes[role] = None
                try:
                    proc = subprocess.Popen(
                        self._normalize_command(command),
                        cwd=self._resolve_cwd(self._audio_cwd(role)),
                        env=self._audio_env(role),
                    )
                except OSError as exc:
                    logging.error("Failed to start %s: %s", label, exc)
                    self.audio_processes[role] = None
                    continue
                self.audio_processes[role] = proc
                logging.info("Started %s pid=%s", label, proc.pid)
            if not started_any:
                logging.warning("mqtt-audio enabled but no publisher/receiver commands configured")

    def _stop_audio_stack(self) -> None:
        timeout = float(self.audio_control_cfg.get("stop_timeout") or 10.0)
        with self.media_lock:
            for role, label in (("receiver", "MQTT audio receiver"), ("publisher", "MQTT audio publisher")):
                proc = self.audio_processes.get(role)
                if not proc:
                    continue
                if proc.poll() is None:
                    proc.terminate()
                    try:
                        proc.wait(timeout=timeout)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        proc.wait(timeout=timeout)
                self.audio_processes[role] = None
                logging.info("Stopped %s", label)

    @staticmethod
    def _heartbeat_payload() -> bytes:
        return json.dumps({"t": int(time.time() * 1000)}).encode("utf-8")

    def _publish_heartbeat(self) -> None:
        if not self.heartbeat_enabled:
            return
        self._publish_remote_raw(
            self.heartbeat_topic,
            self._heartbeat_payload(),
            qos=self.heartbeat_qos,
            retain=self.heartbeat_retain,
        )

    def _publish_heartbeat_if_due(self) -> None:
        if not self.heartbeat_enabled:
            return
        now = time.monotonic()
        if now < self.next_heartbeat_at:
            return
        self.next_heartbeat_at = now + self.heartbeat_interval
        self._publish_heartbeat()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bridge local and remote MQTT brokers.")
    parser.add_argument("--config", help="Path to runtime config JSON.")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    config, config_path = load_config(args.config)
    svc_cfg = service_cfg(config, "mqtt_bridge")
    logging.basicConfig(
        level=getattr(logging, log_level(config, svc_cfg), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    bridge = MqttBridge(config, config_path)

    def _shutdown(_signum: int, _frame: Any) -> None:
        bridge.stop()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    try:
        bridge.start()
    finally:
        bridge.stop()


if __name__ == "__main__":
    main()
