import argparse
import logging
import signal
import subprocess
import sys
import threading
import time
import ssl
from pathlib import Path
from typing import Any, Dict, List, Optional

import paho.mqtt.client as mqtt

from camera_config import CameraProfileError, load_profile


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


def _resolve_tls_paths(tls_cfg: Dict[str, Any]) -> Dict[str, Any]:
    def _resolve(path_value: Any) -> Optional[str]:
        if not path_value:
            return None
        raw = str(path_value).strip()
        if not raw:
            return None
        path = Path(raw)
        if not path.is_absolute():
            path = (Path(__file__).parent / path).resolve()
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


def _apply_tls(client: mqtt.Client, mqtt_cfg: Dict[str, Any]) -> None:
    tls_cfg = _normalize_tls_config(mqtt_cfg.get("tls"))
    if not tls_cfg.get("enabled"):
        return
    tls_cfg = _resolve_tls_paths(tls_cfg)
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


class BitrateMonitor:
    """Subscribe to the robot's video topic and report bitrate in kbps."""

    def __init__(self, profile_name: str, config_path: Optional[str], report_interval: float):
        if report_interval <= 0:
            raise ValueError("report_interval must be positive.")
        self.profile_name = profile_name
        self.config_path = config_path
        self.report_interval = report_interval
        self._client: Optional[mqtt.Client] = None
        self._topic: Optional[str] = None
        self._stop_event = threading.Event()
        self._lock = threading.Lock()
        self._byte_count = 0
        self._frame_count = 0
        self._last_report = time.time()

    def start(self) -> None:
        self._connect()
        logging.info("Connected; waiting for frames to measure bitrate.")

        try:
            while not self._stop_event.is_set():
                time.sleep(0.2)
                self._maybe_report()
        except KeyboardInterrupt:
            logging.info("Exit requested by user.")
        finally:
            self.close()

    def close(self) -> None:
        self._stop_event.set()
        if self._client:
            self._client.loop_stop()
            try:
                self._client.disconnect()
            except Exception:
                logging.debug("MQTT disconnect failed", exc_info=True)
            self._client = None

    def _connect(self) -> None:
        try:
            profile = load_profile(self.profile_name, self.config_path)
        except CameraProfileError as exc:
            raise SystemExit(str(exc)) from exc

        mqtt_cfg = profile["mqtt"]
        brokers = _resolve_brokers(mqtt_cfg.get("broker"), mqtt_cfg.get("network_override"))
        if not brokers:
            raise SystemExit(
                "No MQTT broker configured; set mqtt.broker or mqtt.network_override.away_host."
            )

        port = int(mqtt_cfg.get("port", 1883))
        keepalive = int(mqtt_cfg.get("keepalive", 60))
        username = mqtt_cfg.get("username")
        password = mqtt_cfg.get("password")
        reconnect_seconds = int(mqtt_cfg.get("reconnect_seconds", 10))
        self._topic = mqtt_cfg["topic"]

        client = mqtt.Client()
        if username:
            client.username_pw_set(username, password)
        _apply_tls(client, mqtt_cfg)
        client.on_connect = self._on_connect
        client.on_message = self._on_message
        client.on_disconnect = self._on_disconnect
        client.reconnect_delay_set(min_delay=1, max_delay=reconnect_seconds)

        last_error: Optional[Exception] = None
        for broker in brokers:
            logging.info("Connecting to MQTT broker %s:%s ...", broker, port)
            try:
                client.connect(broker, port, keepalive)
                client.loop_start()
                self._client = client
                logging.info("MQTT connected to %s:%s", broker, port)
                return
            except (ConnectionRefusedError, OSError) as exc:
                last_error = exc
                logging.warning("MQTT connection to %s:%s failed: %s", broker, port, exc)

        raise SystemExit(f"Failed to connect to any MQTT broker: {last_error}")

    def _on_connect(self, client: mqtt.Client, _userdata: Any, _flags: Dict[str, Any], rc: int) -> None:
        if rc != 0:
            logging.error("MQTT connection failed with result code %s", rc)
            return
        if not self._topic:
            logging.error("No topic configured; cannot subscribe.")
            return
        logging.info("MQTT connected; subscribing to %s", self._topic)
        client.subscribe(self._topic)

    def _on_disconnect(self, _client: mqtt.Client, _userdata: Any, rc: int) -> None:
        if rc != 0:
            logging.warning("Unexpected MQTT disconnect (rc=%s); attempting to reconnect.", rc)

    def _on_message(self, _client: mqtt.Client, _userdata: Any, msg: mqtt.MQTTMessage) -> None:
        with self._lock:
            self._byte_count += len(msg.payload)
            self._frame_count += 1

    def _maybe_report(self) -> None:
        now = time.time()
        elapsed = now - self._last_report
        if elapsed < self.report_interval:
            return

        with self._lock:
            bytes_seen = self._byte_count
            frames_seen = self._frame_count
            self._byte_count = 0
            self._frame_count = 0

        if elapsed <= 0:
            return

        bitrate_kbps = (bytes_seen * 8) / 1000.0 / elapsed
        logging.info(
            "Bitrate %.2f kbps over %.2fs (%d frames)",
            bitrate_kbps,
            elapsed,
            frames_seen,
        )
        self._last_report = now


def _resolve_brokers(
    default_broker: Optional[str], override_cfg: Optional[Dict[str, Any]]
) -> List[str]:
    brokers: List[str] = []

    def _append(candidate: Optional[str]) -> None:
        broker = str(candidate or "").strip()
        if broker and broker not in brokers:
            brokers.append(broker)

    if not override_cfg:
        _append(default_broker)
        return brokers

    local_ssid = str(override_cfg.get("ssid") or "").strip()
    local_host = str(override_cfg.get("local_host") or "").strip()
    away_host = str(override_cfg.get("away_host") or default_broker or "").strip()

    if not local_ssid or not local_host or not away_host:
        logging.warning("Incomplete mqtt.network_override configuration; ignoring override.")
        _append(default_broker or away_host or None)
        return brokers

    ssid = _current_wifi_ssid()
    if ssid and ssid == local_ssid:
        logging.info("Detected SSID '%s'. Using local MQTT broker %s.", ssid, local_host)
        _append(local_host)
        _append(away_host)
        return brokers

    if not ssid:
        logging.info(
            "Unable to determine Wi-Fi SSID; trying away broker %s with local fallback %s (expected '%s').",
            away_host,
            local_host,
            local_ssid,
        )
        _append(away_host)
        _append(local_host)
        return brokers

    logging.info(
        "Connected to SSID '%s' (expected '%s'); using away MQTT broker %s.",
        ssid,
        local_ssid,
        away_host,
    )
    _append(away_host)
    return brokers


def _current_wifi_ssid() -> Optional[str]:
    commands = (
        ["iwgetid", "-r"],
        ["nmcli", "-t", "-f", "active,ssid", "dev", "wifi"],
    )
    for cmd in commands:
        try:
            result = subprocess.run(
                cmd,
                check=True,
                capture_output=True,
                text=True,
                timeout=3,
            )
        except (OSError, subprocess.SubprocessError):
            continue

        output = result.stdout.strip()
        if not output:
            continue

        if cmd[0] == "nmcli":
            for line in output.splitlines():
                if line.startswith("yes:"):
                    ssid = line.split(":", 1)[1].strip()
                    if ssid:
                        return ssid
        else:
            return output
    return None


def _parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Subscribe to an MQTT video stream and report bitrate."
    )
    parser.add_argument(
        "--profile",
        default="default",
        help="Camera profile to monitor (default: default).",
    )
    parser.add_argument("--config", help="Path to unified runtime config JSON.")
    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="Reporting interval in seconds (default: 1).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (default: INFO).",
    )
    return parser.parse_args(argv)


def _configure_logging(level: str) -> None:
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )


def main(argv: Optional[list] = None) -> None:
    args = _parse_args(argv)
    _configure_logging(args.log_level)

    monitor = BitrateMonitor(args.profile, args.config, args.interval)

    def _handle_signal(_signum: int, _frame: Any) -> None:
        logging.info("Signal received; shutting down.")
        monitor.close()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _handle_signal)
    monitor.start()


if __name__ == "__main__":
    main(sys.argv[1:])
