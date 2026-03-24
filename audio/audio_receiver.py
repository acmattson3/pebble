#!/usr/bin/env python3
"""
Subscribe to MQTT audio packets and play them through an ALSA device.

Payload format is binary: a fixed header followed by raw PCM16LE bytes.
"""
import argparse
import json
import queue
import signal
import ssl
import struct
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, Optional, Tuple

import paho.mqtt.client as mqtt


DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[1] / "control" / "configs" / "config.json"
if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from control.common.mqtt import create_client

MAGIC = b"PBAT"
CODEC_PCM_S16LE = 1
HEADER_STRUCT = struct.Struct("!4sBBBBHHIQ")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Play MQTT PCM audio to ALSA.")
    parser.add_argument("--config", help=f"Path to unified runtime config JSON (default: {DEFAULT_CONFIG_PATH}).")
    parser.add_argument("--robot-id", help="Robot ID used in the topic path.")
    parser.add_argument(
        "--topic",
        help="MQTT topic to subscribe to (default: pebble/robots/<id>/incoming/audio-stream).",
    )
    parser.add_argument("--host", help="MQTT broker host.")
    parser.add_argument("--port", type=int, help="MQTT broker port.")
    parser.add_argument("--username", help="MQTT username.")
    parser.add_argument("--password", help="MQTT password.")
    parser.add_argument("--keepalive", type=int, help="MQTT keepalive seconds.")
    parser.add_argument("--tls", action="store_true", help="Enable TLS for MQTT.")
    parser.add_argument("--tls-ca", help="Path to CA certificate file.")
    parser.add_argument("--tls-cert", help="Path to client certificate file.")
    parser.add_argument("--tls-key", help="Path to client private key file.")
    parser.add_argument("--tls-insecure", action="store_true", help="Allow insecure TLS certs.")
    parser.add_argument("--tls-ciphers", help="Optional TLS ciphers string.")
    parser.add_argument("--device", help="ALSA playback device name (passed to aplay -D).")
    parser.add_argument("--rate", type=int, help="Fallback sample rate (Hz).")
    parser.add_argument("--channels", type=int, help="Fallback number of audio channels.")
    parser.add_argument(
        "--queue-limit",
        type=int,
        help="Maximum buffered packets before dropping oldest to keep latency low.",
    )
    parser.add_argument(
        "--concealment-ms",
        type=int,
        help="Maximum packet-loss concealment window in milliseconds.",
    )
    parser.add_argument(
        "--qos",
        type=int,
        choices=[0, 1],
        help="MQTT QoS for audio packets (use 0 for lowest latency).",
    )
    return parser.parse_args()


def _load_config(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Failed to parse config file {path}: {exc}") from exc


def _pick_str(primary: Optional[str], secondary: Optional[str]) -> Optional[str]:
    val = (primary or "").strip() if primary is not None else ""
    if val:
        return val
    if secondary is None:
        return None
    sec = str(secondary).strip()
    return sec or None


def _pick_int(primary: Optional[int], secondary: Optional[int], default: int) -> int:
    if primary is not None:
        return int(primary)
    if secondary is not None:
        try:
            return int(secondary)
        except (TypeError, ValueError):
            pass
    return default


def _topic_from_robot(robot_cfg: dict[str, Any], direction: str, metric: str) -> Optional[str]:
    robot_id = str(robot_cfg.get("id") or "").strip()
    if not robot_id:
        return None
    system = str(robot_cfg.get("system") or "pebble")
    component_type = str(robot_cfg.get("type") or "robots")
    return f"{system}/{component_type}/{robot_id}/{direction}/{metric}"


def _runtime_audio_config(cfg: dict[str, Any]) -> Optional[dict[str, Any]]:
    if not isinstance(cfg.get("services"), dict):
        return None

    robot_cfg = cfg.get("robot") if isinstance(cfg.get("robot"), dict) else {}
    services = cfg.get("services") if isinstance(cfg.get("services"), dict) else {}
    bridge_cfg = services.get("mqtt_bridge") if isinstance(services.get("mqtt_bridge"), dict) else {}
    remote_cfg = bridge_cfg.get("remote_mqtt") if isinstance(bridge_cfg.get("remote_mqtt"), dict) else {}
    media_cfg = bridge_cfg.get("media") if isinstance(bridge_cfg.get("media"), dict) else {}
    audio_rx_cfg = media_cfg.get("audio_receiver") if isinstance(media_cfg.get("audio_receiver"), dict) else {}

    robot_id = str(robot_cfg.get("id") or "").strip()
    if not robot_id:
        return None

    return {
        "robot_id": robot_id,
        "host": str(remote_cfg.get("host") or ""),
        "port": int(remote_cfg.get("port") or 1883),
        "keepalive": int(remote_cfg.get("keepalive") or 60),
        "username": str(remote_cfg.get("username") or ""),
        "password": str(remote_cfg.get("password") or ""),
        "tls": remote_cfg.get("tls") if isinstance(remote_cfg.get("tls"), dict) else {"enabled": False},
        "receiver": {
            "topic": str(audio_rx_cfg.get("topic") or _topic_from_robot(robot_cfg, "incoming", "audio-stream") or ""),
            "device": str(audio_rx_cfg.get("device") or "default"),
            "rate": int(audio_rx_cfg.get("rate") or 16000),
            "channels": int(audio_rx_cfg.get("channels") or 1),
            "queue_limit": int(audio_rx_cfg.get("queue_limit") or 48),
            "concealment_ms": int(audio_rx_cfg.get("concealment_ms") or 100),
            "qos": int(audio_rx_cfg.get("qos") or 0),
        },
    }


def _default_topic(robot_id: str) -> str:
    return f"pebble/robots/{robot_id}/incoming/audio-stream"


def _topic_robot_id(topic: str) -> Optional[str]:
    parts = [p for p in topic.strip().split("/") if p]
    if len(parts) < 5:
        return None
    if parts[0] != "pebble":
        return None
    return parts[2]


def _resolve_topic(cli_topic: Optional[str], cfg_topic: Optional[str], robot_id: str) -> str:
    explicit = _pick_str(cli_topic, None)
    if explicit:
        return explicit
    configured = _pick_str(None, cfg_topic)
    if not configured:
        return _default_topic(robot_id)
    topic_robot_id = _topic_robot_id(configured)
    if topic_robot_id and topic_robot_id != robot_id:
        print(
            f"[audio_receiver] Config topic '{configured}' targets robot '{topic_robot_id}', "
            f"but robot_id is '{robot_id}'. Using '{_default_topic(robot_id)}' instead.",
            file=sys.stderr,
        )
        return _default_topic(robot_id)
    return configured


def _normalize_tls_config(value: Optional[dict]) -> dict:
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


def _resolve_tls_paths(tls_cfg: dict, base_dir: Path) -> dict:
    def _resolve(path_value: Optional[str]) -> Optional[str]:
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


def _apply_tls(client: mqtt.Client, tls_cfg: dict, base_dir: Path) -> None:
    if not tls_cfg.get("enabled"):
        return
    tls_cfg = _resolve_tls_paths(tls_cfg, base_dir)
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


def _start_aplay(device: Optional[str], rate: int, channels: int) -> subprocess.Popen:
    def _spawn(selected_device: Optional[str]) -> subprocess.Popen:
        cmd = [
            "aplay",
            "-q",
            "-t",
            "raw",
            "-f",
            "S16_LE",
            "-c",
            str(channels),
            "-r",
            str(rate),
        ]
        if selected_device:
            cmd.extend(["-D", selected_device])
        proc = subprocess.Popen(cmd, stdin=subprocess.PIPE)
        if proc.stdin is None:
            raise RuntimeError("aplay stdin unavailable")
        return proc

    try:
        return _spawn(device)
    except FileNotFoundError as exc:
        raise SystemExit("aplay not found; install ALSA utilities or adjust PATH.") from exc
    except OSError as exc:
        if device:
            print(
                f"[audio_receiver] Failed to open playback device '{device}': {exc}. Falling back to ALSA default.",
                file=sys.stderr,
            )
            try:
                return _spawn(None)
            except Exception as fallback_exc:
                raise SystemExit(f"Failed to launch aplay fallback: {fallback_exc}") from fallback_exc
        raise SystemExit(f"Failed to launch aplay: {exc}") from exc


def _parse_packet(payload: bytes) -> Optional[Tuple[int, int, int, int, bytes]]:
    if len(payload) < HEADER_STRUCT.size:
        return None
    header = payload[: HEADER_STRUCT.size]
    body = payload[HEADER_STRUCT.size :]
    magic, version, codec, channels, _flags, rate, frame_samples, seq, _timestamp_ms = HEADER_STRUCT.unpack(header)
    if magic != MAGIC or version != 1 or codec != CODEC_PCM_S16LE:
        return None
    if channels <= 0 or rate <= 0 or frame_samples <= 0:
        return None
    expected = frame_samples * channels * 2
    if len(body) < expected:
        return None
    pcm = body[:expected]
    return seq, rate, channels, frame_samples, pcm


class PCMPlayer:
    """Feeds PCM to aplay and performs basic packet-loss concealment."""

    def __init__(
        self,
        device: Optional[str],
        fallback_rate: int,
        fallback_channels: int,
        queue_limit: int = 48,
        concealment_ms: int = 100,
    ):
        self.device = device
        self.fallback_rate = max(1, int(fallback_rate))
        self.fallback_channels = max(1, int(fallback_channels))
        self.queue: "queue.Queue[Tuple[int, int, int, int, bytes]]" = queue.Queue(maxsize=max(1, queue_limit))
        self._proc: Optional[subprocess.Popen] = None
        self._current_fmt: Tuple[int, int] = (self.fallback_rate, self.fallback_channels)
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self.concealment_ms = max(0, int(concealment_ms))

        self._expected_seq: Optional[int] = None
        self._last_pcm: Optional[bytes] = None
        self._last_rate = self.fallback_rate
        self._last_channels = self.fallback_channels
        self._frame_seconds = 0.02
        self._concealed_ms = 0.0
        self._last_play_at = time.monotonic()

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        try:
            self.queue.put_nowait((0, 0, 0, 0, b""))
        except queue.Full:
            pass
        self._thread.join(timeout=2.0)
        self._close_proc()

    def submit(self, packet: Tuple[int, int, int, int, bytes]) -> None:
        if self.queue.full():
            try:
                self.queue.get_nowait()
            except queue.Empty:
                pass
        try:
            self.queue.put_nowait(packet)
        except queue.Full:
            pass

    def _close_proc(self) -> None:
        proc = self._proc
        self._proc = None
        if proc:
            try:
                proc.terminate()
                proc.wait(timeout=1.0)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass

    def _ensure_proc(self, rate: int, channels: int) -> None:
        fmt = (rate, channels)
        if self._proc is None or fmt != self._current_fmt or self._proc.poll() is not None:
            self._close_proc()
            self._proc = _start_aplay(self.device, rate, channels)
            self._current_fmt = fmt

    def _write_chunk(self, pcm: bytes, rate: int, channels: int) -> None:
        self._ensure_proc(rate, channels)
        if self._proc and self._proc.stdin:
            self._proc.stdin.write(pcm)
            self._proc.stdin.flush()
        self._last_play_at = time.monotonic()

    def _emit_concealment(self, count: int, rate: int, channels: int, frame_seconds: float) -> int:
        if count <= 0 or not self._last_pcm or self.concealment_ms <= 0:
            return 0
        emitted = 0
        for _ in range(count):
            if self._concealed_ms >= self.concealment_ms:
                break
            self._write_chunk(self._last_pcm, rate, channels)
            self._concealed_ms += frame_seconds * 1000.0
            emitted += 1
        return emitted

    def _maybe_timeout_concealment(self) -> None:
        if not self._last_pcm or self._expected_seq is None:
            return
        if self._concealed_ms >= self.concealment_ms:
            return
        now = time.monotonic()
        if now - self._last_play_at < self._frame_seconds:
            return
        self._emit_concealment(1, self._last_rate, self._last_channels, self._frame_seconds)

    def _play_packet(self, packet: Tuple[int, int, int, int, bytes]) -> None:
        seq, rate, channels, frame_samples, pcm = packet
        if rate <= 0:
            rate = self.fallback_rate
        if channels <= 0:
            channels = self.fallback_channels
        frame_seconds = max(0.001, frame_samples / float(rate))

        expected = self._expected_seq
        if expected is not None:
            if seq < expected:
                return
            if seq > expected:
                missing = seq - expected
                self._emit_concealment(missing, self._last_rate, self._last_channels, self._frame_seconds)

        self._write_chunk(pcm, rate, channels)
        self._expected_seq = seq + 1
        self._last_pcm = pcm
        self._last_rate = rate
        self._last_channels = channels
        self._frame_seconds = frame_seconds
        self._concealed_ms = 0.0

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                packet = self.queue.get(timeout=0.05)
            except queue.Empty:
                self._maybe_timeout_concealment()
                continue
            if self._stop.is_set():
                break
            seq, rate, channels, frame_samples, pcm = packet
            if not pcm:
                continue
            try:
                self._play_packet((seq, rate, channels, frame_samples, pcm))
            except Exception as exc:
                print(f"[audio_receiver] playback error: {exc}", file=sys.stderr)
                time.sleep(0.05)
        self._close_proc()


def main() -> None:
    args = _parse_args()
    cfg_path = Path(args.config).expanduser() if args.config else DEFAULT_CONFIG_PATH
    cfg = _load_config(cfg_path)
    runtime_cfg = _runtime_audio_config(cfg)
    root_cfg = runtime_cfg if runtime_cfg is not None else cfg
    receiver_cfg = root_cfg.get("receiver") if isinstance(root_cfg.get("receiver"), dict) else root_cfg

    robot_id = _pick_str(args.robot_id, root_cfg.get("robot_id"))
    host = _pick_str(args.host, root_cfg.get("host"))
    if not robot_id:
        raise SystemExit("robot_id is required via --robot-id or config file.")
    if not host:
        raise SystemExit("host is required via --host or config file.")

    topic = _resolve_topic(args.topic, receiver_cfg.get("topic"), robot_id)

    port = _pick_int(args.port, root_cfg.get("port"), 1883)
    keepalive = _pick_int(args.keepalive, root_cfg.get("keepalive"), 60)
    qos = max(0, min(1, _pick_int(args.qos, receiver_cfg.get("qos"), 0)))
    username = _pick_str(args.username, root_cfg.get("username"))
    password = args.password if args.password is not None else root_cfg.get("password")
    device = _pick_str(args.device, receiver_cfg.get("device"))
    fallback_rate = _pick_int(args.rate, receiver_cfg.get("rate"), 16000)
    fallback_channels = _pick_int(args.channels, receiver_cfg.get("channels"), 1)
    queue_limit = _pick_int(args.queue_limit, receiver_cfg.get("queue_limit"), 48)
    concealment_ms = _pick_int(args.concealment_ms, receiver_cfg.get("concealment_ms"), 100)
    tls_cfg = _normalize_tls_config(root_cfg.get("tls"))
    if args.tls:
        tls_cfg["enabled"] = True
    if args.tls_ca:
        tls_cfg["ca_cert"] = args.tls_ca
    if args.tls_cert:
        tls_cfg["client_cert"] = args.tls_cert
    if args.tls_key:
        tls_cfg["client_key"] = args.tls_key
    if args.tls_insecure:
        tls_cfg["insecure"] = True
    if args.tls_ciphers:
        tls_cfg["ciphers"] = args.tls_ciphers

    player = PCMPlayer(
        device,
        fallback_rate,
        fallback_channels,
        queue_limit=queue_limit,
        concealment_ms=concealment_ms,
    )

    client = create_client()
    if username:
        client.username_pw_set(username, password or None)
    _apply_tls(client, tls_cfg, cfg_path.parent)

    try:
        client.connect(host, port, keepalive)
    except Exception as exc:  # pragma: no cover - network specific
        raise SystemExit(f"Failed to connect to MQTT broker: {exc}") from exc

    stopped = False

    def _sig_handler(_signum, _frame):
        nonlocal stopped
        stopped = True
        try:
            client.disconnect()
        except Exception:
            pass

    signal.signal(signal.SIGINT, _sig_handler)
    signal.signal(signal.SIGTERM, _sig_handler)

    def _on_connect(m_client: mqtt.Client, _userdata: object, _flags: dict, rc: int) -> None:
        if rc != 0:
            print(f"MQTT connect failed: {mqtt.connack_string(rc)}", file=sys.stderr)
            return
        m_client.subscribe(topic, qos=qos)
        print(
            f"Listening for audio on {topic} (QoS {qos}); playing to {device or 'default'} "
            f"with concealment {concealment_ms} ms"
        )

    def _on_disconnect(_m_client: mqtt.Client, _userdata: object, rc: int) -> None:
        if rc != mqtt.MQTT_ERR_SUCCESS:
            print(f"Unexpected MQTT disconnect (rc={rc}).", file=sys.stderr)

    def _handle_payload(msg: mqtt.MQTTMessage) -> None:
        parsed = _parse_packet(msg.payload)
        if not parsed:
            return
        player.submit(parsed)

    client.on_connect = _on_connect
    client.on_disconnect = _on_disconnect
    client.on_message = lambda _c, _u, m: _handle_payload(m)

    player.start()
    client.loop_start()
    try:
        while not stopped:
            time.sleep(0.2)
    finally:
        player.stop()
        client.loop_stop()
        try:
            client.disconnect()
        except Exception:
            pass
    print("Audio receiver stopped.")


if __name__ == "__main__":
    main()
