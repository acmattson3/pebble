#!/usr/bin/env python3
"""
Capture microphone audio from AV-daemon shared memory and publish PCM packets over MQTT.

Payload format is binary: a fixed header followed by raw PCM16LE bytes.
"""

from __future__ import annotations

import argparse
import json
import signal
import ssl
import struct
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Optional

import paho.mqtt.client as mqtt


DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[1] / "control" / "configs" / "config.json"
if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from control.common.mqtt import create_client

MAGIC = b"PBAT"
CODEC_PCM_S16LE = 1
HEADER_STRUCT = struct.Struct("!4sBBBBHHIQ")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stream audio from AV shared memory to MQTT.")
    parser.add_argument("--config", help=f"Path to unified runtime config JSON (default: {DEFAULT_CONFIG_PATH}).")
    parser.add_argument("--robot-id", help="Robot ID used in the topic path.")
    parser.add_argument(
        "--topic",
        help="MQTT topic to publish to (default: pebble/robots/<id>/outgoing/audio).",
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
    parser.add_argument("--rate", type=int, help="Sample rate (Hz).")
    parser.add_argument("--channels", type=int, help="Number of audio channels.")
    parser.add_argument("--chunk-ms", type=int, help="Chunk size in milliseconds.")
    parser.add_argument("--input-shm", help="AV-daemon shared-memory socket path for microphone input.")
    parser.add_argument("--input-retry", type=float, help="Seconds to wait before retrying unavailable input stream.")
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


def _pick_float(primary: Optional[float], secondary: Optional[float], default: float) -> float:
    if primary is not None:
        return float(primary)
    if secondary is not None:
        try:
            return float(secondary)
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
    audio_pub_cfg = media_cfg.get("audio_publisher") if isinstance(media_cfg.get("audio_publisher"), dict) else {}

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
        "publisher": {
            "topic": str(audio_pub_cfg.get("topic") or _topic_from_robot(robot_cfg, "outgoing", "audio") or ""),
            "input_shm": str(audio_pub_cfg.get("input_shm") or "/tmp/pebble-audio.sock"),
            "input_retry": float(audio_pub_cfg.get("input_retry_seconds") or 2.0),
            "rate": int(audio_pub_cfg.get("rate") or 16000),
            "channels": int(audio_pub_cfg.get("channels") or 1),
            "chunk_ms": int(audio_pub_cfg.get("chunk_ms") or 20),
            "qos": int(audio_pub_cfg.get("qos") or 0),
        },
    }


def _default_topic(robot_id: str) -> str:
    return f"pebble/robots/{robot_id}/outgoing/audio"


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
            f"[audio_publisher] Config topic '{configured}' targets robot '{topic_robot_id}', "
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


def _build_packet(pcm: bytes, seq: int, rate: int, channels: int) -> Optional[bytes]:
    frame_width = max(1, channels) * 2
    if len(pcm) < frame_width:
        return None
    frame_samples = len(pcm) // frame_width
    payload_len = frame_samples * frame_width
    if payload_len <= 0:
        return None
    payload = pcm[:payload_len]
    header = HEADER_STRUCT.pack(
        MAGIC,
        1,
        CODEC_PCM_S16LE,
        channels,
        0,
        rate,
        frame_samples,
        seq,
        int(time.time() * 1000),
    )
    return header + payload


def _start_shm_reader(socket_path: str, rate: int, channels: int) -> subprocess.Popen:
    caps = f"audio/x-raw,format=S16LE,rate={rate},channels={channels},layout=interleaved"
    cmd = [
        "gst-launch-1.0",
        "-q",
        "shmsrc",
        f"socket-path={socket_path}",
        "is-live=true",
        "do-timestamp=true",
        "!",
        caps,
        "!",
        "queue",
        "max-size-time=200000000",
        "leaky=downstream",
        "!",
        "fdsink",
        "fd=1",
        "sync=false",
    ]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if proc.stdout is None:
        raise RuntimeError("gst-launch stdout unavailable")
    return proc


def _stderr_text(proc: subprocess.Popen) -> str:
    if not proc.stderr:
        return ""
    try:
        return proc.stderr.read().decode("utf-8", errors="ignore").strip()
    except Exception:
        return ""


def main() -> None:
    args = _parse_args()
    cfg_path = Path(args.config).expanduser() if args.config else DEFAULT_CONFIG_PATH
    cfg = _load_config(cfg_path)
    runtime_cfg = _runtime_audio_config(cfg)
    root_cfg = runtime_cfg if runtime_cfg is not None else cfg
    publisher_cfg = root_cfg.get("publisher") if isinstance(root_cfg.get("publisher"), dict) else root_cfg

    robot_id = _pick_str(args.robot_id, root_cfg.get("robot_id"))
    host = _pick_str(args.host, root_cfg.get("host"))
    if not robot_id:
        raise SystemExit("robot_id is required via --robot-id or config file.")
    if not host:
        raise SystemExit("host is required via --host or config file.")

    topic = _resolve_topic(args.topic, publisher_cfg.get("topic") or root_cfg.get("topic"), robot_id)

    rate = max(1, _pick_int(args.rate, publisher_cfg.get("rate") or root_cfg.get("rate"), 16000))
    channels = max(1, _pick_int(args.channels, publisher_cfg.get("channels") or root_cfg.get("channels"), 1))
    chunk_ms = max(1, _pick_int(args.chunk_ms, publisher_cfg.get("chunk_ms") or root_cfg.get("chunk_ms"), 20))
    input_shm = _pick_str(args.input_shm, publisher_cfg.get("input_shm") or root_cfg.get("input_shm")) or "/tmp/pebble-audio.sock"
    input_retry = max(0.1, _pick_float(args.input_retry, publisher_cfg.get("input_retry") or root_cfg.get("input_retry"), 2.0))

    port = _pick_int(args.port, root_cfg.get("port"), 1883)
    keepalive = _pick_int(args.keepalive, root_cfg.get("keepalive"), 60)
    qos = max(0, min(1, _pick_int(args.qos, publisher_cfg.get("qos") or root_cfg.get("qos"), 0)))

    username = _pick_str(args.username, root_cfg.get("username"))
    password = args.password if args.password is not None else root_cfg.get("password")
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

    frame_width = channels * 2
    chunk_bytes = max(frame_width, int(rate * frame_width * (chunk_ms / 1000.0)))
    chunk_bytes -= chunk_bytes % frame_width
    if chunk_bytes <= 0:
        chunk_bytes = frame_width

    client = create_client()
    if username:
        client.username_pw_set(username, password or None)
    _apply_tls(client, tls_cfg, cfg_path.parent)

    try:
        client.connect(host, port, keepalive)
    except Exception as exc:  # pragma: no cover - network specific
        raise SystemExit(f"Failed to connect to MQTT broker: {exc}") from exc

    client.loop_start()
    print(
        f"Streaming audio to {topic} from {input_shm} "
        f"({rate} Hz, {channels}ch, {chunk_ms} ms chunks, QoS {qos})"
    )

    stopped = False

    def _sig_handler(_signum, _frame):
        nonlocal stopped
        stopped = True

    signal.signal(signal.SIGINT, _sig_handler)
    signal.signal(signal.SIGTERM, _sig_handler)

    seq = 0
    while not stopped:
        proc: Optional[subprocess.Popen] = None
        pending = b""
        try:
            proc = _start_shm_reader(input_shm, rate, channels)
            time.sleep(0.15)
            if proc.poll() is not None:
                raise RuntimeError(_stderr_text(proc) or f"gst-launch exited rc={proc.returncode}")

            assert proc.stdout is not None
            while not stopped:
                data = proc.stdout.read(chunk_bytes)
                if not data:
                    if proc.poll() is not None:
                        raise RuntimeError(_stderr_text(proc) or f"gst-launch exited rc={proc.returncode}")
                    time.sleep(0.005)
                    continue

                pending += data
                while len(pending) >= chunk_bytes and not stopped:
                    chunk = pending[:chunk_bytes]
                    pending = pending[chunk_bytes:]
                    seq += 1
                    if seq > 0xFFFFFFFF:
                        seq = 1
                    packet = _build_packet(chunk, seq, rate, channels)
                    if packet is not None:
                        client.publish(topic, packet, qos=qos)
        except FileNotFoundError:
            print("[audio_publisher] gst-launch-1.0 not found; retrying.", file=sys.stderr)
            time.sleep(input_retry)
        except Exception as exc:
            print(f"[audio_publisher] Input unavailable ({exc}); retrying.", file=sys.stderr)
            time.sleep(input_retry)
        finally:
            if proc:
                try:
                    proc.terminate()
                    proc.wait(timeout=1.0)
                except Exception:
                    try:
                        proc.kill()
                    except Exception:
                        pass

    client.loop_stop()
    client.disconnect()
    print("Audio publisher stopped.")


if __name__ == "__main__":
    main()
