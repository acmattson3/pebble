#!/usr/bin/env python3
"""Publish shared-memory video frames to MQTT."""

from __future__ import annotations

import argparse
import base64
import json
import ssl
import subprocess
import sys
import time
import zlib
from pathlib import Path
from typing import Any, Dict, List, Optional

import cv2
import numpy as np
import paho.mqtt.client as mqtt

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parents[1]))

import camera_config
from camera_config import CameraProfileError
from control.common.mqtt import create_client

try:
    import RPi.GPIO as GPIO  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    GPIO = None


_GPIO_INITIALIZED = False
DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[1] / "control" / "configs" / "config.json"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stream AV-daemon video to MQTT.")
    parser.add_argument("--profile", default="default", help="Profile name from config.services.mqtt_bridge.media.video_publisher.")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH), help="Path to unified runtime config JSON.")
    parser.add_argument("--robot-id", help="Robot ID used for topic validation/defaulting.")
    parser.add_argument("--topic", help="MQTT topic override.")
    parser.add_argument("--host", help="MQTT broker host override.")
    parser.add_argument("--port", type=int, help="MQTT broker port override.")
    parser.add_argument("--username", help="MQTT username override.")
    parser.add_argument("--password", help="MQTT password override.")
    parser.add_argument("--input-shm", help="AV-daemon shared-memory video socket path.")
    parser.add_argument("--input-retry", type=float, help="Seconds between video source reconnect attempts.")
    return parser.parse_args()


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
            raise CameraProfileError(f"TLS file not found: {path}")
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


def _pick_str(primary: Optional[str], secondary: Optional[str]) -> Optional[str]:
    val = (primary or "").strip() if primary is not None else ""
    if val:
        return val
    if secondary is None:
        return None
    sec = str(secondary).strip()
    return sec or None


def _default_topic(robot_id: str) -> str:
    return f"pebble/robots/{robot_id}/outgoing/front-camera"


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
            f"[camera_publisher] Config topic '{configured}' targets robot '{topic_robot_id}', "
            f"but robot_id is '{robot_id}'. Using '{_default_topic(robot_id)}' instead.",
            file=sys.stderr,
        )
        return _default_topic(robot_id)
    return configured


def _open_shm_capture(socket_path: str, width: int, height: int, fps: float) -> Optional[cv2.VideoCapture]:
    pipeline = (
        f"shmsrc socket-path={socket_path} is-live=true do-timestamp=true ! "
        f"video/x-raw,format=BGR,width={int(width)},height={int(height)},framerate={int(max(1.0, fps))}/1 ! "
        "queue leaky=downstream max-size-buffers=2 ! "
        "videoconvert ! "
        "appsink drop=true max-buffers=1 sync=false"
    )
    cap = cv2.VideoCapture(pipeline, cv2.CAP_GSTREAMER)
    if not cap.isOpened():
        cap.release()
        return None
    return cap


def run_profile(profile_name: str, *, config_path: str, cli_args: Optional[argparse.Namespace] = None) -> None:
    print(f"Publisher script started for profile '{profile_name}'")
    try:
        profile = camera_config.load_profile(profile_name, config_path)
    except CameraProfileError as exc:
        raise SystemExit(str(exc)) from exc

    mqtt_cfg = profile["mqtt"]
    source_cfg = profile.get("source") if isinstance(profile.get("source"), dict) else {}
    encoding_cfg = profile["encoding"]

    robot_id = _pick_str(getattr(cli_args, "robot_id", None), profile_name) or profile_name
    topic = _resolve_topic(getattr(cli_args, "topic", None), mqtt_cfg.get("topic"), robot_id)

    target_fps = float(encoding_cfg["target_fps"])
    keyframe_interval = int(encoding_cfg["keyframe_interval"])
    jpeg_quality = int(encoding_cfg["jpeg_quality"])
    publish_width = int(encoding_cfg.get("publish_width") or source_cfg.get("width") or 640)
    publish_height = int(encoding_cfg.get("publish_height") or source_cfg.get("height") or 480)

    source_socket = _pick_str(getattr(cli_args, "input_shm", None), source_cfg.get("socket_path")) or "/tmp/pebble-video.sock"
    source_width = int(source_cfg.get("width") or 640)
    source_height = int(source_cfg.get("height") or 480)
    source_fps = float(source_cfg.get("fps") or target_fps or 15)
    source_retry = float(
        getattr(cli_args, "input_retry", None)
        or source_cfg.get("reconnect_seconds")
        or profile.get("camera_retry_seconds")
        or 2.0
    )

    mqtt_cfg = dict(mqtt_cfg)
    host_override = _pick_str(getattr(cli_args, "host", None), None)
    if host_override:
        mqtt_cfg["broker"] = host_override
    if getattr(cli_args, "port", None) is not None:
        mqtt_cfg["port"] = int(cli_args.port)
    user_override = _pick_str(getattr(cli_args, "username", None), None)
    if user_override is not None:
        mqtt_cfg["username"] = user_override
    if getattr(cli_args, "password", None) is not None:
        mqtt_cfg["password"] = cli_args.password

    client = _create_mqtt_client(mqtt_cfg)
    charging_indicator = ChargingIndicator(profile.get("charging_indicator", {}))
    rotation = _extract_rotation(profile.get("frame_transform"))

    frame_duration = 1.0 / target_fps if target_fps > 0 else 0.0

    while True:
        capture = _open_shm_capture(source_socket, source_width, source_height, source_fps)
        if capture is None:
            print(
                f"Video source unavailable at {source_socket}. "
                f"Ensure av_daemon is running. Retrying in {source_retry} seconds...",
                file=sys.stderr,
            )
            time.sleep(source_retry)
            continue

        frame_id = 0
        reference_frame: Optional[np.ndarray] = None
        last_send_time = 0.0
        start_time = time.time()
        frame_index = 0

        try:
            while capture.isOpened():
                ret, frame = capture.read()
                if not ret:
                    print("Shared video feed lost. Reconnecting...", file=sys.stderr)
                    break

                frame = _apply_rotation(frame, rotation)
                frame = _resize_for_publish(frame, publish_width, publish_height)

                expected_time = start_time + frame_index * frame_duration
                now = time.time()
                delay = expected_time - now
                if delay > 0:
                    time.sleep(delay)
                    now = time.time()

                if frame_duration == 0 or now - last_send_time >= frame_duration:
                    is_keyframe = reference_frame is None or (frame_id % keyframe_interval) == 0
                    frame_to_send = _prepare_frame(frame, reference_frame, is_keyframe)
                    charging_indicator.decorate(frame_to_send)

                    if _ensure_mqtt_connection(client, mqtt_cfg):
                        payload, decoded_frame = _encode_payload(frame_to_send, frame_id, is_keyframe, jpeg_quality)
                        client.publish(topic, payload)
                        reference_frame = _reconstruct_published_frame(reference_frame, decoded_frame, is_keyframe)
                        last_send_time = now
                        frame_id += 1

                frame_index += 1
        except Exception as exc:
            print(f"Error in video publish loop: {exc}", file=sys.stderr)
        finally:
            capture.release()
            time.sleep(source_retry)


def _create_mqtt_client(cfg: Dict[str, Any]) -> mqtt.Client:
    override_cfg = cfg.get("network_override")
    if override_cfg is not None and not isinstance(override_cfg, dict):
        raise CameraProfileError("mqtt.network_override must be an object.")

    brokers = _resolve_brokers(cfg.get("broker"), override_cfg)
    if not brokers:
        raise CameraProfileError("mqtt.broker or mqtt.network_override.away_host must be provided.")

    port = int(cfg.get("port", 1883))
    keepalive = int(cfg.get("keepalive", 60))
    reconnect_seconds = int(cfg.get("reconnect_seconds", 10))

    host_index = 0
    client = create_client()
    username = cfg.get("username")
    password = cfg.get("password")
    if username:
        client.username_pw_set(username, password)
    _apply_tls(client, cfg)

    while True:
        broker = brokers[host_index]
        print(f"Connecting to MQTT broker {broker}:{port} ...")
        try:
            client.connect(broker, port, keepalive)
            client.loop_start()
            print(f"Connected to MQTT broker {broker}:{port}!")
            return client
        except (ConnectionRefusedError, OSError) as exc:
            print(f"MQTT connection error for {broker}:{port}: {exc}")
        host_index = (host_index + 1) % len(brokers)
        if host_index == 0:
            print(f"All MQTT brokers failed; retrying in {reconnect_seconds} seconds...")
            time.sleep(reconnect_seconds)
        else:
            print("Trying next MQTT broker option...")


def _ensure_mqtt_connection(client: mqtt.Client, cfg: Dict[str, Any]) -> bool:
    if client.is_connected():
        return True

    reconnect_seconds = int(cfg.get("reconnect_seconds", 10))
    print("MQTT disconnected. Attempting reconnect...")
    try:
        client.reconnect()
        print("MQTT reconnected!")
        return True
    except Exception as exc:  # pragma: no cover - hard to trigger deterministically
        print(f"Reconnect failed: {exc}. Waiting {reconnect_seconds} seconds before retry.")
        time.sleep(reconnect_seconds)
        return False


def _prepare_frame(frame: np.ndarray, last_frame: Any, is_keyframe: bool) -> np.ndarray:
    if is_keyframe or last_frame is None:
        return frame.copy()

    delta = frame.astype(np.int16) - last_frame.astype(np.int16)
    delta = np.clip(delta + 128, 0, 255).astype(np.uint8)
    return delta


def _reconstruct_published_frame(
    reference_frame: Optional[np.ndarray],
    decoded_frame: np.ndarray,
    is_keyframe: bool,
) -> np.ndarray:
    if is_keyframe or reference_frame is None:
        return decoded_frame.copy()

    delta = decoded_frame.astype(np.int16) - 128
    base_frame = reference_frame.astype(np.int16)
    return np.clip(base_frame + delta, 0, 255).astype(np.uint8)


def _extract_rotation(transform_config: Any) -> Optional[int]:
    if not isinstance(transform_config, dict) or not transform_config:
        return None

    rotate = transform_config.get("rotate_degrees")
    if rotate is None:
        return None

    try:
        rotate_int = int(rotate) % 360
    except (TypeError, ValueError):
        print(f"Ignoring invalid rotate_degrees value: {rotate!r}")
        return None

    if rotate_int in (0, 90, 180, 270):
        return rotate_int if rotate_int != 0 else None

    print(f"Ignoring unsupported rotate_degrees value: {rotate_int}")
    return None


def _apply_rotation(frame: np.ndarray, rotation: Optional[int]) -> np.ndarray:
    if rotation is None:
        return frame

    if rotation == 90:
        return cv2.rotate(frame, cv2.ROTATE_90_CLOCKWISE)
    if rotation == 180:
        return cv2.rotate(frame, cv2.ROTATE_180)
    if rotation == 270:
        return cv2.rotate(frame, cv2.ROTATE_90_COUNTERCLOCKWISE)
    return frame


def _resize_for_publish(frame: np.ndarray, width: int, height: int) -> np.ndarray:
    if width <= 0 or height <= 0:
        return frame
    h, w = frame.shape[:2]
    if w == width and h == height:
        return frame
    interpolation = cv2.INTER_AREA if width < w or height < h else cv2.INTER_LINEAR
    return cv2.resize(frame, (int(width), int(height)), interpolation=interpolation)


def _encode_payload(frame: np.ndarray, frame_id: int, is_keyframe: bool, jpeg_quality: int) -> tuple[str, np.ndarray]:
    encode_params = [int(cv2.IMWRITE_JPEG_QUALITY), int(jpeg_quality)]
    success, jpeg = cv2.imencode(".jpg", frame, encode_params)
    if not success:
        raise RuntimeError("Failed to encode frame as JPEG.")
    decoded = cv2.imdecode(jpeg, cv2.IMREAD_COLOR)
    if decoded is None:
        raise RuntimeError("Failed to decode locally encoded JPEG frame.")

    compressed = zlib.compress(jpeg.tobytes())
    encoded = base64.b64encode(compressed).decode()
    payload = {
        "id": frame_id,
        "keyframe": is_keyframe,
        "data": encoded,
        "timestamp": time.time(),
    }
    return json.dumps(payload), decoded


def _resolve_brokers(default_broker: Optional[str], override_cfg: Optional[Dict[str, Any]]) -> List[str]:
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
        print("Incomplete mqtt.network_override configuration; ignoring override.")
        _append(default_broker or away_host or None)
        return brokers

    ssid = _current_wifi_ssid()
    if ssid and ssid == local_ssid:
        print(f"Detected SSID '{ssid}'. Using local MQTT broker {local_host}.")
        _append(local_host)
        _append(away_host)
        return brokers

    if not ssid:
        print(
            f"Unable to determine Wi-Fi SSID; trying away MQTT broker {away_host} "
            f"with local fallback {local_host} (expected '{local_ssid}')."
        )
        _append(away_host)
        _append(local_host)
        return brokers

    print(f"Connected to SSID '{ssid}' (expected '{local_ssid}'); using away MQTT broker {away_host}.")
    _append(away_host)
    return brokers


def _current_wifi_ssid() -> Optional[str]:
    try:
        result = subprocess.run(
            ["nmcli", "-t", "-f", "active,ssid", "dev", "wifi"],
            check=True,
            capture_output=True,
            text=True,
            timeout=3,
        )
    except (OSError, subprocess.SubprocessError):
        return None

    output = result.stdout.strip()
    if not output:
        return None
    for line in output.splitlines():
        if line.startswith("yes:"):
            ssid = line.split(":", 1)[1].strip()
            if ssid:
                return ssid
    return None


class ChargingIndicator:
    def __init__(self, config: Dict[str, Any]):
        self.enabled = bool(config.get("enabled"))
        self.pin = config.get("pin")
        self.position = tuple(config.get("position", [10, 10]))
        self.radius = int(config.get("radius", 6))
        self.color = tuple(config.get("color", [0, 0, 255]))

        if not self.enabled:
            return
        if GPIO is None:
            print("GPIO library not available; disabling charging indicator overlay.")
            self.enabled = False
            return
        if self.pin is None:
            print("Charging indicator enabled but no pin configured; disabling overlay.")
            self.enabled = False
            return

        _ensure_gpio_setup()
        GPIO.setup(self.pin, GPIO.IN)

    def decorate(self, frame: np.ndarray) -> None:
        if not self.enabled:
            return
        try:
            is_charging = GPIO.input(self.pin) == GPIO.HIGH
        except Exception as exc:  # pragma: no cover - depends on hardware
            print(f"Failed to read charging pin: {exc}")
            return
        if is_charging:
            cv2.circle(frame, tuple(map(int, self.position)), int(self.radius), tuple(map(int, self.color)), -1)


def _ensure_gpio_setup() -> None:
    global _GPIO_INITIALIZED
    if GPIO is None or _GPIO_INITIALIZED:
        return
    GPIO.setmode(GPIO.BCM)
    _GPIO_INITIALIZED = True


def main() -> None:
    args = _parse_args()
    run_profile(args.profile, config_path=args.config, cli_args=args)


if __name__ == "__main__":
    main()
