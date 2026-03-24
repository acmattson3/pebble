#!/usr/bin/env python3
"""Publish all visible AprilTag IDs + corner locations to MQTT."""

from __future__ import annotations

import argparse
import json
import ssl
import sys
import time
from pathlib import Path
from typing import Any, Dict, Optional

import cv2
import paho.mqtt.client as mqtt

try:
    cv2.setNumThreads(1)
except Exception:
    pass

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = REPO_ROOT / "control" / "configs" / "config.json"


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
            raise SystemExit(f"TLS file not found: {path}")
        return str(path)

    if not tls_cfg.get("enabled"):
        return tls_cfg
    resolved = dict(tls_cfg)
    resolved["ca_cert"] = _resolve(tls_cfg.get("ca_cert"))
    resolved["client_cert"] = _resolve(tls_cfg.get("client_cert"))
    resolved["client_key"] = _resolve(tls_cfg.get("client_key"))
    return resolved


def _apply_tls(client: mqtt.Client, mqtt_cfg: Dict[str, Any], base_dir: Path) -> None:
    tls_cfg = _normalize_tls_config(mqtt_cfg.get("tls"))
    if not tls_cfg.get("enabled"):
        return
    tls_cfg = _resolve_tls_paths(tls_cfg, base_dir)
    client.tls_set(
        ca_certs=tls_cfg.get("ca_cert"),
        certfile=tls_cfg.get("client_cert"),
        keyfile=tls_cfg.get("client_key"),
        cert_reqs=ssl.CERT_REQUIRED,
        tls_version=ssl.PROTOCOL_TLS_CLIENT,
        ciphers=tls_cfg.get("ciphers") or None,
    )
    if tls_cfg.get("insecure"):
        client.tls_insecure_set(True)


def _load_runtime_config(path_value: str) -> tuple[dict[str, Any], Path]:
    if path_value:
        cfg_path = Path(path_value).expanduser()
    else:
        cfg_path = DEFAULT_CONFIG_PATH
    if not cfg_path.is_absolute():
        cfg_path = (Path.cwd() / cfg_path).resolve()
    if not cfg_path.exists():
        raise RuntimeError(f"Missing config file: {cfg_path}")
    try:
        data = json.loads(cfg_path.read_text())
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Failed to parse config file {cfg_path}: {exc}") from exc
    if not isinstance(data, dict):
        raise RuntimeError("Config root must be a JSON object")
    return data, cfg_path


def _extract_rotation(transform_config):
    if not isinstance(transform_config, dict) or not transform_config:
        return None
    rotate = transform_config.get("rotate_degrees")
    if rotate is None:
        return None
    try:
        rotate_int = int(rotate) % 360
    except (TypeError, ValueError):
        return None
    if rotate_int in (0, 90, 180, 270):
        return rotate_int if rotate_int != 0 else None
    return None


def _apply_rotation(frame, rotation: Optional[int]):
    if rotation is None:
        return frame
    if rotation == 90:
        return cv2.rotate(frame, cv2.ROTATE_90_CLOCKWISE)
    if rotation == 180:
        return cv2.rotate(frame, cv2.ROTATE_180)
    if rotation == 270:
        return cv2.rotate(frame, cv2.ROTATE_90_COUNTERCLOCKWISE)
    return frame


def _open_shared_capture(socket_path: str, width: int, height: int, fps: float):
    pipeline = (
        f"shmsrc socket-path={socket_path} is-live=true do-timestamp=true ! "
        f"video/x-raw,format=BGR,width={int(width)},height={int(height)},framerate={int(max(1.0, fps))}/1 ! "
        "queue leaky=downstream max-size-buffers=2 ! "
        "videoconvert ! "
        "appsink drop=true max-buffers=1 sync=false"
    )
    capture = cv2.VideoCapture(pipeline, cv2.CAP_GSTREAMER)
    if not capture.isOpened():
        capture.release()
        return None
    return capture


def _create_detector_params():
    if hasattr(cv2.aruco, "DetectorParameters_create"):
        return cv2.aruco.DetectorParameters_create()
    return cv2.aruco.DetectorParameters()


def build_dictionary(name: str):
    key = name.strip().upper()
    if not key.startswith("DICT_"):
        key = "DICT_" + key
    if not hasattr(cv2.aruco, key):
        raise ValueError(f"Unknown dictionary: {name}")
    return cv2.aruco.getPredefinedDictionary(getattr(cv2.aruco, key))


def detect_markers(frame, dictionary, detector_params):
    if len(frame.shape) == 3 and frame.shape[2] == 3:
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
    else:
        gray = frame
    return cv2.aruco.detectMarkers(gray, dictionary, parameters=detector_params)


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish all visible AprilTags as MQTT locations payloads.")
    parser.add_argument("--config", type=str, default="", help="Path to unified runtime config JSON.")
    parser.add_argument("--robot-id", type=str, default="", help="Robot ID override.")
    parser.add_argument("--mqtt-host", type=str, default="", help="MQTT broker host.")
    parser.add_argument("--mqtt-port", type=int, default=0, help="MQTT broker port.")
    parser.add_argument("--mqtt-username", type=str, default="", help="MQTT username.")
    parser.add_argument("--mqtt-password", type=str, default="", help="MQTT password.")
    parser.add_argument("--keepalive", type=int, default=0, help="MQTT keepalive seconds.")

    parser.add_argument("--video-shm", type=str, default="", help="Path to AV-daemon video shared-memory socket.")
    parser.add_argument("--width", type=int, default=0, help="Expected stream width.")
    parser.add_argument("--height", type=int, default=0, help="Expected stream height.")
    parser.add_argument("--fps", type=float, default=0.0, help="Expected stream FPS.")
    parser.add_argument("--marker-dictionary", type=str, default="APRILTAG_25H9", help="ArUco/AprilTag dictionary.")
    parser.add_argument("--locations-topic", type=str, default="", help="MQTT topic for apriltag locations.")
    parser.add_argument("--overlays-topic", type=str, default="", help="MQTT topic for overlay shapes.")
    parser.add_argument("--rate", type=float, default=12.0, help="Publish rate (Hz).")
    parser.add_argument("--preview", action="store_true", help="Show preview window.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        runtime_cfg, cfg_path = _load_runtime_config(args.config)
    except RuntimeError as exc:
        print(exc, file=sys.stderr)
        return 1

    services_cfg = runtime_cfg.get("services") if isinstance(runtime_cfg.get("services"), dict) else {}
    av_cfg = services_cfg.get("av_daemon") if isinstance(services_cfg.get("av_daemon"), dict) else {}
    av_video_cfg = av_cfg.get("video") if isinstance(av_cfg.get("video"), dict) else {}
    local_mqtt_cfg = runtime_cfg.get("local_mqtt") if isinstance(runtime_cfg.get("local_mqtt"), dict) else {}
    robot_cfg = runtime_cfg.get("robot") if isinstance(runtime_cfg.get("robot"), dict) else {}

    system = str(robot_cfg.get("system") or "pebble").strip() or "pebble"
    robot_type = str(robot_cfg.get("type") or "robots").strip() or "robots"
    robot_id = args.robot_id or str(robot_cfg.get("id") or "").strip()
    if not robot_id:
        print("robot.id is required in config (or pass --robot-id).", file=sys.stderr)
        return 1

    try:
        dictionary = build_dictionary(args.marker_dictionary)
    except ValueError as exc:
        print(exc, file=sys.stderr)
        return 1

    default_width = int(av_video_cfg.get("width") or 1280)
    default_height = int(av_video_cfg.get("height") or 720)
    default_fps = float(av_video_cfg.get("fps") or 15.0)

    stream_socket = str(args.video_shm or av_video_cfg.get("socket_path") or "/tmp/pebble-video.sock")
    stream_width = int(args.width if args.width > 0 else default_width)
    stream_height = int(args.height if args.height > 0 else default_height)
    stream_fps = float(args.fps if args.fps > 0 else default_fps)

    rotation = _extract_rotation({"rotate_degrees": av_video_cfg.get("rotate_degrees")})
    camera_retry = float(av_video_cfg.get("reconnect_seconds") or av_video_cfg.get("input_retry_seconds") or 2.0)

    host = args.mqtt_host or str(local_mqtt_cfg.get("host") or "127.0.0.1")
    port = args.mqtt_port or int(local_mqtt_cfg.get("port") or 1883)
    keepalive = args.keepalive or int(local_mqtt_cfg.get("keepalive") or 60)
    username = args.mqtt_username or str(local_mqtt_cfg.get("username") or "")
    password = args.mqtt_password or str(local_mqtt_cfg.get("password") or "")
    mqtt_cfg = {"tls": local_mqtt_cfg.get("tls") if isinstance(local_mqtt_cfg.get("tls"), dict) else {"enabled": False}}

    base_topic = f"{system}/{robot_type}/{robot_id}"
    locations_topic = args.locations_topic.strip() or f"{base_topic}/outgoing/apriltag-locations"
    overlays_topic = args.overlays_topic.strip() or f"{base_topic}/outgoing/video-overlays"

    client = mqtt.Client()
    if username:
        client.username_pw_set(username, password or None)
    _apply_tls(client, mqtt_cfg, cfg_path.parent)
    try:
        client.connect(host, port, keepalive)
    except Exception as exc:
        print(f"Failed to connect to MQTT broker: {exc}", file=sys.stderr)
        return 1
    client.loop_start()

    detector_params = _create_detector_params()
    interval = 1.0 / max(1.0, float(args.rate))
    last_publish = 0.0
    printed_frame_shape = False

    try:
        while True:
            capture = _open_shared_capture(stream_socket, stream_width, stream_height, stream_fps)
            if capture is None:
                print(f"Shared video source unavailable at {stream_socket}. Retrying.", file=sys.stderr)
                time.sleep(camera_retry)
                continue

            try:
                while capture.isOpened():
                    ret, frame = capture.read()
                    if not ret:
                        print("Shared video feed lost; retrying.", file=sys.stderr)
                        break

                    frame = _apply_rotation(frame, rotation)
                    frame_h, frame_w = frame.shape[:2]
                    if not printed_frame_shape:
                        print(f"[apriltag-locations] received frame size {frame_w}x{frame_h}", file=sys.stderr)
                        printed_frame_shape = True
                    now = time.time()

                    corners, ids, _ = detect_markers(frame, dictionary, detector_params)
                    detections: list[dict[str, Any]] = []
                    shapes: list[list[list[float]]] = []
                    labels: list[dict[str, Any]] = []

                    if ids is not None and len(ids) > 0:
                        ids_flat = ids.flatten().tolist()
                        for idx, marker_id in enumerate(ids_flat):
                            marker_corners = corners[idx]
                            if marker_corners is None:
                                continue
                            pts = marker_corners.reshape(4, 2)
                            corners_norm = [
                                [
                                    float(_clamp(float(pt[0]) / max(1.0, float(frame_w)), 0.0, 1.0)),
                                    float(_clamp(float(pt[1]) / max(1.0, float(frame_h)), 0.0, 1.0)),
                                ]
                                for pt in pts
                            ]
                            center_x = float(sum(p[0] for p in corners_norm) / 4.0)
                            center_y = float(sum(p[1] for p in corners_norm) / 4.0)
                            detections.append(
                                {
                                    "marker_id": int(marker_id),
                                    "corners_norm": corners_norm,
                                    "center_norm": [center_x, center_y],
                                }
                            )
                            shapes.append(corners_norm)
                            labels.append({"marker_id": int(marker_id), "center_norm": [center_x, center_y]})

                        if args.preview:
                            cv2.aruco.drawDetectedMarkers(frame, corners, ids)

                    if now - last_publish >= interval:
                        locations_payload = {
                            "source": "apriltag-locations",
                            "detections": detections,
                            "frame_width": int(frame_w),
                            "frame_height": int(frame_h),
                            "timestamp": now,
                        }
                        overlays_payload = {
                            "source": "apriltag-locations",
                            "shapes": shapes,
                            "labels": labels,
                            "frame_width": int(frame_w),
                            "frame_height": int(frame_h),
                            "timestamp": now,
                        }
                        client.publish(locations_topic, json.dumps(locations_payload), qos=0)
                        client.publish(overlays_topic, json.dumps(overlays_payload), qos=0)
                        last_publish = now

                    if args.preview:
                        cv2.imshow("AprilTag Locations", frame)
                        key = cv2.waitKey(1) & 0xFF
                        if key in (27, ord("q")):
                            raise KeyboardInterrupt
            finally:
                capture.release()
                time.sleep(camera_retry)
    except KeyboardInterrupt:
        pass
    finally:
        try:
            client.publish(
                locations_topic,
                json.dumps(
                    {
                        "source": "apriltag-locations",
                        "detections": [],
                        "frame_width": int(stream_width),
                        "frame_height": int(stream_height),
                        "timestamp": time.time(),
                    }
                ),
                qos=0,
            )
            client.publish(
                overlays_topic,
                json.dumps(
                    {
                        "source": "apriltag-locations",
                        "shapes": [],
                        "labels": [],
                        "frame_width": int(stream_width),
                        "frame_height": int(stream_height),
                        "timestamp": time.time(),
                    }
                ),
                qos=0,
            )
        except Exception:
            pass
        client.loop_stop()
        client.disconnect()
        if args.preview:
            cv2.destroyAllWindows()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
