#!/usr/bin/env python3
"""AprilTag follow decision loop using MQTT tag + odometry feeds."""

from __future__ import annotations

import argparse
import csv
import json
import math
import ssl
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import cv2
import numpy as np
import paho.mqtt.client as mqtt

try:
    cv2.setNumThreads(1)
except Exception:
    pass

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = REPO_ROOT / "control" / "configs" / "config.json"


@dataclass
class OdomPose:
    x_mm: float
    y_mm: float
    heading_rad: float
    distance_mm: float
    timestamp: float


@dataclass
class TagFrame:
    detections: list[dict[str, Any]]
    frame_width: int
    frame_height: int
    timestamp: float


@dataclass
class OdomGoal:
    x_mm: float
    y_mm: float
    created_at: float


class CsvDecisionLogger:
    def __init__(self, path: Path, flush_every: int) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.flush_every = max(1, int(flush_every))
        self._line_count = 0
        self._fh = path.open("w", newline="")
        self._writer = csv.DictWriter(
            self._fh,
            fieldnames=[
                "ts",
                "mode",
                "reason",
                "tag_visible",
                "cmd_x",
                "cmd_z",
                "vision_distance_m",
                "vision_angle_deg",
                "odom_x_mm",
                "odom_y_mm",
                "odom_heading_deg",
                "odom_distance_mm",
                "goal_active",
                "goal_x_mm",
                "goal_y_mm",
                "goal_distance_m",
                "goal_heading_error_deg",
                "publish_seq",
            ],
        )
        self._writer.writeheader()
        self._fh.flush()

    def write(self, row: dict[str, Any]) -> None:
        self._writer.writerow(row)
        self._line_count += 1
        if (self._line_count % self.flush_every) == 0:
            self._fh.flush()

    def close(self) -> None:
        try:
            self._fh.flush()
        except Exception:
            pass
        self._fh.close()


def clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _normalize_angle_rad(theta: float) -> float:
    return math.atan2(math.sin(theta), math.cos(theta))


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


def load_calibration(path: str) -> Tuple[np.ndarray, np.ndarray, Optional[Tuple[int, int]]]:
    fs = cv2.FileStorage(path, cv2.FILE_STORAGE_READ)
    if not fs.isOpened():
        raise RuntimeError(f"Failed to open calibration file: {path}")
    camera_matrix = fs.getNode("camera_matrix").mat()
    dist_coeffs = fs.getNode("distortion_coefficients").mat()
    image_width_node = fs.getNode("image_width")
    image_height_node = fs.getNode("image_height")
    image_width = int(image_width_node.real()) if image_width_node is not None and not image_width_node.empty() else 0
    image_height = int(image_height_node.real()) if image_height_node is not None and not image_height_node.empty() else 0
    fs.release()
    if camera_matrix is None or dist_coeffs is None:
        raise RuntimeError("Calibration file missing camera_matrix or distortion_coefficients")
    calibration_size = (image_width, image_height) if image_width > 0 and image_height > 0 else None
    return camera_matrix, dist_coeffs, calibration_size


def scale_camera_matrix(
    camera_matrix: np.ndarray,
    calibration_size: Optional[Tuple[int, int]],
    frame_size: Tuple[int, int],
) -> np.ndarray:
    if calibration_size is None:
        return camera_matrix

    cal_width, cal_height = calibration_size
    frame_width, frame_height = frame_size
    if cal_width <= 0 or cal_height <= 0:
        return camera_matrix
    if frame_width <= 0 or frame_height <= 0:
        return camera_matrix
    if cal_width == frame_width and cal_height == frame_height:
        return camera_matrix

    scale_x = float(frame_width) / float(cal_width)
    scale_y = float(frame_height) / float(cal_height)
    scaled = camera_matrix.astype(np.float64).copy()
    scaled[0, 0] *= scale_x
    scaled[1, 1] *= scale_y
    scaled[0, 2] *= scale_x
    scaled[1, 2] *= scale_y
    return scaled


def _estimate_pose_from_norm_corners(
    corners_norm: list[list[float]],
    marker_size: float,
    camera_matrix: np.ndarray,
    dist_coeffs: np.ndarray,
    frame_w: int,
    frame_h: int,
) -> Optional[np.ndarray]:
    if len(corners_norm) != 4:
        return None
    image_points: list[list[float]] = []
    for pt in corners_norm:
        if not isinstance(pt, (list, tuple)) or len(pt) != 2:
            return None
        try:
            x = float(pt[0]) * float(frame_w)
            y = float(pt[1]) * float(frame_h)
        except (TypeError, ValueError):
            return None
        image_points.append([x, y])

    image_points_np = np.asarray(image_points, dtype=np.float32).reshape(4, 2)
    half = float(marker_size) / 2.0
    object_points = np.array(
        [
            [-half, half, 0.0],
            [half, half, 0.0],
            [half, -half, 0.0],
            [-half, -half, 0.0],
        ],
        dtype=np.float32,
    )

    ok, rvec, tvec = cv2.solvePnP(
        object_points,
        image_points_np,
        camera_matrix,
        dist_coeffs,
        flags=cv2.SOLVEPNP_ITERATIVE,
    )
    if not ok:
        return None
    if float(tvec[2]) < 0.0:
        tvec = -tvec
        rvec = -rvec
    return tvec.reshape(3)


def _extract_odom_pose(payload: Any) -> Optional[OdomPose]:
    if not isinstance(payload, dict):
        return None
    values = payload.get("value") if isinstance(payload.get("value"), dict) else payload
    if not isinstance(values, dict):
        return None

    def _first_float(mapping: dict[str, Any], keys: tuple[str, ...]) -> Optional[float]:
        for key in keys:
            try:
                value = mapping.get(key)
                if value is None:
                    continue
                return float(value)
            except (TypeError, ValueError):
                continue
        return None

    x_mm = _first_float(values, ("x_mm", "x"))
    y_mm = _first_float(values, ("y_mm", "y"))
    heading_rad = _first_float(values, ("heading_rad", "theta_rad", "heading"))
    heading_deg = _first_float(values, ("heading_deg", "theta_deg"))
    if heading_rad is None and heading_deg is not None:
        heading_rad = math.radians(float(heading_deg))
    if x_mm is None or y_mm is None or heading_rad is None:
        return None

    unit_raw = payload.get("unit")
    unit = str(unit_raw).strip().lower() if unit_raw is not None else ""
    if unit in {"m", "meter", "meters"}:
        x_mm *= 1000.0
        y_mm *= 1000.0

    try:
        distance_mm = float(values.get("distance_mm") or values.get("distance") or 0.0)
    except (TypeError, ValueError):
        distance_mm = 0.0
    if unit in {"m", "meter", "meters"}:
        distance_mm *= 1000.0

    try:
        timestamp = float(payload.get("timestamp") or time.time())
    except (TypeError, ValueError):
        timestamp = time.time()

    return OdomPose(
        x_mm=float(x_mm),
        y_mm=float(y_mm),
        heading_rad=_normalize_angle_rad(float(heading_rad)),
        distance_mm=float(distance_mm),
        timestamp=float(timestamp),
    )


def _extract_tag_frame(payload: Any) -> Optional[TagFrame]:
    if not isinstance(payload, dict):
        return None
    detections_raw = payload.get("detections")
    if not isinstance(detections_raw, list):
        return None
    frame_width = int(payload.get("frame_width") or 0)
    frame_height = int(payload.get("frame_height") or 0)
    if frame_width <= 0 or frame_height <= 0:
        return None
    try:
        timestamp = float(payload.get("timestamp") or time.time())
    except (TypeError, ValueError):
        timestamp = time.time()

    detections: list[dict[str, Any]] = []
    for entry in detections_raw:
        if not isinstance(entry, dict):
            continue
        marker_id = entry.get("marker_id")
        corners_norm = entry.get("corners_norm")
        if not isinstance(marker_id, int) or not isinstance(corners_norm, list):
            continue
        detections.append({"marker_id": marker_id, "corners_norm": corners_norm})

    return TagFrame(
        detections=detections,
        frame_width=frame_width,
        frame_height=frame_height,
        timestamp=timestamp,
    )


def _default_log_path(log_dir_raw: str) -> Path:
    stamp = time.strftime("%Y%m%d_%H%M%S")
    log_dir = Path(log_dir_raw).expanduser()
    if not log_dir.is_absolute():
        log_dir = (REPO_ROOT / log_dir).resolve()
    return log_dir / f"apriltag_odom_follow_{stamp}.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Drive toward a target AprilTag using MQTT tag+odom feeds.")
    parser.add_argument("--config", type=str, default="", help="Path to unified runtime config JSON.")
    parser.add_argument("--robot-id", type=str, default="", help="Robot ID.")
    parser.add_argument("--mqtt-host", type=str, default="", help="MQTT broker host.")
    parser.add_argument("--mqtt-port", type=int, default=0, help="MQTT broker port.")
    parser.add_argument("--mqtt-username", type=str, default="", help="MQTT username.")
    parser.add_argument("--mqtt-password", type=str, default="", help="MQTT password.")
    parser.add_argument("--keepalive", type=int, default=0, help="MQTT keepalive seconds.")

    parser.add_argument("--marker-id", type=int, default=13, help="Target marker ID.")
    parser.add_argument("--marker-size", type=float, required=True, help="Marker side length in meters.")
    parser.add_argument("--calibration", type=str, default="camera_calibration.yaml", help="Path to camera calibration YAML.")
    parser.add_argument("--tags-topic", type=str, default="", help="Incoming tag-locations topic override.")
    parser.add_argument("--odom-topic", type=str, default="", help="Incoming wheel-odometry topic override.")

    parser.add_argument("--target-distance", type=float, default=0.6, help="Desired standoff distance in meters.")
    parser.add_argument("--forward-cone-deg", type=float, default=18.0, help="Angle cone for forward/back motion.")
    parser.add_argument("--distance-threshold", type=float, default=0.05, help="Distance deadband in meters.")
    parser.add_argument("--standoff-hysteresis-m", type=float, default=0.03, help="Extra hold band around standoff to prevent oscillation.")
    parser.add_argument("--vision-forward-kp", type=float, default=2.0, help="Forward proportional gain while centered on visible tag.")
    parser.add_argument("--vision-reverse-kp", type=float, default=2.0, help="Reverse proportional gain while centered on visible tag.")
    parser.add_argument("--vision-forward-floor-distance-m", type=float, default=0.20, help="Distance-above-threshold where forward min-power floor is applied.")
    parser.add_argument("--turn-power", type=float, default=0.5, help="Vision turn command power.")
    parser.add_argument("--drive-power", type=float, default=0.8, help="Vision reverse power if too close.")
    parser.add_argument("--forward-min-power", type=float, default=0.8, help="Min forward power while approaching.")
    parser.add_argument("--forward-max-power", type=float, default=1.0, help="Max forward power while approaching.")
    parser.add_argument("--turn-pulse", type=float, default=0.12, help="Seconds to apply turn/search pulse.")
    parser.add_argument("--turn-cooldown", type=float, default=0.08, help="Seconds between turn pulses.")
    parser.add_argument("--search-reverse-after", type=float, default=0.0, help="Seconds to reverse search direction.")
    parser.add_argument("--search-pause", type=float, default=0.2, help="Seconds to pause between search pulses.")
    parser.add_argument("--search-turn-multiplier", type=float, default=1.5, help="Multiplier on turn-power while searching.")
    parser.add_argument("--lost-timeout", type=float, default=0.7, help="Seconds since last seen before search mode.")
    parser.add_argument("--tag-timeout", type=float, default=0.35, help="Max age for tag-location MQTT frame in seconds.")
    parser.add_argument("--min-goal-advance-m", type=float, default=0.10, help="Minimum forward advance (m) required to create an odom goal.")
    parser.add_argument("--rate", type=float, default=10.0, help="Control loop rate (Hz).")

    parser.add_argument("--odom-goal-tolerance-m", type=float, default=0.06, help="Position tolerance (m) for odom goal reached.")
    parser.add_argument("--goal-hold-s", type=float, default=0.4, help="Seconds to hold still after reaching odom goal before search.")
    parser.add_argument("--odom-heading-kp", type=float, default=1.5, help="Heading gain for odom steering.")
    parser.add_argument("--odom-distance-kp", type=float, default=1.4, help="Distance gain for odom drive.")
    parser.add_argument("--odom-turn-power", type=float, default=0.6, help="Max turn command in odom mode.")
    parser.add_argument("--odom-min-drive-power", type=float, default=0.35, help="Min forward command in odom mode.")
    parser.add_argument("--odom-max-drive-power", type=float, default=0.9, help="Max forward command in odom mode.")
    parser.add_argument("--odom-brake-distance-m", type=float, default=0.25, help="Within this distance, remove odom min-drive floor to brake smoothly.")
    parser.add_argument("--odom-heading-slow-deg", type=float, default=40.0, help="Heading error threshold to scale forward speed.")

    # Backward-compatible no-op args still present in schema/config.
    parser.add_argument("--map-pose-topic", type=str, default="", help="Unused compatibility argument.")
    parser.add_argument("--min-wheel-pwm", type=float, default=0.3, help="Unused compatibility argument.")

    parser.add_argument("--log-file", type=str, default="", help="CSV path for control logs.")
    parser.add_argument("--log-dir", type=str, default="autonomy/apriltag-odom-follow/logs", help="Directory for auto-named CSV logs.")
    parser.add_argument("--log-flush-lines", type=int, default=10, help="Flush log file every N rows.")
    parser.add_argument("--preview", action="store_true", help="Compatibility flag (unused in MQTT-consumer mode).")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        runtime_cfg, cfg_path = _load_runtime_config(args.config)
    except RuntimeError as exc:
        print(exc, file=sys.stderr)
        return 1

    services_cfg = runtime_cfg.get("services") if isinstance(runtime_cfg.get("services"), dict) else {}
    local_mqtt_cfg = runtime_cfg.get("local_mqtt") if isinstance(runtime_cfg.get("local_mqtt"), dict) else {}
    robot_cfg = runtime_cfg.get("robot") if isinstance(runtime_cfg.get("robot"), dict) else {}

    system = str(robot_cfg.get("system") or "pebble").strip() or "pebble"
    robot_type = str(robot_cfg.get("type") or "robots").strip() or "robots"
    robot_id = args.robot_id or str(robot_cfg.get("id") or "").strip()
    if not robot_id:
        print("robot.id is required in config (or pass --robot-id).", file=sys.stderr)
        return 1

    calibration_path = Path(args.calibration).expanduser()
    if not calibration_path.is_absolute():
        candidates = [
            (Path.cwd() / calibration_path).resolve(),
            (Path(__file__).parent / calibration_path).resolve(),
            (cfg_path.parent / calibration_path).resolve(),
        ]
        for candidate in candidates:
            if candidate.exists():
                calibration_path = candidate
                break
        else:
            calibration_path = candidates[0]

    try:
        base_camera_matrix, dist_coeffs, calibration_size = load_calibration(str(calibration_path))
    except RuntimeError as exc:
        print(exc, file=sys.stderr)
        return 1

    mqtt_cfg = {"tls": local_mqtt_cfg.get("tls") if isinstance(local_mqtt_cfg.get("tls"), dict) else {"enabled": False}}
    host = args.mqtt_host or str(local_mqtt_cfg.get("host") or "127.0.0.1")
    if not host:
        print("MQTT host required via --mqtt-host or local_mqtt.host.", file=sys.stderr)
        return 1
    port = args.mqtt_port or int(local_mqtt_cfg.get("port") or 1883)
    keepalive = args.keepalive or int(local_mqtt_cfg.get("keepalive") or 60)
    username = args.mqtt_username or str(local_mqtt_cfg.get("username") or "")
    password = args.mqtt_password or str(local_mqtt_cfg.get("password") or "")

    base_topic = f"{system}/{robot_type}/{robot_id}"
    drive_topic = f"{base_topic}/incoming/drive-values"
    apriltag_topic = f"{base_topic}/outgoing/apriltag-data"
    tags_topic = args.tags_topic.strip() or f"{base_topic}/outgoing/apriltag-locations"
    odom_topic = args.odom_topic.strip() or f"{base_topic}/outgoing/wheel-odometry"

    log_path = Path(args.log_file).expanduser() if args.log_file else _default_log_path(args.log_dir)
    if not log_path.is_absolute():
        log_path = (REPO_ROOT / log_path).resolve()
    logger = CsvDecisionLogger(log_path, flush_every=max(1, int(args.log_flush_lines)))
    print(f"[apriltag-odom-follow] logging decisions to {log_path}", file=sys.stderr)

    state_lock = threading.Lock()
    latest_odom: Optional[OdomPose] = None
    latest_tags: Optional[TagFrame] = None

    def _on_connect(client: mqtt.Client, _userdata: Any, _flags: dict[str, Any], rc: int) -> None:
        if rc != 0:
            return
        client.subscribe(odom_topic, qos=0)
        client.subscribe(tags_topic, qos=0)

    def _on_message(_client: mqtt.Client, _userdata: Any, msg: mqtt.MQTTMessage) -> None:
        nonlocal latest_odom, latest_tags
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
        except json.JSONDecodeError:
            return

        if msg.topic == odom_topic:
            pose = _extract_odom_pose(payload)
            if pose is None:
                return
            with state_lock:
                latest_odom = pose
            return

        if msg.topic == tags_topic:
            frame = _extract_tag_frame(payload)
            if frame is None:
                return
            with state_lock:
                latest_tags = frame

    client = mqtt.Client()
    client.on_connect = _on_connect
    client.on_message = _on_message
    if username:
        client.username_pw_set(username, password or None)
    _apply_tls(client, mqtt_cfg, cfg_path.parent)

    try:
        client.connect(host, port, keepalive)
    except Exception as exc:
        print(f"Failed to connect to MQTT broker: {exc}", file=sys.stderr)
        logger.close()
        return 1
    client.loop_start()

    loop_interval = 1.0 / max(1.0, float(args.rate))
    last_publish = 0.0
    publish_seq = 0

    turn_active_until = 0.0
    turn_cooldown_until = 0.0
    turn_direction = 0.0

    search_active_until = 0.0
    search_pause_until = 0.0
    search_turn_power = float(args.turn_power) * max(0.0, float(args.search_turn_multiplier))
    last_seen_dir = 1.0
    last_reverse_time = 0.0

    last_seen_time = 0.0
    goal: Optional[OdomGoal] = None
    goal_reached = False
    goal_reached_at = 0.0
    standoff_hold_active = False

    odom_goal_tol_mm = max(1.0, float(args.odom_goal_tolerance_m) * 1000.0)
    odom_heading_slow_rad = math.radians(max(1.0, float(args.odom_heading_slow_deg)))
    odom_brake_distance_m = max(0.0, float(args.odom_brake_distance_m))
    min_goal_advance_m = max(0.0, float(args.min_goal_advance_m))
    goal_hold_s = max(0.0, float(args.goal_hold_s))

    camera_matrix_cache: dict[tuple[int, int], np.ndarray] = {}

    def _camera_for_dims(w: int, h: int) -> np.ndarray:
        key = (int(w), int(h))
        cached = camera_matrix_cache.get(key)
        if cached is not None:
            return cached
        scaled = scale_camera_matrix(base_camera_matrix, calibration_size, key)
        camera_matrix_cache[key] = scaled
        return scaled

    def _snapshot_state() -> tuple[Optional[OdomPose], Optional[TagFrame]]:
        with state_lock:
            od = None if latest_odom is None else OdomPose(
                x_mm=float(latest_odom.x_mm),
                y_mm=float(latest_odom.y_mm),
                heading_rad=float(latest_odom.heading_rad),
                distance_mm=float(latest_odom.distance_mm),
                timestamp=float(latest_odom.timestamp),
            )
            tg = None if latest_tags is None else TagFrame(
                detections=list(latest_tags.detections),
                frame_width=int(latest_tags.frame_width),
                frame_height=int(latest_tags.frame_height),
                timestamp=float(latest_tags.timestamp),
            )
            return od, tg

    def _plan_goal_from_pose(pose_now: OdomPose, tvec: np.ndarray) -> Optional[OdomGoal]:
        distance_m = float(np.linalg.norm(tvec))
        advance_m = max(0.0, distance_m - float(args.target_distance))
        if advance_m <= max(float(args.distance_threshold), min_goal_advance_m):
            return None

        lateral_right = float(tvec[0])
        forward = float(tvec[2])
        norm = math.hypot(lateral_right, forward)
        if norm < 1e-6:
            return None

        unit_right = lateral_right / norm
        unit_forward = forward / norm
        advance_mm = advance_m * 1000.0
        local_right_mm = unit_right * advance_mm
        local_forward_mm = unit_forward * advance_mm

        h = float(pose_now.heading_rad)
        world_dx = (local_forward_mm * math.cos(h)) + (local_right_mm * math.sin(h))
        world_dy = (local_forward_mm * math.sin(h)) - (local_right_mm * math.cos(h))
        return OdomGoal(
            x_mm=float(pose_now.x_mm) + world_dx,
            y_mm=float(pose_now.y_mm) + world_dy,
            created_at=time.time(),
        )

    try:
        while True:
            now = time.time()
            odom_pose, tag_frame = _snapshot_state()

            command_x = 0.0
            command_z = 0.0
            mode = "idle"
            reason = "no_target"
            tag_visible = False
            distance_m: Optional[float] = None
            angle_rad: Optional[float] = None

            apriltag_payload = {
                "detected": False,
                "marker_id": int(args.marker_id),
                "corners_norm": [],
                "distance_m": None,
                "angle_rad": None,
                "timestamp": now,
            }

            target_detection: Optional[dict[str, Any]] = None
            if tag_frame is not None and (now - float(tag_frame.timestamp)) <= float(args.tag_timeout):
                for det in tag_frame.detections:
                    if int(det.get("marker_id", -1)) == int(args.marker_id):
                        target_detection = det
                        break

            tvec: Optional[np.ndarray] = None
            if target_detection is not None:
                cam = _camera_for_dims(tag_frame.frame_width, tag_frame.frame_height)
                tvec = _estimate_pose_from_norm_corners(
                    target_detection.get("corners_norm", []),
                    float(args.marker_size),
                    cam,
                    dist_coeffs,
                    int(tag_frame.frame_width),
                    int(tag_frame.frame_height),
                )

            if tvec is not None:
                tag_visible = True
                distance_m = float(np.linalg.norm(tvec))
                z_component = float(tvec[2]) if abs(float(tvec[2])) > 1e-6 else 1e-6
                angle_rad = float(math.atan2(float(tvec[0]), z_component))

                last_seen_time = now
                last_reverse_time = now
                last_seen_dir = -1.0 if angle_rad < 0 else 1.0
                goal_reached = False
                if odom_pose is not None:
                    goal = _plan_goal_from_pose(odom_pose, tvec)

                apriltag_payload = {
                    "detected": True,
                    "marker_id": int(args.marker_id),
                    "corners_norm": list(target_detection.get("corners_norm") or []),
                    "distance_m": distance_m,
                    "angle_rad": angle_rad,
                    "timestamp": now,
                }

                centered = abs(angle_rad) <= math.radians(args.forward_cone_deg)
                mode = "vision"
                if not centered:
                    standoff_hold_active = False
                    desired_dir = -1.0 if angle_rad < 0 else 1.0
                    if now >= turn_active_until and now >= turn_cooldown_until:
                        turn_direction = desired_dir
                        turn_active_until = now + max(0.0, float(args.turn_pulse))
                        turn_cooldown_until = turn_active_until + max(0.0, float(args.turn_cooldown))
                    if now < turn_active_until and turn_direction != 0.0:
                        command_x = turn_direction * float(args.turn_power)
                        reason = "vision_turn"
                    else:
                        command_x = 0.0
                        reason = "vision_turn_cooldown"
                    command_z = 0.0
                else:
                    turn_direction = 0.0
                    err = float(distance_m) - float(args.target_distance)
                    threshold = float(args.distance_threshold)
                    hysteresis = max(0.0, float(args.standoff_hysteresis_m))
                    hold_band = threshold + hysteresis if standoff_hold_active else threshold
                    if abs(err) <= hold_band:
                        if abs(err) <= threshold:
                            standoff_hold_active = True
                        command_x = 0.0
                        command_z = 0.0
                        reason = "vision_hold_standoff"
                    elif err > 0.0:
                        standoff_hold_active = False
                        cone = math.radians(float(args.forward_cone_deg))
                        steer_ratio = 0.0 if cone <= 0 else clamp(float(angle_rad) / cone, -1.0, 1.0)
                        far_err_m = max(0.0, err - threshold)
                        forward_cmd = clamp(
                            far_err_m * float(args.vision_forward_kp),
                            0.0,
                            float(args.forward_max_power),
                        )
                        if far_err_m >= float(args.vision_forward_floor_distance_m):
                            forward_cmd = max(forward_cmd, float(args.forward_min_power))
                        min_power = min(float(args.forward_min_power), float(args.forward_max_power))
                        max_power = max(float(args.forward_min_power), float(args.forward_max_power))
                        if max_power > min_power and forward_cmd > 0.0:
                            span = max_power - min_power
                            steer_gain = clamp((forward_cmd - min_power) / span, 0.0, 1.0)
                        else:
                            steer_gain = 0.5
                        steer = steer_ratio * (0.35 + (0.65 * steer_gain)) * float(args.turn_power)
                        command_x = steer
                        command_z = forward_cmd
                        reason = "vision_drive_forward"
                    else:
                        standoff_hold_active = False
                        near_err_m = max(0.0, abs(err) - threshold)
                        reverse_cmd = clamp(
                            near_err_m * float(args.vision_reverse_kp),
                            0.0,
                            float(args.drive_power),
                        )
                        if reverse_cmd <= 1e-4:
                            command_x = 0.0
                            command_z = 0.0
                            reason = "vision_hold_standoff"
                        else:
                            command_x = 0.0
                            command_z = -reverse_cmd
                            reason = "vision_back_off"

            if not tag_visible:
                standoff_hold_active = False
                age_s = now - last_seen_time
                if goal is not None and odom_pose is not None and not goal_reached:
                    dx = float(goal.x_mm) - float(odom_pose.x_mm)
                    dy = float(goal.y_mm) - float(odom_pose.y_mm)
                    dist_err_mm = math.hypot(dx, dy)
                    if dist_err_mm <= odom_goal_tol_mm:
                        goal_reached = True
                        goal_reached_at = now
                        goal = None
                        command_x = 0.0
                        command_z = 0.0
                        mode = "odometry"
                        reason = "odom_goal_hold"
                    else:
                        bearing = math.atan2(dy, dx)
                        heading_err = _normalize_angle_rad(bearing - float(odom_pose.heading_rad))
                        turn_cmd = clamp(
                            -float(args.odom_heading_kp) * heading_err,
                            -float(args.odom_turn_power),
                            float(args.odom_turn_power),
                        )
                        dist_err_m = dist_err_mm / 1000.0
                        raw_drive_cmd = float(args.odom_distance_kp) * dist_err_m
                        drive_floor = float(args.odom_min_drive_power) if dist_err_m >= odom_brake_distance_m else 0.0
                        drive_cmd = clamp(raw_drive_cmd, drive_floor, float(args.odom_max_drive_power))
                        if abs(heading_err) > odom_heading_slow_rad:
                            excess = abs(heading_err) - odom_heading_slow_rad
                            span = max(1e-6, math.pi - odom_heading_slow_rad)
                            heading_scale = clamp(1.0 - (excess / span), 0.2, 1.0)
                            drive_cmd *= heading_scale
                        command_x = turn_cmd
                        command_z = drive_cmd
                        mode = "odometry"
                        reason = "odom_drive_to_goal"

                goal_hold_elapsed = (now - goal_reached_at) if goal_reached else float("inf")
                if (goal is None or goal_reached) and age_s > float(args.lost_timeout) and goal_hold_elapsed >= goal_hold_s:
                    if float(args.search_reverse_after) > 0.0 and (now - last_reverse_time > float(args.search_reverse_after)):
                        last_seen_dir *= -1.0
                        last_reverse_time = now
                    if now >= search_active_until and now >= search_pause_until:
                        search_active_until = now + max(0.0, float(args.turn_pulse))
                        search_pause_until = search_active_until + max(0.0, float(args.search_pause))
                    if now < search_active_until:
                        command_x = last_seen_dir * search_turn_power
                        command_z = 0.0
                        mode = "search"
                        reason = "search_scan"
                    else:
                        command_x = 0.0
                        command_z = 0.0
                        mode = "search"
                        reason = "search_pause"
                elif mode == "idle":
                    mode = "hold"
                    reason = "lost_wait"

            if now - last_publish >= loop_interval:
                publish_seq += 1
                client.publish(drive_topic, json.dumps({"x": float(command_x), "z": float(command_z)}), qos=1)
                client.publish(apriltag_topic, json.dumps(apriltag_payload), qos=0)

                goal_dist_m = None
                goal_heading_err_deg = None
                if goal is not None and odom_pose is not None:
                    gdx = float(goal.x_mm) - float(odom_pose.x_mm)
                    gdy = float(goal.y_mm) - float(odom_pose.y_mm)
                    goal_dist_m = math.hypot(gdx, gdy) / 1000.0
                    gbearing = math.atan2(gdy, gdx)
                    goal_heading_err_deg = math.degrees(_normalize_angle_rad(gbearing - float(odom_pose.heading_rad)))

                logger.write(
                    {
                        "ts": f"{now:.6f}",
                        "mode": mode,
                        "reason": reason,
                        "tag_visible": int(tag_visible),
                        "cmd_x": f"{float(command_x):.6f}",
                        "cmd_z": f"{float(command_z):.6f}",
                        "vision_distance_m": "" if distance_m is None else f"{float(distance_m):.6f}",
                        "vision_angle_deg": "" if angle_rad is None else f"{math.degrees(float(angle_rad)):.4f}",
                        "odom_x_mm": "" if odom_pose is None else f"{float(odom_pose.x_mm):.3f}",
                        "odom_y_mm": "" if odom_pose is None else f"{float(odom_pose.y_mm):.3f}",
                        "odom_heading_deg": "" if odom_pose is None else f"{math.degrees(float(odom_pose.heading_rad)):.4f}",
                        "odom_distance_mm": "" if odom_pose is None else f"{float(odom_pose.distance_mm):.3f}",
                        "goal_active": int(goal is not None),
                        "goal_x_mm": "" if goal is None else f"{float(goal.x_mm):.3f}",
                        "goal_y_mm": "" if goal is None else f"{float(goal.y_mm):.3f}",
                        "goal_distance_m": "" if goal_dist_m is None else f"{float(goal_dist_m):.6f}",
                        "goal_heading_error_deg": "" if goal_heading_err_deg is None else f"{float(goal_heading_err_deg):.4f}",
                        "publish_seq": int(publish_seq),
                    }
                )
                last_publish = now

            time.sleep(max(0.0, loop_interval * 0.25))
    except KeyboardInterrupt:
        pass
    finally:
        try:
            client.publish(drive_topic, json.dumps({"x": 0.0, "z": 0.0}), qos=1)
            client.publish(
                apriltag_topic,
                json.dumps(
                    {
                        "detected": False,
                        "marker_id": int(args.marker_id),
                        "corners_norm": [],
                        "distance_m": None,
                        "angle_rad": None,
                        "timestamp": time.time(),
                    }
                ),
                qos=0,
            )
        except Exception:
            pass
        client.loop_stop()
        client.disconnect()
        logger.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
