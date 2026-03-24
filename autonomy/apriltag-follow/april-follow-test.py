#!/usr/bin/env python3
"""
Follow AprilTag ID 13 by centering the tag in view, then driving toward it.

Publishes drive commands to:
  pebble/robots/<id>/incoming/drive-values
"""
import argparse
import json
import math
import ssl
import sys
import time
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


def _resolve_path(path_value: str, base_dir: Path) -> Path:
    path = Path(path_value).expanduser()
    if not path.is_absolute():
        path = (base_dir / path).resolve()
    return path


def _estimate_pose(marker_corners: np.ndarray, marker_size: float, camera_matrix: np.ndarray, dist_coeffs: np.ndarray):
    image_points = np.asarray(marker_corners, dtype=np.float32).reshape(4, 2)
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
    # Use ITERATIVE to match legacy ArUco follow behavior more closely.
    # Some builds return ambiguous/unstable front/back solutions with IPPE_SQUARE.
    solve_flag = cv2.SOLVEPNP_ITERATIVE
    ok, rvec, tvec = cv2.solvePnP(object_points, image_points, camera_matrix, dist_coeffs, flags=solve_flag)
    if not ok:
        return None
    if float(tvec[2]) < 0.0:
        # Keep the camera-forward convention stable for steering (`atan2(x, z)`).
        tvec = -tvec
        rvec = -rvec
    return rvec, tvec


def _create_detector_params():
    if hasattr(cv2.aruco, "DetectorParameters_create"):
        return cv2.aruco.DetectorParameters_create()
    return cv2.aruco.DetectorParameters()


def build_dictionary(name: str):
    name = name.strip().upper()
    if not name.startswith("DICT_"):
        name = "DICT_" + name
    if not hasattr(cv2.aruco, name):
        raise ValueError(f"Unknown dictionary: {name}")
    return cv2.aruco.getPredefinedDictionary(getattr(cv2.aruco, name))


def detect_markers(frame, dictionary, detector_params):
    # Use legacy API for better compatibility on SBC OpenCV builds.
    # Detecting on grayscale also reduces work and avoids color-conversion variance.
    if len(frame.shape) == 3 and frame.shape[2] == 3:
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
    else:
        gray = frame
    return cv2.aruco.detectMarkers(gray, dictionary, parameters=detector_params)


def draw_axes(frame, camera_matrix, dist_coeffs, rvec, tvec, length: float) -> None:
    if hasattr(cv2.aruco, "drawAxis"):
        cv2.aruco.drawAxis(frame, camera_matrix, dist_coeffs, rvec, tvec, length)
    elif hasattr(cv2, "drawFrameAxes"):
        cv2.drawFrameAxes(frame, camera_matrix, dist_coeffs, rvec, tvec, length)


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


def _open_shared_capture(socket_path: str, width: int, height: int, fps: float) -> Optional[cv2.VideoCapture]:
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


def clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Drive the robot to follow AprilTag ID 13."
    )
    parser.add_argument(
        "--camera-profile",
        type=str,
        default="",
        help="Camera profile name from unified runtime config (default: default).",
    )
    parser.add_argument(
        "--config",
        type=str,
        default="",
        help="Path to unified runtime config JSON.",
    )
    parser.add_argument(
        "--video-shm",
        type=str,
        default="",
        help="Path to AV-daemon video shared-memory socket.",
    )
    parser.add_argument("--width", type=int, default=0, help="Expected shared-stream width.")
    parser.add_argument("--height", type=int, default=0, help="Expected shared-stream height.")
    parser.add_argument("--fps", type=float, default=0.0, help="Expected shared-stream FPS.")
    parser.add_argument(
        "--marker-dictionary",
        type=str,
        default="APRILTAG_25H9",
        help="ArUco/AprilTag dictionary name.",
    )
    parser.add_argument("--marker-id", type=int, default=13, help="Marker ID to track.")
    parser.add_argument(
        "--marker-size",
        type=float,
        required=True,
        help="Marker side length in meters.",
    )
    parser.add_argument(
        "--calibration",
        type=str,
        default="camera_calibration.yaml",
        help="Path to camera calibration YAML.",
    )
    parser.add_argument("--robot-id", type=str, default="", help="Robot ID.")
    parser.add_argument("--mqtt-host", type=str, default="", help="MQTT broker host.")
    parser.add_argument("--mqtt-port", type=int, default=0, help="MQTT broker port.")
    parser.add_argument("--mqtt-username", type=str, default="", help="MQTT username.")
    parser.add_argument("--mqtt-password", type=str, default="", help="MQTT password.")
    parser.add_argument("--keepalive", type=int, default=0, help="MQTT keepalive seconds.")
    parser.add_argument(
        "--target-distance",
        type=float,
        default=0.6,
        help="Desired standoff distance in meters.",
    )
    parser.add_argument(
        "--forward-cone-deg",
        type=float,
        default=18.0,
        help="Angle cone in degrees for allowing forward/back motion.",
    )
    parser.add_argument(
        "--distance-threshold",
        type=float,
        default=0.05,
        help="Distance deadband in meters before commanding forward/back.",
    )
    parser.add_argument(
        "--turn-power",
        type=float,
        default=0.5,
        help="Motor power for left/right turns (0.5 recommended).",
    )
    parser.add_argument(
        "--drive-power",
        type=float,
        default=0.8,
        help="Motor power for forward/back drive (0.8 recommended).",
    )
    parser.add_argument(
        "--forward-min-power",
        type=float,
        default=0.8,
        help="Minimum motor power while driving forward toward the tag.",
    )
    parser.add_argument(
        "--forward-max-power",
        type=float,
        default=1.0,
        help="Maximum motor power while driving forward toward the tag.",
    )
    parser.add_argument(
        "--turn-pulse",
        type=float,
        default=0.12,
        help="Seconds to apply a turn command before stopping.",
    )
    parser.add_argument(
        "--turn-cooldown",
        type=float,
        default=0.08,
        help="Seconds to pause between turn pulses.",
    )
    parser.add_argument(
        "--search-reverse-after",
        type=float,
        default=0.0,
        help="Seconds after last sighting to reverse scan direction (<=0 disables reversing).",
    )
    parser.add_argument(
        "--search-pause",
        type=float,
        default=0.2,
        help="Seconds to hold still between scan pulses.",
    )
    parser.add_argument(
        "--search-turn-multiplier",
        type=float,
        default=1.5,
        help="Multiplier applied to --turn-power while searching for a lost tag.",
    )
    parser.add_argument(
        "--lost-timeout",
        type=float,
        default=0.7,
        help="Seconds after last seen to stop.",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=10.0,
        help="Control loop rate (Hz).",
    )
    parser.add_argument(
        "--preview",
        action="store_true",
        help="Show camera preview window with overlays.",
    )
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

    calibration_path_raw = str(args.calibration)
    calibration_path = Path(calibration_path_raw).expanduser()
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

    default_width = int(av_video_cfg.get("width") or 640)
    default_height = int(av_video_cfg.get("height") or 480)
    default_fps = float(av_video_cfg.get("fps") or 15.0)

    stream_socket = str(args.video_shm or av_video_cfg.get("socket_path") or "/tmp/pebble-video.sock")
    stream_width = int(args.width if args.width > 0 else default_width)
    stream_height = int(args.height if args.height > 0 else default_height)
    stream_fps = float(args.fps if args.fps > 0 else default_fps)

    rotation = _extract_rotation({"rotate_degrees": av_video_cfg.get("rotate_degrees")})
    camera_retry = float(av_video_cfg.get("reconnect_seconds") or av_video_cfg.get("input_retry_seconds") or 2.0)
    pose_width = stream_width
    pose_height = stream_height
    if rotation in (90, 270):
        pose_width, pose_height = stream_height, stream_width

    try:
        camera_matrix, dist_coeffs, calibration_size = load_calibration(str(calibration_path))
    except RuntimeError as exc:
        print(exc, file=sys.stderr)
        return 1
    camera_matrix = scale_camera_matrix(camera_matrix, calibration_size, (pose_width, pose_height))

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
    overlays_topic = f"{base_topic}/outgoing/video-overlays"

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
    last_seen_time = 0.0
    last_reverse_time = 0.0
    last_publish = 0.0
    interval = 1.0 / max(1.0, args.rate)
    turn_active_until = 0.0
    turn_cooldown_until = 0.0
    turn_direction = 0.0
    last_seen_dir = 1.0
    search_active_until = 0.0
    search_pause_until = 0.0
    search_turn_power = args.turn_power * max(0.0, args.search_turn_multiplier)

    try:
        while True:
            capture = _open_shared_capture(stream_socket, stream_width, stream_height, stream_fps)
            if capture is None:
                print(
                    f"Shared video source unavailable at {stream_socket}. "
                    "Ensure av_daemon is running. Retrying.",
                    file=sys.stderr,
                )
                time.sleep(camera_retry)
                continue

            try:
                while capture.isOpened():
                    ret, frame = capture.read()
                    if not ret:
                        print("Shared video feed lost; retrying.", file=sys.stderr)
                        break

                    frame = _apply_rotation(frame, rotation)

                    corners, ids, _ = detect_markers(frame, dictionary, detector_params)
                    command_x = 0.0
                    command_z = 0.0
                    centered = False
                    distance = None
                    angle_rad = None
                    apriltag_payload = {
                        "detected": False,
                        "marker_id": int(args.marker_id),
                        "corners_norm": [],
                        "distance_m": None,
                        "angle_rad": None,
                        "timestamp": time.time(),
                    }
                    overlays_payload = {
                        "source": "apriltag-follow",
                        "shapes": [],
                        "frame_width": int(stream_width),
                        "frame_height": int(stream_height),
                        "timestamp": time.time(),
                    }

                    if ids is not None:
                        ids_flat = ids.flatten().tolist()
                        if args.marker_id in ids_flat:
                            idx = ids_flat.index(args.marker_id)
                            marker_corners = corners[idx]
                            if marker_corners is not None:
                                last_seen_time = time.time()
                                last_reverse_time = last_seen_time
                                h, w = frame.shape[:2]
                                pts = marker_corners.reshape(4, 2)
                                center_x = float(np.mean(pts[:, 0]))
                                x_error = (center_x - (w / 2.0)) / (w / 2.0)
                                pose = _estimate_pose(marker_corners, args.marker_size, camera_matrix, dist_coeffs)
                                if pose is not None:
                                    rvec, tvec = pose
                                    tvec_vec = tvec.reshape(3)
                                    distance = float(np.linalg.norm(tvec_vec))
                                    z_component = float(tvec_vec[2]) if abs(float(tvec_vec[2])) > 1e-6 else 1e-6
                                    angle_rad = float(math.atan2(float(tvec_vec[0]), z_component))
                                    centered = abs(angle_rad) <= math.radians(args.forward_cone_deg)
                                    last_seen_dir = -1.0 if angle_rad < 0 else 1.0
                                    apriltag_payload = {
                                        "detected": True,
                                        "marker_id": int(args.marker_id),
                                        "corners_norm": [
                                            [
                                                float(clamp(pt[0] / max(1.0, w), 0.0, 1.0)),
                                                float(clamp(pt[1] / max(1.0, h), 0.0, 1.0)),
                                            ]
                                            for pt in pts
                                        ],
                                        "distance_m": distance,
                                        "angle_rad": angle_rad,
                                        "timestamp": time.time(),
                                    }
                                    overlays_payload = {
                                        "source": "apriltag-follow",
                                        "shapes": [apriltag_payload["corners_norm"]],
                                        "frame_width": int(stream_width),
                                        "frame_height": int(stream_height),
                                        "timestamp": time.time(),
                                    }

                                    if not centered:
                                        desired_dir = -1.0 if angle_rad < 0 else 1.0
                                        now = time.time()
                                        if now >= turn_active_until and now >= turn_cooldown_until:
                                            turn_direction = desired_dir
                                            turn_active_until = now + max(0.0, args.turn_pulse)
                                            turn_cooldown_until = turn_active_until + max(0.0, args.turn_cooldown)
                                        if now < turn_active_until and turn_direction != 0.0:
                                            command_x = turn_direction * args.turn_power
                                        else:
                                            command_x = 0.0
                                        command_z = 0.0
                                    else:
                                        turn_direction = 0.0
                                        error = distance - args.target_distance
                                        if abs(error) <= args.distance_threshold:
                                            command_x = 0.0
                                            command_z = 0.0
                                        elif error > 0:
                                            cone = math.radians(args.forward_cone_deg)
                                            steer_ratio = 0.0 if cone <= 0 else clamp(angle_rad / cone, -1.0, 1.0)
                                            min_power = min(args.forward_min_power, args.forward_max_power)
                                            max_power = max(args.forward_min_power, args.forward_max_power)
                                            base = (min_power + max_power) / 2.0
                                            steer = steer_ratio * (max_power - min_power) / 2.0
                                            command_x = steer
                                            command_z = base
                                        else:
                                            command_x = 0.0
                                            command_z = -args.drive_power

                                    if args.preview:
                                        cv2.aruco.drawDetectedMarkers(frame, corners, ids)
                                        draw_axes(frame, camera_matrix, dist_coeffs, rvec, tvec, 0.05)
                                        angle_deg = math.degrees(angle_rad) if angle_rad is not None else 0.0
                                        status = (
                                            f"x_err={x_error:.2f} angle={angle_deg:.1f}deg dist={distance:.2f}m"
                                        )
                                        cv2.putText(
                                            frame,
                                            status,
                                            (10, 30),
                                            cv2.FONT_HERSHEY_SIMPLEX,
                                            0.7,
                                            (0, 255, 0),
                                            2,
                                        )
                                else:
                                    turn_direction = 0.0
                                    command_x = 0.0
                                    command_z = 0.0
                                    if args.preview:
                                        cv2.aruco.drawDetectedMarkers(frame, corners, ids)
                                        cv2.putText(
                                            frame,
                                            "Pose estimation failed",
                                            (10, 30),
                                            cv2.FONT_HERSHEY_SIMPLEX,
                                            0.7,
                                            (0, 0, 255),
                                            2,
                                        )
                            else:
                                turn_direction = 0.0
                                command_x = 0.0
                                command_z = 0.0
                        elif args.preview and len(corners) > 0:
                            cv2.aruco.drawDetectedMarkers(frame, corners, ids)

                    now = time.time()
                    if now - last_seen_time > args.lost_timeout:
                        if args.search_reverse_after > 0.0 and (now - last_reverse_time > args.search_reverse_after):
                            last_seen_dir *= -1.0
                            last_reverse_time = now
                        if now >= search_active_until and now >= search_pause_until:
                            search_active_until = now + max(0.0, args.turn_pulse)
                            search_pause_until = search_active_until + max(0.0, args.search_pause)
                        if now < search_active_until:
                            command_x = last_seen_dir * search_turn_power
                            command_z = 0.0
                        else:
                            command_x = 0.0
                            command_z = 0.0

                    if now - last_publish >= interval:
                        payload = {"x": float(command_x), "z": float(command_z)}
                        client.publish(drive_topic, json.dumps(payload), qos=1)
                        client.publish(apriltag_topic, json.dumps(apriltag_payload), qos=0)
                        client.publish(overlays_topic, json.dumps(overlays_payload), qos=0)
                        last_publish = now

                    if args.preview:
                        cv2.imshow("AprilTag Follow", frame)
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
            client.publish(
                overlays_topic,
                json.dumps(
                    {
                        "source": "apriltag-follow",
                        "shapes": [],
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
