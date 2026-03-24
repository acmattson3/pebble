#!/usr/bin/env python3
"""Auto-tune wheel odometry parameters using AprilTags 13/14/15."""

from __future__ import annotations

import argparse
import json
import logging
import math
import random
import signal
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import cv2
import numpy as np
import paho.mqtt.client as mqtt

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

from control.common.config import load_config, log_level, service_cfg
from control.common.mqtt import mqtt_auth_and_tls
from control.common.odometry_shm import OdometryRawShmReader
from control.common.topics import identity_from_config

from run import DriveCommandTracker, Pose2D, WheelEventTracker, _normalize_angle_rad


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _build_dictionary(name: str):
    clean = name.strip().upper()
    if not clean.startswith("DICT_"):
        clean = "DICT_" + clean
    if not hasattr(cv2.aruco, clean):
        raise ValueError(f"Unknown dictionary: {clean}")
    return cv2.aruco.getPredefinedDictionary(getattr(cv2.aruco, clean))


def _create_detector_params():
    if hasattr(cv2.aruco, "DetectorParameters_create"):
        return cv2.aruco.DetectorParameters_create()
    return cv2.aruco.DetectorParameters()


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


def _load_calibration(path_value: str) -> tuple[np.ndarray, np.ndarray, Optional[tuple[int, int]]]:
    fs = cv2.FileStorage(path_value, cv2.FILE_STORAGE_READ)
    if not fs.isOpened():
        raise RuntimeError(f"Failed to open calibration file: {path_value}")
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


def _scale_camera_matrix(
    camera_matrix: np.ndarray,
    calibration_size: Optional[tuple[int, int]],
    frame_size: tuple[int, int],
) -> np.ndarray:
    if calibration_size is None:
        return camera_matrix
    cal_w, cal_h = calibration_size
    frame_w, frame_h = frame_size
    if cal_w <= 0 or cal_h <= 0 or frame_w <= 0 or frame_h <= 0:
        return camera_matrix
    if cal_w == frame_w and cal_h == frame_h:
        return camera_matrix
    sx = float(frame_w) / float(cal_w)
    sy = float(frame_h) / float(cal_h)
    scaled = camera_matrix.astype(np.float64).copy()
    scaled[0, 0] *= sx
    scaled[1, 1] *= sy
    scaled[0, 2] *= sx
    scaled[1, 2] *= sy
    return scaled


def _extract_rotation(transform_config: dict[str, Any]) -> Optional[int]:
    rotate = transform_config.get("rotate_degrees")
    if rotate is None:
        return None
    try:
        value = int(rotate) % 360
    except (TypeError, ValueError):
        return None
    if value in (90, 180, 270):
        return value
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


def _estimate_pose(
    marker_corners: np.ndarray, marker_size_m: float, camera_matrix: np.ndarray, dist_coeffs: np.ndarray
) -> Optional[tuple[np.ndarray, np.ndarray]]:
    image_points = np.asarray(marker_corners, dtype=np.float32).reshape(4, 2)
    half = float(marker_size_m) / 2.0
    object_points = np.array(
        [[-half, half, 0.0], [half, half, 0.0], [half, -half, 0.0], [-half, -half, 0.0]],
        dtype=np.float32,
    )
    ok, rvec, tvec = cv2.solvePnP(
        object_points,
        image_points,
        camera_matrix,
        dist_coeffs,
        flags=cv2.SOLVEPNP_ITERATIVE,
    )
    if not ok:
        return None
    if float(tvec[2]) < 0.0:
        tvec = -tvec
        rvec = -rvec
    return rvec, tvec


def _detect_markers(frame: np.ndarray, dictionary, detector_params):
    if len(frame.shape) == 3 and frame.shape[2] == 3:
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
    else:
        gray = frame
    return cv2.aruco.detectMarkers(gray, dictionary, parameters=detector_params)


def _robust_mean(values: list[float]) -> tuple[Optional[float], int, float, float]:
    if not values:
        return None, 0, 0.0, 0.0
    if len(values) < 3:
        sorted_values = sorted(values)
        return float(sum(values) / len(values)), len(values), float(sorted_values[len(sorted_values) // 2]), 0.0
    sorted_values = sorted(values)
    mid = len(sorted_values) // 2
    median = float(sorted_values[mid] if len(sorted_values) % 2 else (sorted_values[mid - 1] + sorted_values[mid]) * 0.5)
    deviations = [abs(v - median) for v in values]
    sorted_dev = sorted(deviations)
    dev_mid = len(sorted_dev) // 2
    mad = float(sorted_dev[dev_mid] if len(sorted_dev) % 2 else (sorted_dev[dev_mid - 1] + sorted_dev[dev_mid]) * 0.5)
    sigma = max(1e-9, 1.4826 * mad)
    keep = [v for v in values if abs(v - median) / sigma <= 3.0]
    if not keep:
        keep = [median]
    return float(sum(keep) / len(keep)), len(keep), median, mad


@dataclass
class VisionPose:
    x_m: float
    z_m: float
    yaw_rad: float
    t_mono: float


@dataclass
class OdomSnapshot:
    t_mono: float
    x_mm: float
    y_mm: float
    heading_rad: float
    left_steps_signed: float
    right_steps_signed: float


class OdomModel:
    """Odometry logic matching wheel-odometry/run.py for tuning-time replay."""

    def __init__(
        self,
        left_mm_per_event: float,
        right_mm_per_event: float,
        track_width_mm: float,
        left_direction_sign: float = 1.0,
        right_direction_sign: float = 1.0,
        signal_alpha: float = 0.30,
        slope_alpha: float = 0.35,
        slope_deadband_adc: float = 0.8,
        confirm_samples: int = 2,
        min_extremum_ms: float = 15.0,
        min_swing_adc: float = 24.0,
        swing_alpha: float = 0.2,
        initial_swing_adc: float = 180.0,
        max_progress: float = 0.98,
        interpolation_timeout_ms: float = 180.0,
        stall_timeout_ms: float = 320.0,
        min_activity_adc: float = 0.8,
        command_deadband: float = 0.08,
        command_hold_ms: float = 350.0,
    ) -> None:
        self.left_mm_per_event = float(left_mm_per_event)
        self.right_mm_per_event = float(right_mm_per_event)
        self.track_width_mm = float(track_width_mm)
        self.left_direction_sign = 1 if float(left_direction_sign) >= 0 else -1
        self.right_direction_sign = 1 if float(right_direction_sign) >= 0 else -1

        self.interpolation_timeout_ns = int(max(0.0, interpolation_timeout_ms) * 1_000_000.0)
        self.stall_timeout_ns = int(max(0.0, stall_timeout_ms) * 1_000_000.0)
        self.min_activity_adc = max(0.0, float(min_activity_adc))

        self.left_tracker = WheelEventTracker(
            signal_alpha=float(signal_alpha),
            slope_alpha=float(slope_alpha),
            slope_deadband_adc=float(slope_deadband_adc),
            confirm_samples=max(1, int(confirm_samples)),
            min_extremum_ns=max(0, int(min_extremum_ms * 1_000_000.0)),
            min_swing_adc=max(0.0, float(min_swing_adc)),
            swing_alpha=_clamp(float(swing_alpha), 0.0, 1.0),
            initial_swing_adc=max(1.0, float(initial_swing_adc)),
            max_progress=_clamp(float(max_progress), 0.0, 0.999),
        )
        self.right_tracker = WheelEventTracker(
            signal_alpha=float(signal_alpha),
            slope_alpha=float(slope_alpha),
            slope_deadband_adc=float(slope_deadband_adc),
            confirm_samples=max(1, int(confirm_samples)),
            min_extremum_ns=max(0, int(min_extremum_ms * 1_000_000.0)),
            min_swing_adc=max(0.0, float(min_swing_adc)),
            swing_alpha=_clamp(float(swing_alpha), 0.0, 1.0),
            initial_swing_adc=max(1.0, float(initial_swing_adc)),
            max_progress=_clamp(float(max_progress), 0.0, 0.999),
        )
        self.cmd_tracker = DriveCommandTracker(
            deadband=float(command_deadband),
            hold_seconds=max(0.0, float(command_hold_ms) / 1000.0),
        )

        self.pose = Pose2D()
        self.left_steps_total = 0.0
        self.right_steps_total = 0.0
        self.left_steps_signed_total = 0.0
        self.right_steps_signed_total = 0.0
        self.last_left_event_mono_ns: Optional[int] = None
        self.last_right_event_mono_ns: Optional[int] = None
        self.left_stalled = False
        self.right_stalled = False

    def set_params(self, left_mm_per_event: float, right_mm_per_event: float, track_width_mm: float) -> None:
        self.left_mm_per_event = float(left_mm_per_event)
        self.right_mm_per_event = float(right_mm_per_event)
        self.track_width_mm = float(track_width_mm)

    def set_command(self, x: float, z: float) -> None:
        self.cmd_tracker.update_from_payload({"x": float(x), "z": float(z)})

    def process_sample(self, sample: dict[str, Any], left_channel: str = "a0", right_channel: str = "a1") -> None:
        mono_ns = int(sample.get("mono_ns") or 0)
        if mono_ns <= 0:
            return
        left_value = float(sample.get(left_channel, 0.0))
        right_value = float(sample.get(right_channel, 0.0))

        now_mono = time.monotonic()
        left_sign, right_sign, left_cmd_value, right_cmd_value = self.cmd_tracker.wheel_state(now_mono)

        left_allow_interpolation = (
            left_sign != 0
            and self.last_left_event_mono_ns is not None
            and (mono_ns - self.last_left_event_mono_ns) <= self.interpolation_timeout_ns
        )
        right_allow_interpolation = (
            right_sign != 0
            and self.last_right_event_mono_ns is not None
            and (mono_ns - self.last_right_event_mono_ns) <= self.interpolation_timeout_ns
        )

        left_step = self.left_tracker.update(left_value, mono_ns, left_allow_interpolation)
        right_step = self.right_tracker.update(right_value, mono_ns, right_allow_interpolation)

        left_stalled = (
            left_sign != 0
            and self.last_left_event_mono_ns is not None
            and (mono_ns - self.last_left_event_mono_ns) > self.stall_timeout_ns
        )
        right_stalled = (
            right_sign != 0
            and self.last_right_event_mono_ns is not None
            and (mono_ns - self.last_right_event_mono_ns) > self.stall_timeout_ns
        )
        self.left_stalled = bool(left_stalled and abs(left_cmd_value) > 0.0)
        self.right_stalled = bool(right_stalled and abs(right_cmd_value) > 0.0)

        left_step_total = 0.0
        right_step_total = 0.0
        if left_step.event_count > 0:
            self.last_left_event_mono_ns = mono_ns
            left_step_total += float(left_step.event_count)
        if right_step.event_count > 0:
            self.last_right_event_mono_ns = mono_ns
            right_step_total += float(right_step.event_count)
        if left_sign != 0 and not left_stalled and left_step.activity >= self.min_activity_adc:
            left_step_total += max(0.0, float(left_step.progress_delta))
        if right_sign != 0 and not right_stalled and right_step.activity >= self.min_activity_adc:
            right_step_total += max(0.0, float(right_step.progress_delta))
        if left_sign == 0:
            left_step_total = 0.0
        if right_sign == 0:
            right_step_total = 0.0

        left_delta = left_step_total * self.left_mm_per_event * float(left_sign) * float(self.left_direction_sign)
        right_delta = right_step_total * self.right_mm_per_event * float(right_sign) * float(self.right_direction_sign)

        self.left_steps_total += left_step_total
        self.right_steps_total += right_step_total
        self.left_steps_signed_total += left_step_total * float(left_sign) * float(self.left_direction_sign)
        self.right_steps_signed_total += right_step_total * float(right_sign) * float(self.right_direction_sign)
        self.pose.integrate(left_delta, right_delta, self.track_width_mm)

    def snapshot(self) -> OdomSnapshot:
        return OdomSnapshot(
            t_mono=time.monotonic(),
            x_mm=float(self.pose.x_mm),
            y_mm=float(self.pose.y_mm),
            heading_rad=float(self.pose.heading_rad),
            left_steps_signed=float(self.left_steps_signed_total),
            right_steps_signed=float(self.right_steps_signed_total),
        )


class AutoTuner:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.config, self.config_path = load_config(args.config)
        self.identity = identity_from_config(self.config)
        self.service_cfg = service_cfg(self.config, "serial_mcu_bridge")
        self.local_mqtt_cfg = self.config.get("local_mqtt") if isinstance(self.config.get("local_mqtt"), dict) else {}

        self.host = str(self.local_mqtt_cfg.get("host") or "127.0.0.1")
        self.port = int(self.local_mqtt_cfg.get("port") or 1883)
        self.keepalive = int(self.local_mqtt_cfg.get("keepalive") or 60)
        topics_cfg = self.service_cfg.get("topics") if isinstance(self.service_cfg.get("topics"), dict) else {}
        self.drive_topic = str(topics_cfg.get("drive_values") or self.identity.topic("incoming", "drive-values"))

        self.shm_name = (
            str(args.shm_name).strip()
            if str(args.shm_name).strip()
            else f"{self.identity.system}_{self.identity.type}_{self.identity.robot_id}_odometry_raw"
        )
        self.left_channel = str(args.left_channel).lower()
        self.right_channel = str(args.right_channel).lower()
        if self.left_channel not in {"a0", "a1", "a2"} or self.right_channel not in {"a0", "a1", "a2"}:
            raise SystemExit("--left-channel/--right-channel must be one of a0,a1,a2")

        self.capture: Optional[cv2.VideoCapture] = None
        self.reader: Optional[OdometryRawShmReader] = None
        self.client: Optional[mqtt.Client] = None
        self.last_seq = 0
        self.stop_event = False

        self.dictionary = _build_dictionary(args.marker_dictionary)
        self.detector_params = _create_detector_params()

        av_cfg = service_cfg(self.config, "av_daemon")
        video_cfg = av_cfg.get("video") if isinstance(av_cfg.get("video"), dict) else {}
        self.stream_socket = str(args.video_shm or video_cfg.get("socket_path") or "/tmp/pebble-video.sock")
        self.stream_width = int(args.width if args.width > 0 else int(video_cfg.get("width") or 1280))
        self.stream_height = int(args.height if args.height > 0 else int(video_cfg.get("height") or 720))
        self.stream_fps = float(args.fps if args.fps > 0 else float(video_cfg.get("fps") or 15.0))
        self.camera_retry = float(video_cfg.get("reconnect_seconds") or video_cfg.get("input_retry_seconds") or 2.0)
        self.rotation = _extract_rotation({"rotate_degrees": video_cfg.get("rotate_degrees")})

        cal_path = Path(args.calibration).expanduser()
        if not cal_path.is_absolute():
            cal_path = (Path.cwd() / cal_path).resolve()
        camera_matrix, dist_coeffs, calibration_size = _load_calibration(str(cal_path))
        pose_width = self.stream_width
        pose_height = self.stream_height
        if self.rotation in (90, 270):
            pose_width, pose_height = self.stream_height, self.stream_width
        self.camera_matrix = _scale_camera_matrix(camera_matrix, calibration_size, (pose_width, pose_height))
        self.dist_coeffs = dist_coeffs

        self.tag_sizes_m = {
            int(args.tag13_id): float(args.tag13_size_m),
            int(args.tag14_id): float(args.tag14_size_m),
            int(args.tag15_id): float(args.tag15_size_m),
        }
        self.tag13_id = int(args.tag13_id)
        self.tag14_id = int(args.tag14_id)
        self.tag15_id = int(args.tag15_id)
        self.power_min = _clamp(float(args.power_min), 0.0, 1.0)
        self.power_max = _clamp(float(args.power_max), self.power_min, 1.0)
        self.turn_power = _clamp(float(args.turn_power), self.power_min, self.power_max)
        self.drive_power = _clamp(float(args.drive_power), self.power_min, self.power_max)
        self.search_power = _clamp(float(args.search_power), self.power_min, self.power_max)
        self.turn_pulse_s = max(0.05, float(args.turn_pulse_s))
        self.min_motion_pulse_s = max(0.02, float(args.min_motion_pulse_s))
        self.min_motion_pulse_s = min(self.min_motion_pulse_s, self.turn_pulse_s)
        self.turn_pause_s = max(0.05, float(args.turn_pause_s))
        self.publish_rate_hz = max(4.0, float(args.publish_rate_hz))
        self.frame_rate_limit_hz = max(5.0, float(args.frame_rate_limit_hz))
        self.lost_timeout_s = max(0.2, float(args.lost_timeout_s))
        self.side_tag_blind_grace_s = max(0.0, float(args.side_tag_blind_grace_s))
        self.visibility_recovery_cooldown_s = max(0.0, float(args.visibility_recovery_cooldown_s))
        self.recenter_search_max_s = max(0.2, float(args.recenter_search_max_s))
        self.motion_deadband = max(0.0, float(args.motion_deadband))
        self.trial_loss_grace_s = max(0.0, float(args.trial_loss_grace_s))
        self.trial_turn_min_deg = max(5.0, float(args.trial_turn_min_deg))
        self.trial_turn_max_deg = max(self.trial_turn_min_deg, float(args.trial_turn_max_deg))
        self.trial_single_min_deg = max(5.0, float(args.trial_single_min_deg))
        self.trial_single_max_deg = max(self.trial_single_min_deg, float(args.trial_single_max_deg))
        self.cone_half_angle_deg = max(5.0, float(args.cone_half_angle_deg))
        self.cone_half_angle_rad = math.radians(self.cone_half_angle_deg)
        self.max_forward_back_m = max(0.05, float(args.max_forward_back_m))
        self.trial_straight_min_mm = max(20.0, float(args.trial_straight_min_mm))
        self.trial_straight_max_mm = max(self.trial_straight_min_mm, float(args.trial_straight_max_mm))
        self.min_turn_vis_deg = max(1.0, float(args.min_turn_vis_deg))
        self.min_turn_dist_diff_mm = max(1.0, float(args.min_turn_dist_diff_mm))
        self.min_turn_step_events = max(0.5, float(args.min_turn_step_events))
        self.min_single_vis_deg = max(1.0, float(args.min_single_vis_deg))
        self.min_single_step_events = max(0.5, float(args.min_single_step_events))
        self.min_straight_vis_mm = max(1.0, float(args.min_straight_vis_mm))
        self.min_straight_odom_mm = max(1.0, float(args.min_straight_odom_mm))
        self.straight_scale_min = max(0.01, float(args.straight_scale_min))
        self.straight_scale_max = max(self.straight_scale_min, float(args.straight_scale_max))
        self.trial_power_jitter = _clamp(float(args.trial_power_jitter), 0.0, 0.49)
        seed = None if args.random_seed is None or int(args.random_seed) < 0 else int(args.random_seed)
        self.rng = random.Random(seed)
        self.min_samples_param = max(1, int(args.min_samples_param))
        self.min_samples_track = max(1, int(args.min_samples_track))
        self.max_param_update_fraction = _clamp(float(args.max_param_update_fraction), 0.0, 1.0)
        self.max_track_update_fraction = _clamp(float(args.max_track_update_fraction), 0.0, 1.0)

        self.box_x_m = max(0.05, float(args.box_x_m))
        self.box_z_m = max(0.05, float(args.box_z_m))
        self.max_heading_deg = max(10.0, float(args.max_heading_deg))
        self.max_heading_rad = math.radians(self.max_heading_deg)
        self.guardrail_abort_s = max(0.1, float(args.guardrail_abort_s))
        self.guardrail_recovery_timeout_s = max(1.0, float(args.guardrail_recovery_timeout_s))
        self.max_blind_s = max(10.0, float(args.max_blind_s))
        self.max_vision_jump_m = max(0.01, float(args.max_vision_jump_m))
        self.max_vision_jump_heading_rad = math.radians(max(2.0, float(args.max_vision_jump_heading_deg)))
        self.max_vision_radius_m = max(0.1, float(args.max_vision_radius_m))
        self.vision_guard_max_age_s = max(0.05, float(args.vision_guard_max_age_s))
        self.vision_ema_alpha = _clamp(float(args.vision_ema_alpha), 0.0, 1.0)
        self.enable_guardrails = bool(args.enable_guardrails)

        self.state_path = Path(args.state_file).expanduser()
        if not self.state_path.is_absolute():
            self.state_path = (Path.cwd() / self.state_path).resolve()
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.state = self._load_state()

        self.odom = OdomModel(
            left_mm_per_event=float(self.state["current_values"]["left_mm_per_event"]),
            right_mm_per_event=float(self.state["current_values"]["right_mm_per_event"]),
            track_width_mm=float(self.state["current_values"]["track_width_mm"]),
            left_direction_sign=float(args.left_direction_sign),
            right_direction_sign=float(args.right_direction_sign),
        )

        self.last_publish_at = 0.0
        self.last_frame_at = 0.0
        self.last_any_tag_seen_at = 0.0
        self.last_side_tag_seen_at = 0.0
        self.last_seen_direction = 1.0
        self.last_seen_tag: Optional[int] = None
        self.visible_ids: list[int] = []

        self.tag13_pose: Optional[VisionPose] = None
        self.origin_pose13: Optional[VisionPose] = None
        self.tag13_relative: Optional[VisionPose] = None
        self.tag13_visible = False
        self.last_tag13_seen_at = 0.0
        self.guardrail_violation_since: Optional[float] = None
        self.last_guardrail_log_at = 0.0
        self.guardrail_log_interval_s = 0.75
        self._recovering_visibility = False
        self.last_visibility_recovery_attempt_at = 0.0
        self._trial_active = False
        self.last_trial_lost_code = False

    def _load_state(self) -> dict[str, Any]:
        now = time.time()
        default = {
            "version": 1,
            "created_at": now,
            "updated_at": now,
            "counts": {"calibration_cycles": 0, "accepted_candidates": 0},
            "current_values": {
                "left_mm_per_event": float(self.args.default_left_mm_per_event),
                "right_mm_per_event": float(self.args.default_right_mm_per_event),
                "track_width_mm": float(self.args.default_track_width_mm),
            },
            "candidate_history": [],
            "robust_summary": {},
        }
        if bool(self.args.reset_state):
            logging.info("Reset-state requested; starting with default tuning values.")
            return default
        if not self.state_path.exists():
            return default
        try:
            loaded = json.loads(self.state_path.read_text())
        except json.JSONDecodeError:
            logging.warning("State file invalid JSON; starting fresh: %s", self.state_path)
            return default
        if not isinstance(loaded, dict):
            return default
        if "current_values" not in loaded or not isinstance(loaded.get("current_values"), dict):
            return default
        merged = default
        merged.update(loaded)
        merged["counts"] = dict(default["counts"]) | dict(loaded.get("counts") or {})
        merged["current_values"] = dict(default["current_values"]) | dict(loaded.get("current_values") or {})
        if not isinstance(merged.get("candidate_history"), list):
            merged["candidate_history"] = []
        return merged

    def _candidate_update_limit(self, param: str) -> float:
        if param == "track_width_mm":
            return float(self.max_track_update_fraction)
        return float(self.max_param_update_fraction)

    def _save_state(self) -> None:
        self.state["updated_at"] = time.time()
        self.state_path.write_text(json.dumps(self.state, indent=2, sort_keys=True))

    def _append_candidate(self, trial: str, param: str, value: float, diagnostics: dict[str, Any]) -> None:
        if not math.isfinite(value) or value <= 0.0:
            return
        current_values = self.state.get("current_values") if isinstance(self.state.get("current_values"), dict) else {}
        current_value = float(current_values.get(param, 0.0) or 0.0)
        limit_frac = self._candidate_update_limit(param)
        if current_value > 0.0 and limit_frac > 0.0:
            lo = current_value * (1.0 - limit_frac)
            hi = current_value * (1.0 + limit_frac)
            if value < lo or value > hi:
                logging.info(
                    "Rejected %s candidate %.4f outside bounded update window [%.4f, %.4f].",
                    param,
                    value,
                    lo,
                    hi,
                )
                return
        entry = {
            "timestamp": time.time(),
            "trial": trial,
            "param": param,
            "value": float(value),
            "diagnostics": diagnostics,
        }
        history = self.state.get("candidate_history")
        if not isinstance(history, list):
            history = []
            self.state["candidate_history"] = history
        history.append(entry)
        max_history = max(100, int(self.args.max_candidate_history))
        if len(history) > max_history:
            del history[: len(history) - max_history]
        counts = self.state.get("counts") if isinstance(self.state.get("counts"), dict) else {}
        counts["accepted_candidates"] = int(counts.get("accepted_candidates") or 0) + 1
        self.state["counts"] = counts

    def _recompute_robust_values(self) -> None:
        history = self.state.get("candidate_history") if isinstance(self.state.get("candidate_history"), list) else []
        grouped: dict[str, list[float]] = {"left_mm_per_event": [], "right_mm_per_event": [], "track_width_mm": []}
        for row in history:
            if not isinstance(row, dict):
                continue
            param = row.get("param")
            value = row.get("value")
            if isinstance(param, str) and param in grouped:
                try:
                    number = float(value)
                except (TypeError, ValueError):
                    continue
                if number > 0.0 and math.isfinite(number):
                    grouped[param].append(number)

        summary: dict[str, Any] = {}
        current = self.state.get("current_values") if isinstance(self.state.get("current_values"), dict) else {}
        for param, values in grouped.items():
            robust, used, median, mad = _robust_mean(values)
            min_required = int(self.min_samples_track if param == "track_width_mm" else self.min_samples_param)
            summary[param] = {
                "samples": len(values),
                "used": used,
                "required": min_required,
                "median": median,
                "mad": mad,
                "robust_mean": robust,
            }
            if robust is not None and used >= min_required:
                current[param] = float(robust)
        self.state["robust_summary"] = summary
        self.state["current_values"] = current
        self.odom.set_params(
            left_mm_per_event=float(current["left_mm_per_event"]),
            right_mm_per_event=float(current["right_mm_per_event"]),
            track_width_mm=float(current["track_width_mm"]),
        )

    def _connect(self) -> None:
        self.reader = OdometryRawShmReader(self.shm_name)
        latest = self.reader.read_latest()
        self.last_seq = int(latest["seq"]) if latest else 0

        client = mqtt.Client()
        mqtt_auth_and_tls(client, self.local_mqtt_cfg, self.config_path.parent)
        client.connect(self.host, self.port, self.keepalive)
        client.loop_start()
        self.client = client

    def _disconnect(self) -> None:
        self._publish_drive(0.0, 0.0, force=True)
        if self.capture is not None:
            self.capture.release()
            self.capture = None
        if self.reader is not None:
            self.reader.close()
            self.reader = None
        if self.client is not None:
            try:
                self.client.loop_stop()
                self.client.disconnect()
            except Exception:
                logging.debug("MQTT shutdown failed", exc_info=True)
            self.client = None
        self._save_state()

    def _ensure_capture(self) -> None:
        if self.capture is not None:
            return
        self.capture = _open_shared_capture(self.stream_socket, self.stream_width, self.stream_height, self.stream_fps)
        if self.capture is None:
            raise RuntimeError(f"Shared video source unavailable: {self.stream_socket}")

    def _publish_drive(self, x: float, z: float, force: bool = False) -> None:
        if self.client is None:
            return
        now = time.monotonic()
        interval = 1.0 / self.publish_rate_hz
        if not force and (now - self.last_publish_at) < interval:
            return
        payload = {"x": float(_clamp(x, -1.0, 1.0)), "z": float(_clamp(z, -1.0, 1.0))}
        self.client.publish(self.drive_topic, json.dumps(payload), qos=1)
        self.last_publish_at = now
        self.odom.set_command(payload["x"], payload["z"])

    def _read_shm(self) -> None:
        if self.reader is None:
            return
        rows = self.reader.read_since(self.last_seq)
        for row in rows:
            self.odom.process_sample(row, left_channel=self.left_channel, right_channel=self.right_channel)
            self.last_seq = int(row.get("seq") or self.last_seq)

    @staticmethod
    def _bearing_from_tvec(tvec: np.ndarray) -> float:
        vector = tvec.reshape(3)
        z = float(vector[2])
        if abs(z) < 1e-6:
            z = 1e-6 if z >= 0.0 else -1e-6
        return float(math.atan2(float(vector[0]), z))

    def _side_tag_visible(self) -> bool:
        return (self.tag14_id in self.visible_ids) or (self.tag15_id in self.visible_ids)

    def _should_force_recenter(self, now_mono: Optional[float] = None) -> bool:
        if self.tag13_relative is not None and abs(float(self.tag13_relative.z_m)) > self.max_forward_back_m:
            return True
        if self.tag13_visible:
            return False
        now = time.monotonic() if now_mono is None else float(now_mono)
        if self.last_tag13_seen_at <= 0.0:
            return False
        return (now - self.last_tag13_seen_at) > self.lost_timeout_s

    def _z_in_range(self) -> bool:
        if self.tag13_relative is None:
            return False
        return abs(float(self.tag13_relative.z_m)) <= (self.max_forward_back_m * 0.95)

    def _tag13_fresh(self, max_age_s: float = 0.35) -> bool:
        if not self.tag13_visible or self.tag13_relative is None:
            return False
        return (time.monotonic() - float(self.tag13_relative.t_mono)) <= max(0.05, float(max_age_s))

    def _should_force_recenter_in_trial(self, trial_started_mono: float, now_mono: Optional[float] = None) -> bool:
        now = time.monotonic() if now_mono is None else float(now_mono)
        # Always enforce translational bounds.
        if self.tag13_relative is not None and abs(float(self.tag13_relative.z_m)) > self.max_forward_back_m:
            return True
        # Don't treat very short post-setup/noise losses as trial failure.
        if (now - float(trial_started_mono)) <= self.trial_loss_grace_s:
            return False
        if self.tag13_visible:
            return False
        if self.last_tag13_seen_at <= 0.0:
            return False
        return (now - self.last_tag13_seen_at) > self.lost_timeout_s

    def _update_vision(self, force: bool = False) -> None:
        now = time.monotonic()
        if not force and (now - self.last_frame_at) < (1.0 / self.frame_rate_limit_hz):
            return
        self.last_frame_at = now

        self._ensure_capture()
        assert self.capture is not None
        ok, frame = self.capture.read()
        if not ok:
            self.capture.release()
            self.capture = None
            return
        frame = _apply_rotation(frame, self.rotation)
        corners, ids, _ = _detect_markers(frame, self.dictionary, self.detector_params)

        self.visible_ids = []
        tag13_pose = None
        if ids is not None and len(ids) > 0:
            ids_flat = [int(v) for v in ids.flatten().tolist()]
            self.visible_ids = sorted(ids_flat)
            self.last_any_tag_seen_at = time.monotonic()
            selected_direction: Optional[float] = None

            for idx, marker_id in enumerate(ids_flat):
                size_m = self.tag_sizes_m.get(marker_id)
                if size_m is None:
                    continue
                pose = _estimate_pose(corners[idx], size_m, self.camera_matrix, self.dist_coeffs)
                if pose is None:
                    continue
                rvec, tvec = pose
                r_ct, _ = cv2.Rodrigues(rvec)
                r_tc = r_ct.T
                t_tc = -(r_tc @ tvec.reshape(3, 1)).reshape(3)
                bearing = self._bearing_from_tvec(tvec)
                if marker_id == self.tag13_id:
                    tag13_pose = VisionPose(
                        x_m=float(t_tc[0]),
                        z_m=float(t_tc[2]),
                        yaw_rad=bearing,
                        t_mono=time.monotonic(),
                    )
                    x_cam = float(tvec.reshape(3)[0])
                    if abs(x_cam) > 0.02:
                        selected_direction = 1.0 if x_cam > 0.0 else -1.0

            if self.tag14_id in ids_flat:
                selected_direction = -1.0
                self.last_seen_tag = self.tag14_id
                self.last_side_tag_seen_at = time.monotonic()
            elif self.tag15_id in ids_flat:
                selected_direction = 1.0
                self.last_seen_tag = self.tag15_id
                self.last_side_tag_seen_at = time.monotonic()
            elif self.tag13_id in ids_flat:
                self.last_seen_tag = self.tag13_id
            if selected_direction is not None:
                self.last_seen_direction = selected_direction

        self.tag13_pose = tag13_pose
        self.tag13_visible = self.tag13_pose is not None
        if self.tag13_visible:
            self.last_tag13_seen_at = time.monotonic()

        raw_rel: Optional[VisionPose] = None
        if self.origin_pose13 is not None and self.tag13_pose is not None:
            raw_rel = VisionPose(
                x_m=self.tag13_pose.x_m - self.origin_pose13.x_m,
                z_m=self.tag13_pose.z_m - self.origin_pose13.z_m,
                yaw_rad=_normalize_angle_rad(self.tag13_pose.yaw_rad - self.origin_pose13.yaw_rad),
                t_mono=self.tag13_pose.t_mono,
            )

        if raw_rel is not None:
            current = self.tag13_relative
            radius = math.hypot(raw_rel.x_m, raw_rel.z_m)
            if radius > self.max_vision_radius_m:
                logging.debug("Ignoring vision sample beyond radius: %.3fm > %.3fm", radius, self.max_vision_radius_m)
            else:
                if current is None:
                    self.tag13_relative = raw_rel
                else:
                    # Reject implausible one-frame jumps while we have a recent track.
                    jump_x = abs(raw_rel.x_m - current.x_m)
                    jump_z = abs(raw_rel.z_m - current.z_m)
                    jump_yaw = abs(_normalize_angle_rad(raw_rel.yaw_rad - current.yaw_rad))
                    recent_track = (time.monotonic() - current.t_mono) <= (self.vision_guard_max_age_s * 2.0)
                    if (
                        recent_track
                        and (
                            jump_x > self.max_vision_jump_m
                            or jump_z > self.max_vision_jump_m
                            or jump_yaw > self.max_vision_jump_heading_rad
                        )
                    ):
                        logging.debug(
                            "Ignoring vision jump: dx=%.3f dz=%.3f dyaw_deg=%.1f",
                            jump_x,
                            jump_z,
                            math.degrees(jump_yaw),
                        )
                    else:
                        alpha = self.vision_ema_alpha
                        self.tag13_relative = VisionPose(
                            x_m=current.x_m + alpha * (raw_rel.x_m - current.x_m),
                            z_m=current.z_m + alpha * (raw_rel.z_m - current.z_m),
                            yaw_rad=_normalize_angle_rad(
                                current.yaw_rad + alpha * _normalize_angle_rad(raw_rel.yaw_rad - current.yaw_rad)
                            ),
                            t_mono=raw_rel.t_mono,
                        )
        # Keep last good tag13_relative during temporary occlusions; visibility is tracked separately.

    def _snapshot_trial(self, wait_for_tag13_s: float = 0.0) -> tuple[OdomSnapshot, Optional[VisionPose]]:
        deadline = time.monotonic() + max(0.0, wait_for_tag13_s)
        while time.monotonic() < deadline and not self.stop_event:
            self._read_shm()
            self._update_vision(force=True)
            if self.tag13_visible and self.tag13_relative is not None:
                break
            time.sleep(0.01)
        vis = None
        if self.tag13_relative is not None and (time.monotonic() - self.tag13_relative.t_mono) <= 1.0:
            vis = self.tag13_relative
        return self.odom.snapshot(), vis

    def _run_motion_pulse_with_sampling(self, trial_name: str, x_cmd: float, z_cmd: float) -> bool:
        start_odom, start_vis = self._snapshot_trial(wait_for_tag13_s=0.08)
        if not self._tick(x_cmd, z_cmd, self.turn_pulse_s, allow_visibility_recovery=False):
            return False
        if not self._tick(0.0, 0.0, self.turn_pause_s, allow_visibility_recovery=False):
            return False
        end_odom, end_vis = self._snapshot_trial(wait_for_tag13_s=0.08)
        return self._extract_candidates(
            trial_name,
            start_odom,
            end_odom,
            start_vis,
            end_vis,
            verbose=False,
        )

    def _guardrail_status(self, allow_vision: bool = True) -> Optional[str]:
        now = time.monotonic()
        if allow_vision:
            # Tag13-relative guardrails when fresh.
            vision_fresh = self.tag13_relative is not None and ((now - self.tag13_relative.t_mono) <= self.vision_guard_max_age_s)
            if self.tag13_relative is not None and vision_fresh:
                if abs(self.tag13_relative.x_m) > self.box_x_m:
                    return f"vision_x exceeded ({self.tag13_relative.x_m:.3f}m > {self.box_x_m:.3f}m)"
                if abs(self.tag13_relative.z_m) > self.box_z_m:
                    return f"vision_z exceeded ({self.tag13_relative.z_m:.3f}m > {self.box_z_m:.3f}m)"
                if abs(self.tag13_relative.yaw_rad) > self.max_heading_rad:
                    return (
                        f"vision_heading exceeded ({math.degrees(self.tag13_relative.yaw_rad):.1f}deg > {self.max_heading_deg:.1f}deg)"
                    )
                return None

            # If any tag is visible but pose is not fresh/usable, skip odom guardrails to avoid false trips.
            if self.visible_ids:
                return None

        # Last-resort odometry fallback: keep translational checks only.
        pose = self.odom.pose
        if abs(float(pose.x_mm)) > (self.box_x_m * 1000.0 * 1.2):
            return (
                f"odom_x exceeded ({float(pose.x_mm):.1f}mm > {(self.box_x_m * 1000.0 * 1.2):.1f}mm)"
            )
        if abs(float(pose.y_mm)) > (self.box_z_m * 1000.0 * 1.2):
            return (
                f"odom_y exceeded ({float(pose.y_mm):.1f}mm > {(self.box_z_m * 1000.0 * 1.2):.1f}mm)"
            )
        return None

    def _recover_from_guardrail(self) -> bool:
        self._publish_drive(0.0, 0.0, force=True)
        logging.warning("Attempting guardrail recovery.")
        deadline = time.monotonic() + self.guardrail_recovery_timeout_s
        while not self.stop_event and time.monotonic() < deadline:
            self._read_shm()
            self._update_vision(force=True)
            reason = self._guardrail_status()
            if reason is None:
                self.guardrail_violation_since = None
                self._publish_drive(0.0, 0.0, force=True)
                logging.info("Guardrail recovery complete.")
                return True

            if self.tag13_relative is not None:
                err = _normalize_angle_rad(self.tag13_relative.yaw_rad)
                if abs(err) > math.radians(5.0):
                    direction = 1.0 if err > 0.0 else -1.0
                    if not self._tick(
                        direction * self.turn_power,
                        0.0,
                        self.turn_pulse_s,
                        allow_guardrail_recovery=False,
                        allow_visibility_recovery=False,
                        enforce_guardrails=False,
                    ):
                        return False
                    if not self._tick(
                        0.0,
                        0.0,
                        self.turn_pause_s,
                        allow_guardrail_recovery=False,
                        allow_visibility_recovery=False,
                        enforce_guardrails=False,
                    ):
                        return False
                    continue
                if self.tag13_pose is not None and self.origin_pose13 is not None:
                    dist_now = math.hypot(self.tag13_pose.x_m, self.tag13_pose.z_m)
                    dist_origin = math.hypot(self.origin_pose13.x_m, self.origin_pose13.z_m)
                    dist_err = dist_now - dist_origin
                    if abs(dist_err) > 0.05:
                        z_cmd = self.drive_power if dist_err > 0.0 else -self.drive_power
                        if not self._tick(
                            0.0,
                            z_cmd,
                            self.turn_pulse_s,
                            allow_guardrail_recovery=False,
                            allow_visibility_recovery=False,
                            enforce_guardrails=False,
                        ):
                            return False
                        if not self._tick(
                            0.0,
                            0.0,
                            self.turn_pause_s,
                            allow_guardrail_recovery=False,
                            allow_visibility_recovery=False,
                            enforce_guardrails=False,
                        ):
                            return False
                        continue
            else:
                if not self._recover_visibility():
                    return False
            self._tick(0.0, 0.0, 0.05, allow_guardrail_recovery=False, allow_visibility_recovery=False)

        logging.warning("Guardrail recovery timed out; continuing cautiously.")
        self.guardrail_violation_since = None
        self._publish_drive(0.0, 0.0, force=True)
        return True

    def _tick(
        self,
        x: float,
        z: float,
        duration_s: float,
        allow_guardrail_recovery: bool = True,
        allow_visibility_recovery: bool = True,
        enforce_guardrails: bool = True,
    ) -> bool:
        tick_duration_s = max(0.0, duration_s)
        moving_cmd = abs(float(x)) > self.motion_deadband or abs(float(z)) > self.motion_deadband
        if moving_cmd and tick_duration_s > self.min_motion_pulse_s:
            tick_duration_s = self._rand_uniform(self.min_motion_pulse_s, tick_duration_s)
        stop_time = time.monotonic() + tick_duration_s
        while not self.stop_event and time.monotonic() < stop_time:
            self._publish_drive(x, z, force=False)
            self._read_shm()
            use_vision_now = not moving_cmd
            if use_vision_now:
                self._update_vision(force=False)
            if enforce_guardrails and self.enable_guardrails:
                reason = self._guardrail_status(allow_vision=use_vision_now)
                if reason is not None:
                    now = time.monotonic()
                    if self.guardrail_violation_since is None:
                        self.guardrail_violation_since = now
                    if (now - self.guardrail_violation_since) >= self.guardrail_abort_s:
                        if (now - self.last_guardrail_log_at) >= self.guardrail_log_interval_s:
                            logging.warning("Guardrail exceeded: %s", reason)
                            self.last_guardrail_log_at = now
                        if allow_guardrail_recovery:
                            if not self._recover_from_guardrail():
                                self._publish_drive(0.0, 0.0, force=True)
                                return False
                            self.guardrail_violation_since = None
                        else:
                            self._publish_drive(0.0, 0.0, force=True)
                            return False
                else:
                    self.guardrail_violation_since = None
            else:
                self.guardrail_violation_since = None
            now = time.monotonic()
            blind_for_s = now - self.last_any_tag_seen_at
            if self._trial_active and (not self.tag13_visible) and self.last_tag13_seen_at > 0.0:
                if (now - self.last_tag13_seen_at) > self.lost_timeout_s:
                    self.last_trial_lost_code = True
            if blind_for_s > self.max_blind_s:
                logging.error("No tags visible for %.1fs; exiting for safety.", blind_for_s)
                self._publish_drive(0.0, 0.0, force=True)
                return False
            time.sleep(0.005)
        return True

    def _recover_visibility(self, timeout_s: Optional[float] = None) -> bool:
        if self._recovering_visibility:
            return True
        self._recovering_visibility = True
        self.last_visibility_recovery_attempt_at = time.monotonic()
        self._publish_drive(0.0, 0.0, force=True)
        logging.warning("Recenter requested; recovering toward tag13 reference.")
        start = time.monotonic()
        direction = float(self.last_seen_direction) if self.last_seen_direction != 0.0 else 1.0
        total_limit = float(self.max_blind_s if timeout_s is None else max(0.2, timeout_s))
        turn_search_budget_s = min(self.recenter_search_max_s, total_limit)
        turned_search_s = 0.0

        try:
            while not self.stop_event and (time.monotonic() - start) < total_limit:
                self._update_vision(force=True)
                if self.tag13_visible and self.tag13_relative is not None:
                    logging.info("Recovered tags: %s", self.visible_ids)
                    self._publish_drive(0.0, 0.0, force=True)
                    return True
                # If tag13 is not visible, continue a short turn-search toward the last seen direction.
                if turned_search_s >= turn_search_budget_s:
                    logging.warning("Recenter turn budget reached; alternating direction.")
                    direction *= -1.0
                    turned_search_s = 0.0
                    if not self._tick(0.0, 0.0, self.turn_pause_s, allow_guardrail_recovery=False, allow_visibility_recovery=False, enforce_guardrails=False):
                        return False
                    continue
                pulse_s = min(self.turn_pulse_s, 0.08, turn_search_budget_s - turned_search_s)
                if pulse_s <= 0.0:
                    pulse_s = min(self.turn_pulse_s, 0.08)
                if not self._tick(
                    direction * self.search_power,
                    0.0,
                    pulse_s,
                    allow_guardrail_recovery=False,
                    allow_visibility_recovery=False,
                    enforce_guardrails=False,
                ):
                    return False
                turned_search_s += pulse_s
                if not self._tick(
                    0.0,
                    0.0,
                    self.turn_pause_s,
                    allow_guardrail_recovery=False,
                    allow_visibility_recovery=False,
                    enforce_guardrails=False,
                ):
                    return False

            logging.error("Recovery failed after %.1fs without tag13.", total_limit)
            self._publish_drive(0.0, 0.0, force=True)
            return False
        finally:
            self._recovering_visibility = False

    def _wait_for_origin(self) -> bool:
        stable_count = 0
        required = max(6, int(self.args.origin_stable_frames))
        while not self.stop_event:
            self._tick(0.0, 0.0, 0.05)
            if self.tag13_pose is None:
                stable_count = 0
                continue
            stable_count += 1
            if stable_count >= required:
                self.origin_pose13 = self.tag13_pose
                self.tag13_relative = VisionPose(0.0, 0.0, 0.0, time.monotonic())
                logging.info("Origin established from tag %d.", self.tag13_id)
                return True
        return False

    def _turn_to_heading_zero(self, timeout_s: float = 8.0, allow_recover: bool = False) -> bool:
        deadline = time.monotonic() + max(1.0, timeout_s)
        settle_rad = math.radians(min(12.0, max(4.0, 0.4 * self.cone_half_angle_deg)))
        while not self.stop_event and time.monotonic() < deadline:
            self._tick(0.0, 0.0, 0.01, allow_visibility_recovery=False)
            self._update_vision(force=True)
            if not self._tag13_fresh(max_age_s=0.9):
                # Avoid recovery thrash on short camera gaps while stationary.
                if self.last_tag13_seen_at > 0.0 and (time.monotonic() - self.last_tag13_seen_at) <= self.lost_timeout_s:
                    time.sleep(0.02)
                    continue
                if not allow_recover:
                    return False
                if not self._recover_visibility():
                    return False
                continue
            assert self.tag13_relative is not None
            err = _normalize_angle_rad(self.tag13_relative.yaw_rad)
            if abs(err) <= settle_rad:
                self._publish_drive(0.0, 0.0, force=True)
                return True
            direction = 1.0 if err > 0.0 else -1.0
            if not self._tick(direction * self.turn_power, 0.0, self.turn_pulse_s, allow_visibility_recovery=False):
                return False
            if not self._tick(0.0, 0.0, self.turn_pause_s, allow_visibility_recovery=False):
                return False
        self._publish_drive(0.0, 0.0, force=True)
        return False

    def _ensure_centered(self, timeout_s: float = 8.0) -> bool:
        deadline = time.monotonic() + max(1.0, timeout_s)
        settle_rad = math.radians(min(10.0, max(3.0, 0.35 * self.cone_half_angle_deg)))
        while not self.stop_event and time.monotonic() < deadline:
            if not self._tick(0.0, 0.0, 0.02, allow_visibility_recovery=False):
                return False
            self._update_vision(force=True)
            if not self._tag13_fresh(max_age_s=0.8):
                remain = max(0.4, deadline - time.monotonic())
                if not self._recover_visibility(timeout_s=min(2.5, remain)):
                    continue
                continue
            assert self.tag13_relative is not None
            yaw_err = _normalize_angle_rad(float(self.tag13_relative.yaw_rad))
            z_err = float(self.tag13_relative.z_m)
            if abs(z_err) > self.max_forward_back_m:
                z_cmd = -math.copysign(_clamp(0.65 * self.drive_power, self.power_min, self.power_max), z_err)
                if not self._tick(0.0, z_cmd, min(self.turn_pulse_s, 0.08), allow_visibility_recovery=False):
                    return False
                if not self._tick(0.0, 0.0, self.turn_pause_s, allow_visibility_recovery=False):
                    return False
                continue
            if abs(yaw_err) <= settle_rad:
                self._publish_drive(0.0, 0.0, force=True)
                return True
            turn_dir = 1.0 if yaw_err > 0.0 else -1.0
            if not self._tick(turn_dir * self.turn_power, 0.0, min(self.turn_pulse_s, 0.08), allow_visibility_recovery=False):
                return False
            if not self._tick(0.0, 0.0, self.turn_pause_s, allow_visibility_recovery=False):
                return False
        return False

    def _setup_for_trial(self, trial: dict[str, Any], timeout_s: float = 6.0) -> bool:
        setup_direction = trial.get("setup_direction")
        if setup_direction is None:
            return True
        direction = 1.0 if float(setup_direction) >= 0.0 else -1.0
        target_abs = min(self.cone_half_angle_rad * 0.82, self.cone_half_angle_rad - math.radians(3.0))
        target_abs = max(math.radians(6.0), target_abs)
        target_yaw = direction * target_abs
        tol = math.radians(2.5)
        pulse_s = min(self.turn_pulse_s, 0.06)
        pulse_power = _clamp(0.8 * self.turn_power, self.power_min, self.power_max)
        name = str(trial.get("name") or "trial")
        deadline = time.monotonic() + max(1.0, timeout_s)
        logging.info("Setup %s: placing tag13 toward %s edge (target=%.1fdeg).", name, "right" if direction > 0.0 else "left", math.degrees(target_yaw))
        while not self.stop_event and time.monotonic() < deadline:
            if not self._tick(0.0, 0.0, 0.02, allow_visibility_recovery=False):
                return False
            self._update_vision(force=True)
            if not self.tag13_visible or self.tag13_relative is None:
                if self.last_tag13_seen_at > 0.0 and (time.monotonic() - self.last_tag13_seen_at) <= max(0.20, 1.5 * self.lost_timeout_s):
                    logging.info("Setup %s reached FOV edge via brief tag13 loss; reacquiring tag13.", name)
                    if not self._recover_visibility():
                        return False
                    self._update_vision(force=True)
                    if not self._tag13_fresh(max_age_s=0.9):
                        logging.warning("Setup %s could not reacquire stable tag13; starting new trial.", name)
                        return True
                    self._publish_drive(0.0, 0.0, force=True)
                    return True
                # Long loss during setup: do not recover here; let outer loop recenter.
                return True
            yaw = _normalize_angle_rad(float(self.tag13_relative.yaw_rad))
            if direction > 0.0 and yaw >= (target_yaw - tol):
                self._publish_drive(0.0, 0.0, force=True)
                return True
            if direction < 0.0 and yaw <= (target_yaw + tol):
                self._publish_drive(0.0, 0.0, force=True)
                return True
            if not self._tick(direction * pulse_power, 0.0, pulse_s, allow_visibility_recovery=False):
                return False
            if not self._tick(0.0, 0.0, self.turn_pause_s, allow_visibility_recovery=False):
                return False
        logging.warning("Setup for %s timed out before reaching desired FOV edge; continuing.", name)
        self._publish_drive(0.0, 0.0, force=True)
        return True

    def _generate_trial(self) -> dict[str, Any]:
        cone_target_max = max(5.0, self.cone_half_angle_deg - 2.0)
        turn_right_target = min(self._rand_uniform(self.trial_turn_min_deg, self.trial_turn_max_deg), cone_target_max)
        turn_left_target = min(self._rand_uniform(self.trial_turn_min_deg, self.trial_turn_max_deg), cone_target_max)
        single_left_target = min(self._rand_uniform(self.trial_single_min_deg, self.trial_single_max_deg), cone_target_max)
        single_right_target = min(self._rand_uniform(self.trial_single_min_deg, self.trial_single_max_deg), cone_target_max)
        arc_right_target = self._rand_uniform(self.trial_straight_min_mm, self.trial_straight_max_mm)
        arc_left_target = self._rand_uniform(self.trial_straight_min_mm, self.trial_straight_max_mm)
        straight_fwd_target = self._rand_uniform(self.trial_straight_min_mm, self.trial_straight_max_mm)
        straight_rev_target = self._rand_uniform(self.trial_straight_min_mm, self.trial_straight_max_mm)
        turn_trial_power = self._jittered_power(self.turn_power)
        drive_trial_power = self._jittered_power(self.drive_power)
        trials = [
            {"name": "turn_right_step", "kind": "turn", "direction": +1.0, "target_deg": turn_right_target, "power": turn_trial_power, "setup_direction": +1.0},
            {"name": "turn_left_step", "kind": "turn", "direction": -1.0, "target_deg": turn_left_target, "power": turn_trial_power, "setup_direction": -1.0},
            {"name": "left_only_fwd", "kind": "single", "wheel": "left", "target_deg": single_left_target, "power": drive_trial_power, "setup_direction": None},
            {"name": "right_only_fwd", "kind": "single", "wheel": "right", "target_deg": single_right_target, "power": drive_trial_power, "setup_direction": None},
            {"name": "arc_fwd_right", "kind": "arc", "turn_direction": +1.0, "target_mm": arc_right_target, "power": drive_trial_power, "setup_direction": None},
            {"name": "arc_fwd_left", "kind": "arc", "turn_direction": -1.0, "target_mm": arc_left_target, "power": drive_trial_power, "setup_direction": None},
            {"name": "straight_fwd", "kind": "straight", "direction": +1.0, "target_mm": straight_fwd_target, "power": drive_trial_power, "setup_direction": None},
            {"name": "straight_rev", "kind": "straight", "direction": -1.0, "target_mm": straight_rev_target, "power": drive_trial_power, "setup_direction": None},
            {"name": "straight_fwd", "kind": "straight", "direction": +1.0, "target_mm": straight_fwd_target, "power": drive_trial_power, "setup_direction": None},
        ]
        trial = self.rng.choice(trials)
        logging.info(
            "Trial selected: %s target=%s power=%.2f",
            trial["name"],
            f"{trial.get('target_deg', trial.get('target_mm', 0.0)):.1f}{'deg' if 'target_deg' in trial else 'mm'}",
            float(trial["power"]),
        )
        return trial

    def _execute_trial(self, trial: dict[str, Any]) -> bool:
        kind = str(trial.get("kind") or "")
        self.last_trial_lost_code = False
        self._trial_active = True
        try:
            name = str(trial.get("name") or "trial")
            self._update_vision(force=True)
            if not self._tag13_fresh(max_age_s=0.9):
                if not self._recover_visibility():
                    return False
                self._update_vision(force=True)
            if not self._tag13_fresh(max_age_s=0.9):
                self.last_trial_lost_code = True
                logging.info("Trial %s skipped; tag13 not ready after setup.", name)
                return True
            if kind == "turn":
                return self._run_turn_trial(
                    name,
                    direction=float(trial["direction"]),
                    target_deg=float(trial["target_deg"]),
                    power=float(trial["power"]),
                )
            if kind == "single":
                return self._run_single_wheel_trial(
                    name,
                    wheel=str(trial["wheel"]),
                    target_deg=float(trial["target_deg"]),
                    power=float(trial["power"]),
                )
            if kind == "arc":
                return self._run_arc_forward_trial(
                    name,
                    turn_direction=float(trial["turn_direction"]),
                    target_mm=float(trial["target_mm"]),
                    drive_power=float(trial["power"]),
                )
            if kind == "straight":
                return self._run_straight_trial(
                    name,
                    direction=float(trial["direction"]),
                    target_mm=float(trial["target_mm"]),
                    power=float(trial["power"]),
                )
            logging.error("Unknown trial kind: %s", kind)
            return False
        finally:
            self._trial_active = False

    def _run_turn_trial(
        self,
        name: str,
        direction: float,
        target_deg: float,
        power: Optional[float] = None,
        max_time_s: float = 7.0,
    ) -> bool:
        turn_power = float(self.turn_power if power is None else _clamp(power, self.power_min, self.power_max))
        start_odom, start_vis = self._snapshot_trial(wait_for_tag13_s=0.35)
        target_rad = math.radians(abs(target_deg))
        began = time.monotonic()
        while not self.stop_event and (time.monotonic() - began) < max_time_s:
            now = time.monotonic()
            self._update_vision(force=True)
            if (
                (now - began) > self.trial_loss_grace_s
                and (not self.tag13_visible)
                and self.last_tag13_seen_at > 0.0
                and (now - self.last_tag13_seen_at) > self.lost_timeout_s
            ):
                self.last_trial_lost_code = True
                logging.info("Trial %s halted due to tag13 loss.", name)
                break
            if self.tag13_relative is not None and abs(float(self.tag13_relative.z_m)) > self.max_forward_back_m:
                logging.info("Trial %s halted at forward/back limit.", name)
                break
            if self.tag13_relative is not None and start_vis is not None:
                delta = _normalize_angle_rad(self.tag13_relative.yaw_rad - start_vis.yaw_rad)
                if abs(delta) >= target_rad:
                    break
            else:
                delta_odom = _normalize_angle_rad(self.odom.pose.heading_rad - start_odom.heading_rad)
                if abs(delta_odom) >= target_rad:
                    break
            if not self._run_motion_pulse_with_sampling(name, direction * turn_power, 0.0):
                return False
        self._publish_drive(0.0, 0.0, force=True)
        if not self._tick(0.0, 0.0, 0.25, allow_visibility_recovery=False):
            return False
        end_odom, end_vis = self._snapshot_trial(wait_for_tag13_s=0.35)
        return self._extract_candidates(name, start_odom, end_odom, start_vis, end_vis)

    def _run_straight_trial(
        self,
        name: str,
        direction: float,
        target_mm: float,
        power: Optional[float] = None,
        max_time_s: float = 7.0,
    ) -> bool:
        drive_power = float(self.drive_power if power is None else _clamp(power, self.power_min, self.power_max))
        start_odom, start_vis = self._snapshot_trial(wait_for_tag13_s=0.35)
        target_m = abs(target_mm) / 1000.0
        if start_vis is not None:
            room_m = max(0.0, self.max_forward_back_m - abs(float(start_vis.z_m)))
            if room_m <= 0.03:
                logging.info("Trial %s skipped; insufficient forward/back room (|z|=%.3fm).", name, abs(float(start_vis.z_m)))
                return True
            target_m = min(target_m, max(0.04, 0.7 * room_m))
        began = time.monotonic()
        while not self.stop_event and (time.monotonic() - began) < max_time_s:
            now = time.monotonic()
            self._update_vision(force=True)
            if (
                (now - began) > self.trial_loss_grace_s
                and (not self.tag13_visible)
                and self.last_tag13_seen_at > 0.0
                and (now - self.last_tag13_seen_at) > self.lost_timeout_s
            ):
                self.last_trial_lost_code = True
                logging.info("Trial %s halted due to tag13 loss.", name)
                break
            if self.tag13_relative is not None and abs(float(self.tag13_relative.z_m)) > self.max_forward_back_m:
                logging.info("Trial %s halted at forward/back limit.", name)
                break
            if self.tag13_relative is not None and start_vis is not None:
                dx = self.tag13_relative.x_m - start_vis.x_m
                dz = self.tag13_relative.z_m - start_vis.z_m
                if math.hypot(dx, dz) >= target_m:
                    break
            else:
                d_mm = math.hypot(self.odom.pose.x_mm - start_odom.x_mm, self.odom.pose.y_mm - start_odom.y_mm)
                if d_mm >= abs(target_mm):
                    break
            if not self._run_motion_pulse_with_sampling(name, 0.0, direction * drive_power):
                return False
        self._publish_drive(0.0, 0.0, force=True)
        if not self._tick(0.0, 0.0, 0.25, allow_visibility_recovery=False):
            return False
        end_odom, end_vis = self._snapshot_trial(wait_for_tag13_s=0.35)
        return self._extract_candidates(name, start_odom, end_odom, start_vis, end_vis)

    def _run_arc_forward_trial(
        self,
        name: str,
        turn_direction: float,
        target_mm: float,
        drive_power: Optional[float] = None,
        turn_ratio: float = 0.33,
        max_time_s: float = 9.0,
    ) -> bool:
        p_drive = float(self.drive_power if drive_power is None else _clamp(drive_power, self.power_min, self.power_max))
        p_turn = _clamp(abs(turn_direction) * turn_ratio * p_drive, 0.0, self.power_max)
        x_cmd = math.copysign(p_turn, turn_direction)
        z_cmd = p_drive
        start_odom, start_vis = self._snapshot_trial(wait_for_tag13_s=0.35)
        target_m = abs(target_mm) / 1000.0
        if start_vis is not None:
            room_m = max(0.0, self.max_forward_back_m - abs(float(start_vis.z_m)))
            if room_m <= 0.03:
                logging.info("Trial %s skipped; insufficient forward/back room (|z|=%.3fm).", name, abs(float(start_vis.z_m)))
                return True
            target_m = min(target_m, max(0.05, 0.7 * room_m))
        began = time.monotonic()
        while not self.stop_event and (time.monotonic() - began) < max_time_s:
            now = time.monotonic()
            self._update_vision(force=True)
            if (
                (now - began) > self.trial_loss_grace_s
                and (not self.tag13_visible)
                and self.last_tag13_seen_at > 0.0
                and (now - self.last_tag13_seen_at) > self.lost_timeout_s
            ):
                self.last_trial_lost_code = True
                logging.info("Trial %s halted due to tag13 loss.", name)
                break
            if self.tag13_relative is not None and abs(float(self.tag13_relative.z_m)) > self.max_forward_back_m:
                logging.info("Trial %s halted at forward/back limit.", name)
                break
            if self.tag13_relative is not None and start_vis is not None:
                dx = self.tag13_relative.x_m - start_vis.x_m
                dz = self.tag13_relative.z_m - start_vis.z_m
                if math.hypot(dx, dz) >= target_m:
                    break
            else:
                d_mm = math.hypot(self.odom.pose.x_mm - start_odom.x_mm, self.odom.pose.y_mm - start_odom.y_mm)
                if d_mm >= abs(target_mm):
                    break
            if not self._run_motion_pulse_with_sampling(name, x_cmd, z_cmd):
                return False
        self._publish_drive(0.0, 0.0, force=True)
        if not self._tick(0.0, 0.0, 0.25, allow_visibility_recovery=False):
            return False
        end_odom, end_vis = self._snapshot_trial(wait_for_tag13_s=0.35)
        return self._extract_candidates(name, start_odom, end_odom, start_vis, end_vis)

    def _run_single_wheel_trial(
        self,
        name: str,
        wheel: str,
        target_deg: float,
        power: Optional[float] = None,
        max_time_s: float = 7.0,
    ) -> bool:
        # left-only: left=z+x, right=z-x => z=p/2, x=+p/2
        # right-only: left=z+x, right=z-x => z=p/2, x=-p/2
        p = float(self.drive_power if power is None else _clamp(power, self.power_min, self.power_max))
        if wheel == "left":
            cmd_x, cmd_z = +0.5 * p, +0.5 * p
        else:
            cmd_x, cmd_z = -0.5 * p, +0.5 * p

        start_odom, start_vis = self._snapshot_trial(wait_for_tag13_s=0.35)
        target_rad = math.radians(abs(target_deg))
        began = time.monotonic()
        while not self.stop_event and (time.monotonic() - began) < max_time_s:
            now = time.monotonic()
            self._update_vision(force=True)
            if (
                (now - began) > self.trial_loss_grace_s
                and (not self.tag13_visible)
                and self.last_tag13_seen_at > 0.0
                and (now - self.last_tag13_seen_at) > self.lost_timeout_s
            ):
                self.last_trial_lost_code = True
                logging.info("Trial %s halted due to tag13 loss.", name)
                break
            if self.tag13_relative is not None and abs(float(self.tag13_relative.z_m)) > self.max_forward_back_m:
                logging.info("Trial %s halted at forward/back limit.", name)
                break
            if self.tag13_relative is not None and start_vis is not None:
                delta = _normalize_angle_rad(self.tag13_relative.yaw_rad - start_vis.yaw_rad)
                if abs(delta) >= target_rad:
                    break
            else:
                delta_odom = _normalize_angle_rad(self.odom.pose.heading_rad - start_odom.heading_rad)
                if abs(delta_odom) >= target_rad:
                    break
            if not self._run_motion_pulse_with_sampling(name, cmd_x, cmd_z):
                return False
        self._publish_drive(0.0, 0.0, force=True)
        if not self._tick(0.0, 0.0, 0.25, allow_visibility_recovery=False):
            return False
        end_odom, end_vis = self._snapshot_trial(wait_for_tag13_s=0.35)
        return self._extract_candidates(name, start_odom, end_odom, start_vis, end_vis)

    def _extract_candidates(
        self,
        trial_name: str,
        start_odom: OdomSnapshot,
        end_odom: OdomSnapshot,
        start_vis: Optional[VisionPose],
        end_vis: Optional[VisionPose],
        verbose: bool = True,
    ) -> bool:
        if start_vis is None or end_vis is None:
            if verbose:
                logging.info("Trial %s completed without tag13 boundary poses; candidate update skipped.", trial_name)
            return True

        left_steps = float(end_odom.left_steps_signed - start_odom.left_steps_signed)
        right_steps = float(end_odom.right_steps_signed - start_odom.right_steps_signed)
        odom_dx = float(end_odom.x_mm - start_odom.x_mm)
        odom_dy = float(end_odom.y_mm - start_odom.y_mm)
        odom_dtheta = _normalize_angle_rad(float(end_odom.heading_rad - start_odom.heading_rad))
        vis_dx = float(end_vis.x_m - start_vis.x_m)
        vis_dz = float(end_vis.z_m - start_vis.z_m)
        vis_dtheta = _normalize_angle_rad(float(end_vis.yaw_rad - start_vis.yaw_rad))

        cur = self.state["current_values"]
        left_mm = float(cur["left_mm_per_event"])
        right_mm = float(cur["right_mm_per_event"])
        track_mm = float(cur["track_width_mm"])

        diagnostics = {
            "left_steps": left_steps,
            "right_steps": right_steps,
            "odom_dx_mm": odom_dx,
            "odom_dy_mm": odom_dy,
            "odom_dtheta_rad": odom_dtheta,
            "vis_dx_m": vis_dx,
            "vis_dz_m": vis_dz,
            "vis_dtheta_rad": vis_dtheta,
        }

        accepted = False

        if trial_name.startswith("turn_") or trial_name.startswith("arc_"):
            dist_diff_mm = (right_steps * right_mm) - (left_steps * left_mm)
            if (
                abs(vis_dtheta) >= math.radians(self.min_turn_vis_deg)
                and abs(dist_diff_mm) >= self.min_turn_dist_diff_mm
                and abs(left_steps) >= self.min_turn_step_events
                and abs(right_steps) >= self.min_turn_step_events
            ):
                track_candidate = abs(dist_diff_mm / vis_dtheta)
                if 60.0 <= track_candidate <= 300.0:
                    self._append_candidate(trial_name, "track_width_mm", track_candidate, diagnostics)
                    accepted = True

        if trial_name in {"left_only_fwd"}:
            if (
                abs(vis_dtheta) >= math.radians(self.min_single_vis_deg)
                and abs(left_steps) >= self.min_single_step_events
            ):
                # dtheta ~= -dL/track  (right wheel ~ stationary)
                left_candidate = abs((-track_mm * vis_dtheta) / left_steps)
                if 1.0 <= left_candidate <= 80.0:
                    self._append_candidate(trial_name, "left_mm_per_event", left_candidate, diagnostics)
                    accepted = True
        if trial_name in {"right_only_fwd"}:
            if (
                abs(vis_dtheta) >= math.radians(self.min_single_vis_deg)
                and abs(right_steps) >= self.min_single_step_events
            ):
                # dtheta ~= +dR/track (left wheel ~ stationary)
                right_candidate = abs((track_mm * vis_dtheta) / right_steps)
                if 1.0 <= right_candidate <= 80.0:
                    self._append_candidate(trial_name, "right_mm_per_event", right_candidate, diagnostics)
                    accepted = True

        if trial_name in {"straight_fwd", "straight_rev", "arc_fwd_right", "arc_fwd_left"}:
            vis_dist_mm = 1000.0 * math.hypot(vis_dx, vis_dz)
            odom_dist_mm = math.hypot(odom_dx, odom_dy)
            if vis_dist_mm >= self.min_straight_vis_mm and odom_dist_mm >= self.min_straight_odom_mm:
                scale = vis_dist_mm / max(1e-6, odom_dist_mm)
                if not (self.straight_scale_min <= scale <= self.straight_scale_max):
                    scale = float("nan")
                if not math.isfinite(scale):
                    if verbose:
                        logging.info(
                            "Rejected straight scale %.3f outside [%.3f, %.3f].",
                            vis_dist_mm / max(1e-6, odom_dist_mm),
                            self.straight_scale_min,
                            self.straight_scale_max,
                        )
                    scale = None
                if scale is None:
                    pass
                else:
                    left_candidate = left_mm * scale
                    right_candidate = right_mm * scale
                    if 1.0 <= left_candidate <= 80.0:
                        self._append_candidate(trial_name, "left_mm_per_event", left_candidate, diagnostics)
                        accepted = True
                    if 1.0 <= right_candidate <= 80.0:
                        self._append_candidate(trial_name, "right_mm_per_event", right_candidate, diagnostics)
                        accepted = True

        if accepted:
            self._recompute_robust_values()
            self._save_state()
            cur = self.state["current_values"]
            logging.info(
                "Updated robust params: left=%.4f right=%.4f track=%.3f",
                float(cur["left_mm_per_event"]),
                float(cur["right_mm_per_event"]),
                float(cur["track_width_mm"]),
            )
        else:
            if verbose:
                logging.info("Trial %s produced no accepted candidates.", trial_name)

        return True

    def _increment_cycle(self) -> None:
        counts = self.state.get("counts") if isinstance(self.state.get("counts"), dict) else {}
        counts["calibration_cycles"] = int(counts.get("calibration_cycles") or 0) + 1
        self.state["counts"] = counts
        self._save_state()

    def _rand_uniform(self, low: float, high: float) -> float:
        if high <= low:
            return float(low)
        return float(self.rng.uniform(low, high))

    def _jittered_power(self, base: float) -> float:
        span = float(base) * self.trial_power_jitter
        return float(_clamp(self._rand_uniform(base - span, base + span), self.power_min, self.power_max))

    def run(self) -> int:
        self._connect()
        logging.info("Attached to shm=%s and MQTT drive topic=%s", self.shm_name, self.drive_topic)
        logging.info("Tuning state file: %s", self.state_path)
        logging.info(
            "Initial params: left=%.4f right=%.4f track=%.3f",
            float(self.state["current_values"]["left_mm_per_event"]),
            float(self.state["current_values"]["right_mm_per_event"]),
            float(self.state["current_values"]["track_width_mm"]),
        )
        logging.info("Localization source: tag13-only (origin fixed at startup relative pose).")
        logging.info("Motion guardrails: %s", "enabled" if self.enable_guardrails else "disabled")
        logging.info("Heading cone: +/- %.1f deg from tag13", self.cone_half_angle_deg)
        logging.info("Forward/back range: +/- %.0f mm from tag13-relative startup origin", self.max_forward_back_m * 1000.0)
        logging.info("Motion pulse window: %.3fs..%.3fs", self.min_motion_pulse_s, self.turn_pulse_s)
        logging.info(
            "Candidate gates: min_samples(param=%d,track=%d) max_step(param=%.2f,track=%.2f)",
            self.min_samples_param,
            self.min_samples_track,
            self.max_param_update_fraction,
            self.max_track_update_fraction,
        )

        self.last_any_tag_seen_at = time.monotonic()
        if not self._wait_for_origin():
            return 1

        while not self.stop_event:
            if not self._ensure_centered(timeout_s=8.0):
                logging.error("Unable to center on tag13 before trial; exiting.")
                return 1
            trial = self._generate_trial()
            if not self._setup_for_trial(trial):
                logging.error("Failed to setup %s; exiting.", trial["name"])
                return 1
            if not self._execute_trial(trial):
                return 1
            if self.last_trial_lost_code:
                logging.warning("Lost code during trial; beginning new trial.")
            self._increment_cycle()
            counts = self.state["counts"]
            logging.info(
                "Calibration cycle complete: #%d (trial=%s, candidates=%d)",
                int(counts.get("calibration_cycles") or 0),
                str(trial["name"]),
                int(counts.get("accepted_candidates") or 0),
            )
            self._tick(0.0, 0.0, 0.1, allow_visibility_recovery=False)

        return 0


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Auto-tune wheel odometry against AprilTags 13/14/15.")
    parser.add_argument("--config", default="control/configs/config.json", help="Unified config JSON path.")
    parser.add_argument("--calibration", default="autonomy/apriltag-follow/camera_calibration.yaml", help="Camera calibration YAML.")
    parser.add_argument("--video-shm", default="", help="Override shared video socket path.")
    parser.add_argument("--width", type=int, default=0, help="Override shared stream width.")
    parser.add_argument("--height", type=int, default=0, help="Override shared stream height.")
    parser.add_argument("--fps", type=float, default=0.0, help="Override shared stream fps.")
    parser.add_argument("--marker-dictionary", default="APRILTAG_25H9", help="ArUco/AprilTag dictionary.")

    parser.add_argument("--tag13-id", type=int, default=13, help="Front/reference tag id.")
    parser.add_argument("--tag14-id", type=int, default=14, help="Right-side guard tag id.")
    parser.add_argument("--tag15-id", type=int, default=15, help="Left-side guard tag id.")
    parser.add_argument("--tag13-size-m", type=float, default=0.25, help="Tag 13 size in meters.")
    parser.add_argument("--tag14-size-m", type=float, default=0.145, help="Tag 14 size in meters.")
    parser.add_argument("--tag15-size-m", type=float, default=0.145, help="Tag 15 size in meters.")

    parser.add_argument("--shm-name", default="", help="Shared-memory name (default from robot identity).")
    parser.add_argument("--left-channel", default="a0", help="Left hall channel (a0/a1/a2).")
    parser.add_argument("--right-channel", default="a1", help="Right hall channel (a0/a1/a2).")
    parser.add_argument("--left-direction-sign", type=float, default=1.0, help="Left wheel direction sign (+1/-1).")
    parser.add_argument("--right-direction-sign", type=float, default=1.0, help="Right wheel direction sign (+1/-1).")

    parser.add_argument("--state-file", default="autonomy/wheel-odometry/auto_tune_state.json", help="Persistent local tuning JSON.")
    parser.add_argument("--reset-state", action="store_true", help="Start from default state and ignore existing state file.")
    parser.add_argument("--max-candidate-history", type=int, default=2000, help="Max tuning candidates retained.")
    parser.add_argument("--default-left-mm-per-event", type=float, default=19.164, help="Initial left mm/event.")
    parser.add_argument("--default-right-mm-per-event", type=float, default=19.164, help="Initial right mm/event.")
    parser.add_argument("--default-track-width-mm", type=float, default=130.5, help="Initial track width mm.")
    parser.add_argument("--min-samples-param", type=int, default=6, help="Minimum robust samples before updating left/right mm-per-event.")
    parser.add_argument("--min-samples-track", type=int, default=6, help="Minimum robust samples before updating track width.")
    parser.add_argument(
        "--max-param-update-fraction",
        type=float,
        default=0.20,
        help="Reject left/right candidate values farther than this fraction from current value.",
    )
    parser.add_argument(
        "--max-track-update-fraction",
        type=float,
        default=0.15,
        help="Reject track candidates farther than this fraction from current value.",
    )

    parser.add_argument("--power-min", type=float, default=0.5, help="Minimum motion power.")
    parser.add_argument("--power-max", type=float, default=1.0, help="Maximum motion power.")
    parser.add_argument("--turn-power", type=float, default=0.65, help="Pulse turn power.")
    parser.add_argument("--drive-power", type=float, default=0.6, help="Pulse translation/single-wheel power.")
    parser.add_argument("--search-power", type=float, default=0.6, help="Lost-tag search turn power.")
    parser.add_argument("--turn-pulse-s", type=float, default=0.1, help="Turn/drive pulse-on duration.")
    parser.add_argument(
        "--min-motion-pulse-s",
        type=float,
        default=0.04,
        help="Minimum randomized movement pulse duration (seconds).",
    )
    parser.add_argument("--turn-pause-s", type=float, default=1.0, help="Turn/drive pulse-off duration.")
    parser.add_argument("--publish-rate-hz", type=float, default=15.0, help="Drive publish rate.")
    parser.add_argument("--frame-rate-limit-hz", type=float, default=20.0, help="Max camera processing rate.")
    parser.add_argument("--lost-timeout-s", type=float, default=1.0, help="No-tag timeout before recovery.")
    parser.add_argument(
        "--recenter-search-max-s",
        type=float,
        default=2.5,
        help="Maximum continuous turning time during no-tag recenter before safety exit.",
    )
    parser.add_argument(
        "--side-tag-blind-grace-s",
        type=float,
        default=8.0,
        help="After seeing tag14/15, allow this no-tag gap before starting recovery.",
    )
    parser.add_argument(
        "--visibility-recovery-cooldown-s",
        type=float,
        default=2.0,
        help="Minimum time between visibility recovery attempts.",
    )
    parser.add_argument(
        "--trial-loss-grace-s",
        type=float,
        default=1.2,
        help="Ignore tag13-loss aborts for this long after a trial starts.",
    )
    parser.add_argument(
        "--motion-deadband",
        type=float,
        default=0.05,
        help="Commands above this magnitude are treated as moving; vision checks/recovery are deferred.",
    )
    parser.add_argument("--max-blind-s", type=float, default=60.0, help="Exit only after this long with no visible tags.")
    parser.add_argument("--trial-turn-min-deg", type=float, default=6.0, help="Min random turn target (deg).")
    parser.add_argument("--trial-turn-max-deg", type=float, default=14.0, help="Max random turn target (deg).")
    parser.add_argument("--trial-single-min-deg", type=float, default=6.0, help="Min random single-wheel target (deg).")
    parser.add_argument("--trial-single-max-deg", type=float, default=12.0, help="Max random single-wheel target (deg).")
    parser.add_argument(
        "--cone-half-angle-deg",
        type=float,
        default=30.0,
        help="Keep tests within +/- this heading from tag13.",
    )
    parser.add_argument(
        "--max-forward-back-m",
        type=float,
        default=0.40,
        help="Maximum allowed |z| relative to startup origin in tag13 frame.",
    )
    parser.add_argument("--trial-straight-min-mm", type=float, default=120.0, help="Min random straight target (mm).")
    parser.add_argument("--trial-straight-max-mm", type=float, default=320.0, help="Max random straight target (mm).")
    parser.add_argument("--min-turn-vis-deg", type=float, default=10.0, help="Minimum vision heading change for turn candidates.")
    parser.add_argument("--min-turn-dist-diff-mm", type=float, default=15.0, help="Minimum wheel distance-difference for turn candidates.")
    parser.add_argument("--min-turn-step-events", type=float, default=2.0, help="Minimum per-wheel events for turn candidates.")
    parser.add_argument("--min-single-vis-deg", type=float, default=8.0, help="Minimum vision heading change for single-wheel candidates.")
    parser.add_argument("--min-single-step-events", type=float, default=2.0, help="Minimum wheel events for single-wheel candidates.")
    parser.add_argument("--min-straight-vis-mm", type=float, default=40.0, help="Minimum vision distance for straight candidates.")
    parser.add_argument("--min-straight-odom-mm", type=float, default=15.0, help="Minimum odometry distance for straight candidates.")
    parser.add_argument("--straight-scale-min", type=float, default=0.75, help="Minimum allowed straight-trial scale.")
    parser.add_argument("--straight-scale-max", type=float, default=1.25, help="Maximum allowed straight-trial scale.")
    parser.add_argument(
        "--trial-power-jitter",
        type=float,
        default=0.15,
        help="Per-cycle random power jitter fraction around base turn/drive powers.",
    )
    parser.add_argument(
        "--random-seed",
        type=int,
        default=-1,
        help="Optional RNG seed for reproducible trial randomization (<0 uses non-deterministic seed).",
    )

    parser.add_argument("--box-x-m", type=float, default=0.15, help="Lateral bound around origin (meters).")
    parser.add_argument("--box-z-m", type=float, default=0.25, help="Forward/back bound around origin (meters).")
    parser.add_argument("--max-heading-deg", type=float, default=80.0, help="Absolute heading guardrail from origin.")
    parser.add_argument("--guardrail-abort-s", type=float, default=0.5, help="Continuous violation time before guardrail recovery.")
    parser.add_argument("--guardrail-recovery-timeout-s", type=float, default=8.0, help="Max time to recover from guardrail condition.")
    parser.add_argument("--max-vision-jump-m", type=float, default=0.08, help="Reject one-frame vision jumps above this position delta.")
    parser.add_argument(
        "--max-vision-jump-heading-deg",
        type=float,
        default=18.0,
        help="Reject one-frame vision jumps above this heading delta.",
    )
    parser.add_argument("--max-vision-radius-m", type=float, default=1.2, help="Reject vision relative poses outside this radius.")
    parser.add_argument(
        "--enable-guardrails",
        action="store_true",
        help="Enable motion guardrails (off by default; recovery-only mode by default).",
    )
    parser.add_argument(
        "--vision-guard-max-age-s",
        type=float,
        default=0.5,
        help="Use vision guardrails only when tag13 sample is newer than this.",
    )
    parser.add_argument("--vision-ema-alpha", type=float, default=0.35, help="EMA alpha for accepted vision relative pose.")
    parser.add_argument("--origin-stable-frames", type=int, default=8, help="Consecutive tag13 frames required for origin.")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    config, _ = load_config(args.config)
    serial_cfg = service_cfg(config, "serial_mcu_bridge")
    logging.basicConfig(
        level=getattr(logging, log_level(config, serial_cfg), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    tuner = AutoTuner(args)

    def _shutdown(_signum: int, _frame: Any) -> None:
        tuner.stop_event = True

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    try:
        return tuner.run()
    except KeyboardInterrupt:
        return 0
    finally:
        tuner._disconnect()


if __name__ == "__main__":
    raise SystemExit(main())
