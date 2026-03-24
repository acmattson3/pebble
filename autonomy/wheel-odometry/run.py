#!/usr/bin/env python3
"""Estimate differential-drive pose from shared-memory hall sensor samples."""

from __future__ import annotations

import argparse
import json
import logging
import math
import signal
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import paho.mqtt.client as mqtt

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

from control.common.config import load_config, log_level, service_cfg
from control.common.mqtt import mqtt_auth_and_tls
from control.common.odometry_shm import OdometryRawShmReader
from control.common.topics import identity_from_config


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _coerce_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _normalize_angle_rad(theta: float) -> float:
    return math.atan2(math.sin(theta), math.cos(theta))


def _resolve_path(path_value: str) -> Path:
    path = Path(path_value).expanduser()
    if not path.is_absolute():
        path = (Path.cwd() / path).resolve()
    return path


def _load_tuned_values(path: Path) -> dict[str, float]:
    if not path.exists():
        return {}
    try:
        loaded = json.loads(path.read_text())
    except OSError as exc:
        logging.warning("Unable to read tune-state file %s: %s", path, exc)
        return {}
    except json.JSONDecodeError:
        logging.warning("Tune-state file is invalid JSON: %s", path)
        return {}

    if not isinstance(loaded, dict):
        return {}
    current = loaded.get("current_values")
    if not isinstance(current, dict):
        return {}

    tuned: dict[str, float] = {}
    for key in ("left_mm_per_event", "right_mm_per_event", "track_width_mm"):
        value = _coerce_float(current.get(key), float("nan"))
        if math.isfinite(value) and value > 0.0:
            tuned[key] = value
    return tuned


@dataclass
class WheelStepUpdate:
    event_count: int = 0
    progress_delta: float = 0.0
    activity: float = 0.0


@dataclass
class WheelEventTracker:
    signal_alpha: float
    slope_alpha: float
    slope_deadband_adc: float
    confirm_samples: int
    min_extremum_ns: int
    min_swing_adc: float
    swing_alpha: float
    initial_swing_adc: float
    max_progress: float

    initialized: bool = False
    filtered: float = 0.0
    prev_filtered: float = 0.0
    slope_ema: float = 0.0
    pending_sign: int = 0
    pending_count: int = 0
    last_nonzero_sign: Optional[int] = None
    last_extremum_ns: Optional[int] = None
    last_extremum_value: Optional[float] = None
    swing_est_adc: float = 180.0
    progress: float = 0.0
    extrema_events: int = 0
    rejected_sign_flips: int = 0

    def update(self, sample: float, mono_ns: int, allow_interpolation: bool) -> WheelStepUpdate:
        if not self.initialized:
            self.initialized = True
            self.filtered = sample
            self.prev_filtered = sample
            self.slope_ema = 0.0
            self.pending_sign = 0
            self.pending_count = 0
            self.last_nonzero_sign = None
            self.last_extremum_ns = None
            self.last_extremum_value = None
            self.swing_est_adc = max(1.0, float(self.initial_swing_adc))
            self.progress = 0.0
            return WheelStepUpdate()

        self.filtered += self.signal_alpha * (sample - self.filtered)
        raw_slope = self.filtered - self.prev_filtered
        self.prev_filtered = self.filtered
        self.slope_ema += self.slope_alpha * (raw_slope - self.slope_ema)

        sign_now = 0
        if self.slope_ema > self.slope_deadband_adc:
            sign_now = 1
        elif self.slope_ema < -self.slope_deadband_adc:
            sign_now = -1

        event_count = 0
        if sign_now == 0:
            self.pending_sign = 0
            self.pending_count = 0
        else:
            if sign_now == self.pending_sign:
                self.pending_count += 1
            else:
                self.pending_sign = sign_now
                self.pending_count = 1

            if self.pending_count >= max(1, int(self.confirm_samples)):
                self.pending_count = 0
                if self.last_nonzero_sign is None:
                    self.last_nonzero_sign = sign_now
                elif sign_now != self.last_nonzero_sign:
                    event_ok = True
                    if self.last_extremum_ns is not None and (mono_ns - self.last_extremum_ns) < self.min_extremum_ns:
                        event_ok = False
                    if self.last_extremum_value is not None:
                        if abs(self.filtered - self.last_extremum_value) < self.min_swing_adc:
                            event_ok = False

                    if event_ok:
                        if self.last_extremum_value is not None:
                            observed_swing = abs(self.filtered - self.last_extremum_value)
                            self.swing_est_adc += self.swing_alpha * (observed_swing - self.swing_est_adc)
                        self.last_extremum_value = self.filtered
                        self.last_extremum_ns = mono_ns
                        self.last_nonzero_sign = sign_now
                        self.progress = 0.0
                        self.extrema_events += 1
                        event_count = 1
                    else:
                        self.rejected_sign_flips += 1

        progress_delta = 0.0
        if (
            allow_interpolation
            and self.last_extremum_value is not None
            and self.swing_est_adc > 1.0
        ):
            raw_progress = abs(self.filtered - self.last_extremum_value) / max(1.0, self.swing_est_adc)
            bounded = _clamp(raw_progress, 0.0, self.max_progress)
            if bounded > self.progress:
                progress_delta = bounded - self.progress
                self.progress = bounded

        return WheelStepUpdate(event_count=event_count, progress_delta=progress_delta, activity=abs(self.slope_ema))


@dataclass
class Pose2D:
    x_mm: float = 0.0
    y_mm: float = 0.0
    heading_rad: float = 0.0
    left_mm: float = 0.0
    right_mm: float = 0.0
    distance_mm: float = 0.0

    def integrate(self, left_delta_mm: float, right_delta_mm: float, track_width_mm: float) -> None:
        self.left_mm += left_delta_mm
        self.right_mm += right_delta_mm
        self.distance_mm += abs(left_delta_mm + right_delta_mm) * 0.5

        d_s = 0.5 * (left_delta_mm + right_delta_mm)
        d_theta = (right_delta_mm - left_delta_mm) / track_width_mm
        theta0 = self.heading_rad

        if abs(d_theta) < 1e-9:
            self.x_mm += d_s * math.cos(theta0)
            self.y_mm += d_s * math.sin(theta0)
        else:
            theta1 = theta0 + d_theta
            r_icc = d_s / d_theta
            self.x_mm += r_icc * (math.sin(theta1) - math.sin(theta0))
            self.y_mm += -r_icc * (math.cos(theta1) - math.cos(theta0))

        self.heading_rad = _normalize_angle_rad(theta0 + d_theta)


class DriveCommandTracker:
    def __init__(self, deadband: float, hold_seconds: float) -> None:
        self.deadband = max(0.0, float(deadband))
        self.hold_seconds = max(0.0, float(hold_seconds))
        self.lock = threading.Lock()
        self.left_sign = 0
        self.right_sign = 0
        self.left_cmd = 0.0
        self.right_cmd = 0.0
        self.left_mono = 0.0
        self.right_mono = 0.0

    def update_from_payload(self, payload: dict[str, Any]) -> None:
        if not isinstance(payload, dict):
            return
        value = payload.get("value")
        values = value if isinstance(value, dict) else payload
        x = _coerce_float(values.get("x"), 0.0)
        z = _coerce_float(values.get("z"), 0.0)
        left = _clamp(z + x, -1.0, 1.0)
        right = _clamp(z - x, -1.0, 1.0)
        now = time.monotonic()

        left_sign = 0 if abs(left) < self.deadband else (1 if left > 0 else -1)
        right_sign = 0 if abs(right) < self.deadband else (1 if right > 0 else -1)

        with self.lock:
            self.left_cmd = float(left)
            self.right_cmd = float(right)
            if left_sign != 0:
                self.left_sign = left_sign
                self.left_mono = now
            else:
                # Explicit wheel zero from payload should clear immediately.
                self.left_sign = 0
                self.left_mono = now

            if right_sign != 0:
                self.right_sign = right_sign
                self.right_mono = now
            else:
                # Explicit wheel zero from payload should clear immediately.
                self.right_sign = 0
                self.right_mono = now

    def wheel_state(self, now_mono: float) -> tuple[int, int, float, float]:
        with self.lock:
            left_sign = self.left_sign if (now_mono - self.left_mono) <= self.hold_seconds else 0
            right_sign = self.right_sign if (now_mono - self.right_mono) <= self.hold_seconds else 0
            left_cmd = float(self.left_cmd)
            right_cmd = float(self.right_cmd)
        return left_sign, right_sign, left_cmd, right_cmd


class WheelOdometryRuntime:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.config, self.config_path = load_config(args.config)
        self.identity = identity_from_config(self.config)

        self.local_mqtt_cfg = self.config.get("local_mqtt") if isinstance(self.config.get("local_mqtt"), dict) else {}
        self.local_host = str(self.local_mqtt_cfg.get("host") or "127.0.0.1")
        self.local_port = int(self.local_mqtt_cfg.get("port") or 1883)
        self.local_keepalive = int(self.local_mqtt_cfg.get("keepalive") or 60)

        serial_cfg = service_cfg(self.config, "serial_mcu_bridge")
        topics_cfg = serial_cfg.get("topics") if isinstance(serial_cfg.get("topics"), dict) else {}
        self.drive_topic = str(topics_cfg.get("drive_values") or self.identity.topic("incoming", "drive-values"))
        self.odometry_topic = str(args.topic or self.identity.topic("outgoing", "wheel-odometry"))

        shm_name = str(args.shm_name or "").strip()
        if shm_name:
            self.shm_name = shm_name
        else:
            self.shm_name = f"{self.identity.system}_{self.identity.type}_{self.identity.robot_id}_odometry_raw"

        self.left_channel = str(args.left_channel).lower()
        self.right_channel = str(args.right_channel).lower()
        allowed = {"a0", "a1", "a2"}
        if self.left_channel not in allowed or self.right_channel not in allowed:
            raise SystemExit("--left-channel/--right-channel must be one of a0,a1,a2")

        self.wheel_diameter_mm = float(args.wheel_diameter_mm)
        self.track_width_mm = float(args.track_width_mm)
        self.magnets = max(1, int(args.magnets))
        self.alternating_polarity = bool(args.alternating_polarity)
        self.events_per_rev = self.magnets if self.alternating_polarity else max(1, self.magnets * 2)
        self.mm_per_event = (math.pi * self.wheel_diameter_mm) / float(self.events_per_rev)

        self.tune_state_path = _resolve_path(str(args.tune_state_file))
        self.tuned_values = _load_tuned_values(self.tune_state_path) if bool(args.use_tune_state) else {}

        left_cli_value = float(args.left_mm_per_event)
        right_cli_value = float(args.right_mm_per_event)
        self.left_mm_source = "geometry"
        self.right_mm_source = "geometry"
        if left_cli_value > 0.0:
            self.left_mm_per_event = left_cli_value
            self.left_mm_source = "cli"
        elif "left_mm_per_event" in self.tuned_values:
            self.left_mm_per_event = float(self.tuned_values["left_mm_per_event"])
            self.left_mm_source = f"tune-state:{self.tune_state_path.name}"
        else:
            self.left_mm_per_event = float(self.mm_per_event)

        if right_cli_value > 0.0:
            self.right_mm_per_event = right_cli_value
            self.right_mm_source = "cli"
        elif "right_mm_per_event" in self.tuned_values:
            self.right_mm_per_event = float(self.tuned_values["right_mm_per_event"])
            self.right_mm_source = f"tune-state:{self.tune_state_path.name}"
        else:
            self.right_mm_per_event = float(self.mm_per_event)

        self.left_mm_per_step = float(self.left_mm_per_event)
        self.right_mm_per_step = float(self.right_mm_per_event)

        self.left_direction_sign = 1 if float(args.left_direction_sign) >= 0 else -1
        self.right_direction_sign = 1 if float(args.right_direction_sign) >= 0 else -1
        self.publish_rate_hz = max(1.0, float(args.publish_rate_hz))
        self.idle_publish_rate_hz = max(0.2, float(args.idle_publish_rate_hz))
        self.moving_hold_seconds = max(0.0, float(args.moving_hold_ms) / 1000.0)
        self.shm_poll_s = max(0.001, float(args.shm_poll_ms) / 1000.0)
        self.interpolation_timeout_s = max(0.0, float(args.interpolation_timeout_ms) / 1000.0)
        self.stall_timeout_s = max(0.0, float(args.stall_timeout_ms) / 1000.0)
        self.min_activity_adc = max(0.0, float(args.min_activity_adc))

        self.left_tracker = WheelEventTracker(
            signal_alpha=float(args.signal_alpha),
            slope_alpha=float(args.slope_alpha),
            slope_deadband_adc=float(args.slope_deadband_adc),
            confirm_samples=max(1, int(args.confirm_samples)),
            min_extremum_ns=max(0, int(float(args.min_extremum_ms) * 1_000_000.0)),
            min_swing_adc=max(0.0, float(args.min_swing_adc)),
            swing_alpha=_clamp(float(args.swing_alpha), 0.0, 1.0),
            initial_swing_adc=max(1.0, float(args.initial_swing_adc)),
            max_progress=_clamp(float(args.max_progress), 0.0, 0.999),
        )
        self.right_tracker = WheelEventTracker(
            signal_alpha=float(args.signal_alpha),
            slope_alpha=float(args.slope_alpha),
            slope_deadband_adc=float(args.slope_deadband_adc),
            confirm_samples=max(1, int(args.confirm_samples)),
            min_extremum_ns=max(0, int(float(args.min_extremum_ms) * 1_000_000.0)),
            min_swing_adc=max(0.0, float(args.min_swing_adc)),
            swing_alpha=_clamp(float(args.swing_alpha), 0.0, 1.0),
            initial_swing_adc=max(1.0, float(args.initial_swing_adc)),
            max_progress=_clamp(float(args.max_progress), 0.0, 0.999),
        )

        self.pose = Pose2D()
        self.left_crossings = 0
        self.right_crossings = 0
        self.unknown_direction_events = 0
        self.left_steps_total = 0.0
        self.right_steps_total = 0.0
        self.last_sample_seq = 0
        self.last_motion_mono = 0.0
        self.last_left_event_mono_ns: Optional[int] = None
        self.last_right_event_mono_ns: Optional[int] = None
        self.left_stall_events = 0
        self.right_stall_events = 0
        self.left_stalled = False
        self.right_stalled = False
        self.publish_seq = 0

        self.cmd_tracker = DriveCommandTracker(
            deadband=float(args.command_deadband),
            hold_seconds=max(0.0, float(args.command_hold_ms) / 1000.0),
        )

        self.client: Optional[mqtt.Client] = None
        self.connected = threading.Event()
        self.stop_event = threading.Event()
        self.reader: Optional[OdometryRawShmReader] = None

    def _on_mqtt_connect(self, client: mqtt.Client, _userdata: Any, _flags: dict[str, Any], rc: int) -> None:
        if rc != 0:
            logging.error("Local MQTT connect failed rc=%s", rc)
            return
        logging.info("Local MQTT connected; subscribing %s", self.drive_topic)
        client.subscribe(self.drive_topic, qos=0)
        self.connected.set()

    def _on_mqtt_disconnect(self, _client: mqtt.Client, _userdata: Any, rc: int) -> None:
        self.connected.clear()
        if rc != mqtt.MQTT_ERR_SUCCESS and not self.stop_event.is_set():
            logging.warning("Unexpected MQTT disconnect rc=%s", rc)

    def _on_mqtt_message(self, _client: mqtt.Client, _userdata: Any, msg: mqtt.MQTTMessage) -> None:
        if msg.topic != self.drive_topic:
            return
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
        except json.JSONDecodeError:
            return
        self.cmd_tracker.update_from_payload(payload)

    def _connect_mqtt(self) -> None:
        client = mqtt.Client()
        mqtt_auth_and_tls(client, self.local_mqtt_cfg, self.config_path.parent)
        client.on_connect = self._on_mqtt_connect
        client.on_disconnect = self._on_mqtt_disconnect
        client.on_message = self._on_mqtt_message

        delay = 1.0
        while not self.stop_event.is_set():
            try:
                client.connect(self.local_host, self.local_port, self.local_keepalive)
                client.loop_start()
                self.client = client
                return
            except OSError as exc:
                logging.warning("Local MQTT connect failed: %s", exc)
                self.stop_event.wait(delay)
                delay = min(delay * 2.0, 15.0)

    def _open_reader(self) -> None:
        while not self.stop_event.is_set():
            try:
                self.reader = OdometryRawShmReader(self.shm_name)
                logging.info("Attached to odometry shared memory %s", self.shm_name)
                return
            except FileNotFoundError:
                logging.warning("Shared memory %s not ready; retrying...", self.shm_name)
            except Exception as exc:
                logging.error("Failed opening shared memory %s: %s", self.shm_name, exc)
            self.stop_event.wait(1.0)

    def _process_sample(self, sample: dict[str, Any]) -> None:
        mono_ns = int(sample.get("mono_ns") or 0)
        if mono_ns <= 0:
            return

        left_value = _coerce_float(sample.get(self.left_channel), 0.0)
        right_value = _coerce_float(sample.get(self.right_channel), 0.0)
        now_mono = time.monotonic()
        left_cmd_sign, right_cmd_sign, left_cmd_value, right_cmd_value = self.cmd_tracker.wheel_state(now_mono)
        left_sign = left_cmd_sign
        right_sign = right_cmd_sign

        left_allow_interpolation = False
        right_allow_interpolation = False
        if left_sign != 0 and self.last_left_event_mono_ns is not None:
            left_allow_interpolation = (
                (mono_ns - self.last_left_event_mono_ns) <= int(self.interpolation_timeout_s * 1_000_000_000.0)
            )
        if right_sign != 0 and self.last_right_event_mono_ns is not None:
            right_allow_interpolation = (
                (mono_ns - self.last_right_event_mono_ns) <= int(self.interpolation_timeout_s * 1_000_000_000.0)
            )

        left_step = self.left_tracker.update(left_value, mono_ns, left_allow_interpolation)
        right_step = self.right_tracker.update(right_value, mono_ns, right_allow_interpolation)

        left_stalled = False
        right_stalled = False
        if left_sign != 0 and self.last_left_event_mono_ns is not None:
            left_stalled = (mono_ns - self.last_left_event_mono_ns) > int(self.stall_timeout_s * 1_000_000_000.0)
        if right_sign != 0 and self.last_right_event_mono_ns is not None:
            right_stalled = (mono_ns - self.last_right_event_mono_ns) > int(self.stall_timeout_s * 1_000_000_000.0)

        if left_stalled and not self.left_stalled and left_step.event_count == 0 and abs(left_cmd_value) > 0.0:
            self.left_stall_events += 1
        if right_stalled and not self.right_stalled and right_step.event_count == 0 and abs(right_cmd_value) > 0.0:
            self.right_stall_events += 1
        self.left_stalled = left_stalled
        self.right_stalled = right_stalled

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

        if left_sign == 0 and left_step_total > 0.0:
            self.unknown_direction_events += 1
            left_step_total = 0.0
        if right_sign == 0 and right_step_total > 0.0:
            self.unknown_direction_events += 1
            right_step_total = 0.0

        left_delta = left_step_total * self.left_mm_per_step * float(left_sign) * float(self.left_direction_sign)
        right_delta = right_step_total * self.right_mm_per_step * float(right_sign) * float(self.right_direction_sign)

        self.left_steps_total += left_step_total
        self.right_steps_total += right_step_total
        self.left_crossings = int(self.left_steps_total)
        self.right_crossings = int(self.right_steps_total)
        self.pose.integrate(left_delta, right_delta, self.track_width_mm)
        if left_delta != 0.0 or right_delta != 0.0:
            self.last_motion_mono = now_mono

    def _publish_pose(self) -> None:
        client = self.client
        if client is None:
            return
        is_connected = getattr(client, "is_connected", None)
        if callable(is_connected) and not is_connected():
            return

        heading = _normalize_angle_rad(self.pose.heading_rad)
        self.publish_seq += 1
        payload = {
            "value": {
                "x_mm": round(self.pose.x_mm, 3),
                "y_mm": round(self.pose.y_mm, 3),
                "heading_rad": round(heading, 6),
                "heading_deg": round(math.degrees(heading), 3),
                "left_mm": round(self.pose.left_mm, 3),
                "right_mm": round(self.pose.right_mm, 3),
                "distance_mm": round(self.pose.distance_mm, 3),
                "left_crossings": int(self.left_crossings),
                "right_crossings": int(self.right_crossings),
                "unknown_direction_events": int(self.unknown_direction_events),
                "left_step_total": round(self.left_steps_total, 6),
                "right_step_total": round(self.right_steps_total, 6),
                "left_mm_per_step": round(self.left_mm_per_step, 6),
                "right_mm_per_step": round(self.right_mm_per_step, 6),
                "left_extrema_events": int(self.left_tracker.extrema_events),
                "right_extrema_events": int(self.right_tracker.extrema_events),
                "left_rejected_sign_flips": int(self.left_tracker.rejected_sign_flips),
                "right_rejected_sign_flips": int(self.right_tracker.rejected_sign_flips),
                "left_stall_events": int(self.left_stall_events),
                "right_stall_events": int(self.right_stall_events),
                "sample_seq": int(self.last_sample_seq),
                "publish_seq": int(self.publish_seq),
            },
            "unit": "mm",
            "source": "wheel-odometry",
            "timestamp": time.time(),
        }
        client.publish(self.odometry_topic, json.dumps(payload), qos=int(self.args.qos), retain=False)

    def start(self) -> int:
        logging.info("Odometry topic: %s", self.odometry_topic)
        logging.info(
            "Geometry: diameter=%.2fmm track=%.2fmm magnets=%d events_per_rev=%d mm/event=%.3f",
            self.wheel_diameter_mm,
            self.track_width_mm,
            self.magnets,
            self.events_per_rev,
            self.mm_per_event,
        )
        logging.info(
            "Per-wheel calibration: left_mm/event=%.3f (%s) right_mm/event=%.3f (%s)",
            self.left_mm_per_event,
            self.left_mm_source,
            self.right_mm_per_event,
            self.right_mm_source,
        )
        logging.info(
            "Calibration delta vs geometry mm/event=%.3f: left=%+.1f%% right=%+.1f%%",
            self.mm_per_event,
            ((self.left_mm_per_event / self.mm_per_event) - 1.0) * 100.0,
            ((self.right_mm_per_event / self.mm_per_event) - 1.0) * 100.0,
        )
        if self.tuned_values:
            logging.info("Loaded tune-state values from %s", self.tune_state_path)
        elif bool(self.args.use_tune_state):
            logging.info("No tune-state values loaded from %s", self.tune_state_path)
        logging.info(
            "Step model: mm/step left=%.3f right=%.3f interpolation_timeout=%.0fms stall_timeout=%.0fms",
            self.left_mm_per_step,
            self.right_mm_per_step,
            self.interpolation_timeout_s * 1000.0,
            self.stall_timeout_s * 1000.0,
        )
        logging.info(
            "Channels: left=%s right=%s command_topic=%s",
            self.left_channel,
            self.right_channel,
            self.drive_topic,
        )

        self._connect_mqtt()
        self._open_reader()
        if self.reader is None:
            return 1

        latest = self.reader.read_latest()
        last_seq = int(latest["seq"]) if latest else 0
        next_publish_at = time.monotonic()

        while not self.stop_event.is_set():
            assert self.reader is not None
            rows = self.reader.read_since(last_seq)
            if rows:
                for row in rows:
                    self.last_sample_seq = int(row.get("seq") or self.last_sample_seq)
                    self._process_sample(row)
                    last_seq = int(row.get("seq") or last_seq)

            now = time.monotonic()
            moving = (now - self.last_motion_mono) <= self.moving_hold_seconds
            rate = self.publish_rate_hz if moving else self.idle_publish_rate_hz
            interval_s = 1.0 / max(0.2, rate)
            if now >= next_publish_at:
                self._publish_pose()
                next_publish_at = now + interval_s

            if not rows:
                self.stop_event.wait(self.shm_poll_s)

        return 0

    def stop(self) -> None:
        self.stop_event.set()
        if self.reader is not None:
            try:
                self.reader.close()
            except Exception:
                logging.debug("Shared-memory close failed", exc_info=True)
            self.reader = None
        if self.client is not None:
            try:
                self.client.loop_stop()
                self.client.disconnect()
            except Exception:
                logging.debug("MQTT shutdown failed", exc_info=True)
            self.client = None


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run shared-memory wheel odometry and publish pose to local MQTT.")
    parser.add_argument("--config", default="control/configs/config.json", help="Path to unified runtime config JSON.")
    parser.add_argument("--shm-name", default="", help="Shared-memory name (default from robot identity).")
    parser.add_argument("--topic", default="", help="MQTT topic for outgoing odometry payloads.")
    parser.add_argument("--qos", type=int, default=0, choices=[0, 1], help="MQTT QoS for odometry publishing.")

    parser.add_argument("--left-channel", default="a0", help="Left wheel analog channel: a0/a1/a2.")
    parser.add_argument("--right-channel", default="a1", help="Right wheel analog channel: a0/a1/a2.")
    parser.add_argument("--wheel-diameter-mm", type=float, default=61.0, help="Wheel diameter in millimeters.")
    parser.add_argument("--track-width-mm", type=float, default=130.5, help="Distance between wheels in millimeters.")
    parser.add_argument(
        "--tune-state-file",
        default="autonomy/wheel-odometry/auto_tune_state.json",
        help="Path to auto-tune state JSON (uses current_values when per-wheel event args are <=0).",
    )
    parser.add_argument(
        "--use-tune-state",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Load left/right tuned mm-per-event values from tune-state JSON.",
    )
    parser.add_argument("--magnets", type=int, default=10, help="Number of magnets on wheel ring.")
    parser.add_argument(
        "--alternating-polarity",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Set true when magnets alternate polarity around the wheel.",
    )

    parser.add_argument("--signal-alpha", type=float, default=0.30, help="EMA alpha for filtered signal.")
    parser.add_argument("--slope-alpha", type=float, default=0.35, help="EMA alpha for slope smoothing.")
    parser.add_argument("--slope-deadband-adc", type=float, default=0.8, help="Slope deadband in ADC units/sample.")
    parser.add_argument("--confirm-samples", type=int, default=2, help="Consecutive slope-sign samples to confirm flips.")
    parser.add_argument("--min-extremum-ms", type=float, default=15.0, help="Minimum time between accepted extrema.")
    parser.add_argument("--min-swing-adc", type=float, default=24.0, help="Minimum filtered ADC swing for an event.")
    parser.add_argument("--swing-alpha", type=float, default=0.2, help="EMA gain for per-wheel swing estimate.")
    parser.add_argument("--initial-swing-adc", type=float, default=180.0, help="Initial swing estimate in ADC units.")
    parser.add_argument("--max-progress", type=float, default=0.98, help="Max in-between progress fraction per step.")
    parser.add_argument(
        "--interpolation-timeout-ms",
        type=float,
        default=180.0,
        help="Allow in-between interpolation only this long after an accepted event.",
    )
    parser.add_argument(
        "--stall-timeout-ms",
        type=float,
        default=320.0,
        help="Wheel is considered stalled when no accepted events arrive within this time.",
    )
    parser.add_argument(
        "--min-activity-adc",
        type=float,
        default=0.8,
        help="Minimum absolute slope activity for applying in-between interpolation.",
    )

    parser.add_argument("--command-deadband", type=float, default=0.08, help="Deadband for wheel command sign capture.")
    parser.add_argument(
        "--command-hold-ms",
        type=float,
        default=350.0,
        help="Hold most recent non-zero wheel command sign for this many ms.",
    )
    parser.add_argument(
        "--left-direction-sign",
        type=float,
        default=1.0,
        help="Multiplier (+1/-1) mapping left wheel command sign to forward displacement.",
    )
    parser.add_argument(
        "--right-direction-sign",
        type=float,
        default=1.0,
        help="Multiplier (+1/-1) mapping right wheel command sign to forward displacement.",
    )

    parser.add_argument("--publish-rate-hz", type=float, default=20.0, help="Publish rate while moving.")
    parser.add_argument("--idle-publish-rate-hz", type=float, default=2.0, help="Publish rate while stationary.")
    parser.add_argument("--moving-hold-ms", type=float, default=450.0, help="Motion hold window for moving rate.")
    parser.add_argument("--shm-poll-ms", type=float, default=5.0, help="Shared-memory poll interval in milliseconds.")
    parser.add_argument(
        "--left-mm-per-event",
        type=float,
        default=0.0,
        help="Left wheel distance per accepted extrema event (<=0 uses geometry-derived value).",
    )
    parser.add_argument(
        "--right-mm-per-event",
        type=float,
        default=0.0,
        help="Right wheel distance per accepted extrema event (<=0 uses geometry-derived value).",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    config, _ = load_config(args.config)
    svc_cfg = service_cfg(config, "autonomy_manager")
    logging.basicConfig(
        level=getattr(logging, log_level(config, svc_cfg), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    runtime = WheelOdometryRuntime(args)

    def _shutdown(_signum: int, _frame: Any) -> None:
        runtime.stop()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    try:
        return runtime.start()
    finally:
        runtime.stop()


if __name__ == "__main__":
    raise SystemExit(main())
