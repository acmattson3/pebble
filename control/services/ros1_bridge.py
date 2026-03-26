#!/usr/bin/env python3
"""Bridge Pebble MQTT topics and low-bandwidth ROS 1 state."""

from __future__ import annotations

import argparse
import json
import logging
import math
import signal
import sys
import threading
import time
from pathlib import Path
from typing import Any, Optional

import paho.mqtt.client as mqtt

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parents[2]))

from control.common.config import load_config, log_level, service_cfg
from control.common.mqtt import create_client, mqtt_auth_and_tls
from control.common.topics import identity_from_config


def clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _parse_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _parse_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _decode_payload(payload: bytes) -> Any:
    try:
        return json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None


def _unwrap_value(payload: Any) -> Any:
    if isinstance(payload, dict) and isinstance(payload.get("value"), dict):
        return payload["value"]
    return payload


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _stamp_to_seconds(stamp: Any) -> float | None:
    if stamp is None:
        return None
    secs = getattr(stamp, "secs", None)
    nsecs = getattr(stamp, "nsecs", None)
    if isinstance(secs, (int, float)):
        ns_value = float(nsecs) if isinstance(nsecs, (int, float)) else 0.0
        return float(secs) + (ns_value / 1_000_000_000.0)
    return None


def _header_timestamp_seconds(msg: Any) -> float:
    header = getattr(msg, "header", None)
    ts = _stamp_to_seconds(getattr(header, "stamp", None))
    if ts is not None:
        return ts
    return time.time()


def _frame_id(msg: Any) -> str:
    header = getattr(msg, "header", None)
    frame_id = getattr(header, "frame_id", None)
    return str(frame_id).strip() if isinstance(frame_id, str) else ""


def _child_frame_id(msg: Any) -> str:
    raw = getattr(msg, "child_frame_id", None)
    return str(raw).strip() if isinstance(raw, str) else ""


def _quat_yaw_rad(quat: Any) -> float:
    x = _parse_float(getattr(quat, "x", 0.0), 0.0)
    y = _parse_float(getattr(quat, "y", 0.0), 0.0)
    z = _parse_float(getattr(quat, "z", 0.0), 0.0)
    w = _parse_float(getattr(quat, "w", 1.0), 1.0)
    siny_cosp = 2.0 * (w * z + x * y)
    cosy_cosp = 1.0 - 2.0 * (y * y + z * z)
    return math.atan2(siny_cosp, cosy_cosp)


def _diagnostic_value(statuses: list[Any], status_name: str, key_name: str) -> str | None:
    target_status = status_name.strip()
    target_key = key_name.strip()
    if not target_key:
        return None
    for status in statuses:
        if target_status and str(getattr(status, "name", "")).strip() != target_status:
            continue
        for item in getattr(status, "values", []) or []:
            if str(getattr(item, "key", "")).strip() == target_key:
                value = getattr(item, "value", None)
                if value is None:
                    return None
                return str(value).strip()
    return None


def _normalize_token(value: str) -> str:
    return value.strip().lower()


def _sample_path_points(poses: list[Any], max_points: int) -> list[Any]:
    if max_points <= 0 or len(poses) <= max_points:
        return list(poses)
    if max_points == 1:
        return [poses[-1]]
    sampled: list[Any] = []
    total = len(poses)
    for i in range(max_points):
        idx = round(i * (total - 1) / (max_points - 1))
        sampled.append(poses[int(idx)])
    return sampled


def _goal_status_label(code: int) -> str:
    mapping = {
        0: "PENDING",
        1: "ACTIVE",
        2: "PREEMPTED",
        3: "SUCCEEDED",
        4: "ABORTED",
        5: "REJECTED",
        6: "PREEMPTING",
        7: "RECALLING",
        8: "RECALLED",
        9: "LOST",
    }
    return mapping.get(code, "UNKNOWN")


def _diagnostic_level_label(level: int) -> str:
    return {0: "OK", 1: "WARN", 2: "ERROR", 3: "STALE"}.get(level, "UNKNOWN")


class Ros1Bridge:
    def __init__(self, config: dict[str, Any], config_path: Path, *, ros_modules: Any | None = None) -> None:
        self.config = config
        self.config_path = config_path
        self.identity = identity_from_config(config)
        self.service_cfg = service_cfg(config, "ros1_bridge")
        if not self.service_cfg.get("enabled", False):
            raise SystemExit("ros1_bridge is disabled in config.")

        self.local_mqtt_cfg = _as_dict(config.get("local_mqtt"))
        self.local_host = str(self.local_mqtt_cfg.get("host") or "127.0.0.1")
        self.local_port = int(self.local_mqtt_cfg.get("port") or 1883)
        self.local_keepalive = int(self.local_mqtt_cfg.get("keepalive") or 60)

        topics_cfg = _as_dict(self.service_cfg.get("topics"))
        self.drive_topic = str(topics_cfg.get("drive_values") or self.identity.topic("incoming", "drive-values"))
        self.wheel_odometry_topic = str(
            topics_cfg.get("wheel_odometry") or self.identity.topic("outgoing", "wheel-odometry")
        )
        self.charging_status_topic = str(
            topics_cfg.get("charging_status") or self.identity.topic("outgoing", "charging-status")
        )
        self.charging_level_topic = str(
            topics_cfg.get("charging_level") or self.identity.topic("outgoing", "charging-level")
        )
        self.localization_pose_topic = str(
            topics_cfg.get("localization_pose") or self.identity.topic("outgoing", "localization-pose")
        )
        self.navigation_status_topic = str(
            topics_cfg.get("navigation_status") or self.identity.topic("outgoing", "navigation-status")
        )
        self.navigation_goal_topic = str(
            topics_cfg.get("navigation_goal") or self.identity.topic("outgoing", "navigation-goal")
        )
        self.navigation_local_plan_topic = str(
            topics_cfg.get("navigation_local_plan") or self.identity.topic("outgoing", "navigation/local-plan")
        )
        self.navigation_global_plan_topic = str(
            topics_cfg.get("navigation_global_plan") or self.identity.topic("outgoing", "navigation/global-plan")
        )
        self.diagnostics_topic_out = str(topics_cfg.get("diagnostics") or self.identity.topic("outgoing", "diagnostics"))

        heartbeat_cfg = _as_dict(self.service_cfg.get("heartbeat"))
        self.heartbeat_enabled = bool(heartbeat_cfg.get("enabled", True))
        self.heartbeat_topic = str(heartbeat_cfg.get("topic") or self.identity.topic("outgoing", "online"))
        self.heartbeat_interval = max(0.05, _parse_float(heartbeat_cfg.get("interval_seconds"), 0.25))
        self.heartbeat_qos = int(heartbeat_cfg.get("qos") or 1)
        self.heartbeat_retain = bool(heartbeat_cfg.get("retain", True))
        self.next_heartbeat_at = 0.0

        ros_cfg = _as_dict(self.service_cfg.get("ros"))
        self.node_name = str(ros_cfg.get("node_name") or "pebble_ros1_bridge")
        self.cmd_vel_topic = str(ros_cfg.get("cmd_vel_topic") or "/cmd_vel")
        self.cmd_vel_publish_interval = max(0.0, _parse_float(ros_cfg.get("cmd_vel_publish_interval_seconds"), 0.05))
        self.odometry_topic = str(ros_cfg.get("odometry_topic") or "/odometry/filtered")
        self.navigation_status_ros_topic = str(ros_cfg.get("navigation_status_topic") or "/move_base/status")
        self.navigation_goal_ros_topic = str(ros_cfg.get("navigation_goal_topic") or "/move_base/current_goal")
        self.navigation_local_plan_ros_topic = str(
            ros_cfg.get("navigation_local_plan_topic") or "/move_base/TrajectoryPlannerROS/local_plan"
        )
        self.navigation_global_plan_ros_topic = str(
            ros_cfg.get("navigation_global_plan_topic") or "/move_base/NavfnROS/plan"
        )
        self.diagnostics_ros_topic = str(ros_cfg.get("diagnostics_topic") or "/diagnostics_agg")

        motion_cfg = _as_dict(self.service_cfg.get("motion"))
        self.max_linear_speed_mps = max(0.0, _parse_float(motion_cfg.get("max_linear_speed_mps"), 0.6))
        self.max_angular_speed_radps = max(0.0, _parse_float(motion_cfg.get("max_angular_speed_radps"), 1.2))

        safety_cfg = _as_dict(self.service_cfg.get("safety"))
        self.drive_timeout_seconds = max(0.0, _parse_float(safety_cfg.get("drive_timeout_seconds"), 0.75))
        self.ignore_retained_drive = bool(safety_cfg.get("ignore_retained_drive", True))
        self.stop_on_shutdown = bool(safety_cfg.get("stop_on_shutdown", True))

        telemetry_cfg = _as_dict(self.service_cfg.get("telemetry"))

        wheel_odom_cfg = _as_dict(telemetry_cfg.get("wheel_odometry"))
        self.wheel_odometry_enabled = bool(wheel_odom_cfg.get("enabled", True))
        self.wheel_odometry_qos = int(wheel_odom_cfg.get("qos") or 0)
        self.wheel_odometry_retain = bool(wheel_odom_cfg.get("retain", False))
        self.wheel_odometry_interval = max(0.05, _parse_float(wheel_odom_cfg.get("interval_seconds"), 0.25))
        self.next_wheel_odometry_at = 0.0

        localization_cfg = _as_dict(telemetry_cfg.get("localization_pose"))
        self.localization_pose_enabled = bool(localization_cfg.get("enabled", True))
        self.localization_pose_qos = int(localization_cfg.get("qos") or 0)
        self.localization_pose_retain = bool(localization_cfg.get("retain", False))
        self.localization_pose_interval = max(0.05, _parse_float(localization_cfg.get("interval_seconds"), 0.25))
        self.next_localization_pose_at = 0.0

        nav_status_cfg = _as_dict(telemetry_cfg.get("navigation_status"))
        self.navigation_status_enabled = bool(nav_status_cfg.get("enabled", True))
        self.navigation_status_qos = int(nav_status_cfg.get("qos") or 1)
        self.navigation_status_retain = bool(nav_status_cfg.get("retain", True))
        self.navigation_status_interval = max(0.1, _parse_float(nav_status_cfg.get("interval_seconds"), 0.5))
        self.next_navigation_status_at = 0.0
        self.latest_navigation_goal: dict[str, Any] | None = None

        nav_goal_cfg = _as_dict(telemetry_cfg.get("navigation_goal"))
        self.navigation_goal_enabled = bool(nav_goal_cfg.get("enabled", True))
        self.navigation_goal_qos = int(nav_goal_cfg.get("qos") or 1)
        self.navigation_goal_retain = bool(nav_goal_cfg.get("retain", True))

        local_plan_cfg = _as_dict(telemetry_cfg.get("navigation_local_plan"))
        self.navigation_local_plan_enabled = bool(local_plan_cfg.get("enabled", False))
        self.navigation_local_plan_qos = int(local_plan_cfg.get("qos") or 0)
        self.navigation_local_plan_retain = bool(local_plan_cfg.get("retain", False))
        self.navigation_local_plan_interval = max(0.1, _parse_float(local_plan_cfg.get("interval_seconds"), 1.0))
        self.navigation_local_plan_max_points = max(2, _parse_int(local_plan_cfg.get("max_points"), 40))
        self.next_navigation_local_plan_at = 0.0

        global_plan_cfg = _as_dict(telemetry_cfg.get("navigation_global_plan"))
        self.navigation_global_plan_enabled = bool(global_plan_cfg.get("enabled", False))
        self.navigation_global_plan_qos = int(global_plan_cfg.get("qos") or 0)
        self.navigation_global_plan_retain = bool(global_plan_cfg.get("retain", False))
        self.navigation_global_plan_interval = max(0.1, _parse_float(global_plan_cfg.get("interval_seconds"), 1.0))
        self.navigation_global_plan_max_points = max(2, _parse_int(global_plan_cfg.get("max_points"), 60))
        self.next_navigation_global_plan_at = 0.0

        diagnostics_cfg = _as_dict(telemetry_cfg.get("diagnostics"))
        self.diagnostics_enabled = bool(diagnostics_cfg.get("enabled", True))
        self.diagnostics_qos = int(diagnostics_cfg.get("qos") or 1)
        self.diagnostics_retain = bool(diagnostics_cfg.get("retain", True))
        self.diagnostics_interval = max(0.1, _parse_float(diagnostics_cfg.get("interval_seconds"), 1.0))
        self.diagnostics_max_items = max(1, _parse_int(diagnostics_cfg.get("max_items"), 8))
        self.next_diagnostics_at = 0.0

        charge_level_cfg = _as_dict(telemetry_cfg.get("charging_level"))
        self.charging_level_enabled = bool(charge_level_cfg.get("enabled", True))
        self.charging_level_qos = int(charge_level_cfg.get("qos") or 1)
        self.charging_level_retain = bool(charge_level_cfg.get("retain", True))
        self.charging_level_status_name = str(charge_level_cfg.get("status_name") or "/Jackal Base/General/Battery")
        self.charging_level_key = str(charge_level_cfg.get("key") or "Battery Voltage (V)")

        charge_status_cfg = _as_dict(telemetry_cfg.get("charging_status"))
        self.charging_status_enabled = bool(charge_status_cfg.get("enabled", False))
        self.charging_status_qos = int(charge_status_cfg.get("qos") or 1)
        self.charging_status_retain = bool(charge_status_cfg.get("retain", True))
        self.charging_status_status_name = str(charge_status_cfg.get("status_name") or "")
        self.charging_status_key = str(charge_status_cfg.get("key") or "")
        truthy_values = charge_status_cfg.get("truthy_values")
        falsey_values = charge_status_cfg.get("falsey_values")
        self.charging_status_truthy_values = {
            _normalize_token(str(item))
            for item in (truthy_values if isinstance(truthy_values, list) else ["true", "charging", "yes", "1"])
        }
        self.charging_status_falsey_values = {
            _normalize_token(str(item))
            for item in (falsey_values if isinstance(falsey_values, list) else ["false", "not charging", "no", "0"])
        }

        self.stop_event = threading.Event()
        self.reconnect_lock = threading.Lock()
        self.reconnect_thread: threading.Thread | None = None

        self.client: Optional[mqtt.Client] = None
        self.ros_modules = ros_modules
        self.rospy: Any | None = None
        self.cmd_vel_pub: Any | None = None
        self._ros_initialized = False
        self.subscribers: list[Any] = []

        self.last_drive_command_at: float | None = None
        self.drive_timed_out = False
        self.current_linear_x = 0.0
        self.current_angular_z = 0.0
        self.last_cmd_vel_publish_at: float | None = None

    def start(self) -> None:
        self._init_ros()
        self._connect_local_mqtt()
        while not self.stop_event.is_set():
            self._publish_heartbeat_if_due()
            self._drive_watchdog_tick()
            self._drive_republish_tick()
            self.stop_event.wait(0.05)

    def stop(self) -> None:
        if self.stop_event.is_set():
            return
        self.stop_event.set()
        if self.stop_on_shutdown:
            self._publish_twist(0.0, 0.0)
        client = self.client
        self.client = None
        if client is not None:
            try:
                client.loop_stop()
                client.disconnect()
            except Exception:
                logging.debug("MQTT disconnect failed", exc_info=True)
        if self.rospy is not None:
            signal_shutdown = getattr(self.rospy, "signal_shutdown", None)
            if callable(signal_shutdown):
                try:
                    signal_shutdown("ros1_bridge stopped")
                except Exception:
                    logging.debug("ROS shutdown signal failed", exc_info=True)

    def _load_ros_modules(self) -> Any:
        if self.ros_modules is not None:
            return self.ros_modules
        try:
            import rospy
            from actionlib_msgs.msg import GoalStatusArray
            from diagnostic_msgs.msg import DiagnosticArray
            from geometry_msgs.msg import PoseStamped, Twist
            from nav_msgs.msg import Odometry, Path
        except ImportError as exc:
            raise SystemExit(
                "ros1_bridge requires ROS 1 Python modules (rospy, actionlib_msgs, diagnostic_msgs, geometry_msgs, nav_msgs)."
            ) from exc

        class Modules:
            pass

        modules = Modules()
        modules.rospy = rospy
        modules.GoalStatusArray = GoalStatusArray
        modules.DiagnosticArray = DiagnosticArray
        modules.Odometry = Odometry
        modules.Path = Path
        modules.PoseStamped = PoseStamped
        modules.Twist = Twist
        self.ros_modules = modules
        return modules

    def _init_ros(self) -> None:
        if self._ros_initialized:
            return
        modules = self._load_ros_modules()
        self.rospy = modules.rospy
        self.rospy.init_node(self.node_name, disable_signals=True)
        self.cmd_vel_pub = self.rospy.Publisher(self.cmd_vel_topic, modules.Twist, queue_size=1)

        if self.wheel_odometry_enabled or self.localization_pose_enabled:
            self.subscribers.append(
                self.rospy.Subscriber(self.odometry_topic, modules.Odometry, self._on_odometry, queue_size=1)
            )
        if self.navigation_goal_enabled:
            self.subscribers.append(
                self.rospy.Subscriber(self.navigation_goal_ros_topic, modules.PoseStamped, self._on_navigation_goal, queue_size=1)
            )
        if self.navigation_status_enabled:
            self.subscribers.append(
                self.rospy.Subscriber(
                    self.navigation_status_ros_topic, modules.GoalStatusArray, self._on_navigation_status, queue_size=1
                )
            )
        if self.navigation_local_plan_enabled:
            self.subscribers.append(
                self.rospy.Subscriber(
                    self.navigation_local_plan_ros_topic, modules.Path, self._on_navigation_local_plan, queue_size=1
                )
            )
        if self.navigation_global_plan_enabled:
            self.subscribers.append(
                self.rospy.Subscriber(
                    self.navigation_global_plan_ros_topic, modules.Path, self._on_navigation_global_plan, queue_size=1
                )
            )
        if self.diagnostics_enabled or self.charging_level_enabled or self.charging_status_enabled:
            self.subscribers.append(
                self.rospy.Subscriber(
                    self.diagnostics_ros_topic, modules.DiagnosticArray, self._on_diagnostics, queue_size=1
                )
            )

        self._ros_initialized = True
        logging.info(
            "ROS initialized cmd_vel=%s odometry=%s goal=%s status=%s diagnostics=%s",
            self.cmd_vel_topic,
            self.odometry_topic,
            self.navigation_goal_ros_topic,
            self.navigation_status_ros_topic,
            self.diagnostics_ros_topic,
        )

    def _connect_local_mqtt(self) -> None:
        client = create_client(client_id=f"{self.identity.base}-ros1-bridge")
        mqtt_auth_and_tls(client, self.local_mqtt_cfg, self.config_path.parent)
        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_message = self._on_message
        self.client = client

        retry_delay = 2.0
        while not self.stop_event.is_set():
            logging.info("Connecting to local MQTT %s:%d", self.local_host, self.local_port)
            try:
                client.connect(self.local_host, self.local_port, self.local_keepalive)
                client.loop_start()
                return
            except OSError as exc:
                logging.warning("Local MQTT connection failed: %s", exc)
                self.stop_event.wait(retry_delay)
                retry_delay = min(retry_delay * 2, 60.0)

    def _mqtt_connected(self) -> bool:
        client = self.client
        if client is None:
            return False
        is_connected = getattr(client, "is_connected", None)
        if callable(is_connected):
            return bool(is_connected())
        return True

    def _publish_json(self, topic: str, payload: dict[str, Any], *, qos: int = 0, retain: bool = False) -> bool:
        client = self.client
        if client is None or not self._mqtt_connected():
            return False
        client.publish(topic, json.dumps(payload), qos=qos, retain=retain)
        return True

    def _on_connect(self, client: mqtt.Client, _userdata: Any, _flags: dict[str, Any], rc: int) -> None:
        if rc != 0:
            logging.error("Local MQTT connect failed rc=%s", rc)
            return
        logging.info("Local MQTT connected; subscribing drive topic.")
        client.subscribe(self.drive_topic, qos=0)
        self.next_heartbeat_at = 0.0
        self._publish_heartbeat()

    def _on_disconnect(self, _client: mqtt.Client, _userdata: Any, rc: int) -> None:
        if rc != mqtt.MQTT_ERR_SUCCESS:
            logging.warning("Unexpected local MQTT disconnect rc=%s", rc)
            self._start_reconnect_thread()

    def _start_reconnect_thread(self) -> None:
        if self.stop_event.is_set():
            return
        with self.reconnect_lock:
            if self.reconnect_thread and self.reconnect_thread.is_alive():
                return
            thread = threading.Thread(target=self._reconnect_loop, name="mqtt-reconnect", daemon=True)
            thread.start()
            self.reconnect_thread = thread

    def _reconnect_loop(self) -> None:
        delay = 2.0
        while not self.stop_event.is_set():
            client = self.client
            if client is None:
                return
            is_connected = getattr(client, "is_connected", None)
            if callable(is_connected) and is_connected():
                return
            try:
                client.connect(self.local_host, self.local_port, self.local_keepalive)
                return
            except OSError as exc:
                logging.warning("Local MQTT reconnect failed: %s", exc)
                self.stop_event.wait(delay)
                delay = min(delay * 2, 60.0)

    def _on_message(self, _client: mqtt.Client, _userdata: Any, msg: mqtt.MQTTMessage) -> None:
        if msg.topic != self.drive_topic:
            return
        if self.ignore_retained_drive and bool(msg.retain):
            logging.info("Ignoring retained drive command on %s", msg.topic)
            return
        parsed = _decode_payload(bytes(msg.payload))
        if parsed is None:
            logging.warning("Non-JSON drive payload on %s", msg.topic)
            return
        self._handle_drive_values(parsed)

    def _handle_drive_values(self, payload: Any) -> None:
        values = _unwrap_value(payload)
        if not isinstance(values, dict):
            logging.warning("Drive payload must be an object: %s", payload)
            return
        x = clamp(_parse_float(values.get("x"), 0.0), -1.0, 1.0)
        z = clamp(_parse_float(values.get("z"), 0.0), -1.0, 1.0)
        linear_x = z * self.max_linear_speed_mps
        # Pebble drive-values uses x>0 for "turn right"; ROS Twist uses angular.z>0 for left/CCW.
        angular_z = -x * self.max_angular_speed_radps
        self._publish_twist(linear_x, angular_z)
        self.last_drive_command_at = time.monotonic()
        self.drive_timed_out = False

    def _new_twist(self, linear_x: float, angular_z: float) -> Any:
        assert self.ros_modules is not None
        msg = self.ros_modules.Twist()
        msg.linear.x = float(linear_x)
        msg.linear.y = 0.0
        msg.linear.z = 0.0
        msg.angular.x = 0.0
        msg.angular.y = 0.0
        msg.angular.z = float(angular_z)
        return msg

    def _publish_twist(self, linear_x: float, angular_z: float) -> None:
        self.current_linear_x = float(linear_x)
        self.current_angular_z = float(angular_z)
        if self.cmd_vel_pub is None:
            return
        self.cmd_vel_pub.publish(self._new_twist(linear_x, angular_z))
        self.last_cmd_vel_publish_at = time.monotonic()

    def _drive_watchdog_tick(self) -> None:
        if self.drive_timeout_seconds <= 0:
            return
        if self.last_drive_command_at is None or self.drive_timed_out:
            return
        if (time.monotonic() - self.last_drive_command_at) <= self.drive_timeout_seconds:
            return
        self._publish_twist(0.0, 0.0)
        self.drive_timed_out = True
        logging.info("Issued ROS drive stop (command-timeout)")

    def _drive_republish_tick(self) -> None:
        if self.cmd_vel_publish_interval <= 0:
            return
        if self.last_drive_command_at is None or self.drive_timed_out:
            return
        last_publish_at = self.last_cmd_vel_publish_at
        if last_publish_at is None:
            self._publish_twist(self.current_linear_x, self.current_angular_z)
            return
        if (time.monotonic() - last_publish_at) < self.cmd_vel_publish_interval:
            return
        self._publish_twist(self.current_linear_x, self.current_angular_z)

    def _publish_heartbeat_if_due(self) -> None:
        if not self.heartbeat_enabled:
            return
        now = time.monotonic()
        if now < self.next_heartbeat_at:
            return
        self._publish_heartbeat()

    def _publish_heartbeat(self) -> None:
        if not self.heartbeat_enabled:
            return
        payload = {"t": int(time.time() * 1000)}
        if self._publish_json(self.heartbeat_topic, payload, qos=self.heartbeat_qos, retain=self.heartbeat_retain):
            self.next_heartbeat_at = time.monotonic() + self.heartbeat_interval

    def _wheel_odometry_payload(self, msg: Any) -> dict[str, Any]:
        pose = getattr(getattr(msg, "pose", None), "pose", None)
        twist = getattr(getattr(msg, "twist", None), "twist", None)
        position = getattr(pose, "position", None)
        orientation = getattr(pose, "orientation", None)
        x_m = _parse_float(getattr(position, "x", 0.0), 0.0)
        y_m = _parse_float(getattr(position, "y", 0.0), 0.0)
        yaw_rad = _quat_yaw_rad(orientation)
        payload: dict[str, Any] = {
            "value": {
                "x_mm": round(x_m * 1000.0, 3),
                "y_mm": round(y_m * 1000.0, 3),
                "heading_rad": round(yaw_rad, 6),
                "heading_deg": round(math.degrees(yaw_rad), 3),
            },
            "unit": "mm",
            "source": "ros-odometry",
            "timestamp": _header_timestamp_seconds(msg),
        }
        linear = getattr(twist, "linear", None)
        angular = getattr(twist, "angular", None)
        if linear is not None:
            payload["value"]["linear_speed_mps"] = round(_parse_float(getattr(linear, "x", 0.0), 0.0), 6)
        if angular is not None:
            payload["value"]["angular_speed_radps"] = round(_parse_float(getattr(angular, "z", 0.0), 0.0), 6)
        return payload

    def _localization_pose_payload(self, msg: Any) -> dict[str, Any]:
        pose = getattr(getattr(msg, "pose", None), "pose", None)
        twist = getattr(getattr(msg, "twist", None), "twist", None)
        position = getattr(pose, "position", None)
        orientation = getattr(pose, "orientation", None)
        yaw_rad = _quat_yaw_rad(orientation)
        payload: dict[str, Any] = {
            "value": {
                "x_m": round(_parse_float(getattr(position, "x", 0.0), 0.0), 6),
                "y_m": round(_parse_float(getattr(position, "y", 0.0), 0.0), 6),
                "z_m": round(_parse_float(getattr(position, "z", 0.0), 0.0), 6),
                "yaw_rad": round(yaw_rad, 6),
                "yaw_deg": round(math.degrees(yaw_rad), 3),
            },
            "frame_id": _frame_id(msg),
            "child_frame_id": _child_frame_id(msg),
            "source": self.odometry_topic,
            "timestamp": _header_timestamp_seconds(msg),
        }
        linear = getattr(twist, "linear", None)
        angular = getattr(twist, "angular", None)
        if linear is not None:
            payload["value"]["linear_velocity_mps"] = round(_parse_float(getattr(linear, "x", 0.0), 0.0), 6)
        if angular is not None:
            payload["value"]["angular_velocity_radps"] = round(_parse_float(getattr(angular, "z", 0.0), 0.0), 6)
        return payload

    def _on_odometry(self, msg: Any) -> None:
        now = time.monotonic()
        if self.wheel_odometry_enabled and now >= self.next_wheel_odometry_at:
            if self._publish_json(
                self.wheel_odometry_topic,
                self._wheel_odometry_payload(msg),
                qos=self.wheel_odometry_qos,
                retain=self.wheel_odometry_retain,
            ):
                self.next_wheel_odometry_at = now + self.wheel_odometry_interval
        if self.localization_pose_enabled and now >= self.next_localization_pose_at:
            if self._publish_json(
                self.localization_pose_topic,
                self._localization_pose_payload(msg),
                qos=self.localization_pose_qos,
                retain=self.localization_pose_retain,
            ):
                self.next_localization_pose_at = now + self.localization_pose_interval

    def _navigation_goal_payload(self, msg: Any) -> dict[str, Any]:
        pose = getattr(msg, "pose", None)
        position = getattr(pose, "position", None)
        orientation = getattr(pose, "orientation", None)
        yaw_rad = _quat_yaw_rad(orientation)
        return {
            "value": {
                "frame_id": _frame_id(msg),
                "x_m": round(_parse_float(getattr(position, "x", 0.0), 0.0), 6),
                "y_m": round(_parse_float(getattr(position, "y", 0.0), 0.0), 6),
                "z_m": round(_parse_float(getattr(position, "z", 0.0), 0.0), 6),
                "yaw_rad": round(yaw_rad, 6),
                "yaw_deg": round(math.degrees(yaw_rad), 3),
                "orientation": {
                    "x": round(_parse_float(getattr(orientation, "x", 0.0), 0.0), 6),
                    "y": round(_parse_float(getattr(orientation, "y", 0.0), 0.0), 6),
                    "z": round(_parse_float(getattr(orientation, "z", 0.0), 0.0), 6),
                    "w": round(_parse_float(getattr(orientation, "w", 1.0), 1.0), 6),
                },
            },
            "source": self.navigation_goal_ros_topic,
            "timestamp": _header_timestamp_seconds(msg),
        }

    def _on_navigation_goal(self, msg: Any) -> None:
        self.latest_navigation_goal = self._navigation_goal_payload(msg)
        if self.navigation_goal_enabled:
            self._publish_json(
                self.navigation_goal_topic,
                self.latest_navigation_goal,
                qos=self.navigation_goal_qos,
                retain=self.navigation_goal_retain,
            )

    def _select_goal_status(self, statuses: list[Any]) -> Any | None:
        if not statuses:
            return None
        active_like = {0, 1, 6, 7}
        for status in statuses:
            code = _parse_int(getattr(status, "status", -1), -1)
            if code in active_like:
                return status
        return statuses[-1]

    def _navigation_status_payload(self, msg: Any) -> dict[str, Any]:
        statuses = list(getattr(msg, "status_list", []) or [])
        selected = self._select_goal_status(statuses)
        code = _parse_int(getattr(selected, "status", -1), -1) if selected is not None else -1
        goal_id = getattr(getattr(selected, "goal_id", None), "id", "") if selected is not None else ""
        text = str(getattr(selected, "text", "") or "") if selected is not None else ""
        payload: dict[str, Any] = {
            "value": {
                "active": code in {0, 1, 6, 7},
                "goal_count": len(statuses),
                "code": code,
                "label": _goal_status_label(code),
                "goal_id": str(goal_id or ""),
                "text": text,
            },
            "source": self.navigation_status_ros_topic,
            "timestamp": _header_timestamp_seconds(msg),
        }
        if self.latest_navigation_goal is not None:
            payload["value"]["goal"] = self.latest_navigation_goal.get("value")
        return payload

    def _on_navigation_status(self, msg: Any) -> None:
        now = time.monotonic()
        if not self.navigation_status_enabled or now < self.next_navigation_status_at:
            return
        if self._publish_json(
            self.navigation_status_topic,
            self._navigation_status_payload(msg),
            qos=self.navigation_status_qos,
            retain=self.navigation_status_retain,
        ):
            self.next_navigation_status_at = now + self.navigation_status_interval

    def _path_payload(self, msg: Any, source: str, max_points: int) -> dict[str, Any]:
        poses = list(getattr(msg, "poses", []) or [])
        sampled = _sample_path_points(poses, max_points)
        points = []
        for item in sampled:
            pose = getattr(item, "pose", None)
            position = getattr(pose, "position", None)
            points.append(
                {
                    "x_m": round(_parse_float(getattr(position, "x", 0.0), 0.0), 6),
                    "y_m": round(_parse_float(getattr(position, "y", 0.0), 0.0), 6),
                    "z_m": round(_parse_float(getattr(position, "z", 0.0), 0.0), 6),
                }
            )
        return {
            "value": {
                "frame_id": _frame_id(msg),
                "point_count": len(poses),
                "sampled_point_count": len(points),
                "points": points,
            },
            "source": source,
            "timestamp": _header_timestamp_seconds(msg),
        }

    def _on_navigation_local_plan(self, msg: Any) -> None:
        now = time.monotonic()
        if not self.navigation_local_plan_enabled or now < self.next_navigation_local_plan_at:
            return
        if self._publish_json(
            self.navigation_local_plan_topic,
            self._path_payload(msg, self.navigation_local_plan_ros_topic, self.navigation_local_plan_max_points),
            qos=self.navigation_local_plan_qos,
            retain=self.navigation_local_plan_retain,
        ):
            self.next_navigation_local_plan_at = now + self.navigation_local_plan_interval

    def _on_navigation_global_plan(self, msg: Any) -> None:
        now = time.monotonic()
        if not self.navigation_global_plan_enabled or now < self.next_navigation_global_plan_at:
            return
        if self._publish_json(
            self.navigation_global_plan_topic,
            self._path_payload(msg, self.navigation_global_plan_ros_topic, self.navigation_global_plan_max_points),
            qos=self.navigation_global_plan_qos,
            retain=self.navigation_global_plan_retain,
        ):
            self.next_navigation_global_plan_at = now + self.navigation_global_plan_interval

    def _diagnostics_payload(self, msg: Any) -> dict[str, Any]:
        statuses = list(getattr(msg, "status", []) or [])
        max_level = -1
        items: list[dict[str, Any]] = []
        for status in statuses:
            level = _parse_int(getattr(status, "level", 0), 0)
            max_level = max(max_level, level)
            message = str(getattr(status, "message", "") or "")
            name = str(getattr(status, "name", "") or "")
            if level > 0 or len(items) < self.diagnostics_max_items:
                items.append({"name": name, "level": level, "label": _diagnostic_level_label(level), "message": message})
        trimmed_items = items[: self.diagnostics_max_items]
        return {
            "value": {
                "level": max_level if max_level >= 0 else 0,
                "label": _diagnostic_level_label(max_level if max_level >= 0 else 0),
                "items": trimmed_items,
            },
            "source": self.diagnostics_ros_topic,
            "timestamp": _header_timestamp_seconds(msg),
        }

    def _parse_charging_status_value(self, raw: str | None) -> bool | None:
        if raw is None:
            return None
        token = _normalize_token(raw)
        if token in self.charging_status_truthy_values:
            return True
        if token in self.charging_status_falsey_values:
            return False
        return None

    def _on_diagnostics(self, msg: Any) -> None:
        statuses = list(getattr(msg, "status", []) or [])
        timestamp = _header_timestamp_seconds(msg)
        now = time.monotonic()

        if self.charging_level_enabled:
            raw_level = _diagnostic_value(statuses, self.charging_level_status_name, self.charging_level_key)
            if raw_level is not None:
                level_value = _parse_float(raw_level, float("nan"))
                if not math.isnan(level_value):
                    self._publish_json(
                        self.charging_level_topic,
                        {"value": level_value, "timestamp": timestamp},
                        qos=self.charging_level_qos,
                        retain=self.charging_level_retain,
                    )

        if self.charging_status_enabled:
            raw_status = _diagnostic_value(statuses, self.charging_status_status_name, self.charging_status_key)
            charging = self._parse_charging_status_value(raw_status)
            if charging is not None:
                self._publish_json(
                    self.charging_status_topic,
                    {"value": charging, "timestamp": timestamp},
                    qos=self.charging_status_qos,
                    retain=self.charging_status_retain,
                )

        if self.diagnostics_enabled and now >= self.next_diagnostics_at:
            if self._publish_json(
                self.diagnostics_topic_out,
                self._diagnostics_payload(msg),
                qos=self.diagnostics_qos,
                retain=self.diagnostics_retain,
            ):
                self.next_diagnostics_at = now + self.diagnostics_interval


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bridge Pebble MQTT topics to ROS 1.")
    parser.add_argument("--config", help="Path to runtime config JSON.")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    config, config_path = load_config(args.config)
    svc_cfg = service_cfg(config, "ros1_bridge")
    logging.basicConfig(
        level=getattr(logging, log_level(config, svc_cfg), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    bridge = Ros1Bridge(config, config_path)

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
