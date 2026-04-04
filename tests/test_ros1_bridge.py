from __future__ import annotations

import json
import math
import tempfile
import time
import types
import unittest
from pathlib import Path

from control.services.ros1_bridge import Ros1Bridge
from tests.helpers import FakeMqttClient, FakeMqttMessage, make_base_config


class _Vector3:
    def __init__(self, x: float = 0.0, y: float = 0.0, z: float = 0.0) -> None:
        self.x = x
        self.y = y
        self.z = z


class _Quaternion:
    def __init__(self, yaw_rad: float = 0.0) -> None:
        self.x = 0.0
        self.y = 0.0
        self.z = math.sin(yaw_rad / 2.0)
        self.w = math.cos(yaw_rad / 2.0)


class _Twist:
    def __init__(self, linear_x: float = 0.0, angular_z: float = 0.0) -> None:
        self.linear = _Vector3(x=linear_x)
        self.angular = _Vector3(z=angular_z)


class _RosPublisher:
    def __init__(self, topic: str) -> None:
        self.topic = topic
        self.messages: list[object] = []

    def publish(self, msg: object) -> None:
        self.messages.append(msg)


class _RosSubscriber:
    def __init__(self, topic: str, callback) -> None:
        self.topic = topic
        self.callback = callback


class _Stamp:
    def __init__(self, secs: int = 123, nsecs: int = 456000000) -> None:
        self.secs = secs
        self.nsecs = nsecs


class _Header:
    def __init__(self, frame_id: str = "", secs: int = 123, nsecs: int = 456000000) -> None:
        self.frame_id = frame_id
        self.stamp = _Stamp(secs=secs, nsecs=nsecs)


class _Pose:
    def __init__(self, x: float = 0.0, y: float = 0.0, z: float = 0.0, yaw_rad: float = 0.0) -> None:
        self.position = _Vector3(x=x, y=y, z=z)
        self.orientation = _Quaternion(yaw_rad=yaw_rad)


class _PoseWithCovariance:
    def __init__(self, pose: _Pose) -> None:
        self.pose = pose


class _TwistWithCovariance:
    def __init__(self, twist: _Twist) -> None:
        self.twist = twist


class _Odometry:
    def __init__(
        self,
        *,
        x_m: float,
        y_m: float,
        yaw_rad: float,
        linear_x: float = 0.0,
        angular_z: float = 0.0,
        frame_id: str = "odom",
        child_frame_id: str = "base_link",
        secs: int = 123,
        nsecs: int = 456000000,
    ) -> None:
        self.header = _Header(frame_id=frame_id, secs=secs, nsecs=nsecs)
        self.child_frame_id = child_frame_id
        self.pose = _PoseWithCovariance(_Pose(x=x_m, y=y_m, yaw_rad=yaw_rad))
        self.twist = _TwistWithCovariance(_Twist(linear_x=linear_x, angular_z=angular_z))


class _PoseStamped:
    def __init__(self, *, x_m: float, y_m: float, yaw_rad: float, frame_id: str = "map") -> None:
        self.header = _Header(frame_id=frame_id)
        self.pose = _Pose(x=x_m, y=y_m, yaw_rad=yaw_rad)


class _GoalID:
    def __init__(self, goal_id: str) -> None:
        self.id = goal_id


class _GoalStatus:
    def __init__(self, *, status: int, goal_id: str, text: str = "") -> None:
        self.status = status
        self.goal_id = _GoalID(goal_id)
        self.text = text


class _GoalStatusArray:
    def __init__(self, statuses: list[_GoalStatus]) -> None:
        self.header = _Header()
        self.status_list = statuses


class _PoseStampedInPath:
    def __init__(self, x_m: float, y_m: float, z_m: float = 0.0) -> None:
        self.pose = _Pose(x=x_m, y=y_m, z=z_m)


class _Path:
    def __init__(self, points: list[tuple[float, float]], frame_id: str = "map") -> None:
        self.header = _Header(frame_id=frame_id)
        self.poses = [_PoseStampedInPath(x, y) for x, y in points]


class _KeyValue:
    def __init__(self, key: str, value: str) -> None:
        self.key = key
        self.value = value


class _DiagnosticStatus:
    def __init__(self, *, level: int, name: str, message: str, values: list[_KeyValue]) -> None:
        self.level = level
        self.name = name
        self.message = message
        self.values = values


class _DiagnosticArray:
    def __init__(self, statuses: list[_DiagnosticStatus]) -> None:
        self.header = _Header()
        self.status = statuses


class _FakeTime:
    @staticmethod
    def now():
        return types.SimpleNamespace(secs=123, nsecs=456000000)


class _FakeRospy:
    def __init__(self) -> None:
        self.publishers: dict[str, _RosPublisher] = {}
        self.subscribers: dict[str, _RosSubscriber] = {}
        self.init_calls: list[tuple[str, bool]] = []
        self.shutdown_reason: str | None = None
        self.Time = _FakeTime

    def init_node(self, name: str, disable_signals: bool = False) -> None:
        self.init_calls.append((name, disable_signals))

    def Publisher(self, topic: str, _msg_type: object, queue_size: int = 1) -> _RosPublisher:
        pub = _RosPublisher(topic)
        self.publishers[topic] = pub
        return pub

    def Subscriber(self, topic: str, _msg_type: object, callback, queue_size: int = 1) -> _RosSubscriber:
        sub = _RosSubscriber(topic, callback)
        self.subscribers[topic] = sub
        return sub

    def is_shutdown(self) -> bool:
        return False

    def signal_shutdown(self, reason: str) -> None:
        self.shutdown_reason = reason


class Ros1BridgeTests(unittest.TestCase):
    def _bridge(self, *, drive_enabled: bool = True) -> tuple[Ros1Bridge, _FakeRospy]:
        config = make_base_config("jackal")
        config["services"]["serial_mcu_bridge"]["enabled"] = False
        config["services"]["mqtt_bridge"]["enabled"] = False
        config["services"]["ros1_bridge"]["enabled"] = True
        config["services"]["ros1_bridge"]["drive"] = {"enabled": drive_enabled}
        config["services"]["ros1_bridge"]["motion"] = {
            "max_linear_speed_mps": 0.8,
            "max_angular_speed_radps": 1.5,
        }

        fake_rospy = _FakeRospy()
        ros_modules = types.SimpleNamespace(
            rospy=fake_rospy,
            GoalStatusArray=_GoalStatusArray,
            DiagnosticArray=_DiagnosticArray,
            Odometry=_Odometry,
            Path=_Path,
            PoseStamped=_PoseStamped,
            Twist=_Twist,
        )
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.json"
            cfg_path.write_text(json.dumps(config))
            bridge = Ros1Bridge(config, cfg_path, ros_modules=ros_modules)
        bridge.client = FakeMqttClient()
        bridge._init_ros()
        return bridge, fake_rospy

    def test_drive_values_publish_cmd_vel_with_scaling(self):
        bridge, fake_rospy = self._bridge()
        bridge._handle_drive_values({"x": 0.5, "z": -0.25})

        pub = fake_rospy.publishers[bridge.cmd_vel_topic]
        self.assertEqual(1, len(pub.messages))
        msg = pub.messages[-1]
        self.assertAlmostEqual(-0.2, msg.linear.x)
        self.assertAlmostEqual(-0.75, msg.angular.z)

    def test_drive_command_is_republished_between_mqtt_updates(self):
        bridge, fake_rospy = self._bridge()
        bridge._handle_drive_values({"x": 0.4, "z": 0.5})
        bridge.last_cmd_vel_publish_at = time.monotonic() - (bridge.cmd_vel_publish_interval + 0.01)

        bridge._drive_republish_tick()

        pub = fake_rospy.publishers[bridge.cmd_vel_topic]
        self.assertEqual(2, len(pub.messages))
        msg = pub.messages[-1]
        self.assertAlmostEqual(0.4, msg.linear.x)
        self.assertAlmostEqual(-0.6, msg.angular.z)

    def test_retained_drive_commands_are_ignored(self):
        bridge, fake_rospy = self._bridge()
        msg = FakeMqttMessage(topic=bridge.drive_topic, payload=b'{"x":0.5,"z":0.2}', qos=0, retain=True)
        bridge._on_message(None, None, msg)  # type: ignore[arg-type]

        pub = fake_rospy.publishers[bridge.cmd_vel_topic]
        self.assertEqual([], pub.messages)

    def test_watchdog_sends_zero_twist_after_timeout(self):
        bridge, fake_rospy = self._bridge()
        bridge._handle_drive_values({"x": 0.2, "z": 0.4})
        bridge.last_drive_command_at = time.monotonic() - (bridge.drive_timeout_seconds + 0.1)

        bridge._drive_watchdog_tick()

        pub = fake_rospy.publishers[bridge.cmd_vel_topic]
        self.assertEqual(2, len(pub.messages))
        stop_msg = pub.messages[-1]
        self.assertEqual(0.0, stop_msg.linear.x)
        self.assertEqual(0.0, stop_msg.angular.z)

    def test_drive_topic_is_not_subscribed_when_drive_disabled(self):
        bridge, _fake_rospy = self._bridge(drive_enabled=False)

        bridge._on_connect(bridge.client, None, {}, 0)  # type: ignore[arg-type]

        self.assertEqual([], bridge.client.subscribe_calls)  # type: ignore[union-attr]

    def test_drive_values_are_ignored_when_drive_disabled(self):
        bridge, fake_rospy = self._bridge(drive_enabled=False)

        bridge._handle_drive_values({"x": 0.5, "z": 0.25})

        self.assertNotIn(bridge.cmd_vel_topic, fake_rospy.publishers)

    def test_heartbeat_publishes_to_local_mqtt_topic(self):
        bridge, _fake_rospy = self._bridge()
        bridge._publish_heartbeat()

        self.assertEqual(1, len(bridge.client.publish_calls))  # type: ignore[union-attr]
        topic, payload_raw, qos, retain = bridge.client.publish_calls[-1]  # type: ignore[index,union-attr]
        self.assertEqual(bridge.heartbeat_topic, topic)
        self.assertEqual(bridge.heartbeat_qos, qos)
        self.assertEqual(bridge.heartbeat_retain, retain)
        payload = json.loads(payload_raw)
        self.assertIn("t", payload)

    def test_odometry_publishes_wheel_odometry_and_localization_pose(self):
        bridge, _fake_rospy = self._bridge()
        msg = _Odometry(x_m=1.25, y_m=-0.5, yaw_rad=0.75, linear_x=0.4, angular_z=-0.2)

        bridge._on_odometry(msg)

        self.assertEqual(2, len(bridge.client.publish_calls))  # type: ignore[union-attr]
        wheel_topic, wheel_payload_raw, _qos0, _retain0 = bridge.client.publish_calls[0]  # type: ignore[index,union-attr]
        loc_topic, loc_payload_raw, _qos1, _retain1 = bridge.client.publish_calls[1]  # type: ignore[index,union-attr]

        self.assertEqual(bridge.wheel_odometry_topic, wheel_topic)
        wheel_payload = json.loads(wheel_payload_raw)
        self.assertEqual(1250.0, wheel_payload["value"]["x_mm"])
        self.assertEqual(-500.0, wheel_payload["value"]["y_mm"])
        self.assertAlmostEqual(0.75, wheel_payload["value"]["heading_rad"], places=5)

        self.assertEqual(bridge.localization_pose_topic, loc_topic)
        loc_payload = json.loads(loc_payload_raw)
        self.assertEqual("odom", loc_payload["frame_id"])
        self.assertEqual("base_link", loc_payload["child_frame_id"])
        self.assertAlmostEqual(1.25, loc_payload["value"]["x_m"], places=6)
        self.assertAlmostEqual(-0.5, loc_payload["value"]["y_m"], places=6)

    def test_navigation_goal_and_status_publish_to_mqtt(self):
        bridge, _fake_rospy = self._bridge()

        bridge._on_navigation_goal(_PoseStamped(x_m=2.0, y_m=3.0, yaw_rad=1.2))
        bridge._on_navigation_status(_GoalStatusArray([_GoalStatus(status=1, goal_id="goal-1", text="Moving")]))

        self.assertEqual(2, len(bridge.client.publish_calls))  # type: ignore[union-attr]
        goal_topic, goal_payload_raw, _goal_qos, goal_retain = bridge.client.publish_calls[0]  # type: ignore[index,union-attr]
        status_topic, status_payload_raw, _status_qos, status_retain = bridge.client.publish_calls[1]  # type: ignore[index,union-attr]

        self.assertEqual(bridge.navigation_goal_topic, goal_topic)
        self.assertTrue(goal_retain)
        goal_payload = json.loads(goal_payload_raw)
        self.assertEqual("map", goal_payload["value"]["frame_id"])
        self.assertAlmostEqual(2.0, goal_payload["value"]["x_m"], places=6)

        self.assertEqual(bridge.navigation_status_topic, status_topic)
        self.assertTrue(status_retain)
        status_payload = json.loads(status_payload_raw)
        self.assertTrue(status_payload["value"]["active"])
        self.assertEqual("ACTIVE", status_payload["value"]["label"])
        self.assertEqual("goal-1", status_payload["value"]["goal_id"])
        self.assertIn("goal", status_payload["value"])

    def test_diagnostics_publish_charging_level_and_summary(self):
        bridge, _fake_rospy = self._bridge()
        msg = _DiagnosticArray(
            [
                _DiagnosticStatus(
                    level=0,
                    name="/Jackal Base/General/Battery",
                    message="Battery OK.",
                    values=[_KeyValue("Battery Voltage (V)", "27.7144")],
                ),
                _DiagnosticStatus(
                    level=1,
                    name="/Jackal Base",
                    message="Warning",
                    values=[],
                ),
            ]
        )

        bridge._on_diagnostics(msg)

        self.assertEqual(2, len(bridge.client.publish_calls))  # type: ignore[union-attr]
        level_topic, level_payload_raw, _level_qos, level_retain = bridge.client.publish_calls[0]  # type: ignore[index,union-attr]
        diag_topic, diag_payload_raw, _diag_qos, diag_retain = bridge.client.publish_calls[1]  # type: ignore[index,union-attr]

        self.assertEqual(bridge.charging_level_topic, level_topic)
        self.assertTrue(level_retain)
        self.assertAlmostEqual(27.7144, json.loads(level_payload_raw)["value"], places=4)

        self.assertEqual(bridge.diagnostics_topic_out, diag_topic)
        self.assertTrue(diag_retain)
        diag_payload = json.loads(diag_payload_raw)
        self.assertEqual("WARN", diag_payload["value"]["label"])
        self.assertGreaterEqual(len(diag_payload["value"]["items"]), 1)


if __name__ == "__main__":
    unittest.main()
