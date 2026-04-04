from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


class FakeMqttClient:
    def __init__(self, connected: bool = True):
        self.connected = connected
        self.publish_calls: list[tuple[str, Any, int, bool]] = []
        self.subscribe_calls: list[tuple[str, int]] = []
        self.unsubscribe_calls: list[str] = []
        self.loop_started = False
        self.disconnected = False

    def is_connected(self) -> bool:
        return self.connected

    def publish(self, topic: str, payload: Any, qos: int = 0, retain: bool = False):
        self.publish_calls.append((topic, payload, qos, retain))
        return (0, 1)

    def subscribe(self, topic: str, qos: int = 0):
        self.subscribe_calls.append((topic, qos))
        return (0, 1)

    def unsubscribe(self, topic: str):
        self.unsubscribe_calls.append(topic)
        return (0, 1)

    def loop_start(self) -> None:
        self.loop_started = True

    def loop_stop(self) -> None:
        self.loop_started = False

    def disconnect(self) -> None:
        self.disconnected = True
        self.connected = False


@dataclass
class FakeMqttMessage:
    topic: str
    payload: bytes
    qos: int = 1
    retain: bool = False


class FakeSerial:
    def __init__(self):
        self.writes: list[bytes] = []
        self.flush_count = 0
        self.closed = False

    def write(self, data: bytes) -> None:
        self.writes.append(data)

    def flush(self) -> None:
        self.flush_count += 1

    def close(self) -> None:
        self.closed = True


class FakeProcess:
    _next_pid = 1000

    def __init__(self, running: bool = True):
        FakeProcess._next_pid += 1
        self.pid = FakeProcess._next_pid
        self._running = running
        self.terminate_called = False
        self.kill_called = False
        self.wait_calls: list[float | None] = []

    def poll(self):
        return None if self._running else 0

    def terminate(self) -> None:
        self.terminate_called = True
        self._running = False

    def kill(self) -> None:
        self.kill_called = True
        self._running = False

    def wait(self, timeout: float | None = None):
        self.wait_calls.append(timeout)
        self._running = False
        return 0


class FakePopenFactory:
    def __init__(self):
        self.calls: list[dict[str, Any]] = []
        self.processes: list[FakeProcess] = []

    def __call__(self, cmd, cwd=None, env=None, **kwargs):
        proc = FakeProcess(running=True)
        self.calls.append({"cmd": cmd, "cwd": cwd, "env": env, "kwargs": kwargs})
        self.processes.append(proc)
        return proc


def make_base_config(robot_id: str = "testbot") -> dict[str, Any]:
    return {
        "log_level": "INFO",
        "robot": {
            "system": "pebble",
            "type": "robots",
            "id": robot_id,
        },
        "local_mqtt": {
            "host": "127.0.0.1",
            "port": 1883,
            "keepalive": 60,
            "username": "",
            "password": "",
        },
        "services": {
            "launcher": {
                "enabled": True,
                "restart_delay_seconds": 0.01,
                "shutdown_timeout_seconds": 0.0,
            },
            "av_daemon": {
                "enabled": False,
                "restart_delay_seconds": 0.01,
                "stop_timeout_seconds": 0.1,
                "video": {
                    "enabled": True,
                    "backend": "v4l2",
                    "device": "/dev/video0",
                    "width": 640,
                    "height": 480,
                    "fps": 15,
                    "socket_path": "/tmp/test-video.sock",
                    "wait_for_connection": False,
                },
                "audio": {
                    "enabled": True,
                    "source": "alsa",
                    "device": "default",
                    "rate": 16000,
                    "channels": 1,
                    "socket_path": "/tmp/test-audio.sock",
                    "wait_for_connection": False,
                },
            },
            "mqtt_bridge": {
                "enabled": True,
                "remote_mqtt": {
                    "host": "remote-broker",
                    "port": 1883,
                    "keepalive": 60,
                    "username": "",
                    "password": "",
                },
                "heartbeat": {
                    "enabled": True,
                    "interval_seconds": 0.25,
                    "qos": 1,
                    "retain": True,
                },
                "video_control": {
                    "command": ["python3", "video/camera_publisher.py", "--profile", "default"],
                    "cwd": ".",
                    "stop_timeout": 1,
                    "env": {},
                },
                "audio_control": {
                    "publisher_command": ["python3", "audio/audio_publisher.py", "--input-shm", "/tmp/test-audio.sock"],
                    "receiver_command": ["python3", "audio/audio_receiver.py"],
                    "publisher_cwd": ".",
                    "receiver_cwd": ".",
                    "stop_timeout": 1,
                    "env": {},
                },
            },
            "ros1_bridge": {
                "enabled": False,
                "drive": {
                    "enabled": True,
                },
                "topics": {
                    "drive_values": f"pebble/robots/{robot_id}/incoming/drive-values",
                    "wheel_odometry": f"pebble/robots/{robot_id}/outgoing/wheel-odometry",
                    "charging_status": f"pebble/robots/{robot_id}/outgoing/charging-status",
                    "charging_level": f"pebble/robots/{robot_id}/outgoing/charging-level",
                    "localization_pose": f"pebble/robots/{robot_id}/outgoing/localization-pose",
                    "navigation_status": f"pebble/robots/{robot_id}/outgoing/navigation-status",
                    "navigation_goal": f"pebble/robots/{robot_id}/outgoing/navigation-goal",
                    "navigation_local_plan": f"pebble/robots/{robot_id}/outgoing/navigation/local-plan",
                    "navigation_global_plan": f"pebble/robots/{robot_id}/outgoing/navigation/global-plan",
                    "diagnostics": f"pebble/robots/{robot_id}/outgoing/diagnostics",
                },
                "heartbeat": {
                    "enabled": True,
                    "topic": f"pebble/robots/{robot_id}/outgoing/online",
                    "interval_seconds": 0.25,
                    "qos": 1,
                    "retain": True,
                },
                "ros": {
                    "cmd_vel_topic": "/cmd_vel",
                    "cmd_vel_publish_interval_seconds": 0.05,
                    "odometry_topic": "/odometry/filtered",
                    "navigation_status_topic": "/move_base/status",
                    "navigation_goal_topic": "/move_base/current_goal",
                    "navigation_local_plan_topic": "/move_base/TrajectoryPlannerROS/local_plan",
                    "navigation_global_plan_topic": "/move_base/NavfnROS/plan",
                    "diagnostics_topic": "/diagnostics_agg",
                },
                "motion": {
                    "max_linear_speed_mps": 0.6,
                    "max_angular_speed_radps": 1.2,
                },
                "telemetry": {
                    "wheel_odometry": {
                        "enabled": True,
                        "interval_seconds": 0.25,
                        "qos": 0,
                        "retain": False,
                    },
                    "localization_pose": {
                        "enabled": True,
                        "interval_seconds": 0.25,
                        "qos": 0,
                        "retain": False,
                    },
                    "navigation_status": {
                        "enabled": True,
                        "interval_seconds": 0.5,
                        "qos": 1,
                        "retain": True,
                    },
                    "navigation_goal": {
                        "enabled": True,
                        "qos": 1,
                        "retain": True,
                    },
                    "navigation_local_plan": {
                        "enabled": False,
                        "interval_seconds": 1.0,
                        "qos": 0,
                        "retain": False,
                        "max_points": 40,
                    },
                    "navigation_global_plan": {
                        "enabled": False,
                        "interval_seconds": 1.0,
                        "qos": 0,
                        "retain": False,
                        "max_points": 60,
                    },
                    "diagnostics": {
                        "enabled": True,
                        "interval_seconds": 1.0,
                        "qos": 1,
                        "retain": True,
                        "max_items": 8,
                    },
                    "charging_level": {
                        "enabled": True,
                        "qos": 1,
                        "retain": True,
                        "status_name": "/Jackal Base/General/Battery",
                        "key": "Battery Voltage (V)",
                    },
                    "charging_status": {
                        "enabled": False,
                        "qos": 1,
                        "retain": True,
                        "status_name": "",
                        "key": "",
                    },
                },
                "safety": {
                    "drive_timeout_seconds": 0.75,
                    "ignore_retained_drive": True,
                    "stop_on_shutdown": True,
                },
            },
            "serial_mcu_bridge": {
                "enabled": True,
                "serial": {
                    "port": "/dev/ttyACM0",
                    "baud": 115200,
                    "timeout_seconds": 0.1,
                },
            },
            "soundboard_handler": {
                "enabled": True,
                "playback": {
                    "directory": "./sounds",
                    "player_command": ["aplay"],
                    "stop_timeout": 1,
                    "scan_interval": 1,
                    "env": {},
                },
            },
            "autonomy_manager": {
                "enabled": False,
                "autonomy_root": "autonomy",
                "scan_interval_seconds": 5.0,
                "stop_timeout_seconds": 8.0,
                "topics": {
                    "command": f"pebble/robots/{robot_id}/incoming/autonomy-command",
                    "files": f"pebble/robots/{robot_id}/outgoing/autonomy-files",
                    "status": f"pebble/robots/{robot_id}/outgoing/autonomy-status",
                },
                "scripts": {
                    "apriltag-follow": {
                        "display_name": "AprilTag Follow",
                        "entrypoint": "run.py",
                        "args": [
                            {
                                "key": "tag_id",
                                "label": "Tag ID",
                                "type": "int",
                                "default": 13,
                                "arg": "--marker-id",
                                "min": 0,
                            },
                            {
                                "key": "tag_size_m",
                                "label": "Tag Size (m)",
                                "type": "float",
                                "default": 0.25,
                                "arg": "--marker-size",
                                "min": 0.01,
                            },
                        ],
                    }
                },
            },
        },
    }


def write_config(tmp_path: Path, config: dict[str, Any]) -> Path:
    path = tmp_path / "config.json"
    path.write_text(__import__("json").dumps(config))
    return path
