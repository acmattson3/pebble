import copy
import json
from pathlib import Path
from typing import Any, Optional


DEFAULT_RUNTIME_CONFIG_PATH = Path(__file__).resolve().parents[1] / "control" / "configs" / "config.json"


class CameraProfileError(RuntimeError):
    """Raised when a requested camera profile is missing or invalid."""


def _config_path(path: Optional[str] = None) -> Path:
    if path:
        resolved = Path(path).expanduser()
        if not resolved.is_absolute():
            cwd_candidate = (Path.cwd() / resolved).resolve()
            if cwd_candidate.exists():
                resolved = cwd_candidate
            else:
                resolved = (Path(__file__).parent / resolved).resolve()
        return resolved
    return DEFAULT_RUNTIME_CONFIG_PATH


def _load_json(path: Path) -> dict:
    if not path.exists():
        raise CameraProfileError(f"Missing configuration file: {path}")
    try:
        data = json.loads(path.read_text())
    except json.JSONDecodeError as exc:
        raise CameraProfileError(f"Unable to parse configuration file {path}") from exc
    if not isinstance(data, dict):
        raise CameraProfileError(f"Configuration root must be an object: {path}")
    return data


def _topic_from_identity(robot_cfg: dict[str, Any], direction: str, metric: str) -> str:
    system = str(robot_cfg.get("system") or "pebble")
    component_type = str(robot_cfg.get("type") or "robots")
    robot_id = str(robot_cfg.get("id") or "").strip()
    if not robot_id:
        raise CameraProfileError("robot.id is required in runtime config")
    return f"{system}/{component_type}/{robot_id}/{direction}/{metric}"


def _profiles_from_runtime_config(data: dict[str, Any]) -> dict[str, Any]:
    robot_cfg = data.get("robot") if isinstance(data.get("robot"), dict) else {}
    services = data.get("services") if isinstance(data.get("services"), dict) else {}

    bridge_cfg = services.get("mqtt_bridge") if isinstance(services.get("mqtt_bridge"), dict) else {}
    remote_cfg = bridge_cfg.get("remote_mqtt") if isinstance(bridge_cfg.get("remote_mqtt"), dict) else {}
    media_cfg = bridge_cfg.get("media") if isinstance(bridge_cfg.get("media"), dict) else {}
    video_pub_cfg = media_cfg.get("video_publisher") if isinstance(media_cfg.get("video_publisher"), dict) else {}

    av_cfg = services.get("av_daemon") if isinstance(services.get("av_daemon"), dict) else {}
    av_video_cfg = av_cfg.get("video") if isinstance(av_cfg.get("video"), dict) else {}

    robot_id = str(robot_cfg.get("id") or "").strip()
    if not robot_id:
        raise CameraProfileError("robot.id is required in runtime config")
    remote_host = str(remote_cfg.get("host") or "").strip()
    if not remote_host:
        raise CameraProfileError("services.mqtt_bridge.remote_mqtt.host is required for video publisher")

    profile_name = str(video_pub_cfg.get("profile") or "default").strip() or "default"
    topic = str(video_pub_cfg.get("topic") or _topic_from_identity(robot_cfg, "outgoing", "front-camera"))

    mqtt_cfg = {
        "broker": remote_host,
        "port": int(remote_cfg.get("port") or 1883),
        "username": str(remote_cfg.get("username") or ""),
        "password": str(remote_cfg.get("password") or ""),
        "tls": copy.deepcopy(remote_cfg.get("tls") if isinstance(remote_cfg.get("tls"), dict) else {"enabled": False}),
        "topic": topic,
        "keepalive": int(remote_cfg.get("keepalive") or 60),
        "reconnect_seconds": int(video_pub_cfg.get("reconnect_seconds") or 10),
    }

    source_cfg = {
        "type": "gstreamer-shm",
        "socket_path": str(
            video_pub_cfg.get("input_shm")
            or av_video_cfg.get("socket_path")
            or "/tmp/pebble-video.sock"
        ),
        "width": int(video_pub_cfg.get("source_width") or video_pub_cfg.get("width") or av_video_cfg.get("width") or 640),
        "height": int(
            video_pub_cfg.get("source_height") or video_pub_cfg.get("height") or av_video_cfg.get("height") or 480
        ),
        "fps": int(video_pub_cfg.get("source_fps") or video_pub_cfg.get("fps") or av_video_cfg.get("fps") or 15),
        "reconnect_seconds": float(video_pub_cfg.get("input_retry_seconds") or 2.0),
    }

    publish_width = int(video_pub_cfg.get("publish_width") or source_cfg["width"] or 640)
    publish_height = int(video_pub_cfg.get("publish_height") or source_cfg["height"] or 480)
    target_fps = int(video_pub_cfg.get("publish_fps") or video_pub_cfg.get("target_fps") or source_cfg["fps"] or 15)
    keyframe_interval = video_pub_cfg.get("keyframe_interval")
    if not keyframe_interval:
        keyframe_interval = max(1, target_fps // 3)

    rotate_degrees = video_pub_cfg.get("rotate_degrees")
    if rotate_degrees is None:
        rotate_degrees = av_video_cfg.get("rotate_degrees")

    profile = {
        "mqtt": mqtt_cfg,
        "source": source_cfg,
        "encoding": {
            "publish_width": publish_width,
            "publish_height": publish_height,
            "target_fps": target_fps,
            "keyframe_interval": int(keyframe_interval),
            "jpeg_quality": int(video_pub_cfg.get("jpeg_quality") or 10),
        },
        "frame_transform": {
            "rotate_degrees": int(rotate_degrees or 0)
        },
        "charging_indicator": {"enabled": False},
        "camera_retry_seconds": float(video_pub_cfg.get("input_retry_seconds") or 2.0),
    }

    profiles = {profile_name: profile}
    if profile_name != "default":
        profiles["default"] = copy.deepcopy(profile)
    return profiles


def load_profiles(path: Optional[str] = None) -> dict:
    path_obj = _config_path(path)
    data = _load_json(path_obj)

    profiles = data.get("profiles")
    if isinstance(profiles, dict):
        return profiles

    if isinstance(data.get("services"), dict):
        return _profiles_from_runtime_config(data)

    raise CameraProfileError(f"No profiles section or runtime services found in {path_obj}")


def load_profile(name: str, config_path: Optional[str] = None) -> dict:
    profiles = load_profiles(config_path)
    if name not in profiles:
        available = ", ".join(sorted(profiles))
        raise CameraProfileError(f"Unknown profile '{name}'. Available profiles: {available}")

    profile = copy.deepcopy(profiles[name])
    encoding = profile.setdefault("encoding", {})
    target_fps = int(encoding.setdefault("target_fps", 15))

    keyframe_interval = encoding.get("keyframe_interval")
    if not keyframe_interval:
        encoding["keyframe_interval"] = max(1, target_fps // 3)
    encoding.setdefault("jpeg_quality", 15)

    source = profile.setdefault("source", {})
    source.setdefault("type", "gstreamer-shm")
    source.setdefault("socket_path", "/tmp/pebble-video.sock")
    source.setdefault("width", 640)
    source.setdefault("height", 480)
    source.setdefault("fps", target_fps)
    source.setdefault("reconnect_seconds", 2.0)
    encoding.setdefault("publish_width", int(source.get("width", 640)))
    encoding.setdefault("publish_height", int(source.get("height", 480)))

    profile.setdefault("camera_retry_seconds", 2)
    profile.setdefault("charging_indicator", {"enabled": False})

    mqtt = profile.setdefault("mqtt", {})
    mqtt.setdefault("port", 1883)
    mqtt.setdefault("keepalive", 60)
    mqtt.setdefault("reconnect_seconds", 10)
    if "topic" not in mqtt:
        raise CameraProfileError(f"Profile '{name}' is missing an mqtt.topic value.")

    return profile
