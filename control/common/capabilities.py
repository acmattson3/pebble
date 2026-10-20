from __future__ import annotations

from typing import Any

from control.common.config import enabled_service_instances, service_cfg
from control.common.topics import RobotIdentity, identity_from_config, resolve_mapped_topic, resolve_topic


CAPABILITIES_SCHEMA = "pebble-capabilities/v1"


def _truthy(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return default


def _topic(identity: RobotIdentity, config: dict[str, Any], key: str, default_subtopic: str) -> str:
    return resolve_mapped_topic(identity, config, key, default_subtopic)


def capabilities_topic(identity: RobotIdentity, launcher_cfg: dict[str, Any]) -> str:
    cap_cfg = launcher_cfg.get("capabilities") if isinstance(launcher_cfg.get("capabilities"), dict) else {}
    return resolve_topic(identity, cap_cfg, "outgoing/capabilities")


def build_capabilities_value(config: dict[str, Any]) -> dict[str, Any]:
    identity = identity_from_config(config)

    av_cfg = service_cfg(config, "av_daemon")
    mqtt_bridge_cfg = service_cfg(config, "mqtt_bridge")
    ros1_bridge_cfg = service_cfg(config, "ros1_bridge")
    serial_cfg = service_cfg(config, "serial_mcu_bridge")
    soundboard_cfg = service_cfg(config, "soundboard_handler")
    autonomy_cfg = service_cfg(config, "autonomy_manager")

    av_video_cfg = av_cfg.get("video") if isinstance(av_cfg.get("video"), dict) else {}
    av_audio_cfg = av_cfg.get("audio") if isinstance(av_cfg.get("audio"), dict) else {}

    ros1_drive_cfg = ros1_bridge_cfg.get("drive") if isinstance(ros1_bridge_cfg.get("drive"), dict) else {}
    ros1_telemetry_cfg = ros1_bridge_cfg.get("telemetry") if isinstance(ros1_bridge_cfg.get("telemetry"), dict) else {}
    ros1_charge_status_cfg = (
        ros1_telemetry_cfg.get("charging_status") if isinstance(ros1_telemetry_cfg.get("charging_status"), dict) else {}
    )
    media_cfg = mqtt_bridge_cfg.get("media") if isinstance(mqtt_bridge_cfg.get("media"), dict) else {}
    video_pub_cfg = media_cfg.get("video_publisher") if isinstance(media_cfg.get("video_publisher"), dict) else {}
    audio_pub_cfg = media_cfg.get("audio_publisher") if isinstance(media_cfg.get("audio_publisher"), dict) else {}
    audio_rx_cfg = media_cfg.get("audio_receiver") if isinstance(media_cfg.get("audio_receiver"), dict) else {}
    audio_ctl_cfg = mqtt_bridge_cfg.get("audio_control") if isinstance(mqtt_bridge_cfg.get("audio_control"), dict) else {}
    video_ctl_cfg = mqtt_bridge_cfg.get("video_control") if isinstance(mqtt_bridge_cfg.get("video_control"), dict) else {}
    reboot_ctl_cfg = mqtt_bridge_cfg.get("reboot_control") if isinstance(mqtt_bridge_cfg.get("reboot_control"), dict) else {}
    service_restart_ctl_cfg = (
        mqtt_bridge_cfg.get("service_restart_control")
        if isinstance(mqtt_bridge_cfg.get("service_restart_control"), dict)
        else {}
    )
    git_pull_ctl_cfg = (
        mqtt_bridge_cfg.get("git_pull_control") if isinstance(mqtt_bridge_cfg.get("git_pull_control"), dict) else {}
    )

    serial_instances = enabled_service_instances(config, "serial_mcu_bridge")
    serial_base_cfg: dict[str, Any] = {}
    for _instance_name, instance_cfg in serial_instances:
        protocol_name = str(instance_cfg.get("protocol") or "goob_base_v1").strip().lower()
        if protocol_name == "goob_base_v1":
            serial_base_cfg = instance_cfg
            break

    serial_telemetry_cfg = serial_base_cfg.get("telemetry") if isinstance(serial_base_cfg.get("telemetry"), dict) else {}

    mqtt_bridge_enabled = _truthy(mqtt_bridge_cfg.get("enabled"), False)
    ros1_bridge_enabled = _truthy(ros1_bridge_cfg.get("enabled"), False)
    ros1_drive_enabled = ros1_bridge_enabled and _truthy(ros1_drive_cfg.get("enabled"), True)
    serial_enabled = bool(serial_base_cfg)
    ros1_charging_status_enabled = ros1_bridge_enabled and _truthy(ros1_charge_status_cfg.get("enabled"), False)
    touch_publish_enabled = _truthy(serial_telemetry_cfg.get("publish_touch_sensors"), False)
    soundboard_enabled = _truthy(soundboard_cfg.get("enabled"), False)
    autonomy_enabled = _truthy(autonomy_cfg.get("enabled"), False)
    av_enabled = _truthy(av_cfg.get("enabled"), False)
    video_enabled = av_enabled and _truthy(av_video_cfg.get("enabled"), True)
    audio_enabled = av_enabled and _truthy(av_audio_cfg.get("enabled"), True)

    video_control_configured = bool(video_ctl_cfg.get("command"))
    audio_control_configured = bool(
        audio_ctl_cfg.get("command") or audio_ctl_cfg.get("publisher_command") or audio_ctl_cfg.get("receiver_command")
    )
    reboot_control_configured = bool(reboot_ctl_cfg.get("command"))
    service_restart_control_configured = bool(service_restart_ctl_cfg.get("command"))
    git_pull_control_configured = bool(git_pull_ctl_cfg.get("command"))

    value: dict[str, Any] = {
        "identity": {
            "system": identity.system,
            "type": identity.type,
            "id": identity.robot_id,
        },
        "video": {
            "available": video_enabled,
            "controls": bool(mqtt_bridge_enabled and video_control_configured),
            "topic": resolve_topic(identity, video_pub_cfg, "outgoing/front-camera"),
            "command_topic": _topic(identity, mqtt_bridge_cfg, "video_control", "incoming/front-camera"),
            "flag_topic": _topic(identity, mqtt_bridge_cfg, "mqtt_video", "incoming/flags/mqtt-video"),
            "overlays_topic": resolve_topic(identity, {
                "subtopic": video_pub_cfg.get("overlays_subtopic"),
                "topic": video_pub_cfg.get("overlays_topic"),
            }, "outgoing/video-overlays"),
            "width": int(video_pub_cfg.get("publish_width") or video_pub_cfg.get("width") or av_video_cfg.get("width") or 640),
            "height": int(
                video_pub_cfg.get("publish_height") or video_pub_cfg.get("height") or av_video_cfg.get("height") or 480
            ),
            "fps": int(
                video_pub_cfg.get("publish_fps")
                or video_pub_cfg.get("target_fps")
                or video_pub_cfg.get("fps")
                or av_video_cfg.get("fps")
                or 15
            ),
        },
        "audio": {
            "available": audio_enabled,
            "controls": bool(mqtt_bridge_enabled and audio_control_configured),
            "topic": resolve_topic(identity, audio_pub_cfg, "outgoing/audio"),
            "uplink_topic": resolve_topic(identity, audio_rx_cfg, "incoming/audio-stream"),
            "command_topic": _topic(identity, mqtt_bridge_cfg, "audio_control", "incoming/audio"),
            "flag_topic": _topic(identity, mqtt_bridge_cfg, "mqtt_audio", "incoming/flags/mqtt-audio"),
            "rate": int(audio_pub_cfg.get("rate") or av_audio_cfg.get("rate") or 16000),
            "channels": int(audio_pub_cfg.get("channels") or av_audio_cfg.get("channels") or 1),
        },
        "soundboard": {
            "available": soundboard_enabled,
            "controls": soundboard_enabled,
            "command_topic": _topic(identity, soundboard_cfg, "command", "incoming/soundboard-command"),
            "files_topic": _topic(identity, soundboard_cfg, "files", "outgoing/soundboard-files"),
            "status_topic": _topic(identity, soundboard_cfg, "status", "outgoing/soundboard-status"),
        },
        "autonomy": {
            "available": autonomy_enabled,
            "controls": autonomy_enabled,
            "command_topic": _topic(identity, autonomy_cfg, "command", "incoming/autonomy-command"),
            "files_topic": _topic(identity, autonomy_cfg, "files", "outgoing/autonomy-files"),
            "status_topic": _topic(identity, autonomy_cfg, "status", "outgoing/autonomy-status"),
        },
        "system": {
            "reboot": {
                "available": True,
                "controls": bool(mqtt_bridge_enabled and reboot_control_configured),
                "flag_topic": _topic(identity, mqtt_bridge_cfg, "reboot", "incoming/flags/reboot"),
            },
            "service_restart": {
                "available": True,
                "controls": bool(mqtt_bridge_enabled and service_restart_control_configured),
                "flag_topic": _topic(
                    identity, mqtt_bridge_cfg,
                    "service_restart",
                    "incoming/flags/service-restart",
                ),
            },
            "git_pull": {
                "available": True,
                "controls": bool(mqtt_bridge_enabled and git_pull_control_configured),
                "flag_topic": _topic(identity, mqtt_bridge_cfg, "git_pull", "incoming/flags/git-pull"),
            },
        },
        "drive": {
            "available": bool(serial_enabled or ros1_drive_enabled),
            "controls": bool(serial_enabled or ros1_drive_enabled),
            "topic": (
                _topic(identity, serial_base_cfg, "drive_values", "incoming/drive-values") if serial_enabled else
                _topic(identity, ros1_bridge_cfg, "drive_values", "incoming/drive-values") if ros1_drive_enabled else
                identity.topic("incoming", "drive-values")
            ),
        },
        "lights": {
            "solid": serial_enabled,
            "flash": serial_enabled,
            "solid_topic": _topic(identity, serial_base_cfg, "lights_solid", "incoming/lights-solid"),
            "flash_topic": _topic(identity, serial_base_cfg, "lights_flash", "incoming/lights-flash"),
        },
        "telemetry": {
            "touch_sensors": bool(serial_enabled and touch_publish_enabled),
            "charging_status": bool(serial_enabled or ros1_charging_status_enabled),
            "touch_topic": _topic(identity, serial_base_cfg, "touch_sensors", "outgoing/touch-sensors"),
            "charging_topic": str(
                _topic(identity, serial_base_cfg, "charging_status", "outgoing/charging-status") if serial_enabled else
                _topic(identity, ros1_bridge_cfg, "charging_status", "outgoing/charging-status") if ros1_charging_status_enabled else
                identity.topic("outgoing", "charging-status")
            ),
        },
    }
    return value
