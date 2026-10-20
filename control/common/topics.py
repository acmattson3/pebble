from __future__ import annotations

from dataclasses import dataclass
import re


@dataclass(frozen=True)
class RobotIdentity:
    system: str
    type: str
    robot_id: str

    @property
    def base(self) -> str:
        return f"{self.system}/{self.type}/{self.robot_id}"

    def topic(self, direction: str, metric: str) -> str:
        return f"{self.base}/{direction}/{metric}"


def endpoint_topic(identity: RobotIdentity, relative: str) -> str:
    """Resolve a compact module endpoint; never accept wildcards or another robot."""
    valid = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}\Z")
    parts = relative.split("/") if isinstance(relative, str) else []
    if (len(parts) < 2 or len(parts) > 16 or parts[0] not in {"i", "o"}
            or any(not valid.fullmatch(p) or p in {".", ".."} for p in parts)
            or any(not valid.fullmatch(p) or p in {".", ".."} for p in identity.base.split("/"))):
        raise ValueError("Module endpoint must be a relative i/... or o/... topic")
    return f"{identity.base}/{relative}"


def robot_topic(identity: RobotIdentity, subtopic: str) -> str:
    """Resolve a robot-local MQTT subtopic under the configured identity."""
    valid = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}\Z")
    parts = subtopic.split("/") if isinstance(subtopic, str) else []
    if (not parts or len(parts) > 64
            or any(not valid.fullmatch(part) or part in {".", ".."} for part in parts)
            or any(not valid.fullmatch(part) or part in {".", ".."} for part in identity.base.split("/"))):
        raise ValueError("Robot subtopic must be a relative MQTT topic without wildcards")
    return f"{identity.base}/{subtopic}"


def resolve_topic(identity: RobotIdentity, config: dict, default_subtopic: str) -> str:
    """Resolve a topic setting, preferring identity-relative ``subtopic``.

    ``topic`` remains supported for existing configs and deliberate
    cross-component destinations. The resolver never rewrites that value.
    """
    subtopic = config.get("subtopic")
    topic = config.get("topic")
    if subtopic is not None and not isinstance(subtopic, str):
        raise ValueError("subtopic must be a string")
    if topic is not None and not isinstance(topic, str):
        raise ValueError("topic must be a string")
    has_subtopic = isinstance(subtopic, str) and bool(subtopic.strip())
    has_topic = isinstance(topic, str) and bool(topic.strip())
    if has_subtopic and has_topic:
        raise ValueError("Configure either subtopic or topic, not both")
    if has_subtopic:
        return robot_topic(identity, subtopic.strip())
    if has_topic:
        return topic.strip()
    return robot_topic(identity, default_subtopic)


def resolve_mapped_topic(identity: RobotIdentity, config: dict, key: str, default_subtopic: str) -> str:
    """Resolve a topic in a ``subtopics`` map, with legacy ``topics`` support."""
    subtopics = config.get("subtopics") if isinstance(config.get("subtopics"), dict) else {}
    topics = config.get("topics") if isinstance(config.get("topics"), dict) else {}
    return resolve_topic(identity, {"subtopic": subtopics.get(key), "topic": topics.get(key)}, default_subtopic)


def identity_from_config(config: dict) -> RobotIdentity:
    robot = config.get("robot") or {}
    system = str(robot.get("system") or "pebble").strip() or "pebble"
    comp_type = str(robot.get("type") or "robots").strip() or "robots"
    robot_id = str(robot.get("id") or "").strip()
    if not robot_id:
        raise SystemExit("config.robot.id is required")
    return RobotIdentity(system=system, type=comp_type, robot_id=robot_id)
