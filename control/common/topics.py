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


def identity_from_config(config: dict) -> RobotIdentity:
    robot = config.get("robot") or {}
    system = str(robot.get("system") or "pebble").strip() or "pebble"
    comp_type = str(robot.get("type") or "robots").strip() or "robots"
    robot_id = str(robot.get("id") or "").strip()
    if not robot_id:
        raise SystemExit("config.robot.id is required")
    return RobotIdentity(system=system, type=comp_type, robot_id=robot_id)
