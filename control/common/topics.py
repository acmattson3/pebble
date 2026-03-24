from __future__ import annotations

from dataclasses import dataclass


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


def identity_from_config(config: dict) -> RobotIdentity:
    robot = config.get("robot") or {}
    system = str(robot.get("system") or "pebble").strip() or "pebble"
    comp_type = str(robot.get("type") or "robots").strip() or "robots"
    robot_id = str(robot.get("id") or "").strip()
    if not robot_id:
        raise SystemExit("config.robot.id is required")
    return RobotIdentity(system=system, type=comp_type, robot_id=robot_id)
