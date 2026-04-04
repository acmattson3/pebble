from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import serial


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from control.services.serial_standard import (  # noqa: E402
    MSG_ACK,
    MSG_CMD,
    MSG_DESCRIBE,
    MSG_DESCRIBE_REQ,
    MSG_HELLO,
    MSG_LOG,
    MSG_NACK,
    MSG_PING,
    MSG_PONG,
    MSG_STATE,
    decode_discovery,
    decode_hello,
    decode_packet,
    decode_struct_payload,
    encode_packet,
    encode_struct_payload,
)


EXPECTED_DEVICE_UID = "hs105-sta"
EXPECTED_MODEL = "seeed_xiao_esp32c3_hs105_station_bridge"
EXPECTED_SCHEMA_HASH = "hs105-sta-v1"
EXPECTED_SLOTS = ("o1", "o2", "o3", "o4")
KNOWN_SLOT_INTERFACE_IDS = {
    "o1": {"cmd": 1, "state": 2},
    "o2": {"cmd": 3, "state": 4},
    "o3": {"cmd": 5, "state": 6},
    "o4": {"cmd": 7, "state": 8},
}
STATE_FIELDS = [
    {"name": "bound", "type": "bool"},
    {"name": "reachable", "type": "bool"},
    {"name": "on", "type": "bool"},
]
COMMAND_FIELDS = [{"name": "on", "type": "bool"}]


@dataclass
class SlotState:
    name: str
    state_interface_id: int
    command_interface_id: int
    bound: bool = False
    reachable: bool = False
    on: bool = False
    seq: int = 0
    timestamp_ms: int = 0


class SuiteError(RuntimeError):
    pass


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run HS105 station-mode toggle tests over USB serial.")
    parser.add_argument("--port", default="COM11")
    parser.add_argument("--baud", type=int, default=115200)
    parser.add_argument("--timeout", type=float, default=20.0, help="Startup/discovery timeout in seconds.")
    parser.add_argument("--command-timeout", type=float, default=8.0, help="Per-toggle timeout in seconds.")
    return parser.parse_args()


def _expect(condition: bool, message: str) -> None:
    if not condition:
        raise SuiteError(message)


def _read_frame(ser: serial.Serial, deadline: float) -> bytes | None:
    frame = bytearray()
    while time.monotonic() < deadline:
        chunk = ser.read(1)
        if not chunk:
            continue
        if chunk == b"\x00":
            if frame:
                return bytes(frame)
            continue
        frame.extend(chunk)
    return None


def _build_slots(discovery: dict[str, Any]) -> dict[str, SlotState]:
    interfaces = discovery.get("interfaces")
    _expect(isinstance(interfaces, list), "DESCRIBE payload missing interfaces list")
    slots: dict[str, SlotState] = {}
    command_ids: dict[str, int] = {}

    for interface in interfaces:
        if not isinstance(interface, dict):
            raise SuiteError("DESCRIBE interface entry is not an object")
        name = str(interface.get("name") or "")
        fields = interface.get("encoding", {}).get("fields", [])
        field_names = [str(field.get("name") or "") for field in fields if isinstance(field, dict)]

        if name.endswith("_set"):
            slot_name = name[:-4]
            _expect(slot_name in EXPECTED_SLOTS, f"Unexpected command interface: {name}")
            _expect(interface.get("dir") == "in", f"{name} dir should be 'in'")
            _expect(interface.get("kind") == "command", f"{name} kind should be 'command'")
            _expect(field_names == ["on"], f"{name} command fields mismatch: {field_names}")
            command_ids[slot_name] = int(interface.get("id") or 0)
            continue

        if name.endswith("_state"):
            slot_name = name[:-6]
            _expect(slot_name in EXPECTED_SLOTS, f"Unexpected state interface: {name}")
            _expect(interface.get("dir") == "out", f"{name} dir should be 'out'")
            _expect(interface.get("kind") == "state", f"{name} kind should be 'state'")
            _expect(bool(interface.get("retain")), f"{name} should be retained")
            _expect(field_names == ["bound", "reachable", "on"], f"{name} state fields mismatch: {field_names}")
            slots[slot_name] = SlotState(
                name=slot_name,
                state_interface_id=int(interface.get("id") or 0),
                command_interface_id=0,
            )

    _expect(len(slots) == 4, f"Expected 4 state interfaces, found {len(slots)}")
    _expect(len(command_ids) == 4, f"Expected 4 command interfaces, found {len(command_ids)}")
    for slot_name in EXPECTED_SLOTS:
        _expect(slot_name in slots, f"Missing state interface for {slot_name}")
        _expect(slot_name in command_ids, f"Missing command interface for {slot_name}")
        slots[slot_name].command_interface_id = command_ids[slot_name]
    return slots


def _build_known_slots() -> dict[str, SlotState]:
    slots: dict[str, SlotState] = {}
    for slot_name, interface_ids in KNOWN_SLOT_INTERFACE_IDS.items():
        slots[slot_name] = SlotState(
            name=slot_name,
            state_interface_id=interface_ids["state"],
            command_interface_id=interface_ids["cmd"],
        )
    return slots


def _apply_state(packet: Any, slot: SlotState) -> None:
    values = decode_struct_payload(packet.payload, STATE_FIELDS)
    slot.bound = bool(values["bound"])
    slot.reachable = bool(values["reachable"])
    slot.on = bool(values["on"])
    slot.seq = int(packet.seq)
    slot.timestamp_ms = int(packet.timestamp_ms)


def _wait_for_manifest(
    ser: serial.Serial,
    timeout_seconds: float,
) -> tuple[dict[str, Any], dict[str, Any] | None, dict[str, SlotState], list[str]]:
    deadline = time.monotonic() + timeout_seconds
    hello: dict[str, Any] | None = None
    discovery: dict[str, Any] | None = None
    slots: dict[str, SlotState] = _build_known_slots()
    logs: list[str] = []

    ser.write(encode_packet(MSG_PING))
    ser.write(encode_packet(MSG_DESCRIBE_REQ))
    ser.flush()

    while time.monotonic() < deadline:
        frame = _read_frame(ser, deadline)
        if frame is None:
            break
        packet = decode_packet(frame)

        if packet.msg_type == MSG_HELLO:
            hello = decode_hello(packet.payload)
            continue
        if packet.msg_type == MSG_DESCRIBE:
            discovery = decode_discovery(packet.payload)
            slots = _build_slots(discovery)
            continue
        if packet.msg_type == MSG_LOG:
            logs.append(packet.payload.decode("utf-8", errors="replace").strip())
            continue
        if packet.msg_type == MSG_PONG:
            continue
        if packet.msg_type == MSG_STATE:
            for slot in slots.values():
                if packet.interface_id == slot.state_interface_id:
                    _apply_state(packet, slot)
                    break

        if hello and all(slot.seq > 0 for slot in slots.values()):
            return hello, discovery, slots, logs

    _expect(hello is not None, "No HELLO received")
    missing = [slot.name for slot in slots.values() if slot.seq == 0]
    _expect(not missing, f"Missing initial STATE packets for: {', '.join(missing)}")
    return hello, discovery, slots, logs


def _drain_non_state_logs(ser: serial.Serial, slots: dict[str, SlotState], deadline: float) -> list[str]:
    logs: list[str] = []
    while time.monotonic() < deadline:
        frame = _read_frame(ser, deadline)
        if frame is None:
            break
        packet = decode_packet(frame)
        if packet.msg_type == MSG_LOG:
            logs.append(packet.payload.decode("utf-8", errors="replace").strip())
            continue
        if packet.msg_type == MSG_STATE:
            for slot in slots.values():
                if packet.interface_id == slot.state_interface_id:
                    _apply_state(packet, slot)
                    break
    return logs


def _wait_until_all_reachable(
    ser: serial.Serial,
    slots: dict[str, SlotState],
    timeout_seconds: float,
) -> list[str]:
    deadline = time.monotonic() + timeout_seconds
    logs: list[str] = []
    while time.monotonic() < deadline:
        unreachable = [slot.name for slot in slots.values() if not slot.bound or not slot.reachable]
        if not unreachable:
            return logs
        frame = _read_frame(ser, deadline)
        if frame is None:
            break
        packet = decode_packet(frame)
        if packet.msg_type == MSG_LOG:
            logs.append(packet.payload.decode("utf-8", errors="replace").strip())
            continue
        if packet.msg_type == MSG_STATE:
            for slot in slots.values():
                if packet.interface_id == slot.state_interface_id:
                    _apply_state(packet, slot)
                    break
    unreachable = [slot.name for slot in slots.values() if not slot.bound or not slot.reachable]
    raise SuiteError(f"Not fully reachable before toggle test: {', '.join(unreachable)}")


def _toggle_and_wait(
    ser: serial.Serial,
    slot: SlotState,
    requested_on: bool,
    timeout_seconds: float,
) -> list[str]:
    payload = encode_struct_payload({"on": requested_on}, COMMAND_FIELDS)
    command_seq = int(time.time() * 1000) & 0xFFFFFFFF
    logs: list[str] = []

    ser.write(
        encode_packet(
            MSG_CMD,
            interface_id=slot.command_interface_id,
            seq=command_seq,
            timestamp_ms=0,
            payload=payload,
        )
    )
    ser.flush()

    deadline = time.monotonic() + timeout_seconds
    saw_ack = False

    while time.monotonic() < deadline:
        frame = _read_frame(ser, deadline)
        if frame is None:
            break
        packet = decode_packet(frame)
        if packet.msg_type == MSG_LOG:
            logs.append(packet.payload.decode("utf-8", errors="replace").strip())
            continue
        if packet.msg_type == MSG_ACK and packet.interface_id == slot.command_interface_id and packet.seq == command_seq:
            saw_ack = True
            continue
        if packet.msg_type == MSG_NACK and packet.interface_id == slot.command_interface_id and packet.seq == command_seq:
            reason = packet.payload.decode("utf-8", errors="replace").strip()
            raise SuiteError(f"{slot.name} command NACK: {reason}")
        if packet.msg_type == MSG_STATE and packet.interface_id == slot.state_interface_id:
            _apply_state(packet, slot)
            if saw_ack and slot.reachable and slot.on == requested_on:
                return logs

    raise SuiteError(
        f"{slot.name} did not confirm {'on' if requested_on else 'off'}"
        f" within {timeout_seconds:.1f}s"
    )


def main() -> int:
    args = _parse_args()
    with serial.Serial(args.port, args.baud, timeout=0.25) as ser:
        ser.reset_input_buffer()
        ser.reset_output_buffer()

        hello, discovery, slots, startup_logs = _wait_for_manifest(ser, args.timeout)

        _expect(hello.get("device_uid") == EXPECTED_DEVICE_UID, f"Unexpected device_uid: {hello.get('device_uid')!r}")
        _expect(hello.get("model") == EXPECTED_MODEL, f"Unexpected model: {hello.get('model')!r}")
        _expect(hello.get("schema_hash") == EXPECTED_SCHEMA_HASH, f"Unexpected schema_hash: {hello.get('schema_hash')!r}")
        _expect(int(hello.get("interface_count") or 0) == 8, f"Unexpected interface_count: {hello.get('interface_count')!r}")
        if discovery is not None:
            _expect(
                int(discovery.get("interface_count") or 0) == 8,
                f"Unexpected described interface_count: {discovery.get('interface_count')!r}",
            )

        initial_states = {name: slot.on for name, slot in slots.items()}
        results: list[str] = []
        all_logs = list(startup_logs)
        all_logs.extend(_wait_until_all_reachable(ser, slots, args.timeout))

        for slot_name in EXPECTED_SLOTS:
            slot = slots[slot_name]
            off_logs = _toggle_and_wait(ser, slot, False, args.command_timeout)
            all_logs.extend(off_logs)
            results.append(f"{slot.name}: off OK")

            on_logs = _toggle_and_wait(ser, slot, True, args.command_timeout)
            all_logs.extend(on_logs)
            results.append(f"{slot.name}: on OK")

        restore_logs: list[str] = []
        for slot_name in EXPECTED_SLOTS:
            slot = slots[slot_name]
            desired = initial_states[slot_name]
            if slot.on == desired:
                continue
            restore_logs.extend(_toggle_and_wait(ser, slot, desired, args.command_timeout))
        all_logs.extend(restore_logs)

        tail_logs = _drain_non_state_logs(ser, slots, time.monotonic() + 1.0)
        all_logs.extend(tail_logs)

        print(
            "Device:"
            f" uid={hello.get('device_uid')}"
            f" model={hello.get('model')}"
            f" fw={hello.get('firmware_version')}"
            f" schema={hello.get('schema_hash')}"
        )
        if discovery is None:
            print("Manifest: DESCRIBE missing, used known HS105 interface IDs")
        for slot_name in EXPECTED_SLOTS:
            slot = slots[slot_name]
            print(
                f"{slot.name}: bound={slot.bound} reachable={slot.reachable} on={slot.on}"
                f" state_if={slot.state_interface_id} cmd_if={slot.command_interface_id}"
            )
        print("Results:")
        for line in results:
            print(f"  {line}")
        if all_logs:
            print("Recent logs:")
            for message in all_logs[-10:]:
                print(f"  {message}")
        return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SuiteError as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        raise SystemExit(1)
