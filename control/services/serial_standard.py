from __future__ import annotations

import math
import struct
from dataclasses import dataclass
from typing import Any


PROTOCOL_NAME = "pebble_serial_v1"
PROTOCOL_VERSION = 1

MSG_HELLO = 0x01
MSG_DESCRIBE_REQ = 0x02
MSG_DESCRIBE = 0x03
MSG_SAMPLE = 0x10
MSG_STATE = 0x11
MSG_CMD = 0x20
MSG_ACK = 0x21
MSG_NACK = 0x22
MSG_LOG = 0x30
MSG_PING = 0x31
MSG_PONG = 0x32

HEADER_STRUCT = struct.Struct("<BBBHII")
CRC_STRUCT = struct.Struct("<H")
DISCOVERY_PREFIX_STRUCT = struct.Struct("<BB")
INTERFACE_HEADER_STRUCT = struct.Struct("<HBBBH")
FIELD_HEADER_STRUCT = struct.Struct("<Bff")

INTERFACE_FLAG_RETAIN = 0x01
INTERFACE_FLAG_LOCAL_ONLY = 0x02


class SerialStandardError(ValueError):
    pass


@dataclass(frozen=True)
class Packet:
    version: int
    msg_type: int
    flags: int
    interface_id: int
    seq: int
    timestamp_ms: int
    payload: bytes


def crc16_ccitt(data: bytes, seed: int = 0xFFFF) -> int:
    crc = seed & 0xFFFF
    for byte in data:
        crc ^= byte << 8
        for _ in range(8):
            if crc & 0x8000:
                crc = ((crc << 1) ^ 0x1021) & 0xFFFF
            else:
                crc = (crc << 1) & 0xFFFF
    return crc


def cobs_encode(data: bytes) -> bytes:
    if not data:
        return b"\x01"
    out = bytearray()
    code_index = 0
    out.append(0)
    code = 1
    for byte in data:
        if byte == 0:
            out[code_index] = code
            code_index = len(out)
            out.append(0)
            code = 1
            continue
        out.append(byte)
        code += 1
        if code == 0xFF:
            out[code_index] = code
            code_index = len(out)
            out.append(0)
            code = 1
    out[code_index] = code
    return bytes(out)


def cobs_decode(data: bytes) -> bytes:
    if not data:
        raise SerialStandardError("empty COBS frame")
    out = bytearray()
    index = 0
    length = len(data)
    while index < length:
        code = data[index]
        if code == 0:
            raise SerialStandardError("invalid COBS code byte 0")
        index += 1
        next_index = index + code - 1
        if next_index > length:
            raise SerialStandardError("truncated COBS frame")
        out.extend(data[index:next_index])
        index = next_index
        if code != 0xFF and index < length:
            out.append(0)
    return bytes(out)


def encode_packet(
    msg_type: int,
    *,
    payload: bytes = b"",
    interface_id: int = 0,
    seq: int = 0,
    timestamp_ms: int = 0,
    flags: int = 0,
    version: int = PROTOCOL_VERSION,
) -> bytes:
    header = HEADER_STRUCT.pack(
        version & 0xFF,
        msg_type & 0xFF,
        flags & 0xFF,
        interface_id & 0xFFFF,
        seq & 0xFFFFFFFF,
        timestamp_ms & 0xFFFFFFFF,
    )
    body = header + payload
    crc = CRC_STRUCT.pack(crc16_ccitt(body))
    return cobs_encode(body + crc) + b"\x00"


def decode_packet(frame: bytes) -> Packet:
    decoded = cobs_decode(frame)
    minimum_size = HEADER_STRUCT.size + CRC_STRUCT.size
    if len(decoded) < minimum_size:
        raise SerialStandardError("packet too short")
    body = decoded[:-CRC_STRUCT.size]
    expected_crc = CRC_STRUCT.unpack(decoded[-CRC_STRUCT.size:])[0]
    actual_crc = crc16_ccitt(body)
    if actual_crc != expected_crc:
        raise SerialStandardError(f"CRC mismatch: expected {expected_crc:#06x}, got {actual_crc:#06x}")
    version, msg_type, flags, interface_id, seq, timestamp_ms = HEADER_STRUCT.unpack(body[: HEADER_STRUCT.size])
    payload = body[HEADER_STRUCT.size :]
    return Packet(
        version=version,
        msg_type=msg_type,
        flags=flags,
        interface_id=interface_id,
        seq=seq,
        timestamp_ms=timestamp_ms,
        payload=payload,
    )


TYPE_FORMATS: dict[str, tuple[str, int]] = {
    "bool": ("<?", 1),
    "u8": ("<B", 1),
    "i8": ("<b", 1),
    "u16": ("<H", 2),
    "i16": ("<h", 2),
    "u32": ("<I", 4),
    "i32": ("<i", 4),
    "f32": ("<f", 4),
}

DIR_TO_CODE = {"in": 1, "out": 2}
CODE_TO_DIR = {value: key for key, value in DIR_TO_CODE.items()}

KIND_TO_CODE = {"sample": 1, "state": 2, "command": 3, "event": 4}
CODE_TO_KIND = {value: key for key, value in KIND_TO_CODE.items()}

ENCODING_KIND_TO_CODE = {"struct_v1": 1}
CODE_TO_ENCODING_KIND = {value: key for key, value in ENCODING_KIND_TO_CODE.items()}

TYPE_TO_CODE = {"bool": 1, "u8": 2, "i8": 3, "u16": 4, "i16": 5, "u32": 6, "i32": 7, "f32": 8}
CODE_TO_TYPE = {value: key for key, value in TYPE_TO_CODE.items()}


def _encode_short_string(value: Any) -> bytes:
    encoded = str(value or "").encode("utf-8")
    if len(encoded) > 255:
        raise SerialStandardError("string too long for short-string encoding")
    return bytes((len(encoded),)) + encoded


def _decode_short_string(payload: bytes, index: int) -> tuple[str, int]:
    if index >= len(payload):
        raise SerialStandardError("truncated short-string length")
    length = payload[index]
    index += 1
    end = index + length
    if end > len(payload):
        raise SerialStandardError("truncated short-string bytes")
    return payload[index:end].decode("utf-8"), end


def _field_dict(field: Any) -> dict[str, Any]:
    if isinstance(field, dict):
        return field
    if isinstance(field, (list, tuple)) and len(field) >= 2:
        result: dict[str, Any] = {"name": field[0], "type": field[1]}
        if len(field) >= 3:
            result["scale"] = field[2]
        if len(field) >= 4:
            result["offset"] = field[3]
        if len(field) >= 5:
            result["unit"] = field[4]
        return result
    raise SerialStandardError(f"invalid field spec: {field!r}")


def _pack_discovery_prefix(
    *,
    schema_version: int,
    interface_count: int,
    schema_hash: Any,
    device_uid: Any,
    firmware_version: Any,
    model: Any,
) -> bytearray:
    if not 0 <= int(interface_count) <= 255:
        raise SerialStandardError("interface_count must fit in u8")
    payload = bytearray(DISCOVERY_PREFIX_STRUCT.pack(schema_version & 0xFF, interface_count & 0xFF))
    payload.extend(_encode_short_string(schema_hash))
    payload.extend(_encode_short_string(device_uid))
    payload.extend(_encode_short_string(firmware_version))
    payload.extend(_encode_short_string(model))
    return payload


def _unpack_discovery_prefix(payload: bytes) -> tuple[dict[str, Any], int]:
    if len(payload) < DISCOVERY_PREFIX_STRUCT.size:
        raise SerialStandardError("truncated discovery prefix")
    schema_version, interface_count = DISCOVERY_PREFIX_STRUCT.unpack(payload[: DISCOVERY_PREFIX_STRUCT.size])
    index = DISCOVERY_PREFIX_STRUCT.size
    schema_hash, index = _decode_short_string(payload, index)
    device_uid, index = _decode_short_string(payload, index)
    firmware_version, index = _decode_short_string(payload, index)
    model, index = _decode_short_string(payload, index)
    return (
        {
            "protocol": PROTOCOL_NAME,
            "schema_version": int(schema_version),
            "interface_count": int(interface_count),
            "schema_hash": schema_hash,
            "device_uid": device_uid,
            "firmware_version": firmware_version,
            "model": model,
        },
        index,
    )


def encode_hello(
    *,
    schema_version: int = 1,
    interface_count: int = 0,
    schema_hash: Any = "",
    device_uid: Any = "",
    firmware_version: Any = "",
    model: Any = "",
) -> bytes:
    return bytes(
        _pack_discovery_prefix(
            schema_version=schema_version,
            interface_count=interface_count,
            schema_hash=schema_hash,
            device_uid=device_uid,
            firmware_version=firmware_version,
            model=model,
        )
    )


def decode_hello(payload: bytes) -> dict[str, Any]:
    result, index = _unpack_discovery_prefix(payload)
    if index != len(payload):
        raise SerialStandardError("extra bytes in hello payload")
    return result


def encode_discovery(discovery: dict[str, Any]) -> bytes:
    interfaces = discovery.get("interfaces")
    if not isinstance(interfaces, list):
        raise SerialStandardError("discovery interfaces must be a list")
    payload = _pack_discovery_prefix(
        schema_version=int(discovery.get("schema_version") or 1),
        interface_count=len(interfaces),
        schema_hash=discovery.get("schema_hash") or "",
        device_uid=discovery.get("device_uid") or "",
        firmware_version=discovery.get("firmware_version") or "",
        model=discovery.get("model") or "",
    )
    for interface in interfaces:
        if not isinstance(interface, dict):
            raise SerialStandardError("interface must be an object")
        encoding = interface.get("encoding")
        if not isinstance(encoding, dict):
            raise SerialStandardError("interface missing encoding object")
        fields = encoding.get("fields")
        if not isinstance(fields, list):
            raise SerialStandardError("interface missing encoding fields")
        dir_name = str(interface.get("dir") or "").strip().lower()
        kind_name = str(interface.get("kind") or "").strip().lower()
        encoding_kind = str(encoding.get("kind") or "").strip().lower()
        if dir_name not in DIR_TO_CODE:
            raise SerialStandardError(f"unsupported interface dir: {dir_name}")
        if kind_name not in KIND_TO_CODE:
            raise SerialStandardError(f"unsupported interface kind: {kind_name}")
        if encoding_kind not in ENCODING_KIND_TO_CODE:
            raise SerialStandardError(f"unsupported encoding kind: {encoding_kind}")
        flags = 0
        if bool(interface.get("retain", False)):
            flags |= INTERFACE_FLAG_RETAIN
        if bool(interface.get("local_only", False)):
            flags |= INTERFACE_FLAG_LOCAL_ONLY
        payload.extend(
            INTERFACE_HEADER_STRUCT.pack(
                int(interface.get("id") or 0) & 0xFFFF,
                DIR_TO_CODE[dir_name],
                KIND_TO_CODE[kind_name],
                flags,
                int(interface.get("rate_hz") or 0) & 0xFFFF,
            )
        )
        payload.extend(_encode_short_string(interface.get("name") or ""))
        payload.extend(_encode_short_string(interface.get("channel") or ""))
        payload.extend(_encode_short_string(interface.get("profile") or ""))
        field_count = len(fields)
        if field_count > 255:
            raise SerialStandardError("field count must fit in u8")
        payload.append(ENCODING_KIND_TO_CODE[encoding_kind])
        payload.append(field_count & 0xFF)
        for field in fields:
            field_dict = _field_dict(field)
            field_name = str(field_dict.get("name") or "").strip()
            type_name = str(field_dict.get("type") or "").strip()
            if not field_name:
                raise SerialStandardError("field missing name")
            if type_name not in TYPE_TO_CODE:
                raise SerialStandardError(f"unsupported field type: {type_name}")
            payload.extend(_encode_short_string(field_name))
            payload.extend(
                FIELD_HEADER_STRUCT.pack(
                    TYPE_TO_CODE[type_name],
                    float(field_dict.get("scale", 1.0)),
                    float(field_dict.get("offset", 0.0)),
                )
            )
            payload.extend(_encode_short_string(field_dict.get("unit") or ""))
    return bytes(payload)


def decode_discovery(payload: bytes) -> dict[str, Any]:
    discovery, index = _unpack_discovery_prefix(payload)
    interfaces: list[dict[str, Any]] = []
    for _ in range(discovery["interface_count"]):
        end = index + INTERFACE_HEADER_STRUCT.size
        if end > len(payload):
            raise SerialStandardError("truncated interface header")
        interface_id, dir_code, kind_code, flags, rate_hz = INTERFACE_HEADER_STRUCT.unpack(payload[index:end])
        index = end
        if dir_code not in CODE_TO_DIR:
            raise SerialStandardError(f"unknown dir_code: {dir_code}")
        if kind_code not in CODE_TO_KIND:
            raise SerialStandardError(f"unknown kind_code: {kind_code}")
        name, index = _decode_short_string(payload, index)
        channel, index = _decode_short_string(payload, index)
        profile, index = _decode_short_string(payload, index)
        if index + 2 > len(payload):
            raise SerialStandardError("truncated encoding header")
        encoding_kind_code = payload[index]
        field_count = payload[index + 1]
        index += 2
        encoding_kind = CODE_TO_ENCODING_KIND.get(encoding_kind_code)
        if encoding_kind is None:
            raise SerialStandardError(f"unknown encoding kind: {encoding_kind_code}")
        fields: list[dict[str, Any]] = []
        for _field_index in range(field_count):
            field_name, index = _decode_short_string(payload, index)
            end = index + FIELD_HEADER_STRUCT.size
            if end > len(payload):
                raise SerialStandardError("truncated field header")
            type_code, scale, offset_value = FIELD_HEADER_STRUCT.unpack(payload[index:end])
            index = end
            type_name = CODE_TO_TYPE.get(type_code)
            if type_name is None:
                raise SerialStandardError(f"unknown field type code: {type_code}")
            unit, index = _decode_short_string(payload, index)
            field: dict[str, Any] = {"name": field_name, "type": type_name}
            if not math.isclose(scale, 1.0, rel_tol=1e-7, abs_tol=1e-7):
                field["scale"] = float(scale)
            if not math.isclose(offset_value, 0.0, rel_tol=1e-7, abs_tol=1e-7):
                field["offset"] = float(offset_value)
            if unit:
                field["unit"] = unit
            fields.append(field)
        interface: dict[str, Any] = {
            "id": int(interface_id),
            "name": name,
            "dir": CODE_TO_DIR[dir_code],
            "kind": CODE_TO_KIND[kind_code],
            "channel": channel,
            "profile": profile,
            "rate_hz": int(rate_hz),
            "encoding": {
                "kind": encoding_kind,
                "fields": fields,
            },
        }
        if flags & INTERFACE_FLAG_RETAIN:
            interface["retain"] = True
        if flags & INTERFACE_FLAG_LOCAL_ONLY:
            interface["local_only"] = True
        interfaces.append(interface)
    if index != len(payload):
        raise SerialStandardError("extra bytes in discovery payload")
    discovery["interfaces"] = interfaces
    return discovery


def struct_payload_size(fields: list[dict[str, Any]]) -> int:
    size = 0
    for field in fields:
        field_dict = _field_dict(field)
        type_name = str(field_dict.get("type") or "").strip()
        if type_name not in TYPE_FORMATS:
            raise SerialStandardError(f"unsupported field type: {type_name}")
        size += TYPE_FORMATS[type_name][1]
    return size


def decode_struct_payload(payload: bytes, fields: list[dict[str, Any]]) -> dict[str, Any]:
    expected_size = struct_payload_size(fields)
    if len(payload) != expected_size:
        raise SerialStandardError(f"payload size mismatch: expected {expected_size}, got {len(payload)}")
    values: dict[str, Any] = {}
    offset = 0
    for field in fields:
        field_dict = _field_dict(field)
        field_name = str(field_dict.get("name") or "").strip()
        type_name = str(field_dict.get("type") or "").strip()
        if not field_name:
            raise SerialStandardError("field missing name")
        fmt, size = TYPE_FORMATS[type_name]
        raw_value = struct.unpack(fmt, payload[offset : offset + size])[0]
        offset += size
        if type_name == "bool":
            values[field_name] = bool(raw_value)
            continue
        scale = field_dict.get("scale")
        offset_value = field_dict.get("offset")
        if scale is None and offset_value is None and not type_name.startswith("f"):
            values[field_name] = int(raw_value)
            continue
        decoded_value = float(raw_value)
        if scale is not None:
            decoded_value *= float(scale)
        if offset_value is not None:
            decoded_value += float(offset_value)
        values[field_name] = decoded_value
    return values


def encode_struct_payload(values: dict[str, Any], fields: list[dict[str, Any]]) -> bytes:
    payload = bytearray()
    for field in fields:
        field_dict = _field_dict(field)
        field_name = str(field_dict.get("name") or "").strip()
        type_name = str(field_dict.get("type") or "").strip()
        if not field_name:
            raise SerialStandardError("field missing name")
        if field_name not in values:
            raise SerialStandardError(f"missing field value: {field_name}")
        fmt, _size = TYPE_FORMATS[type_name]
        value = values[field_name]
        if type_name == "bool":
            payload.extend(struct.pack(fmt, bool(value)))
            continue
        scale = field_dict.get("scale")
        offset_value = field_dict.get("offset")
        if type_name.startswith("f"):
            payload.extend(struct.pack(fmt, float(value)))
            continue
        raw_value = float(value)
        if offset_value is not None:
            raw_value -= float(offset_value)
        if scale is not None:
            scale_value = float(scale)
            if scale_value == 0.0:
                raise SerialStandardError(f"invalid zero scale for {field_name}")
            raw_value /= scale_value
        payload.extend(struct.pack(fmt, int(round(raw_value))))
    return bytes(payload)
