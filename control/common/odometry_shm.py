from __future__ import annotations

import struct
import time
from contextlib import suppress
from multiprocessing import shared_memory
from typing import Optional

try:
    # Python's resource_tracker can incorrectly unlink shared-memory names
    # from attach-only readers at process shutdown. Unregistering avoids
    # deleting the segment name out from under the writer/other readers.
    from multiprocessing import resource_tracker  # type: ignore
except Exception:  # pragma: no cover - platform/runtime dependent
    resource_tracker = None


MAGIC = b"PBODRAW1"
VERSION = 1

HEADER_FORMAT = "<8sHHIQ"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

SLOT_FORMAT = "<QQiiii"
SLOT_SIZE = struct.calcsize(SLOT_FORMAT)

_UNREGISTERED_TRACKER_NAMES: set[str] = set()


def _resource_tracker_unregister(name: str) -> None:
    tracker = resource_tracker
    if tracker is None or not name:
        return
    if name in _UNREGISTERED_TRACKER_NAMES:
        return
    with suppress(Exception):
        tracker.unregister(name, "shared_memory")
        _UNREGISTERED_TRACKER_NAMES.add(name)


class OdometryRawShmWriter:
    def __init__(self, name: str, slots: int = 2048) -> None:
        if not isinstance(name, str) or not name.strip():
            raise ValueError("shared-memory name is required")
        if int(slots) < 1:
            raise ValueError("slots must be >= 1")

        self.name = name.strip()
        self.slots = int(slots)
        self.size = HEADER_SIZE + (self.slots * SLOT_SIZE)

        self.created = False
        try:
            shm = shared_memory.SharedMemory(name=self.name, create=True, size=self.size)
            self.created = True
        except FileExistsError:
            shm = shared_memory.SharedMemory(name=self.name, create=False)
            if shm.size != self.size:
                shm.close()
                raise ValueError(
                    f"Existing shared-memory '{self.name}' has size={shm.size}, expected={self.size}."
                )

        self._shm = shm
        self._buf = shm.buf
        self._seq = 0
        self._write_header(seq=0)

    def _write_header(self, seq: int) -> None:
        struct.pack_into(HEADER_FORMAT, self._buf, 0, MAGIC, VERSION, self.slots, 0, int(seq))

    def write_sample(self, a0: int, a1: int, a2: int, *, chg: Optional[bool] = None, mono_ns: Optional[int] = None) -> int:
        self._seq += 1
        seq = self._seq
        idx = (seq - 1) % self.slots
        offset = HEADER_SIZE + (idx * SLOT_SIZE)
        timestamp = int(mono_ns) if mono_ns is not None else time.monotonic_ns()
        chg_value = -1 if chg is None else (1 if bool(chg) else 0)
        struct.pack_into(
            SLOT_FORMAT,
            self._buf,
            offset,
            int(seq),
            timestamp,
            int(a0),
            int(a1),
            int(a2),
            chg_value,
        )
        self._write_header(seq=seq)
        return seq

    def close(self) -> None:
        self._shm.close()

    def unlink(self) -> None:
        name = self._shm._name
        _resource_tracker_unregister(name)
        try:
            if getattr(shared_memory, "_USE_POSIX", False) and getattr(shared_memory, "_posixshmem", None) is not None:
                shared_memory._posixshmem.shm_unlink(name)
            else:  # pragma: no cover - non-POSIX fallback
                self._shm.unlink()
        except FileNotFoundError:
            return
        finally:
            _UNREGISTERED_TRACKER_NAMES.discard(name)


class OdometryRawShmReader:
    def __init__(self, name: str) -> None:
        if not isinstance(name, str) or not name.strip():
            raise ValueError("shared-memory name is required")
        self.name = name.strip()
        self._shm = shared_memory.SharedMemory(name=self.name, create=False)
        self._buf = self._shm.buf
        _resource_tracker_unregister(self._shm._name)

        magic, version, slots, _reserved, _seq = self._read_header()
        if magic != MAGIC:
            self._shm.close()
            raise ValueError(f"Unexpected shm magic for '{self.name}': {magic!r}")
        if version != VERSION:
            self._shm.close()
            raise ValueError(f"Unsupported shm version for '{self.name}': {version}")
        if slots < 1:
            self._shm.close()
            raise ValueError(f"Invalid slot count for '{self.name}': {slots}")

        self.slots = int(slots)
        min_size = HEADER_SIZE + (self.slots * SLOT_SIZE)
        if self._shm.size < min_size:
            self._shm.close()
            raise ValueError(f"Shared-memory '{self.name}' is too small: {self._shm.size} < {min_size}")

    def _read_header(self) -> tuple[bytes, int, int, int, int]:
        return struct.unpack_from(HEADER_FORMAT, self._buf, 0)

    def _read_seq(self, seq: int) -> Optional[dict]:
        if seq < 1:
            return None
        idx = (seq - 1) % self.slots
        offset = HEADER_SIZE + (idx * SLOT_SIZE)
        slot_seq, mono_ns, a0, a1, a2, chg_value = struct.unpack_from(SLOT_FORMAT, self._buf, offset)
        if slot_seq != seq:
            return None
        return {
            "seq": int(slot_seq),
            "mono_ns": int(mono_ns),
            "a0": int(a0),
            "a1": int(a1),
            "a2": int(a2),
            "chg": None if chg_value < 0 else bool(chg_value),
        }

    def read_latest(self) -> Optional[dict]:
        _magic, _version, _slots, _reserved, write_seq = self._read_header()
        if write_seq <= 0:
            return None
        return self._read_seq(int(write_seq))

    def read_since(self, last_seq: int) -> list[dict]:
        _magic, _version, _slots, _reserved, write_seq = self._read_header()
        end_seq = int(write_seq)
        if end_seq <= int(last_seq):
            return []
        min_seq = max(1, end_seq - self.slots + 1)
        start_seq = max(int(last_seq) + 1, min_seq)
        out: list[dict] = []
        for seq in range(start_seq, end_seq + 1):
            row = self._read_seq(seq)
            if row is not None:
                out.append(row)
        return out

    def close(self) -> None:
        self._shm.close()
