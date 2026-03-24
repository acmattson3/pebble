from __future__ import annotations

import os
import unittest
import uuid

from control.common.odometry_shm import OdometryRawShmReader, OdometryRawShmWriter


class OdometrySharedMemoryTests(unittest.TestCase):
    def _name(self) -> str:
        return f"pebble_test_odometry_{os.getpid()}_{uuid.uuid4().hex[:8]}"

    def test_round_trip_latest_sample(self):
        name = self._name()
        writer = OdometryRawShmWriter(name=name, slots=16)
        reader = OdometryRawShmReader(name=name)
        try:
            writer.write_sample(10, 20, 30, chg=True, mono_ns=123456789)
            latest = reader.read_latest()
            self.assertIsNotNone(latest)
            assert latest is not None
            self.assertEqual(latest["seq"], 1)
            self.assertEqual(latest["mono_ns"], 123456789)
            self.assertEqual(latest["a0"], 10)
            self.assertEqual(latest["a1"], 20)
            self.assertEqual(latest["a2"], 30)
            self.assertTrue(latest["chg"])
        finally:
            reader.close()
            writer.close()
            writer.unlink()

    def test_read_since_respects_ring_capacity(self):
        name = self._name()
        writer = OdometryRawShmWriter(name=name, slots=4)
        reader = OdometryRawShmReader(name=name)
        try:
            for i in range(1, 8):
                writer.write_sample(i, i + 1, i + 2, chg=False, mono_ns=i * 1000)

            rows = reader.read_since(last_seq=0)
            self.assertEqual([row["seq"] for row in rows], [4, 5, 6, 7])

            rows = reader.read_since(last_seq=5)
            self.assertEqual([row["seq"] for row in rows], [6, 7])
        finally:
            reader.close()
            writer.close()
            writer.unlink()


if __name__ == "__main__":
    unittest.main()
