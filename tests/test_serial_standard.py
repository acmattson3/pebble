from __future__ import annotations

import unittest

from control.services.serial_standard import (
    MSG_STATE,
    cobs_decode,
    cobs_encode,
    decode_discovery,
    decode_hello,
    decode_packet,
    decode_struct_payload,
    encode_discovery,
    encode_hello,
    encode_packet,
    encode_struct_payload,
)


class SerialStandardTests(unittest.TestCase):
    def test_cobs_round_trip(self):
        payload = b"\x11\x00\x22\x00\x33"
        self.assertEqual(cobs_decode(cobs_encode(payload)), payload)

    def test_packet_round_trip(self):
        encoded = encode_packet(MSG_STATE, payload=b"\x01\x02", interface_id=7, seq=42, timestamp_ms=1234, flags=3)
        packet = decode_packet(encoded[:-1])
        self.assertEqual(packet.msg_type, MSG_STATE)
        self.assertEqual(packet.interface_id, 7)
        self.assertEqual(packet.seq, 42)
        self.assertEqual(packet.timestamp_ms, 1234)
        self.assertEqual(packet.flags, 3)
        self.assertEqual(packet.payload, b"\x01\x02")

    def test_hello_round_trip(self):
        payload = encode_hello(
            schema_version=1,
            interface_count=2,
            schema_hash="goob-imu-v2",
            device_uid="goob-imu-nano",
            firmware_version="2026.03.28",
            model="arduino-nano",
        )
        decoded = decode_hello(payload)
        self.assertEqual(decoded["interface_count"], 2)
        self.assertEqual(decoded["schema_hash"], "goob-imu-v2")
        self.assertEqual(decoded["device_uid"], "goob-imu-nano")

    def test_discovery_round_trip(self):
        discovery = {
            "schema_version": 1,
            "schema_hash": "goob-imu-v2",
            "device_uid": "goob-imu-nano",
            "firmware_version": "2026.03.28",
            "model": "arduino-nano",
            "interfaces": [
                {
                    "id": 1,
                    "name": "imu_fast",
                    "dir": "out",
                    "kind": "sample",
                    "channel": "sensors/imu-fast",
                    "profile": "imu.motion.v1",
                    "rate_hz": 100,
                    "local_only": True,
                    "encoding": {
                        "kind": "struct_v1",
                        "fields": [
                            {"name": "ax", "type": "i16", "scale": 0.001, "unit": "mps2"},
                            {"name": "ay", "type": "i16", "scale": 0.001, "unit": "mps2"},
                        ],
                    },
                },
                {
                    "id": 2,
                    "name": "imu",
                    "dir": "out",
                    "kind": "state",
                    "channel": "sensors/imu",
                    "profile": "imu.summary.v1",
                    "rate_hz": 5,
                    "retain": True,
                    "encoding": {
                        "kind": "struct_v1",
                        "fields": [
                            {"name": "accel_norm_g", "type": "u16", "scale": 0.001, "unit": "g"},
                            {"name": "samples_ok", "type": "u32"},
                        ],
                    },
                },
            ],
        }
        decoded = decode_discovery(encode_discovery(discovery))
        self.assertEqual(decoded["device_uid"], "goob-imu-nano")
        self.assertEqual(len(decoded["interfaces"]), 2)
        self.assertTrue(decoded["interfaces"][0]["local_only"])
        self.assertTrue(decoded["interfaces"][1]["retain"])
        self.assertAlmostEqual(decoded["interfaces"][0]["encoding"]["fields"][0]["scale"], 0.001, places=6)

    def test_struct_payload_round_trip(self):
        fields = [
            {"name": "ax", "type": "i16", "scale": 0.001},
            {"name": "samples_ok", "type": "u32"},
        ]
        encoded = encode_struct_payload({"ax": 9.807, "samples_ok": 123}, fields)
        decoded = decode_struct_payload(encoded, fields)
        self.assertAlmostEqual(decoded["ax"], 9.807, places=3)
        self.assertEqual(decoded["samples_ok"], 123)


if __name__ == "__main__":
    unittest.main()
