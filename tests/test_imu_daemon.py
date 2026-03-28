from __future__ import annotations

import json
import math
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from control.services.imu_daemon import ImuDaemon, ImuProcessedSample, ImuProcessor, RawImuReading
from control.services.imu_daemon.service import (
    MPU6050_REG_CONFIG,
    MPU6050_REG_PWR_MGMT_1,
    MPU6050_REG_SMPLRT_DIV,
    MPU6050_REG_WHO_AM_I,
    Mpu6050Sensor,
)
from tests.helpers import FakeMqttClient, make_base_config


class FakeInitI2cDevice:
    def __init__(self, read_script: dict[int, list[object]], write_failures: dict[tuple[int, int], list[object]] | None = None) -> None:
        self.read_script = {register: list(values) for register, values in read_script.items()}
        self.write_failures = {
            key: list(values) for key, values in (write_failures or {}).items()
        }
        self.open_calls = 0
        self.close_calls = 0
        self.writes: list[tuple[int, int]] = []

    def open(self) -> None:
        self.open_calls += 1

    def close(self) -> None:
        self.close_calls += 1

    def read_reg(self, register: int) -> int:
        scripted = self.read_script.get(register)
        if not scripted:
            raise AssertionError(f"Unexpected read of register 0x{register:02x}")
        value = scripted.pop(0)
        if isinstance(value, Exception):
            raise value
        return int(value)

    def write_reg(self, register: int, value: int) -> None:
        self.writes.append((register, value))
        scripted = self.write_failures.get((register, value))
        if not scripted:
            return
        outcome = scripted.pop(0)
        if isinstance(outcome, Exception):
            raise outcome


class ImuDaemonTests(unittest.TestCase):
    def _daemon(self) -> ImuDaemon:
        config = make_base_config("imubot")
        config["services"]["imu_daemon"]["enabled"] = True
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "config.json"
            path.write_text(json.dumps(config))
            return ImuDaemon(config, path)

    def _sample(self, seq: int, epoch_ms: int, monotonic_s: float) -> ImuProcessedSample:
        return ImuProcessedSample(
            seq=seq,
            epoch_ms=epoch_ms,
            monotonic_s=monotonic_s,
            dt_s=0.02,
            accel_g=(0.0, 0.0, 1.0),
            accel_mps2=(0.0, 0.0, 9.80665),
            gyro_dps=(0.1, -0.2, 0.05),
            temp_c=24.5,
            roll_deg=1.2,
            pitch_deg=-3.4,
            accel_norm_g=1.0,
        )

    def test_processor_applies_gyro_bias_and_gravity_scale(self):
        processor = ImuProcessor(
            accel_lsb_per_g=16384.0,
            gyro_lsb_per_dps=131.0,
            accel_alpha=1.0,
            gyro_alpha=1.0,
            orientation_alpha=0.0,
        )
        processor.set_gyro_bias_raw((131.0, -262.0, 0.0))
        sample = processor.update(
            RawImuReading(
                monotonic_s=1.0,
                epoch_ms=1000,
                accel_raw=(0, 0, 16384),
                gyro_raw=(131, -262, 0),
                temp_raw=0,
            )
        )
        self.assertAlmostEqual(sample.accel_mps2[2], 9.80665, places=4)
        self.assertAlmostEqual(sample.gyro_dps[0], 0.0, places=4)
        self.assertAlmostEqual(sample.gyro_dps[1], 0.0, places=4)
        self.assertAlmostEqual(sample.temp_c, 36.53, places=2)
        self.assertAlmostEqual(sample.roll_deg, 0.0, places=3)
        self.assertAlmostEqual(sample.pitch_deg, 0.0, places=3)

    def test_processor_orientation_uses_accel_reference(self):
        processor = ImuProcessor(
            accel_lsb_per_g=16384.0,
            gyro_lsb_per_dps=131.0,
            accel_alpha=1.0,
            gyro_alpha=1.0,
            orientation_alpha=0.0,
        )
        raw = int(round(16384.0 / math.sqrt(2.0)))
        sample = processor.update(
            RawImuReading(
                monotonic_s=1.0,
                epoch_ms=1000,
                accel_raw=(raw, 0, raw),
                gyro_raw=(0, 0, 0),
                temp_raw=0,
            )
        )
        self.assertAlmostEqual(sample.roll_deg, 0.0, places=2)
        self.assertAlmostEqual(sample.pitch_deg, -45.0, delta=1.0)

    def test_daemon_defaults_topics_and_device(self):
        daemon = self._daemon()
        self.assertEqual("pebble/robots/imubot/outgoing/sensors/imu-fast", daemon.high_rate_topic)
        self.assertEqual("pebble/robots/imubot/outgoing/sensors/imu", daemon.low_rate_topic)
        self.assertEqual("/dev/i2c-2", daemon.device_path)
        self.assertEqual(0x68, daemon.device_address)

    def test_daemon_publishes_high_and_low_rate_topics(self):
        daemon = self._daemon()
        daemon.client = FakeMqttClient()
        daemon.high_rate_hz = 20.0
        daemon.low_rate_hz = 2.0

        daemon._handle_processed_sample(self._sample(1, 1000, 1.0), now=1.0)
        daemon._handle_processed_sample(self._sample(2, 1020, 1.02), now=1.02)
        daemon._handle_processed_sample(self._sample(3, 1060, 1.06), now=1.06)
        daemon._handle_processed_sample(self._sample(4, 1510, 1.51), now=1.51)

        calls = daemon.client.publish_calls  # type: ignore[union-attr]
        self.assertEqual(
            [call[0] for call in calls],
            [
                daemon.high_rate_topic,
                daemon.low_rate_topic,
                daemon.high_rate_topic,
                daemon.high_rate_topic,
                daemon.low_rate_topic,
            ],
        )
        low_payload = json.loads(calls[-1][1])
        self.assertEqual(low_payload["value"]["health"]["samples_ok"], 0)
        self.assertEqual(low_payload["value"]["orientation_deg"]["pitch"], -3.4)

    def test_sensor_open_retries_transient_wake_write_without_reopen(self):
        sensor = Mpu6050Sensor(
            "/dev/i2c-2",
            0x68,
            minimal_init=False,
            dlpf_config=3,
            sample_rate_divider=19,
            gyro_full_scale=0,
            accel_full_scale=0,
            read_retries=2,
            retry_backoff_seconds=0.0,
        )
        fake_device = FakeInitI2cDevice(
            read_script={
                MPU6050_REG_WHO_AM_I: [0x68, 0x68],
                MPU6050_REG_PWR_MGMT_1: [0x40, 0x40],
            },
            write_failures={
                (MPU6050_REG_PWR_MGMT_1, 0x00): [TimeoutError(110, "Connection timed out")],
            },
        )
        sensor.device = fake_device  # type: ignore[assignment]

        with mock.patch("control.services.imu_daemon.service.time.sleep", return_value=None):
            sensor.open()

        self.assertEqual(2, fake_device.writes.count((MPU6050_REG_PWR_MGMT_1, 0x00)))
        self.assertIn((MPU6050_REG_CONFIG, 0x03), fake_device.writes)
        self.assertIn((MPU6050_REG_SMPLRT_DIV, 0x13), fake_device.writes)

    def test_sensor_open_tolerates_nonessential_config_write_failure(self):
        sensor = Mpu6050Sensor(
            "/dev/i2c-2",
            0x68,
            minimal_init=False,
            dlpf_config=3,
            sample_rate_divider=19,
            gyro_full_scale=0,
            accel_full_scale=0,
            read_retries=2,
            retry_backoff_seconds=0.0,
        )
        fake_device = FakeInitI2cDevice(
            read_script={
                MPU6050_REG_WHO_AM_I: [0x68],
                MPU6050_REG_PWR_MGMT_1: [0x00],
            },
            write_failures={
                (MPU6050_REG_CONFIG, 0x03): [OSError("nack"), OSError("nack")],
            },
        )
        sensor.device = fake_device  # type: ignore[assignment]

        with mock.patch("control.services.imu_daemon.service.time.sleep", return_value=None):
            sensor.open()

        self.assertIn((MPU6050_REG_CONFIG, 0x03), fake_device.writes)
        self.assertIn((MPU6050_REG_SMPLRT_DIV, 0x13), fake_device.writes)

    def test_sensor_open_minimal_init_only_wakes_device(self):
        sensor = Mpu6050Sensor(
            "/dev/i2c-2",
            0x68,
            minimal_init=True,
            dlpf_config=3,
            sample_rate_divider=19,
            gyro_full_scale=0,
            accel_full_scale=0,
            read_retries=2,
            retry_backoff_seconds=0.0,
        )
        fake_device = FakeInitI2cDevice(read_script={})
        sensor.device = fake_device  # type: ignore[assignment]

        with mock.patch("control.services.imu_daemon.service.time.sleep", return_value=None):
            sensor.open()

        self.assertEqual([(MPU6050_REG_PWR_MGMT_1, 0x00)], fake_device.writes)


if __name__ == "__main__":
    unittest.main()
