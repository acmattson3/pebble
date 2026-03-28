#!/usr/bin/env python3
"""Read MPU-6050 IMU data over local I2C and publish filtered MQTT telemetry."""

from __future__ import annotations

import argparse
import ctypes
import fcntl
import json
import logging
import math
import os
import signal
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import paho.mqtt.client as mqtt

from ...common.config import load_config, log_level, service_cfg
from ...common.mqtt import mqtt_auth_and_tls
from ...common.topics import identity_from_config


I2C_M_RD = 0x0001
I2C_RDWR = 0x0707

MPU6050_REG_SMPLRT_DIV = 0x19
MPU6050_REG_CONFIG = 0x1A
MPU6050_REG_GYRO_CONFIG = 0x1B
MPU6050_REG_ACCEL_CONFIG = 0x1C
MPU6050_REG_ACCEL_XOUT_H = 0x3B
MPU6050_REG_PWR_MGMT_1 = 0x6B
MPU6050_REG_WHO_AM_I = 0x75

GRAVITY_MPS2 = 9.80665


def _parse_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_int(value: Any, default: int) -> int:
    if isinstance(value, str):
        text = value.strip().lower()
        if text.startswith("0x"):
            try:
                return int(text, 16)
            except ValueError:
                return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _int16(msb: int, lsb: int) -> int:
    value = (int(msb) << 8) | int(lsb)
    if value & 0x8000:
        value -= 0x10000
    return value


def _vector_lpf(previous: Optional[tuple[float, float, float]], current: tuple[float, float, float], alpha: float) -> tuple[float, float, float]:
    if previous is None:
        return current
    return tuple(previous[i] + alpha * (current[i] - previous[i]) for i in range(3))


def _vector_payload(values: tuple[float, float, float], digits: int = 4) -> dict[str, float]:
    return {
        "x": round(float(values[0]), digits),
        "y": round(float(values[1]), digits),
        "z": round(float(values[2]), digits),
    }


class _I2cMsg(ctypes.Structure):
    _fields_ = [
        ("addr", ctypes.c_uint16),
        ("flags", ctypes.c_uint16),
        ("len", ctypes.c_uint16),
        ("buf", ctypes.c_void_p),
    ]


class _I2cRdwrIoctlData(ctypes.Structure):
    _fields_ = [
        ("msgs", ctypes.POINTER(_I2cMsg)),
        ("nmsgs", ctypes.c_uint32),
    ]


class LinuxI2cDevice:
    def __init__(self, path: str, address: int) -> None:
        self.path = path
        self.address = int(address)
        self.fd: Optional[int] = None

    def open(self) -> None:
        if self.fd is not None:
            return
        self.fd = os.open(self.path, os.O_RDWR | os.O_CLOEXEC)

    def close(self) -> None:
        fd = self.fd
        self.fd = None
        if fd is None:
            return
        os.close(fd)

    def transfer(self, write_data: bytes, read_len: int = 0) -> bytes:
        fd = self.fd
        if fd is None:
            raise OSError("I2C device is not open")

        write_buf = (ctypes.c_ubyte * len(write_data))(*write_data)
        write_msg = _I2cMsg(
            addr=self.address,
            flags=0,
            len=len(write_data),
            buf=ctypes.cast(write_buf, ctypes.c_void_p).value,
        )

        if read_len > 0:
            read_buf = (ctypes.c_ubyte * read_len)()
            read_msg = _I2cMsg(
                addr=self.address,
                flags=I2C_M_RD,
                len=read_len,
                buf=ctypes.cast(read_buf, ctypes.c_void_p).value,
            )
            msgs = (_I2cMsg * 2)(write_msg, read_msg)
            ioctl_data = _I2cRdwrIoctlData(msgs=msgs, nmsgs=2)
            fcntl.ioctl(fd, I2C_RDWR, ioctl_data)
            return bytes(read_buf)

        msgs = (_I2cMsg * 1)(write_msg)
        ioctl_data = _I2cRdwrIoctlData(msgs=msgs, nmsgs=1)
        fcntl.ioctl(fd, I2C_RDWR, ioctl_data)
        return b""

    def write_reg(self, register: int, value: int) -> None:
        self.transfer(bytes((register & 0xFF, value & 0xFF)))

    def read_reg(self, register: int) -> int:
        return self.transfer(bytes((register & 0xFF,)), read_len=1)[0]

    def read_block(self, register: int, length: int) -> bytes:
        return self.transfer(bytes((register & 0xFF,)), read_len=length)


@dataclass
class RawImuReading:
    monotonic_s: float
    epoch_ms: int
    accel_raw: tuple[int, int, int]
    gyro_raw: tuple[int, int, int]
    temp_raw: int


@dataclass
class ImuProcessedSample:
    seq: int
    epoch_ms: int
    monotonic_s: float
    dt_s: float
    accel_g: tuple[float, float, float]
    accel_mps2: tuple[float, float, float]
    gyro_dps: tuple[float, float, float]
    temp_c: float
    roll_deg: float
    pitch_deg: float
    accel_norm_g: float


class Mpu6050Sensor:
    def __init__(
        self,
        path: str,
        address: int,
        *,
        minimal_init: bool,
        dlpf_config: int,
        sample_rate_divider: int,
        gyro_full_scale: int,
        accel_full_scale: int,
        read_retries: int,
        retry_backoff_seconds: float,
    ) -> None:
        self.path = path
        self.address = address
        self.minimal_init = bool(minimal_init)
        self.dlpf_config = int(dlpf_config) & 0x07
        self.sample_rate_divider = int(sample_rate_divider) & 0xFF
        self.gyro_full_scale = int(gyro_full_scale) & 0x03
        self.accel_full_scale = int(accel_full_scale) & 0x03
        self.read_retries = max(1, int(read_retries))
        self.retry_backoff_seconds = max(0.0, float(retry_backoff_seconds))

        self.device = LinuxI2cDevice(self.path, self.address)
        self.accel_lsb_per_g = {0: 16384.0, 1: 8192.0, 2: 4096.0, 3: 2048.0}[self.accel_full_scale]
        self.gyro_lsb_per_dps = {0: 131.0, 1: 65.5, 2: 32.8, 3: 16.4}[self.gyro_full_scale]

    def _reg_read_retry(self, register: int, *, attempts: int, delay_s: float) -> int:
        last_exc: Optional[Exception] = None
        for attempt in range(max(1, int(attempts))):
            try:
                return self.device.read_reg(register)
            except Exception as exc:
                last_exc = exc
                if attempt + 1 < attempts:
                    time.sleep(max(0.0, delay_s))
        if last_exc is None:
            raise OSError(f"Failed to read MPU-6050 register 0x{register:02x}")
        raise last_exc

    def _reg_write_retry(self, register: int, value: int, *, attempts: int, delay_s: float) -> None:
        last_exc: Optional[Exception] = None
        for attempt in range(max(1, int(attempts))):
            try:
                self.device.write_reg(register, value)
                return
            except Exception as exc:
                last_exc = exc
                if attempt + 1 < attempts:
                    time.sleep(max(0.0, delay_s))
        if last_exc is None:
            raise OSError(f"Failed to write MPU-6050 register 0x{register:02x}")
        raise last_exc

    def _configure_nondefault_registers(self) -> None:
        # Only touch non-default settings so flaky buses incur the fewest writes.
        writes: list[tuple[int, int, str]] = []
        if self.dlpf_config != 0:
            writes.append((MPU6050_REG_CONFIG, self.dlpf_config, "CONFIG"))
        if self.sample_rate_divider != 0:
            writes.append((MPU6050_REG_SMPLRT_DIV, self.sample_rate_divider, "SMPLRT_DIV"))
        if self.gyro_full_scale != 0:
            writes.append((MPU6050_REG_GYRO_CONFIG, self.gyro_full_scale << 3, "GYRO_CONFIG"))
        if self.accel_full_scale != 0:
            writes.append((MPU6050_REG_ACCEL_CONFIG, self.accel_full_scale << 3, "ACCEL_CONFIG"))

        for register, value, label in writes:
            try:
                self._reg_write_retry(register, value, attempts=max(2, self.read_retries), delay_s=max(0.02, self.retry_backoff_seconds))
            except Exception as exc:
                logging.warning("MPU-6050 init: leaving %s at default after write failure: %s", label, exc)
                continue
            time.sleep(0.01)

    def open(self) -> None:
        last_exc: Optional[Exception] = None
        init_attempts = max(2, self.read_retries + 1)
        for attempt in range(init_attempts):
            self.device.close()
            try:
                self.device.open()
                if self.minimal_init:
                    self._reg_write_retry(
                        MPU6050_REG_PWR_MGMT_1,
                        0x00,
                        attempts=max(3, self.read_retries + 1),
                        delay_s=max(0.05, self.retry_backoff_seconds),
                    )
                    time.sleep(0.12)
                    return

                who_am_i = self._reg_read_retry(
                    MPU6050_REG_WHO_AM_I,
                    attempts=max(2, self.read_retries),
                    delay_s=max(0.02, self.retry_backoff_seconds),
                )
                if who_am_i not in (0x68, 0x69):
                    raise OSError(f"Unexpected MPU-6050 WHO_AM_I value 0x{who_am_i:02x}")

                time.sleep(0.05)
                try:
                    power_state = self._reg_read_retry(
                        MPU6050_REG_PWR_MGMT_1,
                        attempts=2,
                        delay_s=max(0.02, self.retry_backoff_seconds),
                    )
                except Exception as exc:
                    logging.warning("MPU-6050 init: could not read PWR_MGMT_1 before wake; assuming sleep: %s", exc)
                    power_state = 0x40

                if power_state & 0x40:
                    self._reg_write_retry(
                        MPU6050_REG_PWR_MGMT_1,
                        0x00,
                        attempts=max(3, self.read_retries + 1),
                        delay_s=max(0.05, self.retry_backoff_seconds),
                    )
                    time.sleep(0.12)

                self._configure_nondefault_registers()
                return
            except Exception as exc:
                last_exc = exc
                self.device.close()
                if attempt + 1 < init_attempts:
                    time.sleep(max(0.1, self.retry_backoff_seconds * 2.0))
        if last_exc is None:
            raise OSError("MPU-6050 open failed without a recorded exception")
        raise last_exc

    def close(self) -> None:
        self.device.close()

    def read_raw(self) -> RawImuReading:
        last_exc: Optional[Exception] = None
        for attempt in range(self.read_retries):
            try:
                block = self.device.read_block(MPU6050_REG_ACCEL_XOUT_H, 14)
                if len(block) != 14:
                    raise OSError(f"Short MPU-6050 read: got {len(block)} bytes")
                return RawImuReading(
                    monotonic_s=time.monotonic(),
                    epoch_ms=int(time.time() * 1000.0),
                    accel_raw=(
                        _int16(block[0], block[1]),
                        _int16(block[2], block[3]),
                        _int16(block[4], block[5]),
                    ),
                    temp_raw=_int16(block[6], block[7]),
                    gyro_raw=(
                        _int16(block[8], block[9]),
                        _int16(block[10], block[11]),
                        _int16(block[12], block[13]),
                    ),
                )
            except Exception as exc:
                last_exc = exc
                if attempt + 1 >= self.read_retries:
                    break
                if self.retry_backoff_seconds > 0.0:
                    time.sleep(self.retry_backoff_seconds)
        assert last_exc is not None
        raise last_exc


class ImuProcessor:
    def __init__(
        self,
        *,
        accel_lsb_per_g: float,
        gyro_lsb_per_dps: float,
        accel_alpha: float,
        gyro_alpha: float,
        orientation_alpha: float,
    ) -> None:
        self.accel_lsb_per_g = float(accel_lsb_per_g)
        self.gyro_lsb_per_dps = float(gyro_lsb_per_dps)
        self.accel_alpha = _clamp(float(accel_alpha), 0.0, 1.0)
        self.gyro_alpha = _clamp(float(gyro_alpha), 0.0, 1.0)
        self.orientation_alpha = _clamp(float(orientation_alpha), 0.0, 1.0)
        self.seq = 0
        self.last_monotonic_s: Optional[float] = None
        self.filtered_accel_g: Optional[tuple[float, float, float]] = None
        self.filtered_gyro_dps: Optional[tuple[float, float, float]] = None
        self.roll_deg: Optional[float] = None
        self.pitch_deg: Optional[float] = None
        self.gyro_bias_raw = (0.0, 0.0, 0.0)

    def set_gyro_bias_raw(self, bias_raw: tuple[float, float, float]) -> None:
        self.gyro_bias_raw = tuple(float(value) for value in bias_raw)

    def reset(self) -> None:
        self.seq = 0
        self.last_monotonic_s = None
        self.filtered_accel_g = None
        self.filtered_gyro_dps = None
        self.roll_deg = None
        self.pitch_deg = None

    def update(self, reading: RawImuReading) -> ImuProcessedSample:
        self.seq += 1
        dt_s = 0.0
        if self.last_monotonic_s is not None:
            dt_s = max(0.0, reading.monotonic_s - self.last_monotonic_s)
        self.last_monotonic_s = reading.monotonic_s

        accel_g_raw = tuple(float(value) / self.accel_lsb_per_g for value in reading.accel_raw)
        gyro_dps_raw = tuple(
            (float(reading.gyro_raw[i]) - self.gyro_bias_raw[i]) / self.gyro_lsb_per_dps
            for i in range(3)
        )

        self.filtered_accel_g = _vector_lpf(self.filtered_accel_g, accel_g_raw, self.accel_alpha)
        self.filtered_gyro_dps = _vector_lpf(self.filtered_gyro_dps, gyro_dps_raw, self.gyro_alpha)
        assert self.filtered_accel_g is not None
        assert self.filtered_gyro_dps is not None

        accel_norm_g = math.sqrt(sum(axis * axis for axis in self.filtered_accel_g))
        ax, ay, az = self.filtered_accel_g
        roll_acc_deg = math.degrees(math.atan2(ay, az if abs(az) > 1e-9 else 1e-9))
        pitch_acc_deg = math.degrees(math.atan2(-ax, math.sqrt((ay * ay) + (az * az))))

        if self.roll_deg is None or self.pitch_deg is None:
            self.roll_deg = roll_acc_deg
            self.pitch_deg = pitch_acc_deg
        else:
            roll_gyro_deg = self.roll_deg + (self.filtered_gyro_dps[0] * dt_s)
            pitch_gyro_deg = self.pitch_deg + (self.filtered_gyro_dps[1] * dt_s)
            if accel_norm_g < 0.25:
                blend = 1.0
            else:
                blend = self.orientation_alpha
            self.roll_deg = (blend * roll_gyro_deg) + ((1.0 - blend) * roll_acc_deg)
            self.pitch_deg = (blend * pitch_gyro_deg) + ((1.0 - blend) * pitch_acc_deg)

        accel_mps2 = tuple(axis * GRAVITY_MPS2 for axis in self.filtered_accel_g)
        temp_c = (float(reading.temp_raw) / 340.0) + 36.53

        return ImuProcessedSample(
            seq=self.seq,
            epoch_ms=reading.epoch_ms,
            monotonic_s=reading.monotonic_s,
            dt_s=dt_s,
            accel_g=self.filtered_accel_g,
            accel_mps2=accel_mps2,
            gyro_dps=self.filtered_gyro_dps,
            temp_c=temp_c,
            roll_deg=float(self.roll_deg),
            pitch_deg=float(self.pitch_deg),
            accel_norm_g=accel_norm_g,
        )


class ImuDaemon:
    def __init__(self, config: dict[str, Any], config_path: Path) -> None:
        self.config = config
        self.config_path = config_path
        self.identity = identity_from_config(config)
        self.service_cfg = service_cfg(config, "imu_daemon")
        if not self.service_cfg.get("enabled", True):
            raise SystemExit("imu_daemon is disabled in config.")

        topics_cfg = self.service_cfg.get("topics") if isinstance(self.service_cfg.get("topics"), dict) else {}
        self.high_rate_topic = str(topics_cfg.get("high_rate") or self.identity.topic("outgoing", "sensors/imu-fast"))
        self.low_rate_topic = str(topics_cfg.get("low_rate") or self.identity.topic("outgoing", "sensors/imu"))

        publish_cfg = self.service_cfg.get("publish") if isinstance(self.service_cfg.get("publish"), dict) else {}
        self.high_rate_hz = max(0.0, _parse_float(publish_cfg.get("high_rate_hz"), 50.0))
        self.low_rate_hz = max(0.0, _parse_float(publish_cfg.get("low_rate_hz"), 5.0))
        self.read_hz = max(1.0, _parse_float(publish_cfg.get("read_hz"), max(self.high_rate_hz, self.low_rate_hz, 50.0)))
        self.high_rate_qos = max(0, min(2, _parse_int(publish_cfg.get("high_rate_qos"), 0)))
        self.low_rate_qos = max(0, min(2, _parse_int(publish_cfg.get("low_rate_qos"), 1)))
        self.high_rate_retain = bool(publish_cfg.get("high_rate_retain", False))
        self.low_rate_retain = bool(publish_cfg.get("low_rate_retain", False))

        device_cfg = self.service_cfg.get("device") if isinstance(self.service_cfg.get("device"), dict) else {}
        self.device_path = str(device_cfg.get("path") or "/dev/i2c-2")
        self.device_address = _parse_int(device_cfg.get("address"), 0x68)
        self.sensor_read_retries = max(1, _parse_int(device_cfg.get("read_retries"), 4))
        self.sensor_retry_backoff_seconds = max(0.0, _parse_float(device_cfg.get("retry_backoff_seconds"), 0.01))
        self.reopen_backoff_seconds = max(0.1, _parse_float(device_cfg.get("reopen_backoff_seconds"), 1.0))
        self.reset_after_consecutive_errors = max(1, _parse_int(device_cfg.get("reset_after_consecutive_errors"), 5))

        mpu_cfg = self.service_cfg.get("mpu6050") if isinstance(self.service_cfg.get("mpu6050"), dict) else {}
        self.minimal_init = bool(mpu_cfg.get("minimal_init", False))
        self.dlpf_config = max(0, min(6, _parse_int(mpu_cfg.get("dlpf_config"), 3)))
        self.sample_rate_divider = max(0, min(255, _parse_int(mpu_cfg.get("sample_rate_divider"), 19)))
        self.gyro_full_scale = max(0, min(3, _parse_int(mpu_cfg.get("gyro_full_scale"), 0)))
        self.accel_full_scale = max(0, min(3, _parse_int(mpu_cfg.get("accel_full_scale"), 0)))

        calibration_cfg = self.service_cfg.get("calibration") if isinstance(self.service_cfg.get("calibration"), dict) else {}
        self.calibration_samples = max(0, _parse_int(calibration_cfg.get("startup_samples"), 100))
        self.calibration_timeout_seconds = max(0.0, _parse_float(calibration_cfg.get("timeout_seconds"), 5.0))
        self.calibration_sample_delay_seconds = max(0.0, _parse_float(calibration_cfg.get("sample_delay_seconds"), 0.01))

        filter_cfg = self.service_cfg.get("filter") if isinstance(self.service_cfg.get("filter"), dict) else {}
        self.processor = ImuProcessor(
            accel_lsb_per_g={0: 16384.0, 1: 8192.0, 2: 4096.0, 3: 2048.0}[self.accel_full_scale],
            gyro_lsb_per_dps={0: 131.0, 1: 65.5, 2: 32.8, 3: 16.4}[self.gyro_full_scale],
            accel_alpha=_parse_float(filter_cfg.get("accel_low_pass_alpha"), 0.25),
            gyro_alpha=_parse_float(filter_cfg.get("gyro_low_pass_alpha"), 0.25),
            orientation_alpha=_parse_float(filter_cfg.get("orientation_alpha"), 0.98),
        )

        self.local_mqtt_cfg = config.get("local_mqtt") if isinstance(config.get("local_mqtt"), dict) else {}
        self.local_host = str(self.local_mqtt_cfg.get("host") or "127.0.0.1")
        self.local_port = _parse_int(self.local_mqtt_cfg.get("port"), 1883)
        self.local_keepalive = _parse_int(self.local_mqtt_cfg.get("keepalive"), 60)

        self.stop_event = threading.Event()
        self.reconnect_lock = threading.Lock()
        self.client: Optional[mqtt.Client] = None
        self.reconnect_thread: Optional[threading.Thread] = None
        self.sensor: Optional[Mpu6050Sensor] = None

        self.next_read_at = 0.0
        self.next_reopen_at = 0.0
        self.last_high_publish_at = 0.0
        self.last_low_publish_at = 0.0
        self.last_sample: Optional[ImuProcessedSample] = None
        self.last_low_payload: Optional[dict[str, Any]] = None

        self.samples_ok = 0
        self.samples_error = 0
        self.consecutive_errors = 0
        self.reinitializations = 0
        self.calibrated_sample_count = 0

    def _build_sensor(self) -> Mpu6050Sensor:
        return Mpu6050Sensor(
            self.device_path,
            self.device_address,
            minimal_init=self.minimal_init,
            dlpf_config=self.dlpf_config,
            sample_rate_divider=self.sample_rate_divider,
            gyro_full_scale=self.gyro_full_scale,
            accel_full_scale=self.accel_full_scale,
            read_retries=self.sensor_read_retries,
            retry_backoff_seconds=self.sensor_retry_backoff_seconds,
        )

    def start(self) -> None:
        self._connect_local_mqtt()
        while not self.stop_event.is_set():
            now = time.monotonic()
            if not self._ensure_sensor_ready(now):
                self.stop_event.wait(0.1)
                continue
            if now < self.next_read_at:
                self.stop_event.wait(min(0.05, self.next_read_at - now))
                continue
            self.next_read_at = now + (1.0 / self.read_hz)
            try:
                assert self.sensor is not None
                processed = self.processor.update(self.sensor.read_raw())
            except Exception as exc:
                self._handle_sensor_error(exc)
                continue
            self.samples_ok += 1
            self.consecutive_errors = 0
            self.last_sample = processed
            self._handle_processed_sample(processed, now=now)

    def stop(self) -> None:
        self.stop_event.set()
        self._close_sensor()
        client = self.client
        self.client = None
        if client is not None:
            try:
                client.loop_stop()
                client.disconnect()
            except Exception:
                logging.debug("MQTT disconnect failed", exc_info=True)

    def _connect_local_mqtt(self) -> None:
        client = mqtt.Client()
        mqtt_auth_and_tls(client, self.local_mqtt_cfg, self.config_path.parent)
        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        self.client = client

        delay = 2.0
        while not self.stop_event.is_set():
            try:
                client.connect(self.local_host, self.local_port, self.local_keepalive)
                client.loop_start()
                return
            except OSError as exc:
                logging.warning("Local MQTT connection failed: %s", exc)
                self.stop_event.wait(delay)
                delay = min(delay * 2.0, 60.0)

    def _on_connect(self, _client: mqtt.Client, _userdata: Any, _flags: dict[str, Any], rc: int) -> None:
        if rc != 0:
            logging.error("Local MQTT connect failed rc=%s", rc)
            return
        logging.info("Local MQTT connected for IMU publish: %s / %s", self.high_rate_topic, self.low_rate_topic)
        if self.last_low_payload is not None:
            self._publish_json(self.low_rate_topic, self.last_low_payload, qos=self.low_rate_qos, retain=self.low_rate_retain)

    def _on_disconnect(self, _client: mqtt.Client, _userdata: Any, rc: int) -> None:
        if rc != mqtt.MQTT_ERR_SUCCESS:
            logging.warning("Unexpected local MQTT disconnect rc=%s", rc)
            self._start_reconnect_thread()

    def _start_reconnect_thread(self) -> None:
        if self.stop_event.is_set():
            return
        with self.reconnect_lock:
            if self.reconnect_thread and self.reconnect_thread.is_alive():
                return
            thread = threading.Thread(target=self._reconnect_loop, name="imu-daemon-reconnect", daemon=True)
            thread.start()
            self.reconnect_thread = thread

    def _reconnect_loop(self) -> None:
        delay = 2.0
        while not self.stop_event.is_set():
            client = self.client
            if client is None:
                return
            is_connected = getattr(client, "is_connected", None)
            if callable(is_connected) and is_connected():
                return
            try:
                client.connect(self.local_host, self.local_port, self.local_keepalive)
                return
            except OSError as exc:
                logging.warning("IMU MQTT reconnect failed: %s", exc)
                self.stop_event.wait(delay)
                delay = min(delay * 2.0, 60.0)

    def _publish_json(self, topic: str, payload: dict[str, Any], *, qos: int, retain: bool) -> None:
        client = self.client
        if client is None:
            return
        is_connected = getattr(client, "is_connected", None)
        if callable(is_connected) and not is_connected():
            return
        client.publish(topic, json.dumps(payload), qos=qos, retain=retain)

    def _close_sensor(self) -> None:
        sensor = self.sensor
        self.sensor = None
        if sensor is None:
            return
        try:
            sensor.close()
        except Exception:
            logging.debug("Failed closing IMU sensor", exc_info=True)

    def _ensure_sensor_ready(self, now: float) -> bool:
        if self.sensor is not None:
            return True
        if now < self.next_reopen_at:
            return False
        sensor: Optional[Mpu6050Sensor] = None
        try:
            sensor = self._build_sensor()
            sensor.open()
            self.sensor = sensor
            self.reinitializations += 1
            self._run_startup_calibration(sensor)
            self.next_read_at = now
            logging.info("IMU ready on %s addr=0x%02x", self.device_path, self.device_address)
            return True
        except Exception as exc:
            if sensor is not None:
                try:
                    sensor.close()
                except Exception:
                    logging.debug("Failed closing IMU sensor after init error", exc_info=True)
            self._close_sensor()
            self.next_reopen_at = now + self.reopen_backoff_seconds
            logging.warning("IMU init failed; retrying in %.2fs: %s", self.reopen_backoff_seconds, exc)
            return False

    def _run_startup_calibration(self, sensor: Mpu6050Sensor) -> None:
        self.processor.reset()
        self.calibrated_sample_count = 0
        if self.calibration_samples <= 0:
            self.processor.set_gyro_bias_raw((0.0, 0.0, 0.0))
            return

        deadline = time.monotonic() + self.calibration_timeout_seconds
        gyro_totals = [0.0, 0.0, 0.0]
        samples = 0
        while samples < self.calibration_samples and time.monotonic() < deadline and not self.stop_event.is_set():
            try:
                reading = sensor.read_raw()
            except Exception as exc:
                logging.debug("IMU calibration read failed: %s", exc)
                if self.calibration_sample_delay_seconds > 0.0:
                    self.stop_event.wait(self.calibration_sample_delay_seconds)
                continue
            for i in range(3):
                gyro_totals[i] += float(reading.gyro_raw[i])
            samples += 1
            if self.calibration_sample_delay_seconds > 0.0:
                self.stop_event.wait(self.calibration_sample_delay_seconds)

        self.calibrated_sample_count = samples
        if samples <= 0:
            logging.warning("IMU calibration skipped; no stable startup samples")
            self.processor.set_gyro_bias_raw((0.0, 0.0, 0.0))
            return

        bias = tuple(total / float(samples) for total in gyro_totals)
        self.processor.set_gyro_bias_raw(bias)
        logging.info(
            "IMU gyro bias calibrated from %d samples: x=%.1f y=%.1f z=%.1f raw",
            samples,
            bias[0],
            bias[1],
            bias[2],
        )

    def _handle_sensor_error(self, exc: Exception) -> None:
        self.samples_error += 1
        self.consecutive_errors += 1
        logging.warning("IMU read failed (%d consecutive): %s", self.consecutive_errors, exc)
        if self.consecutive_errors < self.reset_after_consecutive_errors:
            return
        logging.warning("IMU exceeded error threshold; reinitializing sensor")
        self._close_sensor()
        self.processor.reset()
        self.next_reopen_at = time.monotonic() + self.reopen_backoff_seconds

    def _build_sample_payload(self, sample: ImuProcessedSample) -> dict[str, Any]:
        return {
            "t": int(sample.epoch_ms),
            "seq": int(sample.seq),
            "dt": round(float(sample.dt_s), 4),
            "value": {
                "accel_mps2": _vector_payload(sample.accel_mps2),
                "gyro_dps": _vector_payload(sample.gyro_dps),
                "temp_c": round(float(sample.temp_c), 3),
                "orientation_deg": {
                    "roll": round(float(sample.roll_deg), 3),
                    "pitch": round(float(sample.pitch_deg), 3),
                },
            },
        }

    def _build_low_rate_payload(self, sample: ImuProcessedSample) -> dict[str, Any]:
        bias_dps = tuple(value / self.processor.gyro_lsb_per_dps for value in self.processor.gyro_bias_raw)
        return {
            "t": int(sample.epoch_ms),
            "seq": int(sample.seq),
            "value": {
                "accel_mps2": _vector_payload(sample.accel_mps2),
                "gyro_dps": _vector_payload(sample.gyro_dps),
                "temp_c": round(float(sample.temp_c), 3),
                "orientation_deg": {
                    "roll": round(float(sample.roll_deg), 3),
                    "pitch": round(float(sample.pitch_deg), 3),
                },
                "accel_norm_g": round(float(sample.accel_norm_g), 4),
                "gyro_bias_dps": _vector_payload(bias_dps),
                "health": {
                    "device": self.device_path,
                    "address": f"0x{self.device_address:02x}",
                    "samples_ok": int(self.samples_ok),
                    "samples_error": int(self.samples_error),
                    "consecutive_errors": int(self.consecutive_errors),
                    "reinitializations": int(self.reinitializations),
                    "calibration_samples": int(self.calibrated_sample_count),
                },
            },
        }

    def _handle_processed_sample(self, sample: ImuProcessedSample, *, now: Optional[float] = None) -> None:
        publish_now = time.monotonic() if now is None else float(now)
        if self.high_rate_hz > 0.0:
            high_interval = 1.0 / self.high_rate_hz
            if self.last_high_publish_at <= 0.0 or (publish_now - self.last_high_publish_at) >= high_interval:
                self.last_high_publish_at = publish_now
                self._publish_json(
                    self.high_rate_topic,
                    self._build_sample_payload(sample),
                    qos=self.high_rate_qos,
                    retain=self.high_rate_retain,
                )

        if self.low_rate_hz > 0.0:
            low_interval = 1.0 / self.low_rate_hz
            if self.last_low_publish_at <= 0.0 or (publish_now - self.last_low_publish_at) >= low_interval:
                self.last_low_publish_at = publish_now
                payload = self._build_low_rate_payload(sample)
                self.last_low_payload = payload
                self._publish_json(self.low_rate_topic, payload, qos=self.low_rate_qos, retain=self.low_rate_retain)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read a local MPU-6050 IMU and publish MQTT telemetry.")
    parser.add_argument("--config", help="Path to runtime config JSON.")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    config, config_path = load_config(args.config)
    daemon_cfg = service_cfg(config, "imu_daemon")
    logging.basicConfig(
        level=getattr(logging, log_level(config, daemon_cfg), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    daemon = ImuDaemon(config, config_path)

    def _shutdown(_signum: int, _frame: Any) -> None:
        daemon.stop()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    try:
        daemon.start()
    finally:
        daemon.stop()


if __name__ == "__main__":
    main()
