#!/usr/bin/env python3
"""Local AV daemon that owns camera/microphone devices and exports shared streams."""

from __future__ import annotations

import argparse
import logging
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, Callable, Optional

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parents[2]))

from control.common.config import load_config, log_level, service_cfg


def _bool_to_gst(value: bool) -> str:
    return "true" if value else "false"


def _positive_int(value: Any, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def _gst_scalar(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _legacy_libcamera_key_to_property(key: str) -> Optional[str]:
    known = {
        "awbenable": "awb-enable",
        "aeenable": "ae-enable",
    }
    normalized = key.replace("-", "").replace("_", "").lower()
    return known.get(normalized)


def _as_bool_text(value: Any) -> Optional[str]:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return "true" if int(value) != 0 else "false"
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return "true"
    if text in {"0", "false", "no", "off"}:
        return "false"
    return None


def _libcamera_control_properties(video_cfg: dict[str, Any]) -> list[str]:
    controls_cfg = video_cfg.get("camera_controls")
    if controls_cfg is None or controls_cfg == "":
        controls_cfg = video_cfg.get("controls")

    properties: list[str] = []

    if isinstance(controls_cfg, dict):
        for key, value in controls_cfg.items():
            prop = _legacy_libcamera_key_to_property(str(key))
            if not prop:
                logging.warning("Ignoring unsupported legacy libcamera control key '%s'.", key)
                continue
            bool_text = _as_bool_text(value)
            if bool_text is None:
                logging.warning("Ignoring libcamera control '%s' with non-boolean value '%s'.", key, value)
                continue
            properties.append(f"{prop}={bool_text}")
        return properties

    text = str(controls_cfg or "").strip()
    if not text:
        return []

    # Allow explicit property pairs for advanced usage:
    # "ae-enable=true,awb-enable=true"
    for part in text.split(","):
        token = part.strip()
        if not token:
            continue
        if "=" not in token:
            logging.warning("Ignoring malformed camera_controls token '%s'. Expected key=value.", token)
            continue
        properties.append(token)
    return properties


def _rotate_filter(rotate_degrees: int) -> list[str]:
    rotate = rotate_degrees % 360
    if rotate == 0:
        return []
    method = {
        90: "clockwise",
        180: "rotate-180",
        270: "counterclockwise",
    }.get(rotate)
    if method is None:
        logging.warning("Unsupported rotate_degrees=%s; ignoring.", rotate_degrees)
        return []
    return ["videoflip", f"method={method}", "!"]


def _v4l2_raw_format(capture_format: str) -> Optional[str]:
    fmt = capture_format.strip().lower()
    if fmt in {"", "auto", "raw"}:
        return None
    if fmt == "yuyv":
        return "YUY2"
    if fmt == "mjpeg" or fmt == "mjpg" or fmt == "jpeg":
        return None
    return fmt.upper()


def _v4l2_io_mode(io_mode: Any) -> Optional[str]:
    text = str(io_mode or "").strip().lower()
    if not text:
        return None
    aliases = {
        "auto": "auto",
        "rw": "rw",
        "readwrite": "rw",
        "mmap": "mmap",
        "userptr": "userptr",
        "dmabuf": "dmabuf",
        "dmabuf-import": "dmabuf-import",
        "dmabuf_import": "dmabuf-import",
    }
    resolved = aliases.get(text)
    if resolved is None:
        logging.warning("Unsupported v4l2 io_mode=%s; ignoring.", io_mode)
    return resolved


def build_video_pipeline(video_cfg: dict[str, Any]) -> tuple[list[str], Path]:
    backend_raw = str(video_cfg.get("backend") or "v4l2").strip().lower()
    backend = {"picamera2": "libcamera", "arducam": "libcamera"}.get(backend_raw, backend_raw)
    if backend != backend_raw:
        logging.info("Mapped video backend '%s' to '%s'.", backend_raw, backend)
    width = _positive_int(video_cfg.get("width"), 640)
    height = _positive_int(video_cfg.get("height"), 480)
    fps = _positive_int(video_cfg.get("fps"), 15)
    rotate_degrees = int(video_cfg.get("rotate_degrees") or 0)
    capture_format = str(video_cfg.get("capture_format") or "auto").strip().lower()

    socket_path = Path(str(video_cfg.get("socket_path") or "/tmp/pebble-video.sock")).expanduser()
    shm_size = _positive_int(video_cfg.get("shm_size"), 16 * 1024 * 1024)
    wait_for_connection = bool(video_cfg.get("wait_for_connection", False))

    source: list[str]
    caps: str
    decode_mjpeg = False
    if backend == "v4l2":
        device = str(video_cfg.get("device") or "/dev/video0").strip()
        source = ["v4l2src", f"device={device}", "do-timestamp=true"]
        io_mode = _v4l2_io_mode(video_cfg.get("io_mode"))
        if io_mode:
            source.append(f"io-mode={io_mode}")
        if capture_format in {"mjpeg", "mjpg", "jpeg"}:
            caps = f"image/jpeg,width={width},height={height},framerate={fps}/1"
            decode_mjpeg = True
        else:
            raw_format = _v4l2_raw_format(capture_format)
            format_clause = f",format={raw_format}" if raw_format else ""
            caps = f"video/x-raw{format_clause},width={width},height={height},framerate={fps}/1"
    elif backend == "libcamera":
        source = ["libcamerasrc"]
        camera_name = str(video_cfg.get("camera_name") or "").strip()
        if camera_name:
            source.append(f"camera-name={camera_name}")
        camera_controls = _libcamera_control_properties(video_cfg)
        source.extend(camera_controls)
        caps = f"video/x-raw,width={width},height={height},framerate={fps}/1"
        if capture_format not in {"", "auto", "raw"}:
            logging.warning("capture_format=%s is ignored for libcamera backend.", capture_format)
    else:
        raise ValueError(f"Unsupported av_daemon video backend '{backend}'.")

    command: list[str] = [
        "gst-launch-1.0",
        "-q",
        "-e",
        *source,
        "!",
        caps,
        "!",
    ]
    if decode_mjpeg:
        command += ["jpegparse", "!", "jpegdec", "!"]
    command += [
        "videoconvert",
        "!",
        "video/x-raw,format=BGR",
        "!",
        *_rotate_filter(rotate_degrees),
        "queue",
        "max-size-buffers=2",
        "leaky=downstream",
        "!",
        "shmsink",
        f"socket-path={socket_path}",
        f"shm-size={shm_size}",
        f"wait-for-connection={_bool_to_gst(wait_for_connection)}",
        "sync=false",
    ]
    return command, socket_path


def build_audio_pipeline(audio_cfg: dict[str, Any]) -> tuple[list[str], Path]:
    source = str(audio_cfg.get("source") or "alsa").strip().lower()
    rate = _positive_int(audio_cfg.get("rate"), 16000)
    channels = _positive_int(audio_cfg.get("channels"), 1)

    socket_path = Path(str(audio_cfg.get("socket_path") or "/tmp/pebble-audio.sock")).expanduser()
    shm_size = _positive_int(audio_cfg.get("shm_size"), 512 * 1024)
    wait_for_connection = bool(audio_cfg.get("wait_for_connection", False))

    source_elem: list[str]
    if source == "alsa":
        source_elem = ["alsasrc"]
        device = str(audio_cfg.get("device") or "").strip()
        if device:
            source_elem.append(f"device={device}")
        source_elem.append("do-timestamp=true")
    elif source == "auto":
        source_elem = ["autoaudiosrc"]
    else:
        raise ValueError(f"Unsupported av_daemon audio source '{source}'.")

    caps = f"audio/x-raw,format=S16LE,rate={rate},channels={channels},layout=interleaved"

    command: list[str] = [
        "gst-launch-1.0",
        "-q",
        "-e",
        *source_elem,
        "!",
        caps,
        "!",
        "queue",
        "max-size-time=200000000",
        "leaky=downstream",
        "!",
        "shmsink",
        f"socket-path={socket_path}",
        f"shm-size={shm_size}",
        f"wait-for-connection={_bool_to_gst(wait_for_connection)}",
        "sync=false",
    ]
    return command, socket_path


class ManagedPipeline:
    def __init__(
        self,
        name: str,
        command_factory: Callable[[], tuple[list[str], Path]],
        restart_delay: float,
        stop_timeout: float,
        env: dict[str, str],
        cwd: Optional[str],
    ) -> None:
        self.name = name
        self.command_factory = command_factory
        self.restart_delay = max(0.1, float(restart_delay))
        self.stop_timeout = max(0.5, float(stop_timeout))
        self.env = env
        self.cwd = cwd

        self.process: Optional[subprocess.Popen] = None
        self.next_start_at = 0.0
        self.socket_path: Optional[Path] = None

    def tick(self, now: float) -> None:
        proc = self.process
        if proc is None:
            if now >= self.next_start_at:
                self._start(now)
            return

        rc = proc.poll()
        if rc is None:
            return

        logging.warning("%s pipeline exited rc=%s; scheduling restart.", self.name, rc)
        self.process = None
        self.next_start_at = now + self.restart_delay

    def stop(self) -> None:
        proc = self.process
        self.process = None
        if proc and proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=self.stop_timeout)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=self.stop_timeout)
        self._remove_socket()

    def _start(self, now: float) -> None:
        try:
            command, socket_path = self.command_factory()
        except Exception as exc:
            logging.error("Failed to build %s pipeline command: %s", self.name, exc)
            self.next_start_at = now + self.restart_delay
            return

        self.socket_path = socket_path
        self._remove_socket()

        try:
            self.process = subprocess.Popen(command, env=self.env, cwd=self.cwd)
        except OSError as exc:
            logging.error("Failed to start %s pipeline: %s", self.name, exc)
            self.process = None
            self.next_start_at = now + self.restart_delay
            return

        logging.info("Started %s pipeline pid=%s", self.name, self.process.pid)

    def _remove_socket(self) -> None:
        path = self.socket_path
        if not path:
            return
        try:
            if path.exists() or path.is_socket():
                path.unlink()
        except OSError:
            logging.debug("Failed to remove old socket %s", path, exc_info=True)


class AvDaemon:
    def __init__(self, config: dict[str, Any], config_path: Path) -> None:
        self.config = config
        self.config_path = config_path
        self.svc_cfg = service_cfg(config, "av_daemon")
        if not self.svc_cfg.get("enabled", False):
            raise SystemExit("av_daemon is disabled in config.")

        self.stop_event = threading.Event()

        restart_delay = float(self.svc_cfg.get("restart_delay_seconds") or 2.0)
        stop_timeout = float(self.svc_cfg.get("stop_timeout_seconds") or 5.0)

        env = os.environ.copy()
        env_cfg = self.svc_cfg.get("env")
        if isinstance(env_cfg, dict):
            env.update({str(k): str(v) for k, v in env_cfg.items()})

        cwd = self.svc_cfg.get("cwd")
        resolved_cwd: Optional[str] = None
        if cwd:
            path = Path(str(cwd)).expanduser()
            if not path.is_absolute():
                path = (config_path.parent / path).resolve()
            resolved_cwd = str(path)

        self.pipelines: list[ManagedPipeline] = []

        video_cfg = self.svc_cfg.get("video") if isinstance(self.svc_cfg.get("video"), dict) else {}
        if video_cfg.get("enabled", True):
            self.pipelines.append(
                ManagedPipeline(
                    name="video",
                    command_factory=lambda: build_video_pipeline(video_cfg),
                    restart_delay=restart_delay,
                    stop_timeout=stop_timeout,
                    env=env,
                    cwd=resolved_cwd,
                )
            )

        audio_cfg = self.svc_cfg.get("audio") if isinstance(self.svc_cfg.get("audio"), dict) else {}
        if audio_cfg.get("enabled", True):
            self.pipelines.append(
                ManagedPipeline(
                    name="audio",
                    command_factory=lambda: build_audio_pipeline(audio_cfg),
                    restart_delay=restart_delay,
                    stop_timeout=stop_timeout,
                    env=env,
                    cwd=resolved_cwd,
                )
            )

        if not self.pipelines:
            raise SystemExit("av_daemon has no enabled pipelines (video/audio).")

    def start(self) -> None:
        while not self.stop_event.is_set():
            now = time.monotonic()
            for pipeline in self.pipelines:
                pipeline.tick(now)
            self.stop_event.wait(0.2)

    def stop(self) -> None:
        self.stop_event.set()
        for pipeline in self.pipelines:
            pipeline.stop()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run local AV daemon pipelines.")
    parser.add_argument("--config", help="Path to runtime config JSON.")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    config, config_path = load_config(args.config)
    svc = service_cfg(config, "av_daemon")

    logging.basicConfig(
        level=getattr(logging, log_level(config, svc), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    daemon = AvDaemon(config, config_path)

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
