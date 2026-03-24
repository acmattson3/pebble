#!/usr/bin/env python3
"""Print currently visible AprilTag/Aruco IDs from the shared video stream."""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, Optional

import cv2


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = REPO_ROOT / "control" / "configs" / "config.json"


def _load_runtime_config(path_value: str) -> tuple[dict[str, Any], Path]:
    cfg_path = Path(path_value).expanduser() if path_value else DEFAULT_CONFIG_PATH
    if not cfg_path.is_absolute():
        cfg_path = (Path.cwd() / cfg_path).resolve()
    if not cfg_path.exists():
        raise RuntimeError(f"Missing config file: {cfg_path}")
    try:
        data = json.loads(cfg_path.read_text())
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Failed to parse config file {cfg_path}: {exc}") from exc
    if not isinstance(data, dict):
        raise RuntimeError("Config root must be a JSON object")
    return data, cfg_path


def _extract_rotation(transform_config: Dict[str, Any]) -> Optional[int]:
    rotate = transform_config.get("rotate_degrees")
    if rotate is None:
        return None
    try:
        value = int(rotate) % 360
    except (TypeError, ValueError):
        return None
    if value in (90, 180, 270):
        return value
    return None


def _apply_rotation(frame, rotation: Optional[int]):
    if rotation is None:
        return frame
    if rotation == 90:
        return cv2.rotate(frame, cv2.ROTATE_90_CLOCKWISE)
    if rotation == 180:
        return cv2.rotate(frame, cv2.ROTATE_180)
    if rotation == 270:
        return cv2.rotate(frame, cv2.ROTATE_90_COUNTERCLOCKWISE)
    return frame


def _open_shared_capture(socket_path: str, width: int, height: int, fps: float):
    pipeline = (
        f"shmsrc socket-path={socket_path} is-live=true do-timestamp=true ! "
        f"video/x-raw,format=BGR,width={int(width)},height={int(height)},framerate={int(max(1.0, fps))}/1 ! "
        "queue leaky=downstream max-size-buffers=2 ! "
        "videoconvert ! "
        "appsink drop=true max-buffers=1 sync=false"
    )
    capture = cv2.VideoCapture(pipeline, cv2.CAP_GSTREAMER)
    if not capture.isOpened():
        capture.release()
        return None
    return capture


def _create_detector_params():
    if hasattr(cv2.aruco, "DetectorParameters_create"):
        return cv2.aruco.DetectorParameters_create()
    return cv2.aruco.DetectorParameters()


def build_dictionary(name: str):
    clean = name.strip().upper()
    if not clean.startswith("DICT_"):
        clean = "DICT_" + clean
    if not hasattr(cv2.aruco, clean):
        raise ValueError(f"Unknown dictionary: {clean}")
    return cv2.aruco.getPredefinedDictionary(getattr(cv2.aruco, clean))


def detect_markers(frame, dictionary, detector_params):
    # Use legacy API on SBCs for better compatibility.
    if len(frame.shape) == 3 and frame.shape[2] == 3:
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
    else:
        gray = frame
    return cv2.aruco.detectMarkers(gray, dictionary, parameters=detector_params)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Print visible marker IDs from live shared video.")
    parser.add_argument("--config", type=str, default="", help="Unified runtime config path.")
    parser.add_argument("--video-shm", type=str, default="", help="Override shared video socket path.")
    parser.add_argument("--width", type=int, default=0, help="Override stream width.")
    parser.add_argument("--height", type=int, default=0, help="Override stream height.")
    parser.add_argument("--fps", type=float, default=0.0, help="Override stream fps.")
    parser.add_argument(
        "--marker-dictionary",
        type=str,
        default="APRILTAG_25H9",
        help="ArUco/AprilTag dictionary name.",
    )
    parser.add_argument("--print-hz", type=float, default=4.0, help="Console print rate.")
    parser.add_argument("--preview", action="store_true", help="Show preview with marker outlines.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    try:
        runtime_cfg, _cfg_path = _load_runtime_config(args.config)
    except RuntimeError as exc:
        print(exc, file=sys.stderr)
        return 1

    services_cfg = runtime_cfg.get("services") if isinstance(runtime_cfg.get("services"), dict) else {}
    av_cfg = services_cfg.get("av_daemon") if isinstance(services_cfg.get("av_daemon"), dict) else {}
    av_video_cfg = av_cfg.get("video") if isinstance(av_cfg.get("video"), dict) else {}

    socket_path = str(args.video_shm or av_video_cfg.get("socket_path") or "/tmp/pebble-video.sock")
    width = int(args.width if args.width > 0 else (av_video_cfg.get("width") or 640))
    height = int(args.height if args.height > 0 else (av_video_cfg.get("height") or 480))
    fps = float(args.fps if args.fps > 0 else (av_video_cfg.get("fps") or 15.0))
    retry_seconds = float(av_video_cfg.get("reconnect_seconds") or av_video_cfg.get("input_retry_seconds") or 2.0)
    rotation = _extract_rotation({"rotate_degrees": av_video_cfg.get("rotate_degrees")})

    try:
        dictionary = build_dictionary(args.marker_dictionary)
    except ValueError as exc:
        print(exc, file=sys.stderr)
        return 1

    detector_params = _create_detector_params()
    interval = 1.0 / max(0.5, float(args.print_hz))
    next_print = 0.0

    print(
        f"Live marker monitor started: shm={socket_path} size={width}x{height} fps={fps} "
        f"dict={args.marker_dictionary}"
    )

    capture = None
    try:
        while True:
            if capture is None:
                capture = _open_shared_capture(socket_path, width, height, fps)
                if capture is None:
                    print(f"Shared video unavailable at {socket_path}; retrying in {retry_seconds:.1f}s", file=sys.stderr)
                    time.sleep(max(0.2, retry_seconds))
                    continue

            ok, frame = capture.read()
            if not ok:
                print("Frame read failed; reopening shared stream.", file=sys.stderr)
                capture.release()
                capture = None
                time.sleep(0.2)
                continue

            frame = _apply_rotation(frame, rotation)
            corners, ids, _rejected = detect_markers(frame, dictionary, detector_params)

            now = time.time()
            if now >= next_print:
                if ids is None or len(ids) == 0:
                    print(f"{time.strftime('%H:%M:%S')} visible_ids=[]")
                else:
                    values = sorted(int(v) for v in ids.flatten().tolist())
                    print(f"{time.strftime('%H:%M:%S')} visible_ids={values}")
                next_print = now + interval

            if args.preview:
                display = frame
                if ids is not None and len(ids) > 0:
                    cv2.aruco.drawDetectedMarkers(display, corners, ids)
                cv2.imshow("live-marker-ids", display)
                key = cv2.waitKey(1) & 0xFF
                if key in (27, ord("q")):
                    break
            else:
                # Prevent maxing one CPU core when preview is disabled.
                time.sleep(0.005)

    except KeyboardInterrupt:
        pass
    finally:
        if capture is not None:
            capture.release()
        if args.preview:
            cv2.destroyAllWindows()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
