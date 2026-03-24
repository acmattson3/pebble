#!/usr/bin/env python3
import argparse
import sys
import time

import cv2
import numpy as np


def build_dictionary(name):
    name = name.strip().upper()
    if not name.startswith("DICT_"):
        name = "DICT_" + name
    if not hasattr(cv2.aruco, name):
        raise ValueError(f"Unknown dictionary: {name}")
    return cv2.aruco.getPredefinedDictionary(getattr(cv2.aruco, name))


def detect_markers(frame, dictionary, detector_params):
    if hasattr(cv2.aruco, "ArucoDetector"):
        detector = cv2.aruco.ArucoDetector(dictionary, detector_params)
        return detector.detectMarkers(frame)
    return cv2.aruco.detectMarkers(frame, dictionary, parameters=detector_params)


def open_shared_capture(socket_path, width, height, fps):
    pipeline = (
        f"shmsrc socket-path={socket_path} is-live=true do-timestamp=true ! "
        f"video/x-raw,format=BGR,width={int(width)},height={int(height)},framerate={int(max(1.0, fps))}/1 ! "
        "queue leaky=downstream max-size-buffers=2 ! "
        "videoconvert ! "
        "appsink drop=true max-buffers=1 sync=false"
    )
    cap = cv2.VideoCapture(pipeline, cv2.CAP_GSTREAMER)
    if not cap.isOpened():
        cap.release()
        return None
    return cap


def load_calibration(path):
    fs = cv2.FileStorage(path, cv2.FILE_STORAGE_READ)
    if not fs.isOpened():
        raise RuntimeError(f"Failed to open calibration file: {path}")
    camera_matrix = fs.getNode("camera_matrix").mat()
    dist_coeffs = fs.getNode("distortion_coefficients").mat()
    fs.release()
    if camera_matrix is None or dist_coeffs is None:
        raise RuntimeError("Calibration file missing camera_matrix or distortion_coefficients")
    return camera_matrix, dist_coeffs


def parse_args():
    parser = argparse.ArgumentParser(
        description="Report marker position from the shared video stream at 10 Hz."
    )
    parser.add_argument(
        "--video-shm",
        type=str,
        default="/tmp/pebble-video.sock",
        help="AV-daemon shared video socket path",
    )
    parser.add_argument("--width", type=int, default=640, help="Expected stream width")
    parser.add_argument("--height", type=int, default=480, help="Expected stream height")
    parser.add_argument("--fps", type=float, default=15.0, help="Expected stream FPS")
    parser.add_argument(
        "--marker-dictionary",
        type=str,
        default="APRILTAG_25H9",
        help="ArUco/AprilTag dictionary name (e.g., APRILTAG_25H9)",
    )
    parser.add_argument("--marker-id", type=int, default=0, help="Marker ID to detect")
    parser.add_argument(
        "--marker-size",
        type=float,
        required=True,
        help="Marker side length in meters",
    )
    parser.add_argument(
        "--calibration",
        type=str,
        default="camera_calibration.yaml",
        help="Path to OpenCV YAML calibration file",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    try:
        dictionary = build_dictionary(args.marker_dictionary)
    except ValueError as exc:
        print(exc, file=sys.stderr)
        return 1

    try:
        camera_matrix, dist_coeffs = load_calibration(args.calibration)
    except RuntimeError as exc:
        print(exc, file=sys.stderr)
        return 1

    cap = open_shared_capture(args.video_shm, args.width, args.height, args.fps)
    if cap is None:
        print(
            f"Failed to open shared video stream at {args.video_shm}. "
            "Ensure av_daemon is running.",
            file=sys.stderr,
        )
        return 1

    detector_params = cv2.aruco.DetectorParameters()

    target_interval = 0.1
    next_time = time.time()

    try:
        while True:
            ret, frame = cap.read()
            if not ret:
                print("Failed to read frame", file=sys.stderr)
                break

            corners, ids, _ = detect_markers(frame, dictionary, detector_params)

            x_val = float("nan")
            y_val = float("nan")
            distance = float("nan")

            if ids is not None:
                ids_flat = ids.flatten().tolist()
                if args.marker_id in ids_flat:
                    idx = ids_flat.index(args.marker_id)
                    marker_corners = corners[idx]
                    rvecs, tvecs, _ = cv2.aruco.estimatePoseSingleMarkers(
                        marker_corners, args.marker_size, camera_matrix, dist_coeffs
                    )
                    tvec = tvecs[0][0]
                    x_val = float(tvec[0])
                    y_val = float(tvec[1])
                    distance = float(np.linalg.norm(tvec))

            print(f"X: {x_val:.4f}\tY: {y_val:.4f}\tD: {distance:.4f}")

            next_time += target_interval
            sleep_time = next_time - time.time()
            if sleep_time > 0:
                time.sleep(sleep_time)
            else:
                next_time = time.time()
    except KeyboardInterrupt:
        pass
    finally:
        cap.release()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
