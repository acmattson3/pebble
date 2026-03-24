#!/usr/bin/env python3
import argparse
import os
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


def parse_args():
    parser = argparse.ArgumentParser(
        description="Calibrate a shared video stream using a single ArUco/AprilTag marker."
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
        help="ArUco/AprilTag dictionary name (e.g., APRILTAG_25H9, ARUCO_ORIGINAL)",
    )
    parser.add_argument("--marker-id", type=int, default=0, help="Marker ID to detect")
    parser.add_argument(
        "--marker-size",
        type=float,
        required=True,
        help="Marker side length in meters",
    )
    parser.add_argument(
        "--samples",
        type=int,
        default=30,
        help="Number of calibration samples to collect (0 = all images in input dir)",
    )
    parser.add_argument(
        "--min-marker-area-fraction",
        type=float,
        default=0.01,
        help="Minimum marker area as a fraction of the image area",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="camera_calibration.yaml",
        help="Output YAML path",
    )
    parser.add_argument(
        "--preview",
        action="store_true",
        help="Show live preview window",
    )
    parser.add_argument(
        "--input-dir",
        type=str,
        default="",
        help="Directory of images to calibrate from instead of live capture",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Print debug messages",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    try:
        dictionary = build_dictionary(args.marker_dictionary)
    except ValueError as exc:
        print(exc, file=sys.stderr)
        return 1

    detector_params = cv2.aruco.DetectorParameters()

    if not args.input_dir and args.samples <= 0:
        print("--samples must be > 0 for live capture", file=sys.stderr)
        return 1

    object_points = []
    image_points = []

    marker_size = float(args.marker_size)
    half = marker_size / 2.0
    objp = np.array(
        [
            [-half, half, 0.0],
            [half, half, 0.0],
            [half, -half, 0.0],
            [-half, -half, 0.0],
        ],
        dtype=np.float32,
    )

    if args.preview:
        cv2.namedWindow("Calibration", cv2.WINDOW_NORMAL)

    print("\nCalibration guidance:")
    print("- Capture samples at different distances, angles, and positions")
    print("- Avoid glare and motion blur; keep all four corners visible")
    print("- Collect a few dozen good samples for reliable calibration\n")

    last_frame_shape = None

    def process_frame(frame, allow_capture):
        nonlocal last_frame_shape

        h, w = frame.shape[:2]
        last_frame_shape = (w, h)

        corners, ids, _ = detect_markers(frame, dictionary, detector_params)

        valid_marker = False
        marker_corners = None
        area_fraction = 0.0

        if ids is not None:
            ids_flat = ids.flatten().tolist()
            if args.marker_id in ids_flat:
                idx = ids_flat.index(args.marker_id)
                candidate = corners[idx]
                if candidate.shape[1] == 4:
                    marker_corners = candidate.reshape(4, 2)
                    area = cv2.contourArea(marker_corners.astype(np.float32))
                    area_fraction = area / float(w * h)
                    valid_marker = area_fraction >= args.min_marker_area_fraction

        if args.preview:
            preview = frame.copy()
            if ids is not None and len(corners) > 0:
                cv2.aruco.drawDetectedMarkers(preview, corners, ids)
            status_total = args.samples if args.samples > 0 else "-"
            status = f"Samples: {len(object_points)}/{status_total}"
            if marker_corners is not None:
                status += f" | Area: {area_fraction:.4f}"
            cv2.putText(
                preview,
                status,
                (10, 30),
                cv2.FONT_HERSHEY_SIMPLEX,
                0.8,
                (0, 255, 0),
                2,
            )
            if allow_capture:
                hint = "Space: capture | q/esc: quit"
            else:
                hint = "q/esc: quit"
            cv2.putText(
                preview,
                hint,
                (10, 60),
                cv2.FONT_HERSHEY_SIMPLEX,
                0.6,
                (0, 255, 0),
                1,
            )
            cv2.imshow("Calibration", preview)

        return marker_corners, valid_marker, area_fraction

    if args.input_dir:
        if not os.path.isdir(args.input_dir):
            print(f"Input directory not found: {args.input_dir}", file=sys.stderr)
            return 1
        extensions = {".jpg", ".jpeg", ".png", ".bmp", ".tif", ".tiff"}
        files = [
            os.path.join(args.input_dir, name)
            for name in sorted(os.listdir(args.input_dir))
            if os.path.splitext(name)[1].lower() in extensions
        ]
        if not files:
            print("No images found in input directory", file=sys.stderr)
            return 1
        target_samples = args.samples if args.samples > 0 else None
        for path in files:
            frame = cv2.imread(path)
            if frame is None:
                if args.debug:
                    print(f"Failed to read image {path}")
                continue
            if last_frame_shape is not None and frame.shape[1::-1] != last_frame_shape:
                if args.debug:
                    print(f"Skipping {path}: size mismatch")
                continue
            marker_corners, valid_marker, area_fraction = process_frame(frame, False)
            if marker_corners is None:
                if args.debug:
                    print(f"No marker detected in {path}")
                if args.preview:
                    key = cv2.waitKey(1) & 0xFF
                    if key in (27, ord("q")):
                        break
                continue
            if not valid_marker:
                if args.debug:
                    print(
                        f"Marker too small in {path}: {area_fraction:.4f} < {args.min_marker_area_fraction:.4f}"
                    )
                if args.preview:
                    key = cv2.waitKey(1) & 0xFF
                    if key in (27, ord("q")):
                        break
                continue
            object_points.append(objp.copy())
            image_points.append(marker_corners.astype(np.float32))
            if args.debug:
                print(
                    f"Accepted {path} ({len(object_points)}/{target_samples or 'all'})"
                )
            if args.preview:
                key = cv2.waitKey(1) & 0xFF
                if key in (27, ord("q")):
                    break
            if target_samples is not None and len(object_points) >= target_samples:
                break
    else:
        cap = open_shared_capture(args.video_shm, args.width, args.height, args.fps)
        if cap is None:
            print(
                f"Failed to open shared video stream at {args.video_shm}. "
                "Ensure av_daemon is running.",
                file=sys.stderr,
            )
            return 1

        while len(object_points) < args.samples:
            ret, frame = cap.read()
            if not ret:
                print("Failed to read frame", file=sys.stderr)
                break

            marker_corners, valid_marker, area_fraction = process_frame(frame, True)

            key = cv2.waitKey(1) & 0xFF
            if key in (27, ord("q")):
                break
            if key == ord(" "):
                if marker_corners is None:
                    if args.debug:
                        print("No marker detected")
                    continue
                if not valid_marker:
                    if args.debug:
                        print(
                            f"Marker too small: {area_fraction:.4f} < {args.min_marker_area_fraction:.4f}"
                        )
                    continue
                object_points.append(objp.copy())
                image_points.append(marker_corners.astype(np.float32))
                if args.debug:
                    print(
                        f"Captured sample {len(object_points)}/{args.samples} (area {area_fraction:.4f})"
                    )
                else:
                    print(f"Captured sample {len(object_points)}/{args.samples}")
                time.sleep(0.15)

        cap.release()
    if args.preview:
        cv2.destroyAllWindows()

    if len(object_points) < 3:
        print("Not enough samples collected for calibration", file=sys.stderr)
        return 1

    image_size = last_frame_shape
    rms, camera_matrix, dist_coeffs, rvecs, tvecs = cv2.calibrateCamera(
        object_points, image_points, image_size, None, None
    )

    print(f"\nCalibration complete. Reprojection RMS error: {rms:.4f}")

    fs = cv2.FileStorage(args.output, cv2.FILE_STORAGE_WRITE)
    if not fs.isOpened():
        print(f"Failed to open output file {args.output}", file=sys.stderr)
        return 1

    fs.write("image_width", int(image_size[0]))
    fs.write("image_height", int(image_size[1]))
    fs.write("camera_matrix", camera_matrix)
    fs.write("distortion_coefficients", dist_coeffs)
    fs.write("reprojection_error", float(rms))
    fs.write("marker_dictionary", args.marker_dictionary)
    fs.write("marker_id", int(args.marker_id))
    fs.write("marker_size", float(marker_size))
    fs.write("num_samples", int(len(object_points)))
    fs.release()

    print(f"Saved calibration to: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
