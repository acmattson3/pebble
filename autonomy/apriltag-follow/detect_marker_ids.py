#!/usr/bin/env python3
import argparse
import os
import sys

import cv2


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


def parse_args():
    parser = argparse.ArgumentParser(
        description="Detect ArUco/AprilTag marker IDs in images."
    )
    parser.add_argument(
        "--input-dir",
        type=str,
        default="sample_images",
        help="Directory of images to scan",
    )
    parser.add_argument(
        "--marker-dictionary",
        type=str,
        default="APRILTAG_25H9",
        help="ArUco/AprilTag dictionary name (e.g., APRILTAG_25H9)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    try:
        dictionary = build_dictionary(args.marker_dictionary)
    except ValueError as exc:
        print(exc, file=sys.stderr)
        return 1

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
        print("No images found", file=sys.stderr)
        return 1

    detector_params = cv2.aruco.DetectorParameters()

    for path in files:
        frame = cv2.imread(path)
        if frame is None:
            print(f"{path}: failed to read")
            continue
        corners, ids, _ = detect_markers(frame, dictionary, detector_params)
        if ids is None or len(ids) == 0:
            print(f"{path}: no markers")
            continue
        ids_list = ", ".join(str(val) for val in ids.flatten().tolist())
        print(f"{path}: {ids_list}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
