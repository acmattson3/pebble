#!/usr/bin/env python3
import argparse
import os
import sys
import time

import cv2


def _open_shared_capture(socket_path: str, width: int, height: int, fps: float):
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
        description="Capture periodic shared-stream images into a sample directory."
    )
    parser.add_argument(
        "--video-shm",
        type=str,
        default="/tmp/pebble-video.sock",
        help="AV-daemon shared video socket path",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="sample_images",
        help="Directory to store captured images",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=50,
        help="Number of images to capture",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=2.0,
        help="Seconds between captures",
    )
    parser.add_argument("--width", type=int, default=640, help="Expected stream width")
    parser.add_argument("--height", type=int, default=480, help="Expected stream height")
    parser.add_argument("--fps", type=float, default=15.0, help="Expected stream FPS")
    return parser.parse_args()


def main():
    args = parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    cap = _open_shared_capture(args.video_shm, args.width, args.height, args.fps)
    if cap is None:
        print(
            f"Failed to open shared video stream at {args.video_shm}. "
            "Ensure av_daemon is running.",
            file=sys.stderr,
        )
        return 1

    print(
        f"Capturing {args.count} images every {args.interval:.1f}s to {args.output_dir}..."
    )
    print("Starting in 10 seconds...")
    for remaining in range(10, 0, -1):
        print(f"{remaining}...")
        time.sleep(1)

    for index in range(args.count):
        ret, frame = cap.read()
        if not ret:
            print("Failed to read frame from shared stream", file=sys.stderr)
            break

        filename = os.path.join(args.output_dir, f"sample_{index:03d}.jpg")
        if not cv2.imwrite(filename, frame):
            print(f"Failed to write {filename}", file=sys.stderr)
            break

        print(f"Saved {filename}")
        print(f"Capture! ({index + 1}/{args.count})")
        if index < args.count - 1:
            time.sleep(args.interval)

    cap.release()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
