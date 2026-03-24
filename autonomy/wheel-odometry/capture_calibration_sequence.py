#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import sys
import time
from dataclasses import dataclass
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

from control.common.odometry_shm import OdometryRawShmReader


@dataclass(frozen=True)
class Step:
    key: str
    title: str
    instruction: str
    duration_s: float


def default_steps(short: bool = False) -> list[Step]:
    scale = 0.4 if short else 1.0
    base = [
        Step("idle_baseline", "Idle Baseline", "Keep robot stationary.", 30.0),
        Step("left_fwd_low", "Left Wheel Forward (Low)", "Drive LEFT wheel forward only at low speed.", 20.0),
        Step("left_fwd_med", "Left Wheel Forward (Medium)", "Drive LEFT wheel forward only at medium speed.", 20.0),
        Step("left_rev_low", "Left Wheel Reverse (Low)", "Drive LEFT wheel reverse only at low speed.", 20.0),
        Step("right_fwd_low", "Right Wheel Forward (Low)", "Drive RIGHT wheel forward only at low speed.", 20.0),
        Step("right_fwd_med", "Right Wheel Forward (Medium)", "Drive RIGHT wheel forward only at medium speed.", 20.0),
        Step("right_rev_low", "Right Wheel Reverse (Low)", "Drive RIGHT wheel reverse only at low speed.", 20.0),
        Step("spin_left", "In-Place Spin Left", "Spin in place to the LEFT at medium speed.", 20.0),
        Step("spin_right", "In-Place Spin Right", "Spin in place to the RIGHT at medium speed.", 20.0),
        Step("straight_fwd", "Straight Forward", "Drive straight FORWARD at medium speed.", 20.0),
        Step("straight_rev", "Straight Reverse", "Drive straight REVERSE at medium speed.", 20.0),
        Step("idle_cooldown", "Idle Cooldown", "Stop movement and keep robot stationary.", 15.0),
    ]
    if scale == 1.0:
        return base
    return [Step(s.key, s.title, s.instruction, max(5.0, s.duration_s * scale)) for s in base]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Capture wheel hall-sensor shared-memory data with guided prompts. "
            "Writes one CSV row per sample with step metadata."
        )
    )
    parser.add_argument(
        "--shm-name",
        default="pebble_robots_example_odometry_raw",
        help="Shared-memory name from services.serial_mcu_bridge.odometry_shm.name",
    )
    parser.add_argument(
        "--output",
        default="",
        help="CSV output path. Default: odometry_calibration_YYYYmmdd_HHMMSS.csv",
    )
    parser.add_argument(
        "--countdown",
        type=int,
        default=5,
        help="Seconds to count down before each step starts.",
    )
    parser.add_argument(
        "--poll-ms",
        type=float,
        default=10.0,
        help="Polling interval in milliseconds while reading shared memory.",
    )
    parser.add_argument(
        "--short",
        action="store_true",
        help="Run a shorter sequence for quick validation.",
    )
    return parser.parse_args()


def countdown(seconds: int) -> None:
    for remaining in range(int(seconds), 0, -1):
        print(f"  Starting in {remaining}...", flush=True)
        time.sleep(1.0)


def prompt_to_continue(step: Step, idx: int, total: int) -> str:
    print()
    print(f"[Step {idx}/{total}] {step.title}")
    print(f"Instruction: {step.instruction}")
    print(f"Duration: {step.duration_s:.1f}s")
    answer = input("Press Enter to run, type 's' to skip, or 'q' to quit: ").strip().lower()
    return answer


def format_iso_utc(ts: float) -> str:
    return dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc).isoformat()


def main() -> int:
    args = parse_args()
    steps = default_steps(short=args.short)

    output_path = Path(args.output).expanduser() if args.output else None
    if output_path is None:
        stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d_%H%M%S")
        output_path = Path(f"odometry_calibration_{stamp}.csv")
    if not output_path.is_absolute():
        output_path = (Path.cwd() / output_path).resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Attaching to shared memory: {args.shm_name}")
    try:
        reader = OdometryRawShmReader(args.shm_name)
    except Exception as exc:
        print(f"Failed to open shared memory '{args.shm_name}': {exc}", file=sys.stderr)
        return 1

    capture_id = dt.datetime.now(dt.timezone.utc).strftime("capture_%Y%m%dT%H%M%SZ")
    poll_s = max(0.001, float(args.poll_ms) / 1000.0)
    countdown_s = max(0, int(args.countdown))

    total_rows = 0
    total_steps_run = 0

    print(f"Writing CSV: {output_path}")
    print("Notes:")
    print("- Samples during countdown are discarded.")
    print("- Ctrl+C stops capture safely.")

    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "capture_id",
                "host_iso8601_utc",
                "host_unix_ns",
                "host_mono_ns",
                "step_index",
                "step_key",
                "step_title",
                "step_elapsed_s",
                "sample_seq",
                "sample_mono_ns",
                "a0",
                "a1",
                "a2",
                "chg",
            ]
        )

        try:
            for idx, step in enumerate(steps, start=1):
                choice = prompt_to_continue(step, idx, len(steps))
                if choice == "q":
                    print("Quitting by user request.")
                    break
                if choice == "s":
                    print("Skipped.")
                    continue

                if countdown_s > 0:
                    countdown(countdown_s)

                latest = reader.read_latest()
                last_seq = int(latest["seq"]) if latest else 0

                print(f"Running step '{step.key}'...")
                total_steps_run += 1
                step_start_mono = time.monotonic()
                next_status = step_start_mono + 1.0
                step_rows = 0

                while True:
                    now_mono = time.monotonic()
                    elapsed = now_mono - step_start_mono
                    if elapsed >= step.duration_s:
                        break

                    rows = reader.read_since(last_seq)
                    if rows:
                        host_ns = time.time_ns()
                        host_mono_ns = time.monotonic_ns()
                        host_iso = format_iso_utc(time.time())
                        for row in rows:
                            writer.writerow(
                                [
                                    capture_id,
                                    host_iso,
                                    host_ns,
                                    host_mono_ns,
                                    idx,
                                    step.key,
                                    step.title,
                                    f"{elapsed:.6f}",
                                    row["seq"],
                                    row["mono_ns"],
                                    row["a0"],
                                    row["a1"],
                                    row["a2"],
                                    "" if row["chg"] is None else int(bool(row["chg"])),
                                ]
                            )
                            last_seq = row["seq"]
                            step_rows += 1
                            total_rows += 1

                    if now_mono >= next_status:
                        print(f"  {elapsed:5.1f}s / {step.duration_s:.1f}s  rows={step_rows}", flush=True)
                        next_status += 1.0

                    time.sleep(poll_s)

                f.flush()
                print(f"Completed '{step.key}' with {step_rows} rows.")

        except KeyboardInterrupt:
            print("\nCapture interrupted by user.")
        finally:
            reader.close()

    print()
    print("Capture complete.")
    print(f"Steps run: {total_steps_run}")
    print(f"Rows written: {total_rows}")
    print(f"CSV: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
