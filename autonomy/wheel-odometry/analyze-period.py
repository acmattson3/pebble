#!/usr/bin/env python3
import argparse
import csv
from pathlib import Path

import numpy as np


def parse_args():
    parser = argparse.ArgumentParser(
        description="Analyze wheel hall-sensor capture CSV and estimate rotation period."
    )
    parser.add_argument(
        "--input",
        type=str,
        default="wheel_odometry_capture.csv",
        help="Input CSV path from test-data-gather.py",
    )
    parser.add_argument(
        "--magnets",
        type=int,
        default=10,
        help="Number of magnets on the wheel",
    )
    parser.add_argument(
        "--alternating-polarity",
        action="store_true",
        default=True,
        help="Magnets alternate polarity around the wheel (default: true)",
    )
    parser.add_argument(
        "--outlier-factor",
        type=float,
        default=3.0,
        help="Drop rows where delta_us > outlier_factor * median(delta_us)",
    )
    return parser.parse_args()


def load_capture(path: Path):
    delta_us = []
    analog = []
    with path.open(newline="", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            try:
                delta_us.append(float(row["delta_us"]))
                analog.append(float(row["analog_value"]))
            except (KeyError, ValueError):
                continue
    return np.array(delta_us, dtype=float), np.array(analog, dtype=float)


def build_time_seconds(delta_us: np.ndarray):
    t = np.zeros(len(delta_us), dtype=float)
    if len(delta_us) > 1:
        t[1:] = np.cumsum(delta_us[1:]) / 1e6
    return t


def crossing_times(t: np.ndarray, y: np.ndarray, rising: bool):
    times = []
    for i in range(1, len(y)):
        y0 = y[i - 1]
        y1 = y[i]
        if rising:
            crossed = y0 < 0 <= y1
        else:
            crossed = y0 > 0 >= y1

        if crossed and y1 != y0:
            # Linear interpolation improves timing accuracy vs. raw sample index.
            frac = (-y0) / (y1 - y0)
            tc = t[i - 1] + frac * (t[i] - t[i - 1])
            times.append(tc)
    return np.array(times, dtype=float)


def summarize_period(times: np.ndarray):
    if len(times) < 2:
        return None
    dt = np.diff(times)
    if len(dt) == 0:
        return None
    return {
        "count": len(dt),
        "mean_s": float(np.mean(dt)),
        "std_s": float(np.std(dt)),
        "median_s": float(np.median(dt)),
    }


def main():
    args = parse_args()
    input_path = Path(args.input)
    if not input_path.exists():
        print(f"Input file not found: {input_path}")
        return 1

    delta_us, analog = load_capture(input_path)
    if len(delta_us) < 10:
        print("Not enough rows to analyze.")
        return 1

    median_delta = float(np.median(delta_us))
    mask = (delta_us > 0) & (delta_us <= args.outlier_factor * median_delta)
    filtered_count = int(np.sum(mask))
    dropped = len(delta_us) - filtered_count
    if filtered_count < 10:
        print("Not enough rows after outlier filtering.")
        return 1

    delta_f = delta_us[mask]
    analog_f = analog[mask]
    t = build_time_seconds(delta_f)
    y = analog_f - np.median(analog_f)

    rising = crossing_times(t, y, rising=True)
    falling = crossing_times(t, y, rising=False)
    all_crossings = np.sort(np.concatenate([rising, falling]))

    rising_summary = summarize_period(rising)
    falling_summary = summarize_period(falling)
    all_summary = summarize_period(all_crossings)

    if rising_summary is None and falling_summary is None:
        print("Could not detect enough zero crossings.")
        return 1

    # Rising/falling same-direction crossing interval is one signal cycle.
    # With alternating polarity, one signal cycle corresponds to two magnets.
    cycle_s = None
    if rising_summary and falling_summary:
        cycle_s = float(np.mean([rising_summary["mean_s"], falling_summary["mean_s"]]))
    elif rising_summary:
        cycle_s = rising_summary["mean_s"]
    else:
        cycle_s = falling_summary["mean_s"]

    magnets_per_cycle = 2 if args.alternating_polarity else 1
    wheel_period_s = cycle_s * (args.magnets / magnets_per_cycle)
    rev_per_s = 1.0 / wheel_period_s
    rpm = rev_per_s * 60.0
    magnet_period_s = wheel_period_s / args.magnets

    print(f"Input: {input_path}")
    print(f"Rows: {len(delta_us)} (filtered {dropped} outlier rows)")
    print(
        f"Sample delta_us median={np.median(delta_f):.1f} "
        f"mean={np.mean(delta_f):.1f} std={np.std(delta_f):.1f}"
    )
    if rising_summary:
        print(
            "Rising zero-cross period: "
            f"{rising_summary['mean_s']:.6f}s ± {rising_summary['std_s']:.6f}s "
            f"(n={rising_summary['count']})"
        )
    if falling_summary:
        print(
            "Falling zero-cross period: "
            f"{falling_summary['mean_s']:.6f}s ± {falling_summary['std_s']:.6f}s "
            f"(n={falling_summary['count']})"
        )
    if all_summary:
        print(
            "Any zero-cross spacing: "
            f"{all_summary['mean_s']:.6f}s ± {all_summary['std_s']:.6f}s "
            "(one magnet for alternating polarity)"
        )

    print()
    print(f"Estimated signal cycle period: {cycle_s:.6f}s")
    print(f"Estimated wheel period: {wheel_period_s:.6f}s/rev")
    print(f"Estimated wheel speed: {rev_per_s:.4f} rev/s ({rpm:.2f} RPM)")
    print(f"Estimated time per magnet: {magnet_period_s:.6f}s")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
