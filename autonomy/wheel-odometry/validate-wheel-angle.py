#!/usr/bin/env python3
import argparse
import csv
import datetime
import sys
import time
from pathlib import Path

try:
    import serial
except ImportError:
    print("Missing dependency: pyserial. Install with `python -m pip install pyserial`.", file=sys.stderr)
    raise SystemExit(1)

if not hasattr(serial, "Serial") or not hasattr(serial, "SerialException"):
    serial_path = getattr(serial, "__file__", "<unknown>")
    print(
        "Imported 'serial' is not pyserial.\n"
        f"Loaded module path: {serial_path}\n"
        "Fix with:\n"
        "  python -m pip uninstall -y serial\n"
        "  python -m pip install pyserial",
        file=sys.stderr,
    )
    raise SystemExit(1)


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Forward-only wheel angle validator from hall sensor stream. "
            "Detects extrema using slope sign changes with noise rejection."
        )
    )
    parser.add_argument("--port", type=str, default="COM9", help="Serial port (example: COM9)")
    parser.add_argument("--baud", type=int, default=460800, help="Serial baud rate")
    parser.add_argument("--timeout", type=float, default=1.0, help="Serial read timeout in seconds")
    parser.add_argument("--magnets", type=int, default=10, help="Number of magnets on wheel")
    parser.add_argument(
        "--signal-alpha",
        type=float,
        default=0.35,
        help="EMA gain for analog smoothing",
    )
    parser.add_argument(
        "--slope-alpha",
        type=float,
        default=0.25,
        help="EMA gain for slope smoothing",
    )
    parser.add_argument(
        "--slope-deadband",
        type=float,
        default=0.0005,
        help="Deadband for slope sign in ADC/us (filters +/- noise near zero slope)",
    )
    parser.add_argument(
        "--confirm-samples",
        type=int,
        default=2,
        help="Required consecutive non-zero slope-sign samples before accepting a new sign",
    )
    parser.add_argument(
        "--min-extremum-us",
        type=float,
        default=15000.0,
        help="Minimum time between accepted extrema events",
    )
    parser.add_argument(
        "--min-swing-adc",
        type=float,
        default=24.0,
        help="Minimum filtered ADC change between extrema to accept event",
    )
    parser.add_argument(
        "--initial-swing-adc",
        type=float,
        default=180.0,
        help="Initial extrema-to-extrema swing estimate for interpolation",
    )
    parser.add_argument(
        "--swing-alpha",
        type=float,
        default=0.2,
        help="EMA gain for updating swing estimate",
    )
    parser.add_argument("--print-hz", type=float, default=20.0, help="Console print rate in Hz")
    parser.add_argument(
        "--log-csv",
        type=str,
        default="",
        help="Optional CSV path for live estimates",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=0,
        help="Stop after this many valid serial rows (0 = run until Ctrl+C)",
    )
    return parser.parse_args()


def parse_serial_line(line: str):
    parts = line.strip().split()
    if len(parts) != 2:
        raise ValueError(f"expected 2 fields, got {len(parts)}")
    return int(parts[0]), int(parts[1])


def slope_sign(slope_value: float, deadband: float):
    if slope_value > deadband:
        return 1
    if slope_value < -deadband:
        return -1
    return 0


def main():
    args = parse_args()
    deg_per_magnet = 360.0 / float(args.magnets)

    print("Forward-only estimator.")
    print("Event model: slope sign-change -> reached a magnet extreme.")
    print("Angle model: angle = (magnet_count + in_between_progress) * 360/magnets")

    log_file = None
    log_writer = None
    if args.log_csv:
        log_path = Path(args.log_csv)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_file = log_path.open("w", newline="", encoding="utf-8")
        log_writer = csv.writer(log_file)
        log_writer.writerow(
            [
                "host_iso8601",
                "device_time_us",
                "analog_raw",
                "analog_filtered",
                "slope_adc_per_us",
                "slope_sign",
                "locked",
                "magnet_count",
                "swing_est_adc",
                "progress",
                "angle_deg",
                "angle_rev",
            ]
        )

    rows = 0
    device_time_us = 0.0
    filtered = None
    prev_filtered = None
    slope_ema = 0.0
    current_sign = 0
    last_nonzero_sign = None
    pending_sign = 0
    pending_count = 0

    locked = False
    magnet_count = 0
    last_extremum_time_us = None
    last_extremum_value = None
    swing_est_adc = float(args.initial_swing_adc)

    printed_at = time.monotonic()

    try:
        with serial.Serial(args.port, args.baud, timeout=args.timeout) as ser:
            print(f"Reading serial from {args.port} @ {args.baud}. Press Ctrl+C to stop.")

            while True:
                raw = ser.readline()
                if not raw:
                    continue

                line = raw.decode("ascii", errors="replace").strip()
                if not line:
                    continue

                try:
                    delta_us, analog_raw = parse_serial_line(line)
                except ValueError:
                    continue

                if delta_us <= 0:
                    continue

                device_time_us += float(delta_us)
                rows += 1

                if filtered is None:
                    filtered = float(analog_raw)
                    prev_filtered = filtered
                    continue

                filtered = filtered + args.signal_alpha * (float(analog_raw) - filtered)
                raw_slope = (filtered - prev_filtered) / float(delta_us)
                slope_ema = slope_ema + args.slope_alpha * (raw_slope - slope_ema)
                prev_filtered = filtered

                sign_now = slope_sign(slope_ema, args.slope_deadband)
                if sign_now == 0:
                    pending_sign = 0
                    pending_count = 0
                else:
                    if sign_now == pending_sign:
                        pending_count += 1
                    else:
                        pending_sign = sign_now
                        pending_count = 1

                    if pending_count >= max(1, args.confirm_samples):
                        current_sign = pending_sign
                        pending_count = 0

                        if last_nonzero_sign is None:
                            last_nonzero_sign = current_sign
                        elif current_sign != last_nonzero_sign:
                            event_ok = True
                            if last_extremum_time_us is not None:
                                if (device_time_us - last_extremum_time_us) < args.min_extremum_us:
                                    event_ok = False

                            if last_extremum_value is not None:
                                if abs(filtered - last_extremum_value) < args.min_swing_adc:
                                    event_ok = False

                            if event_ok:
                                if not locked:
                                    locked = True
                                    magnet_count = 0
                                else:
                                    magnet_count += 1

                                if last_extremum_value is not None:
                                    observed_swing = abs(filtered - last_extremum_value)
                                    swing_est_adc = (1.0 - args.swing_alpha) * swing_est_adc + args.swing_alpha * observed_swing

                                last_extremum_value = filtered
                                last_extremum_time_us = device_time_us
                                last_nonzero_sign = current_sign

                progress = 0.0
                if locked and last_extremum_value is not None:
                    progress = abs(filtered - last_extremum_value) / max(1.0, swing_est_adc)
                    progress = max(0.0, min(0.999, progress))

                angle_deg = (magnet_count + progress) * deg_per_magnet if locked else 0.0

                now = time.monotonic()
                if now - printed_at >= (1.0 / max(0.5, args.print_hz)):
                    lock_text = "LOCKED" if locked else "UNLOCKED"
                    print(
                        f"{lock_text}  angle_deg={angle_deg:9.3f}  rev={angle_deg/360.0:8.4f}  "
                        f"magnets={magnet_count:5d}  prog={progress:0.3f}  "
                        f"slope={slope_ema: .6f}  swing_est={swing_est_adc:6.2f}  analog={analog_raw:4d}"
                    )
                    printed_at = now

                if log_writer is not None:
                    host_now = datetime.datetime.now(datetime.timezone.utc).isoformat()
                    log_writer.writerow(
                        [
                            host_now,
                            int(device_time_us),
                            analog_raw,
                            f"{filtered:.3f}",
                            f"{slope_ema:.9f}",
                            current_sign,
                            int(locked),
                            magnet_count,
                            f"{swing_est_adc:.3f}",
                            f"{progress:.6f}",
                            f"{angle_deg:.6f}",
                            f"{angle_deg/360.0:.9f}",
                        ]
                    )
                    if rows % 200 == 0:
                        log_file.flush()

                if args.max_rows > 0 and rows >= args.max_rows:
                    break

    except serial.SerialException as exc:
        print(f"Serial error: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("\nStopping.")
    finally:
        if log_file is not None:
            log_file.flush()
            log_file.close()
        print(f"Processed rows: {rows}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
