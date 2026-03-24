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
    print("Missing dependency: pyserial. Install with `pip install pyserial`.", file=sys.stderr)
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
        description="Capture Arduino hall sensor serial output into CSV."
    )
    parser.add_argument(
        "--port",
        type=str,
        default="COM9",
        help="Serial port name (example: COM9)",
    )
    parser.add_argument("--baud", type=int, default=250000, help="Serial baud rate")
    parser.add_argument(
        "--output",
        type=str,
        default="wheel_odometry_capture.csv",
        help="Output CSV file path",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=1.0,
        help="Serial read timeout in seconds",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=0,
        help="Stop after this many valid rows (0 = no limit)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite output file instead of appending",
    )
    return parser.parse_args()


def parse_serial_line(line):
    parts = line.strip().split()
    if len(parts) != 2:
        raise ValueError(f"expected 2 fields, got {len(parts)}")
    delta_us = int(parts[0])
    analog_value = int(parts[1])
    return delta_us, analog_value


def main():
    args = parse_args()

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    file_mode = "w" if args.overwrite else "a"
    should_write_header = args.overwrite or (not output_path.exists()) or output_path.stat().st_size == 0

    rows_written = 0

    print(f"Opening serial port {args.port} @ {args.baud}...")
    try:
        with serial.Serial(args.port, args.baud, timeout=args.timeout) as ser, output_path.open(
            file_mode, newline="", encoding="utf-8"
        ) as csv_file:
            writer = csv.writer(csv_file)
            if should_write_header:
                writer.writerow(["host_iso8601", "host_unix_us", "delta_us", "analog_value"])
                csv_file.flush()

            print(f"Writing data to {output_path}")
            print("Press Ctrl+C to stop.")

            while True:
                raw = ser.readline()
                if not raw:
                    continue

                line = raw.decode("ascii", errors="replace").strip()
                if not line:
                    continue

                try:
                    delta_us, analog_value = parse_serial_line(line)
                except ValueError as exc:
                    print(f"Skipping malformed line '{line}': {exc}", file=sys.stderr)
                    continue

                now = datetime.datetime.now(datetime.timezone.utc)
                writer.writerow([now.isoformat(), time.time_ns() // 1_000, delta_us, analog_value])
                rows_written += 1

                if rows_written % 100 == 0:
                    csv_file.flush()
                    print(f"Rows written: {rows_written}")

                if args.max_rows > 0 and rows_written >= args.max_rows:
                    print(f"Reached max rows: {args.max_rows}")
                    break
    except serial.SerialException as exc:
        print(f"Serial error: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("\nStopping capture.")
    finally:
        print(f"Total rows written: {rows_written}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
