#!/usr/bin/env python3
#!/usr/bin/env python3
"""Live terminal monitor for MPU6050-Mega-Diagnostic firmware."""

from __future__ import annotations

import argparse
import csv
import math
import queue
import sys
import threading
import time
from typing import Any

import serial
from serial.tools import list_ports


def choose_port(requested: str) -> str:
    if requested.lower() != "auto":
        return requested

    ports = list(list_ports.comports())
    mega_matches = [
        p for p in ports
        if (p.vid == 0x2341 and p.pid == 0x0042)
        or "mega 2560" in (p.description or "").lower()
        or "mega 2560" in (p.product or "").lower()
    ]
    if len(mega_matches) == 1:
        return mega_matches[0].device
    if len(mega_matches) > 1:
        choices = ", ".join(p.device for p in mega_matches)
        raise SystemExit(f"Several Mega boards found ({choices}); specify --port.")

    acm_ports = [p.device for p in ports if p.device.startswith("/dev/ttyACM")]
    if len(acm_ports) == 1:
        return acm_ports[0]
    choices = ", ".join(acm_ports or [p.device for p in ports]) or "none"
    raise SystemExit(f"Could not identify the Mega automatically. Available serial ports: {choices}. Use --port DEVICE.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", default="auto", help="serial device, or 'auto' (default: auto)")
    parser.add_argument("--baud", type=int, default=115200)
    parser.add_argument("--refresh-hz", type=float, default=5.0, help="display refresh rate (default: 5)")
    parser.add_argument("--duration", type=float, default=0.0, help="stop after this many seconds; 0 runs until Ctrl+C")
    parser.add_argument("--visual", action="store_true", help="open a graphical 3D axes and acceleration view (requires Tk)")
    return parser.parse_args()


def fnum(row: dict[str, str], key: str, default: float = math.nan) -> float:
    try:
        return float(row[key])
    except (KeyError, TypeError, ValueError):
        return default


def render(port: str, baud: int, state: str, row: dict[str, str] | None, tty: bool) -> None:
    if tty:
        sys.stdout.write("\x1b[H")
    print("Pebble MPU-6050 live monitor")
    print(f"Port: {port} @ {baud} baud    State: {state}")
    print()
    if row is None:
        print("Waiting for calibrated samples...")
        print("Keep the sensor still while the Mega calibrates at startup.")
    else:
        def val(key: str, fmt: str = ".2f") -> str:
            number = fnum(row, key)
            return "—" if not math.isfinite(number) else format(number, fmt)

        print(f"Sequence {row.get('seq', '?')}    MCU time {val('t_ms', '.0f')} ms    Temp {val('temp_c')} °C")
        print()
        print("Orientation (degrees; quaternion is preferred near pitch +/-90°; yaw drifts without a magnetometer)")
        print(f"  Roll {val('roll_deg'):>8}     Pitch {val('pitch_deg'):>8}     Yaw {val('yaw_deg'):>8}")
        print("Quaternion (w, x, y, z)")
        print(f"  {val('qw', '.5f'):>8}, {val('qx', '.5f'):>8}, {val('qy', '.5f'):>8}, {val('qz', '.5f'):>8}")
        print()
        print(f"Acceleration: x={val('ax_mps2')}  y={val('ay_mps2')}  z={val('az_mps2')} m/s²")
        print(f"Gyro:          x={val('gx_dps')}  y={val('gy_dps')}  z={val('gz_dps')} °/s")
        print(f"Acceleration magnitude: {val('accel_norm_g', '.3f')} g")
    if tty:
        sys.stdout.write("\x1b[J")
    sys.stdout.flush()


def run_visual(port: str, baud: int) -> int:
    try:
        import tkinter as tk
    except ImportError:
        print("Tk is unavailable. Install python3-tk or run without --visual.", file=sys.stderr)
        return 1

    events: queue.Queue[tuple[str, Any]] = queue.Queue()
    stop = threading.Event()

    def serial_reader() -> None:
        header: list[str] | None = None
        try:
            with serial.Serial(port, baud, timeout=0.25) as ser:
                events.put(("status", "Connected; waiting for Mega firmware"))
                while not stop.is_set():
                    line = ser.readline().decode("utf-8", errors="replace").strip()
                    if not line:
                        continue
                    if line.startswith("BOOT,"):
                        events.put(("status", "Firmware booted"))
                    elif line.startswith("FOUND,"):
                        events.put(("status", line.replace(",", "   ")))
                    elif line.startswith("CALIBRATING,"):
                        events.put(("status", "Calibrating: keep the IMU still"))
                    elif line.startswith("CAL_OK,"):
                        events.put(("status", "Calibrated; streaming orientation"))
                    elif line.startswith("ERROR,"):
                        events.put(("status", line))
                    elif line.startswith("t_ms,"):
                        header = next(csv.reader([line]))
                    elif header:
                        values = next(csv.reader([line]))
                        if len(values) == len(header):
                            row = dict(zip(header, values))
                            if all(math.isfinite(fnum(row, k)) for k in ("qw", "qx", "qy", "qz", "ax_mps2", "ay_mps2", "az_mps2")):
                                events.put(("sample", row))
        except serial.SerialException as exc:
            events.put(("status", f"Serial error: {exc}"))
        finally:
            events.put(("closed", None))

    root = tk.Tk()
    root.title("Pebble IMU • live orientation and acceleration")
    root.geometry("1050x760")
    root.minsize(820, 620)
    bg = "#101722"
    panel = "#172231"
    fg = "#edf4ff"
    muted = "#9eb0c6"
    colors = ("#ff6878", "#6ce0a0", "#69adff")
    root.configure(bg=bg)
    canvas = tk.Canvas(root, bg=bg, highlightthickness=0)
    canvas.pack(fill="both", expand=True)

    state: dict[str, Any] = {
        "status": f"Opening {port} @ {baud}; serial open may reset the Mega",
        "row": None,
        "gravity": None,
        "target": [0.0, 0.0, 0.0],
        "ball": [0.0, 0.0, 0.0],
        "velocity": [0.0, 0.0, 0.0],
        "last_sample_ms": None,
        "last_frame": time.monotonic(),
    }

    def rotate_by_quaternion(v: tuple[float, float, float], row: dict[str, str]) -> tuple[float, float, float]:
        w, x, y, z = (fnum(row, k, 0.0) for k in ("qw", "qx", "qy", "qz"))
        # q * [0,v] * conjugate(q), written without temporary quaternions.
        tx = 2.0 * (y * v[2] - z * v[1])
        ty = 2.0 * (z * v[0] - x * v[2])
        tz = 2.0 * (x * v[1] - y * v[0])
        return (
            v[0] + w * tx + (y * tz - z * ty),
            v[1] + w * ty + (z * tx - x * tz),
            v[2] + w * tz + (x * ty - y * tx),
        )

    def add_sample(row: dict[str, str]) -> None:
        ax, ay, az = (fnum(row, k) for k in ("ax_mps2", "ay_mps2", "az_mps2"))
        sample_ms = fnum(row, "t_ms", 0.0)
        prev_ms = state["last_sample_ms"]
        dt = 0.02 if prev_ms is None else max(0.001, min(0.1, (sample_ms - prev_ms) / 1000.0))
        state["last_sample_ms"] = sample_ms
        if state["gravity"] is None:
            state["gravity"] = [ax, ay, az]
        gravity = state["gravity"]
        dynamic = [ax - gravity[0], ay - gravity[1], az - gravity[2]]
        alpha = 1.0 - math.exp(-dt / 1.25)
        for i, value in enumerate((ax, ay, az)):
            gravity[i] += alpha * (value - gravity[i])
        scale = 2.0 * 9.80665
        state["target"] = [max(-1.0, min(1.0, value / scale)) for value in dynamic]
        state["row"] = row

    def project(center: tuple[float, float], v: tuple[float, float, float], scale: float) -> tuple[float, float]:
        # Isometric projection keeps three labelled axes legible in a terminal-sized window.
        cx, cy = center
        return cx + scale * (v[0] - v[1]) * 0.7071, cy + scale * ((v[0] + v[1]) * 0.4082 - v[2])

    def arrow(x0: float, y0: float, x1: float, y1: float, color: str, width: int = 3, dash: tuple[int, int] | None = None) -> None:
        kwargs: dict[str, Any] = {"fill": color, "width": width, "arrow": tk.LAST, "arrowshape": (10, 12, 5)}
        if dash:
            kwargs["dash"] = dash
        canvas.create_line(x0, y0, x1, y1, **kwargs)

    def draw() -> None:
        canvas.delete("all")
        width = max(canvas.winfo_width(), 820)
        height = max(canvas.winfo_height(), 620)
        canvas.create_text(28, 24, text="PEBBLE  /  IMU LIVE VIEW", anchor="w", fill=fg, font=("TkDefaultFont", 17, "bold"))
        canvas.create_text(28, 51, text=f"{port}  •  {state['status']}", anchor="w", fill=muted, font=("TkDefaultFont", 10))

        margin = 24
        gap = 20
        top = 78
        panel_h = height - top - 150
        panel_w = (width - margin * 2 - gap) / 2
        left_x = margin
        right_x = margin + panel_w + gap
        for x in (left_x, right_x):
            canvas.create_rectangle(x, top, x + panel_w, top + panel_h, fill=panel, outline="#26374b", width=1)
        canvas.create_text(left_x + 20, top + 22, text="ORIENTATION  •  BODY AXES", anchor="w", fill=fg, font=("TkDefaultFont", 12, "bold"))
        canvas.create_text(right_x + 20, top + 22, text="MOTION CUE  •  DYNAMIC ACCELERATION", anchor="w", fill=fg, font=("TkDefaultFont", 12, "bold"))

        center_left = (left_x + panel_w / 2, top + panel_h * 0.57)
        center_right = (right_x + panel_w / 2, top + panel_h * 0.57)
        axis_scale = min(panel_w * 0.25, panel_h * 0.24)
        row = state["row"]
        # Faint fixed reference frame.
        for v, label in (((1, 0, 0), "X"), ((0, 1, 0), "Y"), ((0, 0, 1), "Z")):
            end = project(center_left, v, axis_scale * 0.70)
            arrow(*center_left, *end, "#344457", 1, (3, 4))
            canvas.create_text(end[0], end[1] - 12, text=label, fill="#607187", font=("TkDefaultFont", 9))
        if row:
            for v, label, color in (((1, 0, 0), "X", colors[0]), ((0, 1, 0), "Y", colors[1]), ((0, 0, 1), "Z", colors[2])):
                rotated = rotate_by_quaternion(v, row)
                end = project(center_left, rotated, axis_scale)
                arrow(*center_left, *end, color, 4)
                canvas.create_text(end[0] + 10, end[1] - 10, text=label, fill=color, font=("TkDefaultFont", 12, "bold"))
        else:
            canvas.create_text(*center_left, text="Waiting for samples", fill=muted, font=("TkDefaultFont", 11))

        # Acceleration axes and spring-damped ball. The slow gravity estimate
        # is subtracted so the ball returns toward zero when motion settles.
        accel_scale = min(panel_w * 0.22, panel_h * 0.22)
        axis_vectors = ((1, 0, 0), (0, 1, 0), (0, 0, 1))
        for i, (v, label) in enumerate(zip(axis_vectors, ("X", "Y", "Z"))):
            end = project(center_right, v, accel_scale)
            neg = project(center_right, tuple(-n for n in v), accel_scale)
            canvas.create_line(*neg, *end, fill="#40536a", width=1)
            canvas.create_text(end[0] + (10 if i != 1 else -10), end[1] - 12, text=label, fill=colors[i], font=("TkDefaultFont", 10, "bold"))
        ball = tuple(state["ball"])
        ball_pos = project(center_right, ball, accel_scale)
        if row:
            canvas.create_line(*center_right, *ball_pos, fill="#ffd166", width=3)
            radius = 10
            canvas.create_oval(ball_pos[0] - radius, ball_pos[1] - radius, ball_pos[0] + radius, ball_pos[1] + radius,
                               fill="#ffd166", outline="#fff1bd", width=2)
        else:
            canvas.create_oval(center_right[0] - 8, center_right[1] - 8, center_right[0] + 8, center_right[1] + 8,
                               fill="#ffd166", outline="")
        canvas.create_text(right_x + 20, top + panel_h - 20,
                           text="Ball shows acceleration minus slow gravity estimate; spring damping recenters it.",
                           anchor="w", fill=muted, font=("TkDefaultFont", 9))

        # Numeric readout under the visualization.
        read_y = top + panel_h + 24
        if row:
            roll, pitch, yaw = (fnum(row, k) for k in ("roll_deg", "pitch_deg", "yaw_deg"))
            canvas.create_text(margin, read_y, text=f"Roll {roll:7.2f}°     Pitch {pitch:7.2f}°     Yaw {yaw:7.2f}°",
                               anchor="w", fill=fg, font=("TkFixedFont", 12, "bold"))
            canvas.create_text(margin, read_y + 30,
                               text="Quaternion  " + "  ".join(f"{fnum(row, k):+.4f}" for k in ("qw", "qx", "qy", "qz")),
                               anchor="w", fill=muted, font=("TkFixedFont", 10))
            raw = [fnum(row, k) for k in ("ax_mps2", "ay_mps2", "az_mps2")]
            target = [fnum(row, k) for k in ("gx_dps", "gy_dps", "gz_dps")]
            dyn = [raw[i] - state["gravity"][i] for i in range(3)]
            canvas.create_text(margin, read_y + 54,
                               text=f"Accel {raw[0]:+.2f}, {raw[1]:+.2f}, {raw[2]:+.2f} m/s²     Motion {dyn[0]:+.2f}, {dyn[1]:+.2f}, {dyn[2]:+.2f} m/s²",
                               anchor="w", fill=muted, font=("TkFixedFont", 10))
            canvas.create_text(margin, read_y + 77,
                               text=f"Gyro {target[0]:+.2f}, {target[1]:+.2f}, {target[2]:+.2f} °/s     |a| {fnum(row, 'accel_norm_g'):.3f} g",
                               anchor="w", fill=muted, font=("TkFixedFont", 10))
        else:
            canvas.create_text(margin, read_y, text="Hold the sensor still during startup calibration.", anchor="w", fill=muted,
                               font=("TkDefaultFont", 11))

    def poll_events() -> None:
        while True:
            try:
                kind, payload = events.get_nowait()
            except queue.Empty:
                break
            if kind == "status":
                state["status"] = str(payload)
            elif kind == "sample":
                add_sample(payload)
        root.after(20, poll_events)

    def animate() -> None:
        now = time.monotonic()
        dt = min(0.05, max(0.001, now - state["last_frame"]))
        state["last_frame"] = now
        # Critically damped spring, updated independently of sensor packet rate.
        stiffness, damping = 42.0, 12.0
        for i in range(3):
            acceleration = stiffness * (state["target"][i] - state["ball"][i]) - damping * state["velocity"][i]
            state["velocity"][i] += acceleration * dt
            state["ball"][i] += state["velocity"][i] * dt
            state["ball"][i] = max(-1.25, min(1.25, state["ball"][i]))
        draw()
        root.after(33, animate)

    def close() -> None:
        stop.set()
        root.destroy()

    root.protocol("WM_DELETE_WINDOW", close)
    threading.Thread(target=serial_reader, name="imu-serial-reader", daemon=True).start()
    poll_events()
    animate()
    root.mainloop()
    stop.set()
    return 0


def main() -> int:
    args = parse_args()
    if args.refresh_hz <= 0:
        raise SystemExit("--refresh-hz must be greater than zero")
    port = choose_port(args.port)
    if args.visual:
        return run_visual(port, args.baud)
    tty = sys.stdout.isatty()
    state = "opening serial"
    latest: dict[str, str] | None = None
    header: list[str] | None = None
    last_render = 0.0
    last_plain_print = 0.0
    start = time.monotonic()
    refresh_interval = 1.0 / args.refresh_hz

    try:
        # Opening USB serial may reset the Mega. Its boot/calibration messages
        # are consumed below before the first valid sample is displayed.
        with serial.Serial(port, args.baud, timeout=0.25) as ser:
            state = "connected; waiting for firmware"
            while True:
                line = ser.readline().decode("utf-8", errors="replace").strip()
                if line:
                    if line.startswith("BOOT,"):
                        state = "firmware booted"
                    elif line.startswith("FOUND,"):
                        state = line.replace(",", "  ")
                    elif line.startswith("CALIBRATING,"):
                        state = "calibrating; hold still"
                    elif line.startswith("CAL_OK,"):
                        state = "calibrated; reading IMU"
                    elif line.startswith("ERROR,"):
                        state = line
                    elif line.startswith("t_ms,"):
                        header = next(csv.reader([line]))
                    elif header:
                        values = next(csv.reader([line]))
                        if len(values) == len(header):
                            latest = dict(zip(header, values))

                now = time.monotonic()
                if now - last_render >= refresh_interval:
                    if tty:
                        if last_render == 0:
                            sys.stdout.write("\x1b[2J")
                        render(port, args.baud, state, latest, tty=True)
                    elif latest and (last_plain_print == 0 or now - last_plain_print >= 1.0):
                        print(
                            f"{latest.get('seq', '?'):>5}  "
                            f"roll {fnum(latest, 'roll_deg'):7.2f}°  "
                            f"pitch {fnum(latest, 'pitch_deg'):7.2f}°  "
                            f"yaw {fnum(latest, 'yaw_deg'):7.2f}°  "
                            f"|a| {fnum(latest, 'accel_norm_g'):5.3f} g  "
                            f"gyro ({fnum(latest, 'gx_dps'):.2f}, {fnum(latest, 'gy_dps'):.2f}, {fnum(latest, 'gz_dps'):.2f})°/s",
                            flush=True,
                        )
                        last_plain_print = now
                    last_render = now

                if args.duration and now - start >= args.duration:
                    if tty:
                        print("\nStopped after requested duration.")
                    return 0
    except KeyboardInterrupt:
        if tty:
            print("\nStopped.")
        return 0
    except serial.SerialException as exc:
        print(f"Could not read {port}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
