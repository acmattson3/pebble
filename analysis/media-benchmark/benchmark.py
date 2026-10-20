#!/usr/bin/env python3
"""Compare Pebble codecs offline using an already-running AV shared stream.

Capture never opens the camera directly, publishes MQTT, or changes service flags.
Encoding CPU excludes capture/resize, receiver work, metrics, and network transport.
"""
from __future__ import annotations

import argparse
import ast
import csv
import hashlib
import json
import math
import re
import resource
import struct
import subprocess
import time
from pathlib import Path

import cv2
import numpy as np


def command(args):
    p = subprocess.run(args, capture_output=True, text=True)
    if p.returncode:
        raise RuntimeError(f"{args[0]} failed: {p.stderr[-2500:]}")
    return p.stderr


def cpu_children():
    r = resource.getrusage(resource.RUSAGE_CHILDREN)
    return r.ru_utime + r.ru_stime


def capture(args):
    root = args.directory
    root.mkdir(parents=True, exist_ok=True)
    config = json.loads(args.config.read_text())
    v = config["services"]["av_daemon"]["video"]
    socket = Path(v["socket_path"])
    if not socket.exists():
        raise RuntimeError("No active shared capture; camera was not started.")
    w, h, fps = (int(v[k]) for k in ("width", "height", "fps"))
    pipe = (f"shmsrc socket-path={socket} is-live=true do-timestamp=true ! "
            f"video/x-raw,format=BGR,width={w},height={h},framerate={fps}/1 ! "
            "queue leaky=downstream max-size-buffers=2 ! "
            "appsink drop=true max-buffers=1 sync=false")
    cap = cv2.VideoCapture(pipe, cv2.CAP_GSTREAMER)
    if not cap.isOpened():
        raise RuntimeError("Could not read the existing shared stream.")
    count = 0
    begin = time.monotonic()
    with (root / "live.bgr").open("wb") as live, (root / "motion.bgr").open("wb") as motion:
        for i in range(args.frames):
            ok, frame = cap.read()
            if not ok:
                break
            cv2.resize(frame, (640, 360), interpolation=cv2.INTER_AREA).tofile(live)
            # Explicit synthetic workload: pan a crop of each captured frame;
            # horizontal view flips at frames 41 and 81 simulate abrupt scene changes.
            cw, ch = int(w * .75), int(h * .75)
            x = round((w-cw) * (.5 + .48 * math.sin(i * .11)))
            y = round((h-ch) * (.5 + .45 * math.sin(i * .07)))
            crop = frame[y:y+ch, x:x+cw]
            if 41 <= i < 81:
                crop = cv2.flip(crop, 1)
            cv2.resize(crop, (640, 360), interpolation=cv2.INTER_AREA).tofile(motion)
            count += 1
    cap.release()
    if count < 45:
        raise RuntimeError(f"Only {count} frames captured; at least 45 required.")
    publisher = config["services"]["mqtt_bridge"]["media"]["video_publisher"]
    metadata = {
        "frames": count, "width": 640, "height": 360, "fps": fps,
        "capture_elapsed_seconds": time.monotonic()-begin,
        "reference": "BGR decoded by the existing camera capture; not sensor ground truth",
        "workloads": {"live": "Unmodified live shared capture, resized to 640x360",
                      "motion": "Synthetic moving crop, with horizontal view flips at frames 41 and 81"},
        "current_publisher": {k: publisher.get(k) for k in
                              ("publish_fps", "jpeg_quality", "keyframe_interval")},
    }
    (root / "capture.json").write_text(json.dumps(metadata, indent=2)+"\n")
    print(json.dumps(metadata), flush=True)


def load_codec(path):
    source = path.read_text()
    names = {"_prepare_frame", "_reconstruct_published_frame", "_encode_payload"}
    constants = {"VIDEO_MAGIC", "VIDEO_FRAME_JPEG", "VIDEO_FRAME_DELTA_JPEG", "VIDEO_PACKET_HEADER"}
    nodes = []
    for n in ast.parse(source).body:
        if isinstance(n, ast.FunctionDef) and n.name in names:
            nodes.append(n)
        elif isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id in constants for t in n.targets):
            nodes.append(n)
    ns = {"cv2": cv2, "np": np, "struct": struct, "time": time}
    code = "from __future__ import annotations\n" + ast.unparse(ast.Module(body=nodes, type_ignores=[]))
    exec(compile(code, str(path), "exec"), ns)
    return ns, hashlib.sha256(source.encode()).hexdigest()


def raw_input(path, meta):
    return ["-f", "rawvideo", "-pixel_format", "bgr24", "-video_size",
            f"{meta['width']}x{meta['height']}", "-framerate", str(meta["fps"]), "-i", str(path)]


def quality(ref, decoded, meta, out):
    filters = ("[0:v]format=yuv420p,split=2[r1][r2];"
               "[1:v]format=yuv420p,split=2[d1][d2];"
               f"[d1][r1]ssim=stats_file={out / 'ssim.txt'}[s];"
               f"[d2][r2]psnr=stats_file={out / 'psnr.txt'}[p]")
    stderr = command(["ffmpeg", "-hide_banner", "-nostdin", "-threads", "2",
                      *raw_input(ref, meta), "-threads", "2", *raw_input(decoded, meta),
                      "-filter_complex_threads", "1", "-filter_complex", filters,
                      "-map", "[s]", "-map", "[p]", "-f", "null", "-"])
    ssim = float(re.findall(r"SSIM Y:([\d.]+)", stderr)[-1])
    psnr = float(re.findall(r"PSNR y:([\d.]+)", stderr)[-1])
    frame_ssim = [float(x) for x in re.findall(r"Y:([\d.]+)", (out / "ssim.txt").read_text())]
    return {"ssim_y": ssim, "psnr_y_db": psnr,
            "ssim_y_p05": float(np.percentile(frame_ssim, 5)),
            "worst_frame": int(np.argmin(frame_ssim))}


def run(args):
    root = args.directory
    meta = json.loads((root / "capture.json").read_text())
    codec, digest = load_codec(args.publisher)
    header_size = codec["VIDEO_PACKET_HEADER"].size
    frames_count, h, w = (meta[k] for k in ("frames", "height", "width"))
    jobs = [(f"jpeg-q{q}", "jpeg", q) for q in (3, 5, 8, 10, 15, 20, 30)]
    jobs += [(f"delta-q{q}-g5", "delta", q) for q in (8, 10, 20)]
    jobs += [(f"h264-{preset}-{rate}k", "h264", (preset, rate, meta["fps"]))
             for preset in ("ultrafast", "veryfast") for rate in (180, 400, 600)]
    results = []
    if args.matched_gop:
        jobs = [(f"h264-ultrafast-{rate}k-g5", "h264", ("ultrafast", rate, 5))
                for rate in (180, 400)]
        selected_names = {job[0] for job in jobs}
        results = [row for row in json.loads((root / "results.json").read_text())["results"]
                   if row["codec"] not in selected_names]
        for row in results:
            row["gop_frames"] = 5 if row["codec"].startswith("delta") else (meta["fps"] if row["codec"].startswith("h264") else 1)
    for workload in ("live", "motion"):
        ref = root / f"{workload}.bgr"
        frames = np.memmap(ref, dtype=np.uint8, mode="r", shape=(frames_count, h, w, 3))
        cv2.imwrite(str(root / f"{workload}-reference.png"), frames[43])
        for name, kind, setting in jobs:
            out = root / workload / name
            out.mkdir(parents=True, exist_ok=True)
            decoded = root / "decoded.bgr"
            sizes, enc_cpu, enc_wall, dec_cpu = [], 0., 0., 0.
            if kind in ("jpeg", "delta"):
                sender_ref = receiver_ref = None
                with decoded.open("wb") as dest:
                    for i, frame in enumerate(frames):
                        t, c = time.perf_counter(), time.process_time()
                        if kind == "delta":
                            key = i % 5 == 0
                            prepared = codec["_prepare_frame"](frame, sender_ref, key)
                            packet, sender_decoded = codec["_encode_payload"](prepared, i, key, setting)
                            sender_ref = codec["_reconstruct_published_frame"](sender_ref, sender_decoded, key)
                            blob = packet[header_size:]
                        else:
                            key = True
                            ok, blob = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, setting])
                            if not ok:
                                raise RuntimeError("JPEG encoding failed")
                        enc_cpu += time.process_time()-c
                        enc_wall += time.perf_counter()-t
                        sizes.append(len(blob)+header_size)
                        c = time.process_time()
                        image = cv2.imdecode(np.frombuffer(blob, np.uint8), cv2.IMREAD_COLOR)
                        receiver_ref = codec["_reconstruct_published_frame"](receiver_ref, image, key)
                        dec_cpu += time.process_time()-c
                        receiver_ref.tofile(dest)
            else:
                preset, rate, gop = setting
                bitstream = out / "stream.h264"
                c, t = cpu_children(), time.perf_counter()
                command(["ffmpeg", "-hide_banner", "-nostdin", "-y", "-threads", "2",
                         *raw_input(ref, meta), "-an", "-vf", "format=yuv420p",
                         "-filter_threads", "1", "-c:v", "libx264", "-threads", "2",
                         "-preset", preset, "-tune", "zerolatency", "-profile:v", "baseline",
                         "-g", str(gop), "-keyint_min", str(gop),
                         "-sc_threshold", "0", "-bf", "0", "-b:v", f"{rate}k",
                         "-maxrate", f"{rate}k", "-bufsize", f"{rate//2}k",
                         "-f", "h264", str(bitstream)])
                enc_cpu, enc_wall = cpu_children()-c, time.perf_counter()-t
                c = cpu_children()
                command(["ffmpeg", "-hide_banner", "-nostdin", "-y", "-threads", "2",
                         "-i", str(bitstream), "-filter_threads", "1", "-pix_fmt", "bgr24",
                         "-f", "rawvideo", str(decoded)])
                dec_cpu = cpu_children()-c
                sizes = [(bitstream.stat().st_size+header_size*frames_count)/frames_count]*frames_count
            expected = frames_count*h*w*3
            if decoded.stat().st_size != expected:
                raise RuntimeError(f"Wrong decoded length: {name}")
            recon = np.memmap(decoded, dtype=np.uint8, mode="r", shape=(frames_count,h,w,3))
            cv2.imwrite(str(out / "frame-43.png"), recon[43])
            del recon
            row = {"workload": workload, "codec": name, "frames": frames_count,
                   "app_kbps": sum(sizes)*8/(frames_count/meta["fps"])/1000,
                   "encoder_cpu_ms_frame": enc_cpu*1000/frames_count,
                   "encoder_wall_ms_frame": enc_wall*1000/frames_count,
                   "encoder_core_percent_at_fps": enc_cpu/(frames_count/meta["fps"])*100,
                   "receiver_cpu_ms_frame": dec_cpu*1000/frames_count,
                   "header_bytes_frame": header_size,
                   "gop_frames": setting[2] if kind == "h264" else (5 if kind == "delta" else 1),
                   **quality(ref, decoded, meta, out)}
            decoded.unlink()
            results.append(row)
            document = {"metadata": meta, "publisher_sha256": digest,
                        "codec_threads": {"opencv": 1, "x264": 2},
                        "h264": {"default_keyframe_seconds": 1, "matched_gop_frames": 5, "bframes": 0,
                                 "vbv_buffer_seconds": .5, "tune": "zerolatency"},
                        "results": results}
            (root / "results.json").write_text(json.dumps(document, indent=2)+"\n")
            with (root / "results.csv").open("w") as f:
                writer = csv.DictWriter(f, fieldnames=list(row))
                writer.writeheader(); writer.writerows(results)
            print(json.dumps(row), flush=True)
    for workload in ("live", "motion"):
        if (root/f"{workload}-reference.mkv").exists():
            continue
        command(["ffmpeg", "-hide_banner", "-nostdin", "-y", "-threads", "2",
                 *raw_input(root/f"{workload}.bgr", meta), "-c:v", "ffv1", "-level", "1",
                 "-threads", "2", str(root/f"{workload}-reference.mkv")])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("capture", "run"))
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--config", type=Path, default=Path("/opt/pebble/control/configs/config.json"))
    parser.add_argument("--publisher", type=Path, default=Path("/opt/pebble/video/camera_publisher.py"))
    parser.add_argument("--frames", type=int, default=120)
    parser.add_argument("--matched-gop", action="store_true",
                        help="Append H.264 profiles with the same five-frame GOP as the delta baseline")
    arguments = parser.parse_args()
    cv2.setNumThreads(1)
    (capture if arguments.action == "capture" else run)(arguments)
