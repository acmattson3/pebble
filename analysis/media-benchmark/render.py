#!/usr/bin/env python3
"""Render benchmark figures and a Markdown report from results.json."""
import argparse
import json
from pathlib import Path

import cv2
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("directory", type=Path)
args = parser.parse_args()
root = args.directory
doc = json.loads((root / "results.json").read_text())
rows = doc["results"]
index = {(r["workload"], r["codec"]): r for r in rows}

families = [
    ("Independent JPEG", lambda n: n.startswith("jpeg"), "o", "#0072B2"),
    ("Pebble delta, GOP 5", lambda n: n.startswith("delta"), "^", "#D55E00"),
    ("x264 ultrafast, GOP 15", lambda n: n.startswith("h264-ultrafast") and not n.endswith("g5"), "s", "#009E73"),
    ("x264 veryfast, GOP 15", lambda n: n.startswith("h264-veryfast"), "D", "#CC79A7"),
    ("x264 ultrafast, GOP 5", lambda n: n.startswith("h264") and n.endswith("g5"), "X", "#E69F00"),
]
fig, axes = plt.subplots(1, 2, figsize=(12, 4.8), constrained_layout=True)
for ax, workload, title in zip(axes, ("live", "motion"), ("Live camera view", "Synthetic pan and scene changes")):
    for label, match, marker, color in families:
        points = sorted((r for r in rows if r["workload"] == workload and match(r["codec"])), key=lambda r: r["app_kbps"])
        if points:
            ax.plot([r["app_kbps"] for r in points], [r["ssim_y"] for r in points],
                    marker=marker, color=color, label=label, linewidth=1.3)
    ax.set(title=title, xlabel="Application bitrate (kbps)", ylabel="Mean luma SSIM (higher is closer to reference)")
    ax.set_ylim(.86, 1.)
    ax.grid(alpha=.22)
axes[0].legend(fontsize=8, loc="lower right")
fig.suptitle("Goob software codec comparison: 640×360, 15 fps, 120 frames")
fig.savefig(root / "quality-vs-bitrate.svg")
fig.savefig(root / "quality-vs-bitrate.png", dpi=150)
plt.close(fig)

selected = [("delta-q8-g5", "Delta Q8 / GOP5"), ("jpeg-q3", "JPEG Q3"),
            ("jpeg-q8", "JPEG Q8"), ("h264-ultrafast-180k", "H.264 180k / GOP15"),
            ("h264-ultrafast-180k-g5", "H.264 180k / GOP5")]
fig, ax = plt.subplots(figsize=(10, 4.8), constrained_layout=True)
x = np.arange(len(selected))
for offset, workload, label in ((-.18, "live", "Live view"), (.18, "motion", "Synthetic motion")):
    values = [index[(workload, name)]["encoder_core_percent_at_fps"] for name, _ in selected]
    bars = ax.bar(x+offset, values, width=.36, label=label)
    ax.bar_label(bars, fmt="%.1f%%", fontsize=8)
ax.set_xticks(x, [label for _, label in selected], rotation=12, ha="right")
ax.set(ylabel="Encoder CPU (% of one core at 15 fps)", title="Goob encoder CPU: capture, receiver, metrics and network excluded")
ax.grid(axis="y", alpha=.2)
ax.legend()
fig.savefig(root / "encoder-cpu.svg")
fig.savefig(root / "encoder-cpu.png", dpi=150)
plt.close(fig)

previews = [(None, "Reference"), ("jpeg-q3", "Independent JPEG Q3"),
            ("delta-q8-g5", "Current Pebble delta Q8"),
            ("jpeg-q8", "Independent JPEG Q8"),
            ("h264-ultrafast-180k-g5", "H.264 ultrafast 180k / GOP5"),
            ("h264-ultrafast-400k-g5", "H.264 ultrafast 400k / GOP5")]
for workload in ("live", "motion"):
    fig, axes = plt.subplots(2, 3, figsize=(11, 11), constrained_layout=True)
    for ax, (name, label) in zip(axes.flat, previews):
        path = root/f"{workload}-reference.png" if name is None else root/workload/name/"frame-43.png"
        image = cv2.cvtColor(cv2.imread(str(path)), cv2.COLOR_BGR2RGB)
        # Fixed matching crop across codecs. Motion frame 43 has a flipped view.
        x = 0 if workload == "live" else 400
        ax.imshow(image[25:345, x:x+240], interpolation="nearest")
        if name:
            r = index[(workload, name)]
            label += f"\n{r['app_kbps']:.0f} kbps; SSIM {r['ssim_y']:.3f}"
        else:
            label += "\nShared capture before benchmark encoding"
        ax.set_title(label, fontsize=10)
        ax.axis("off")
    fig.suptitle(f"{'Live view' if workload == 'live' else 'Synthetic motion'}: detail crop of frame 43, nearest-neighbor display\nScores and rates summarize all 120 frames; this image shows one frame.", fontsize=12)
    fig.savefig(root/f"quality-{workload}.png", dpi=140)
    plt.close(fig)

text = ["# Goob codec benchmark — October 19, 2026", "",
        "Software codecs measured on Goob (Orange Pi Zero 2W / H618, Debian 12, kernel 6.1.31).",
        "Both workloads use 120 reference frames at 640×360 and 15 fps. All codecs see the same frames within each workload.", "",
        "## Main findings", "",
        "- At nearly equal bitrate on this live view, the existing delta codec preserves more detail than independent JPEG quality 3.",
        "- Independent JPEG quality 8 approximates the delta baseline's luma fidelity, but needs more bitrate on the live view. On synthetic motion its rate is slightly lower than delta.",
        "- Software x264 ultrafast is a promising replacement: lower measured bitrate and encoding CPU than the current delta codec, with higher luma SSIM in both workloads. The five-frame GOP variants confirm the result with the same periodic recovery interval as delta.",
        "- These samples do not establish perceptual equivalence, sustained thermal performance, or live MQTT/browser latency. Inspect the detail images and lossless reference clips.", "",
        "## Selected results", "",
        "CPU is percent of one CPU core at 15 fps (100% means one fully occupied core). Application bitrate includes a 19-byte envelope per frame, excludes MQTT/network overhead, and reports actual size rather than the requested x264 target.", ""]
for workload, title in (("live", "Live camera view"), ("motion", "Synthetic pan and scene changes")):
    text += [f"### {title}", "", "| Codec | kbps | Encoder CPU | Mean SSIM Y | p05 SSIM Y | PSNR Y |", "|---|---:|---:|---:|---:|---:|"]
    for name in ("delta-q8-g5", "jpeg-q3", "jpeg-q8", "h264-ultrafast-180k", "h264-ultrafast-180k-g5", "h264-ultrafast-400k-g5"):
        r = index[(workload, name)]
        text.append(f"| {name} | {r['app_kbps']:.1f} | {r['encoder_core_percent_at_fps']:.1f}% | {r['ssim_y']:.4f} | {r['ssim_y_p05']:.4f} | {r['psnr_y_db']:.2f} dB |")
    text += ["", f"![Detail comparisons](quality-{workload}.png)", ""]
text += ["## Figures and artifacts", "", "![Quality versus bitrate](quality-vs-bitrate.png)", "", "![Encoder CPU](encoder-cpu.png)", "",
         "- `results.json` and `results.csv`: all codec settings and measurements.",
         "- `live-reference.mkv`, `motion-reference.mkv`: lossless reference clips.",
         "- `live/` and `motion/`: decoded frame 43, per-frame SSIM/PSNR logs, and encoded H.264 streams.", "",
         "## Interpretation and limits", "",
         "The live camera points at a bench power supply and a mostly plain wall. Whole-frame SSIM is influenced by that wall; the detail crops expose the instrument display and edges for human review. The motion clip is generated from moving crops of the captured frames and horizontal flips at frames 41 and 81. It is a repeatable stress case, not real robot motion.", "",
         "SSIM and PSNR measure additional luma distortion relative to the decoded camera frames, which already contain camera MJPEG losses. They do not directly measure text legibility, AprilTag performance, color fidelity, or perception task success. Every JPEG quality number refers to OpenCV's encoder setting and is not equivalent to another codec's quality setting.", "",
         "Encoding CPU excludes capture and resize. Delta includes its actual deployed sender helpers (difference calculation, JPEG encode/decode, and reference reconstruction); JPEG avoids sender reference decoding. H.264 includes process startup, BGR-to-YUV conversion and x264 encoding with two threads. OpenCV uses one thread. The run used nice 10 alongside existing robot services. Receiver measurements exclude the current web server's extra JPEG encode. Offline wall time is not live end-to-end latency.", "",
         "Default H.264 profiles use baseline, zerolatency, no B-frames, a one-second GOP, and 0.5-second VBV. The `-g5` profiles match the current delta keyframe interval of five frames. Hardware H.264 encoding was not used. No robot service or MQTT topic was changed.", "",
         "The paper's 181.18 kbps archived median is not a matched-condition measurement of this scene, frame rate and software revision. This experiment measures the current 15 fps configuration directly; it should not be used to infer a regression against that archive.", "",
         "## Where to start", "",
         "See [the first implementation milestone](../START-HERE.md). Define the minimal module/actions/endpoints contract and demonstrate shared drive controls plus a robot-local custom module. Then move AV lifecycle into the resource manager and add Opus in both directions. Prototype H.264 over MQTT with the existing software encoder and validate live latency/recovery before investigating a hardware encoder driver.", "",
         f"Publisher source SHA-256: `{doc['publisher_sha256']}`.", ""]
(root / "REPORT.md").write_text("\n".join(text))
print(f"Wrote report and figures for {len(rows)} codec/workload combinations to {root}")
