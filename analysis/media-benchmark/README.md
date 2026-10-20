# Goob media benchmark

`benchmark.py` captures an existing AV-daemon shared stream and compares codecs
offline on the robot. It does not open the camera directly, publish MQTT, or
change media flags. OpenCV, NumPy, FFmpeg with libx264, and a running shared
capture are required.

```bash
python3 benchmark.py capture --directory /tmp/pebble-media-benchmark
OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 python3 benchmark.py run \
  --directory /tmp/pebble-media-benchmark
OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 python3 benchmark.py run \
  --directory /tmp/pebble-media-benchmark --matched-gop
```

Defaults are 120 frames at the configured capture frame rate, resized to 640x360.
The second workload applies a moving crop and two horizontal view flips to the
same source frames. This is a synthetic motion stress workload, not a recording
of robot movement.

The delta encoder uses the deployed publisher's actual helper functions, loaded
from its source through Python's AST. The output records that source's SHA-256.
Delta quality 8 / keyframe interval 5 matches Goob's settings at capture time.

Measurements:

- Application bitrate includes encoded bytes plus the existing 19-byte PBVD
  envelope per frame. H.264 includes an equivalent proposed envelope. MQTT,
  TCP/IP, TLS, broker fan-out, and retransmission costs are excluded.
- JPEG and delta encoder CPU time includes production encoding and, for delta,
  sender reference reconstruction. Capture, resize, receiver decoding, quality
  measurements, and image export are excluded from encoding CPU time.
- H.264 encoder CPU time includes FFmpeg startup, raw input reading, BGR-to-YUV
  conversion, and software x264 encoding. OpenCV uses one thread; x264 uses two.
  CPU usage is reported as percentage of one core at the configured frame rate.
- H.264 uses baseline, zero latency tuning, no B-frames, a one-second GOP, and
  a 0.5-second VBV buffer. Target bitrate is a setting; tables report actual size.
  Additional `--matched-gop` profiles use five-frame GOPs to match the delta
  baseline's periodic keyframe recovery interval. The recorded run used nice 10.
- Luma SSIM and PSNR compare decoded outputs against the shared camera frames
  after conversion to YUV420. This measures additional codec distortion, not
  sensor-to-display fidelity. The camera has already applied MJPEG compression.
- Receiver CPU time excludes the current web server's extra JPEG encoding.
- These are offline codec measurements. Wall time is not end-to-end latency.
  The benchmark runs alongside the robot's services and cannot predict behavior
  under network congestion or establish perceptual equivalence by itself.

Outputs include JSON/CSV, per-frame quality logs, a reference image and decoded
frame 43 for each codec, H.264 bitstreams, and lossless reference clips. The raw
reference files and temporary decoder output require several hundred MB.

Optional report rendering requires Matplotlib as well as OpenCV and NumPy:

```bash
python3 render.py /tmp/pebble-media-benchmark
```

Recorded results: [October 19 report](results-2026-10-19/REPORT.md).
Proposed next work: [first implementation milestone](START-HERE.md).
