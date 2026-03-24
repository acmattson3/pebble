from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from control.services.av_daemon import AvDaemon, build_audio_pipeline, build_video_pipeline
from tests.helpers import make_base_config


class AvDaemonTests(unittest.TestCase):
    def test_build_video_pipeline_v4l2(self):
        cmd, sock = build_video_pipeline(
            {
                "backend": "v4l2",
                "device": "/dev/video2",
                "io_mode": "rw",
                "width": 800,
                "height": 600,
                "fps": 25,
                "rotate_degrees": 180,
                "socket_path": "/tmp/pb-video.sock",
                "wait_for_connection": True,
            }
        )
        self.assertEqual(cmd[0], "gst-launch-1.0")
        self.assertIn("v4l2src", cmd)
        self.assertIn("device=/dev/video2", cmd)
        self.assertIn("io-mode=rw", cmd)
        self.assertIn("video/x-raw,width=800,height=600,framerate=25/1", cmd)
        self.assertIn("videoflip", cmd)
        self.assertIn("method=rotate-180", cmd)
        self.assertIn("socket-path=/tmp/pb-video.sock", cmd)
        self.assertIn("wait-for-connection=true", cmd)
        self.assertEqual(sock, Path("/tmp/pb-video.sock"))

    def test_build_video_pipeline_libcamera(self):
        cmd, sock = build_video_pipeline(
            {
                "backend": "libcamera",
                "camera_name": "camera0",
                "camera_controls": {"AwbEnable": 1, "AeEnable": 1},
                "width": 640,
                "height": 480,
                "fps": 15,
                "socket_path": "/tmp/pb-video-libcamera.sock",
            }
        )
        self.assertIn("libcamerasrc", cmd)
        self.assertIn("camera-name=camera0", cmd)
        self.assertIn("awb-enable=true", cmd)
        self.assertIn("ae-enable=true", cmd)
        self.assertIn("socket-path=/tmp/pb-video-libcamera.sock", cmd)
        self.assertEqual(sock, Path("/tmp/pb-video-libcamera.sock"))

    def test_build_video_pipeline_arducam_backend_alias(self):
        cmd, _sock = build_video_pipeline(
            {
                "backend": "arducam",
                "width": 640,
                "height": 480,
                "fps": 15,
            }
        )
        self.assertIn("libcamerasrc", cmd)

    def test_build_video_pipeline_v4l2_mjpeg(self):
        cmd, sock = build_video_pipeline(
            {
                "backend": "v4l2",
                "capture_format": "mjpeg",
                "device": "/dev/video1",
                "width": 1280,
                "height": 720,
                "fps": 60,
                "socket_path": "/tmp/pb-video-mjpeg.sock",
            }
        )
        self.assertIn("v4l2src", cmd)
        self.assertIn("device=/dev/video1", cmd)
        self.assertIn("image/jpeg,width=1280,height=720,framerate=60/1", cmd)
        self.assertIn("jpegparse", cmd)
        self.assertIn("jpegdec", cmd)
        self.assertIn("video/x-raw,format=BGR", cmd)
        self.assertIn("socket-path=/tmp/pb-video-mjpeg.sock", cmd)
        self.assertEqual(sock, Path("/tmp/pb-video-mjpeg.sock"))

    def test_build_audio_pipeline_alsa(self):
        cmd, sock = build_audio_pipeline(
            {
                "source": "alsa",
                "device": "plughw:1,0",
                "rate": 22050,
                "channels": 2,
                "socket_path": "/tmp/pb-audio.sock",
                "wait_for_connection": False,
            }
        )
        self.assertIn("alsasrc", cmd)
        self.assertIn("device=plughw:1,0", cmd)
        self.assertIn("audio/x-raw,format=S16LE,rate=22050,channels=2,layout=interleaved", cmd)
        self.assertIn("socket-path=/tmp/pb-audio.sock", cmd)
        self.assertIn("wait-for-connection=false", cmd)
        self.assertEqual(sock, Path("/tmp/pb-audio.sock"))

    def test_disabled_daemon_exits(self):
        config = make_base_config("avbot")
        config["services"]["av_daemon"]["enabled"] = False
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.json"
            cfg_path.write_text("{}")
            with self.assertRaises(SystemExit):
                AvDaemon(config, cfg_path)


if __name__ == "__main__":
    unittest.main()
