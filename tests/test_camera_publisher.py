from __future__ import annotations

import sys
import unittest
from pathlib import Path

import numpy as np


REPO_ROOT = Path(__file__).resolve().parents[1]
VIDEO_DIR = REPO_ROOT / "video"
if str(VIDEO_DIR) not in sys.path:
    sys.path.insert(0, str(VIDEO_DIR))

import camera_publisher


class CameraPublisherDeltaTests(unittest.TestCase):
    def test_reconstructed_reference_allows_saturated_pixels_to_continue_brightening(self):
        reference = np.zeros((1, 1, 3), dtype=np.uint8)
        current = np.full((1, 1, 3), 255, dtype=np.uint8)

        delta = camera_publisher._prepare_frame(current, reference, is_keyframe=False)
        reference = camera_publisher._reconstruct_published_frame(reference, delta, is_keyframe=False)
        self.assertTrue(np.array_equal(reference, np.full((1, 1, 3), 127, dtype=np.uint8)))

        delta = camera_publisher._prepare_frame(current, reference, is_keyframe=False)
        reference = camera_publisher._reconstruct_published_frame(reference, delta, is_keyframe=False)
        self.assertTrue(np.array_equal(reference, np.full((1, 1, 3), 254, dtype=np.uint8)))

        delta = camera_publisher._prepare_frame(current, reference, is_keyframe=False)
        reference = camera_publisher._reconstruct_published_frame(reference, delta, is_keyframe=False)
        self.assertTrue(np.array_equal(reference, current))

    def test_keyframe_replaces_reference_frame(self):
        prior = np.zeros((1, 1, 3), dtype=np.uint8)
        keyframe = np.array([[[12, 34, 56]]], dtype=np.uint8)

        reference = camera_publisher._reconstruct_published_frame(prior, keyframe, is_keyframe=True)

        self.assertTrue(np.array_equal(reference, keyframe))
        self.assertIsNot(reference, keyframe)


if __name__ == "__main__":
    unittest.main()
