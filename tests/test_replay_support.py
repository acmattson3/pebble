from __future__ import annotations

import datetime
import json
import sys
import unittest
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "web-interface" / "replay"))

from replay_support import (  # noqa: E402
    _classify_activity,
    _is_session_activity_topic,
    _merge_activity_into_pending,
    _pending_from_activity,
    _public_event_from_doc,
    _rewrite_system_id,
    ReplayArchive,
)


def _doc(topic: str, payload: Any, ts: datetime.datetime | None = None) -> dict[str, Any]:
    return {
        "topic": topic,
        "payload": json.dumps(payload).encode("utf-8"),
        "ts": ts or datetime.datetime(2026, 7, 20, 12, 0, tzinfo=datetime.timezone.utc),
    }


class FakeCursor:
    def __init__(self, docs: list[dict[str, Any]]) -> None:
        self.docs = docs

    def sort(self, *_args):
        return self

    def limit(self, limit: int):
        self.docs = self.docs[:limit]
        return self

    def __iter__(self):
        return iter(self.docs)


class FakeHistory:
    def __init__(self, docs: list[dict[str, Any]]) -> None:
        self.docs = docs

    def find(self, *_args, **_kwargs):
        return FakeCursor(self.docs)


class EventArchive(ReplayArchive):
    def __init__(self, docs: list[dict[str, Any]], activity_counts: dict[str, int] | None = None) -> None:
        super().__init__({"enabled": False})
        self.docs = docs
        self.activity_counts = activity_counts or {}

    def get_session(self, _session_id: str):
        return {
            "id": "session-1",
            "startTs": "2026-07-20T11:59:55+00:00",
            "endTs": "2026-07-20T12:00:10+00:00",
            "firstEventTs": "2026-07-20T12:00:00+00:00",
            "lastEventTs": "2026-07-20T12:00:05+00:00",
            "componentIds": ["mind"],
            "components": [{"id": "mind", "type": "robots"}],
            "activityCounts": self.activity_counts,
        }

    def _history(self):
        return FakeHistory(self.docs)


class ReplaySupportTests(unittest.TestCase):
    def test_warning_logs_do_not_qualify_sessions(self):
        activity = _classify_activity(
            _doc(
                "pebble/robots/mind/outgoing/logs",
                {
                    "level": "WARNING",
                    "service": "serial_mcu_bridge",
                    "message": "Failed to open serial port /dev/pebble-esp32c3",
                },
            )
        )
        self.assertIsNone(activity)

    def test_discovery_topic_filter_excludes_logs_and_media_payloads(self):
        self.assertTrue(_is_session_activity_topic("pebble/robots/mind/incoming/flags/mqtt-audio"))
        self.assertTrue(_is_session_activity_topic("pebble/robots/mind/outgoing/autonomy-status"))
        self.assertFalse(_is_session_activity_topic("pebble/robots/mind/outgoing/logs"))
        self.assertTrue(_is_session_activity_topic("pebble/robots/audiobox/outgoing/audio"))

    def test_republish_topic_rewrites_system_only(self):
        self.assertEqual(
            _rewrite_system_id("pebble/robots/audiobox/outgoing/audio"),
            "replay/robots/audiobox/outgoing/audio",
        )
        self.assertEqual(
            _rewrite_system_id("other/widgets/audiobox/incoming/flags/mqtt-audio"),
            "replay/widgets/audiobox/incoming/flags/mqtt-audio",
        )
        self.assertIsNone(_rewrite_system_id("short/topic"))

    def test_raw_media_packets_extend_session_without_count_spam(self):
        first = _classify_activity(_doc("pebble/robots/audiobox/outgoing/audio", {"seq": 1}))
        second = _classify_activity(
            _doc(
                "pebble/robots/audiobox/outgoing/audio",
                {"seq": 2},
                datetime.datetime(2026, 7, 20, 12, 0, 1, tzinfo=datetime.timezone.utc),
            )
        )
        pending = _pending_from_activity(first)
        _merge_activity_into_pending(pending, second)
        self.assertEqual(pending["event_count"], 1)
        self.assertEqual(pending["activity_counts"], {"audio": 1})
        self.assertEqual(pending["last_event_ts"], second["ts"])

    def test_audio_video_stop_commands_are_events(self):
        audio_event = _public_event_from_doc(_doc("pebble/robots/audiobox/incoming/audio", {"value": False}))
        video_event = _public_event_from_doc(_doc("pebble/robots/fred/incoming/front-camera", {"value": False}))
        self.assertEqual(audio_event["kind"], "audio-command")
        self.assertEqual(audio_event["state"], "off")
        self.assertEqual(video_event["kind"], "front-camera-command")
        self.assertEqual(video_event["state"], "off")

    def test_session_events_serialize_major_events(self):
        archive = EventArchive(
            [
                _doc("pebble/robots/mind/outgoing/logs", {"level": "WARNING", "message": "ignored"}),
                _doc("pebble/robots/mind/incoming/flags/mqtt-audio", {"value": True}),
                _doc("pebble/robots/mind/incoming/autonomy-command", {"action": "start", "file": "follow.py"}),
            ]
        )
        events = archive.session_events("session-1")
        self.assertEqual([event["kind"] for event in events], ["mqtt-audio", "autonomy"])
        self.assertEqual(events[0]["componentId"], "mind")
        self.assertIn("follow.py", events[1]["label"])

    def test_session_events_include_synthetic_media_boundaries(self):
        archive = EventArchive([], {"audio": 1})
        events = archive.session_events("session-1")
        self.assertEqual([event["label"] for event in events], ["mind audio start", "mind audio stop"])
        self.assertEqual([event["state"] for event in events], ["start", "stop"])


if __name__ == "__main__":
    unittest.main()
