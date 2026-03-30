from __future__ import annotations

import base64
import bisect
import datetime
import json
import logging
import math
import re
import threading
import time
import zlib
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

try:
    import cv2  # type: ignore
    import numpy as np  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    cv2 = None
    np = None

try:
    from pymongo import ASCENDING, DESCENDING, MongoClient
except ImportError:  # pragma: no cover - optional dependency
    ASCENDING = None  # type: ignore[assignment]
    DESCENDING = None  # type: ignore[assignment]
    MongoClient = None


VIDEO_SUPPORT = cv2 is not None and np is not None
AUDIO_PACKET_HEADER_SIZE = 24
AUDIO_MAGIC = b"PBAT"
SESSION_GAP_SECONDS = 120.0
SESSION_PAD_SECONDS = 5.0
VIDEO_STALE_SECONDS = 5.0
DISCOVERY_BATCH_SIZE = 5000
DISCOVERY_STATE_ID = "default"
REPLAY_PAGE_PATH = Path(__file__).resolve().parent / "replay_page.html"


def _utc_now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def _dt_to_epoch(value: Any) -> Optional[float]:
    if isinstance(value, datetime.datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=datetime.timezone.utc)
        return value.timestamp()
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _epoch_to_dt(value: float) -> datetime.datetime:
    return datetime.datetime.fromtimestamp(float(value), tz=datetime.timezone.utc)


def _isoformat(value: Any) -> Optional[str]:
    if isinstance(value, datetime.datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=datetime.timezone.utc)
        return value.isoformat()
    epoch = _dt_to_epoch(value)
    if epoch is None:
        return None
    return _epoch_to_dt(epoch).isoformat()


def _coerce_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return None


def _payload_text(doc: Dict[str, Any]) -> Optional[str]:
    payload = doc.get("payload")
    if payload is None:
        return None
    if isinstance(payload, bytes):
        try:
            return payload.decode("utf-8")
        except UnicodeDecodeError:
            return None
    if isinstance(payload, str):
        return payload
    try:
        return bytes(payload).decode("utf-8")
    except Exception:
        return None


def _payload_json(doc: Dict[str, Any]) -> Optional[Any]:
    text = _payload_text(doc)
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def _normalize_bool_payload(payload: Any) -> Optional[bool]:
    if isinstance(payload, bool):
        return payload
    if isinstance(payload, (int, float)):
        return bool(payload)
    if isinstance(payload, dict):
        for key in ("value", "enabled", "playing", "running"):
            if key in payload:
                result = _coerce_bool(payload.get(key))
                if result is not None:
                    return result
    return None


def _parse_component_topic(topic: str) -> Optional[Dict[str, str]]:
    parts = topic.split("/")
    if len(parts) < 5 or parts[0] != "pebble":
        return None
    return {
        "system": parts[0],
        "component_type": parts[1],
        "component_id": parts[2],
        "direction": parts[3],
        "metric": "/".join(parts[4:]),
    }


def _is_meaningful_drive(payload: Any) -> bool:
    values = payload.get("value") if isinstance(payload, dict) and isinstance(payload.get("value"), dict) else payload
    if not isinstance(values, dict):
        return False
    for key in ("x", "z", "y", "l", "r"):
        value = _coerce_float(values.get(key))
        if value is not None and abs(value) > 0.02:
            return True
    return False


def _classify_activity(doc: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    topic = str(doc.get("topic") or "")
    parsed = _parse_component_topic(topic)
    if not parsed:
        return None
    payload = _payload_json(doc)
    metric = parsed["metric"]
    component_id = parsed["component_id"]
    component_type = parsed["component_type"]
    tags: set[str] = set()

    if metric in {"flags/mqtt-video", "flags/mqtt-audio"}:
        state = _normalize_bool_payload(payload)
        if state is not None:
            tags.add(metric.replace("flags/", ""))
    elif metric == "drive-values":
        if _is_meaningful_drive(payload):
            tags.add("drive")
        else:
            return None
    elif metric in {"front-camera", "audio"} and parsed["direction"] == "outgoing":
        tags.add(metric)
    elif metric in {"front-camera", "audio"} and parsed["direction"] == "incoming":
        state = _normalize_bool_payload(payload)
        if state:
            tags.add(f"{metric}-command")
        else:
            return None
    elif metric in {"lights-solid", "lights-flash"}:
        tags.add("lights")
    elif metric == "soundboard-command":
        tags.add("soundboard")
    elif metric == "soundboard-status":
        state = _normalize_bool_payload(payload)
        if state or (isinstance(payload, dict) and payload.get("error")):
            tags.add("soundboard")
        else:
            return None
    elif metric == "autonomy-command":
        tags.add("autonomy")
    elif metric == "autonomy-status":
        state = _normalize_bool_payload(payload)
        if state or (isinstance(payload, dict) and payload.get("error")):
            tags.add("autonomy")
        else:
            return None
    elif metric in {"flags/reboot", "flags/service-restart", "flags/git-pull"}:
        state = _normalize_bool_payload(payload)
        if state:
            tags.add(metric.split("/")[-1])
        else:
            return None
    elif metric == "logs":
        if not isinstance(payload, dict):
            return None
        level = str(payload.get("level") or "").upper()
        if level in {"WARNING", "ERROR", "CRITICAL"}:
            tags.add("logs")
        else:
            return None
    else:
        return None

    ts_epoch = _dt_to_epoch(doc.get("ts"))
    if ts_epoch is None:
        return None
    return {
        "ts": ts_epoch,
        "component_id": component_id,
        "component_type": component_type,
        "topic": topic,
        "tags": sorted(tags),
    }


def _session_doc_from_pending(pending: Dict[str, Any]) -> Dict[str, Any]:
    first_event_ts = float(pending["first_event_ts"])
    last_event_ts = float(pending["last_event_ts"])
    start_ts = first_event_ts - SESSION_PAD_SECONDS
    end_ts = last_event_ts + SESSION_PAD_SECONDS
    component_ids = sorted(pending["component_ids"])
    component_types = pending["component_types"]
    activity_counts = dict(sorted(pending["activity_counts"].items()))
    return {
        "start_ts": _epoch_to_dt(start_ts),
        "end_ts": _epoch_to_dt(end_ts),
        "first_event_ts": _epoch_to_dt(first_event_ts),
        "last_event_ts": _epoch_to_dt(last_event_ts),
        "duration_s": max(0.0, end_ts - start_ts),
        "event_count": int(pending["event_count"]),
        "component_ids": component_ids,
        "components": [{"id": cid, "type": component_types.get(cid, "robots")} for cid in component_ids],
        "activity_counts": activity_counts,
        "created_at": _utc_now(),
        "updated_at": _utc_now(),
    }


def _pending_from_activity(activity: Dict[str, Any]) -> Dict[str, Any]:
    component_id = str(activity["component_id"])
    component_type = str(activity["component_type"])
    counts = {tag: 1 for tag in activity["tags"]}
    return {
        "first_event_ts": float(activity["ts"]),
        "last_event_ts": float(activity["ts"]),
        "event_count": 1,
        "component_ids": {component_id},
        "component_types": {component_id: component_type},
        "activity_counts": counts,
    }


def _merge_activity_into_pending(pending: Dict[str, Any], activity: Dict[str, Any]) -> None:
    component_id = str(activity["component_id"])
    pending["last_event_ts"] = float(activity["ts"])
    pending["event_count"] = int(pending["event_count"]) + 1
    pending["component_ids"].add(component_id)
    pending["component_types"][component_id] = str(activity["component_type"])
    counts = pending["activity_counts"]
    for tag in activity["tags"]:
        counts[tag] = int(counts.get(tag, 0)) + 1


class ReplayArchive:
    def __init__(self, mqtt_history_cfg: Dict[str, Any], logger: Optional[logging.Logger] = None) -> None:
        self.logger = logger or logging.getLogger(__name__)
        self.enabled = bool(mqtt_history_cfg.get("enabled")) and MongoClient is not None
        self._cfg = mqtt_history_cfg
        self._client: Optional[Any] = None
        self._db: Optional[Any] = None
        self._lock = threading.Lock()
        self._discovery_thread: Optional[threading.Thread] = None
        self._discovery_running = False
        self._discovery_error: Optional[str] = None
        self._latest_status: Dict[str, Any] = {
            "running": False,
            "error": None,
            "updated": None,
            "sessionsDiscovered": 0,
        }
        self._video_preload_lock = threading.Lock()
        self._video_preload_jobs: Dict[str, Dict[str, Any]] = {}
        self._video_preload_order: List[str] = []
        self._stream_lock = threading.Lock()
        self._active_stream_tokens: Dict[str, str] = {}

    def _connect(self) -> Optional[Any]:
        if not self.enabled:
            return None
        if self._db is not None:
            return self._db
        with self._lock:
            if self._db is not None:
                return self._db
            uri = str(self._cfg.get("uri") or "mongodb://127.0.0.1:27017").strip()
            database_name = str(self._cfg.get("database") or "pebble").strip() or "pebble"
            client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            client.admin.command("ping")
            db = client[database_name]
            self._client = client
            self._db = db
            self._ensure_indexes()
            return db

    def _ensure_indexes(self) -> None:
        db = self._db
        if db is None or ASCENDING is None or DESCENDING is None:
            return
        db[self._cfg.get("collection") or "mqtt_history"].create_index([("topic", ASCENDING), ("ts", ASCENDING)])
        db["replay_sessions"].create_index([("start_ts", DESCENDING)])
        db["replay_sessions"].create_index([("end_ts", DESCENDING)])
        db["replay_discovery_state"].create_index([("updated_at", DESCENDING)])

    def is_available(self) -> bool:
        return self._connect() is not None

    def render_page(self) -> str:
        return REPLAY_PAGE_PATH.read_text(encoding="utf-8")

    def _history(self) -> Any:
        db = self._connect()
        if db is None:
            raise RuntimeError("Replay archive unavailable")
        return db[str(self._cfg.get("collection") or "mqtt_history")]

    def _sessions(self) -> Any:
        db = self._connect()
        if db is None:
            raise RuntimeError("Replay archive unavailable")
        return db["replay_sessions"]

    def _state(self) -> Any:
        db = self._connect()
        if db is None:
            raise RuntimeError("Replay archive unavailable")
        return db["replay_discovery_state"]

    def _touch_video_job(self, session_id: str) -> None:
        if session_id in self._video_preload_order:
            self._video_preload_order.remove(session_id)
        self._video_preload_order.append(session_id)
        while len(self._video_preload_order) > 2:
            oldest = self._video_preload_order.pop(0)
            self._video_preload_jobs.pop(oldest, None)

    def start_video_preload(self, session_id: str, component_ids: Optional[List[str]] = None) -> bool:
        session = self.get_session(session_id)
        if not session or not VIDEO_SUPPORT:
            return False
        selected = component_ids or list(session.get("componentIds") or [])
        component_map = {item.get("id"): item.get("type") or "robots" for item in session.get("components") or []}
        with self._video_preload_lock:
            job = self._video_preload_jobs.get(session_id)
            if job and job.get("thread") and job["thread"].is_alive():
                for component_id in selected:
                    job["components"].setdefault(
                        component_id,
                        {
                            "type": component_map.get(component_id, "robots"),
                            "state": "queued",
                            "progress": 0.0,
                            "processedFrames": 0,
                            "totalFrames": 0,
                            "frames": [],
                            "timestamps": [],
                            "error": None,
                        },
                    )
                self._touch_video_job(session_id)
                return False
            components = {}
            for component_id in selected:
                components[component_id] = {
                    "type": component_map.get(component_id, "robots"),
                    "state": "queued",
                    "progress": 0.0,
                    "processedFrames": 0,
                    "totalFrames": 0,
                    "frames": [],
                    "timestamps": [],
                    "error": None,
                }
            job = {
                "session": session,
                "components": components,
                "startedAt": _isoformat(_utc_now()),
            }
            thread = threading.Thread(
                target=self._run_video_preload,
                args=(session_id,),
                name=f"replay-video-preload-{session_id}",
                daemon=True,
            )
            job["thread"] = thread
            self._video_preload_jobs[session_id] = job
            self._touch_video_job(session_id)
            thread.start()
            return True

    def video_preload_status(self, session_id: str) -> Dict[str, Any]:
        with self._video_preload_lock:
            job = self._video_preload_jobs.get(session_id)
            if not job:
                return {"sessionId": session_id, "components": {}, "running": False}
            components = {}
            running = False
            for component_id, info in job.get("components", {}).items():
                state = str(info.get("state") or "queued")
                if state in {"queued", "loading"}:
                    running = True
                components[component_id] = {
                    "state": state,
                    "progress": float(info.get("progress") or 0.0),
                    "processedFrames": int(info.get("processedFrames") or 0),
                    "totalFrames": int(info.get("totalFrames") or 0),
                    "error": info.get("error"),
                }
            return {"sessionId": session_id, "components": components, "running": running}

    def _run_video_preload(self, session_id: str) -> None:
        session = self.get_session(session_id)
        if not session:
            return
        start_s = _dt_to_epoch(datetime.datetime.fromisoformat(str(session["startTs"]))) or 0.0
        end_s = _dt_to_epoch(datetime.datetime.fromisoformat(str(session["endTs"]))) or start_s
        history = self._history()
        component_map = {item.get("id"): item.get("type") or "robots" for item in session.get("components") or []}

        for component_id, current_info in list((self._video_preload_jobs.get(session_id) or {}).get("components", {}).items()):
            component_type = component_map.get(component_id, current_info.get("type") or "robots")
            topic = f"pebble/{component_type}/{component_id}/outgoing/front-camera"
            docs = list(
                history.find(
                    {"topic": topic, "ts": {"$gte": _epoch_to_dt(start_s), "$lte": _epoch_to_dt(end_s)}},
                    {"ts": 1, "payload": 1},
                ).sort("ts", 1)
            )
            with self._video_preload_lock:
                job = self._video_preload_jobs.get(session_id)
                if not job:
                    return
                info = job["components"].setdefault(component_id, {})
                info["totalFrames"] = len(docs)
                info["state"] = "loading"

            if not docs:
                with self._video_preload_lock:
                    job = self._video_preload_jobs.get(session_id)
                    if not job:
                        return
                    info = job["components"].setdefault(component_id, {})
                    info["state"] = "no-video"
                    info["progress"] = 100.0
                continue

            last_frame = None
            frames: List[bytes] = []
            timestamps: List[float] = []
            try:
                for index, doc in enumerate(docs, start=1):
                    packet = _payload_json(doc)
                    if not isinstance(packet, dict) or not packet.get("data"):
                        continue
                    compressed = base64.b64decode(packet["data"])
                    jpeg_bytes = zlib.decompress(compressed)
                    frame_array = np.frombuffer(jpeg_bytes, dtype=np.uint8)
                    packet_frame = cv2.imdecode(frame_array, cv2.IMREAD_COLOR)
                    if packet_frame is None:
                        continue
                    if bool(packet.get("keyframe")) or last_frame is None:
                        reconstructed = packet_frame
                    else:
                        delta = packet_frame.astype(np.int16) - 128
                        reconstructed = np.clip(last_frame.astype(np.int16) + delta, 0, 255).astype(np.uint8)
                    last_frame = reconstructed
                    ok, encoded = cv2.imencode(".jpg", reconstructed)
                    if not ok:
                        continue
                    frames.append(encoded.tobytes())
                    timestamps.append(float(_dt_to_epoch(doc.get("ts")) or start_s))
                    with self._video_preload_lock:
                        job = self._video_preload_jobs.get(session_id)
                        if not job:
                            return
                        info = job["components"].setdefault(component_id, {})
                        info["frames"] = frames
                        info["timestamps"] = timestamps
                        info["processedFrames"] = index
                        info["progress"] = 100.0 * (index / max(1, len(docs)))
                with self._video_preload_lock:
                    job = self._video_preload_jobs.get(session_id)
                    if not job:
                        return
                    info = job["components"].setdefault(component_id, {})
                    info["state"] = "ready"
                    info["progress"] = 100.0
            except Exception as exc:
                with self._video_preload_lock:
                    job = self._video_preload_jobs.get(session_id)
                    if not job:
                        return
                    info = job["components"].setdefault(component_id, {})
                    info["state"] = "error"
                    info["error"] = str(exc)

    def start_discovery(self) -> bool:
        if not self.is_available():
            return False
        with self._lock:
            if self._discovery_thread is not None and self._discovery_thread.is_alive():
                return False
            thread = threading.Thread(target=self._run_discovery, name="replay-session-discovery", daemon=True)
            self._discovery_thread = thread
            self._discovery_running = True
            self._discovery_error = None
            self._latest_status["running"] = True
            self._latest_status["error"] = None
            thread.start()
            return True

    def discovery_status(self) -> Dict[str, Any]:
        status = dict(self._latest_status)
        status["running"] = bool(self._discovery_thread and self._discovery_thread.is_alive())
        state_doc = self._state().find_one({"_id": DISCOVERY_STATE_ID}) if self.is_available() else None
        if state_doc:
            status["lastScannedTs"] = _isoformat(state_doc.get("last_scanned_ts"))
            status["pendingSession"] = bool(state_doc.get("pending_session"))
        return status

    def _run_discovery(self) -> None:
        discovered = 0
        try:
            history = self._history()
            sessions = self._sessions()
            state_coll = self._state()
            state = state_coll.find_one({"_id": DISCOVERY_STATE_ID}) or {"_id": DISCOVERY_STATE_ID}
            last_scanned_ts = state.get("last_scanned_ts")
            pending = state.get("pending_session")
            if pending:
                pending["component_ids"] = set(pending.get("component_ids") or [])
                pending["component_types"] = dict(pending.get("component_types") or {})
                pending["activity_counts"] = dict(pending.get("activity_counts") or {})
            query: Dict[str, Any] = {
                "$or": [
                    {"topic": {"$regex": r"/incoming/flags/mqtt-video$"}},
                    {"topic": {"$regex": r"/incoming/flags/mqtt-audio$"}},
                    {"topic": {"$regex": r"/incoming/drive-values$"}},
                    {"topic": {"$regex": r"/incoming/lights-solid$"}},
                    {"topic": {"$regex": r"/incoming/lights-flash$"}},
                    {"topic": {"$regex": r"/incoming/soundboard-command$"}},
                    {"topic": {"$regex": r"/outgoing/soundboard-status$"}},
                    {"topic": {"$regex": r"/incoming/autonomy-command$"}},
                    {"topic": {"$regex": r"/outgoing/autonomy-status$"}},
                    {"topic": {"$regex": r"/outgoing/logs$"}},
                    {"topic": {"$regex": r"/incoming/front-camera$"}},
                    {"topic": {"$regex": r"/incoming/audio$"}},
                    {"topic": {"$regex": r"/incoming/flags/reboot$"}},
                    {"topic": {"$regex": r"/incoming/flags/service-restart$"}},
                    {"topic": {"$regex": r"/incoming/flags/git-pull$"}},
                ]
            }
            if isinstance(last_scanned_ts, datetime.datetime):
                query["ts"] = {"$gt": last_scanned_ts}

            newest_ts: Optional[datetime.datetime] = None
            cursor = history.find(query, {"ts": 1, "topic": 1, "payload": 1}).sort("ts", ASCENDING or 1).batch_size(
                DISCOVERY_BATCH_SIZE
            )
            for doc in cursor:
                newest_ts = doc.get("ts")
                activity = _classify_activity(doc)
                if activity is None:
                    continue
                if pending is None:
                    pending = _pending_from_activity(activity)
                    continue
                if float(activity["ts"]) - float(pending["last_event_ts"]) <= SESSION_GAP_SECONDS:
                    _merge_activity_into_pending(pending, activity)
                    continue
                sessions.insert_one(_session_doc_from_pending(pending))
                discovered += 1
                pending = _pending_from_activity(activity)

            now_epoch = time.time()
            if pending is not None and (now_epoch - float(pending["last_event_ts"])) > SESSION_GAP_SECONDS:
                sessions.insert_one(_session_doc_from_pending(pending))
                discovered += 1
                pending = None

            state_update = {
                "_id": DISCOVERY_STATE_ID,
                "last_scanned_ts": newest_ts or last_scanned_ts,
                "pending_session": None
                if pending is None
                else {
                    "first_event_ts": float(pending["first_event_ts"]),
                    "last_event_ts": float(pending["last_event_ts"]),
                    "event_count": int(pending["event_count"]),
                    "component_ids": sorted(pending["component_ids"]),
                    "component_types": dict(pending["component_types"]),
                    "activity_counts": dict(pending["activity_counts"]),
                },
                "updated_at": _utc_now(),
            }
            state_coll.replace_one({"_id": DISCOVERY_STATE_ID}, state_update, upsert=True)
            self._latest_status.update(
                {
                    "running": False,
                    "error": None,
                    "updated": _isoformat(_utc_now()),
                    "sessionsDiscovered": discovered,
                }
            )
        except Exception as exc:  # pragma: no cover - runtime integration
            self.logger.exception("Replay session discovery failed")
            self._discovery_error = str(exc)
            self._latest_status.update({"running": False, "error": str(exc), "updated": _isoformat(_utc_now())})
        finally:
            self._discovery_running = False

    def list_sessions(self, limit: int = 250, before: Optional[datetime.datetime] = None) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        if not self.is_available():
            return results
        query: Dict[str, Any] = {}
        if isinstance(before, datetime.datetime):
            query["start_ts"] = {"$lte": before}
        cursor = (
            self._sessions()
            .find(query, {"created_at": 0})
            .sort("start_ts", -1)
            .limit(max(1, min(limit, 1000)))
        )
        for doc in cursor:
            results.append(
                {
                    "id": str(doc.get("_id")),
                    "startTs": _isoformat(doc.get("start_ts")),
                    "endTs": _isoformat(doc.get("end_ts")),
                    "durationS": float(doc.get("duration_s") or 0.0),
                    "eventCount": int(doc.get("event_count") or 0),
                    "componentIds": list(doc.get("component_ids") or []),
                    "components": list(doc.get("components") or []),
                    "activityCounts": dict(doc.get("activity_counts") or {}),
                }
            )
        return results

    def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        if not self.is_available():
            return None
        try:
            from bson import ObjectId  # type: ignore
            doc = self._sessions().find_one({"_id": ObjectId(session_id)})
        except Exception:
            return None
        if not doc:
            return None
        return {
            "id": str(doc.get("_id")),
            "startTs": _isoformat(doc.get("start_ts")),
            "endTs": _isoformat(doc.get("end_ts")),
            "durationS": float(doc.get("duration_s") or 0.0),
            "eventCount": int(doc.get("event_count") or 0),
            "componentIds": list(doc.get("component_ids") or []),
            "components": list(doc.get("components") or []),
            "activityCounts": dict(doc.get("activity_counts") or {}),
        }

    def frame_step_time(self, session_id: str, at_s: float, direction: str, component_ids: Iterable[str]) -> Optional[float]:
        if not self.is_available():
            return None
        session = self.get_session(session_id)
        if not session:
            return None
        candidates = list(component_ids) or list(session.get("componentIds") or [])
        with self._video_preload_lock:
            job = self._video_preload_jobs.get(session_id)
            if job:
                best_cached: Optional[float] = None
                for component_id in candidates:
                    info = (job.get("components") or {}).get(component_id) or {}
                    timestamps = list(info.get("timestamps") or [])
                    if not timestamps:
                        continue
                    if direction == "next":
                        idx = bisect.bisect_right(timestamps, at_s)
                        if idx >= len(timestamps):
                            continue
                        candidate = timestamps[idx]
                        best_cached = candidate if best_cached is None else min(best_cached, candidate)
                    else:
                        idx = bisect.bisect_left(timestamps, at_s) - 1
                        if idx < 0:
                            continue
                        candidate = timestamps[idx]
                        best_cached = candidate if best_cached is None else max(best_cached, candidate)
                if best_cached is not None:
                    return best_cached
        history = self._history()
        best: Optional[float] = None
        sort_dir = 1 if direction == "next" else -1
        comparator = "$gt" if direction == "next" else "$lt"
        component_map = {item.get("id"): item.get("type") or "robots" for item in session.get("components") or []}
        for component_id in candidates:
            component_type = component_map.get(component_id, "robots")
            topic = f"pebble/{component_type}/{component_id}/outgoing/front-camera"
            doc = history.find_one(
                {"topic": topic, "ts": {comparator: _epoch_to_dt(at_s)}},
                {"ts": 1},
                sort=[("ts", sort_dir)],
            )
            epoch = _dt_to_epoch(doc.get("ts")) if doc else None
            if epoch is None:
                continue
            if best is None:
                best = epoch
            elif direction == "next":
                best = min(best, epoch)
            else:
                best = max(best, epoch)
        return best

    def _latest_doc_before(self, topic: str, at_s: float) -> Optional[Dict[str, Any]]:
        return self._history().find_one({"topic": topic, "ts": {"$lte": _epoch_to_dt(at_s)}}, sort=[("ts", -1)])

    def _recent_logs(self, component_id: str, component_type: str, session_start_s: float, at_s: float, limit: int = 12) -> List[Dict[str, Any]]:
        topic = f"pebble/{component_type}/{component_id}/outgoing/logs"
        cursor = (
            self._history()
            .find({"topic": topic, "ts": {"$gte": _epoch_to_dt(session_start_s), "$lte": _epoch_to_dt(at_s)}})
            .sort("ts", -1)
            .limit(max(1, limit))
        )
        items: List[Dict[str, Any]] = []
        for doc in cursor:
            payload = _payload_json(doc)
            items.append(
                {
                    "ts": _isoformat(doc.get("ts")),
                    "level": payload.get("level") if isinstance(payload, dict) else None,
                    "service": payload.get("service") if isinstance(payload, dict) else None,
                    "message": payload.get("message") if isinstance(payload, dict) else (_payload_text(doc) or ""),
                }
            )
        items.reverse()
        return items

    def _heartbeat_state(self, component_id: str, component_type: str, at_s: float) -> Dict[str, Any]:
        topic = f"pebble/{component_type}/{component_id}/outgoing/online"
        cursor = list(
            self._history()
            .find({"topic": topic, "ts": {"$lte": _epoch_to_dt(at_s)}}, {"ts": 1, "payload": 1})
            .sort("ts", -1)
            .limit(8)
        )
        if not cursor:
            return {"status": "unknown", "online": None, "ageS": None}
        times = [_dt_to_epoch(doc.get("ts")) for doc in reversed(cursor)]
        times = [t for t in times if t is not None]
        latest = times[-1] if times else None
        if latest is None:
            return {"status": "unknown", "online": None, "ageS": None}
        avg_interval = None
        if len(times) >= 2:
            diffs = [b - a for a, b in zip(times[:-1], times[1:]) if b > a]
            if diffs:
                avg_interval = sum(diffs) / len(diffs)
        window = max(1.0, 4.0 * avg_interval) if avg_interval is not None else 2.0
        age = max(0.0, at_s - latest)
        online = age <= window
        return {
            "status": "online" if online else "offline",
            "online": online,
            "ageS": age,
            "intervalS": avg_interval,
        }

    def _capabilities_state(self, component_id: str, component_type: str, at_s: float) -> Dict[str, Any]:
        topic = f"pebble/{component_type}/{component_id}/outgoing/capabilities"
        doc = self._latest_doc_before(topic, at_s)
        payload = _payload_json(doc or {}) if doc else None
        value = payload.get("value") if isinstance(payload, dict) and isinstance(payload.get("value"), dict) else payload
        return value if isinstance(value, dict) else {}

    def _component_snapshot(self, component_id: str, component_type: str, at_s: float, session_start_s: float) -> Dict[str, Any]:
        capabilities = self._capabilities_state(component_id, component_type, at_s)
        prefix = f"pebble/{component_type}/{component_id}"
        charging_doc = self._latest_doc_before(f"{prefix}/outgoing/charging-status", at_s)
        charge_level_doc = self._latest_doc_before(f"{prefix}/outgoing/charging-level", at_s)
        drive_doc = self._latest_doc_before(f"{prefix}/incoming/drive-values", at_s)
        light_solid_doc = self._latest_doc_before(f"{prefix}/incoming/lights-solid", at_s)
        light_flash_doc = self._latest_doc_before(f"{prefix}/incoming/lights-flash", at_s)
        autonomy_doc = self._latest_doc_before(f"{prefix}/outgoing/autonomy-status", at_s)
        soundboard_doc = self._latest_doc_before(f"{prefix}/outgoing/soundboard-status", at_s)
        video_flag_doc = self._latest_doc_before(f"{prefix}/incoming/flags/mqtt-video", at_s)
        audio_flag_doc = self._latest_doc_before(f"{prefix}/incoming/flags/mqtt-audio", at_s)
        odom_doc = self._latest_doc_before(f"{prefix}/outgoing/wheel-odometry", at_s)
        overlay_doc = self._latest_doc_before(f"{prefix}/outgoing/video-overlays", at_s)
        audio_doc = self._latest_doc_before(f"{prefix}/outgoing/audio", at_s)
        video_doc = self._latest_doc_before(f"{prefix}/outgoing/front-camera", at_s)

        charging_payload = _payload_json(charging_doc or {}) if charging_doc else None
        charge_level_payload = _payload_json(charge_level_doc or {}) if charge_level_doc else None
        drive_payload = _payload_json(drive_doc or {}) if drive_doc else None
        light_solid_payload = _payload_json(light_solid_doc or {}) if light_solid_doc else None
        light_flash_payload = _payload_json(light_flash_doc or {}) if light_flash_doc else None
        autonomy_payload = _payload_json(autonomy_doc or {}) if autonomy_doc else None
        soundboard_payload = _payload_json(soundboard_doc or {}) if soundboard_doc else None
        odom_payload = _payload_json(odom_doc or {}) if odom_doc else None
        overlay_payload = _payload_json(overlay_doc or {}) if overlay_doc else None

        latest_light_doc = None
        latest_light_payload: Optional[Dict[str, Any]] = None
        light_mode = None
        for candidate_doc, mode, candidate_payload in (
            (light_solid_doc, "solid", light_solid_payload),
            (light_flash_doc, "flash", light_flash_payload),
        ):
            if candidate_doc is None:
                continue
            if latest_light_doc is None or (candidate_doc.get("ts") and candidate_doc.get("ts") > latest_light_doc.get("ts")):
                latest_light_doc = candidate_doc
                latest_light_payload = candidate_payload if isinstance(candidate_payload, dict) else None
                light_mode = mode

        drive_values = drive_payload.get("value") if isinstance(drive_payload, dict) and isinstance(drive_payload.get("value"), dict) else drive_payload
        if not isinstance(drive_values, dict):
            drive_values = {}
        drive_age = None
        if drive_doc and drive_doc.get("ts"):
            drive_age = at_s - float(_dt_to_epoch(drive_doc.get("ts")) or at_s)
        if drive_age is not None and drive_age > 1.0:
            drive_values = {"x": 0.0, "z": 0.0, "y": 0.0}

        heartbeat = self._heartbeat_state(component_id, component_type, at_s)
        video_flag = _normalize_bool_payload(_payload_json(video_flag_doc or {}) if video_flag_doc else None)
        audio_flag = _normalize_bool_payload(_payload_json(audio_flag_doc or {}) if audio_flag_doc else None)
        video_active = bool(video_flag) or (
            video_doc is not None and (_dt_to_epoch(video_doc.get("ts")) or 0.0) >= (at_s - 2.0)
        )
        audio_active = bool(audio_flag) or (
            audio_doc is not None and (_dt_to_epoch(audio_doc.get("ts")) or 0.0) >= (at_s - 1.0)
        )
        charging_value = None
        if isinstance(charging_payload, dict):
            charging_value = _normalize_bool_payload(charging_payload)
        battery_voltage = None
        if isinstance(charge_level_payload, dict):
            value = charge_level_payload.get("value")
            if isinstance(value, (int, float)):
                battery_voltage = float(value)

        autonomy_state = autonomy_payload.get("value") if isinstance(autonomy_payload, dict) and isinstance(autonomy_payload.get("value"), dict) else autonomy_payload
        soundboard_state = soundboard_payload.get("value") if isinstance(soundboard_payload, dict) and isinstance(soundboard_payload.get("value"), dict) else soundboard_payload
        odom_values = odom_payload.get("value") if isinstance(odom_payload, dict) and isinstance(odom_payload.get("value"), dict) else odom_payload

        return {
            "id": component_id,
            "name": component_id,
            "type": component_type,
            "heartbeat": heartbeat,
            "charging": charging_value,
            "batteryVoltage": battery_voltage,
            "drive": {
                "x": float(drive_values.get("x") or 0.0),
                "z": float(drive_values.get("z") or 0.0),
                "y": float(drive_values.get("y") or 0.0),
                "active": _is_meaningful_drive(drive_values),
            },
            "lights": {
                "mode": light_mode,
                "value": latest_light_payload or {},
            },
            "video": {
                "available": bool(capabilities.get("video")) or video_doc is not None,
                "active": video_active,
                "displayed": False,
                "lastFrameTs": _isoformat(video_doc.get("ts")) if video_doc else None,
            },
            "audio": {
                "available": bool(capabilities.get("audio")) or audio_doc is not None,
                "active": audio_active,
                "selected": False,
                "lastPacketTs": _isoformat(audio_doc.get("ts")) if audio_doc else None,
            },
            "autonomy": autonomy_state if isinstance(autonomy_state, dict) else {},
            "soundboard": soundboard_state if isinstance(soundboard_state, dict) else {},
            "odometry": odom_values if isinstance(odom_values, dict) else {},
            "overlays": overlay_payload if isinstance(overlay_payload, dict) else {},
            "logs": self._recent_logs(component_id, component_type, session_start_s, at_s),
        }

    def snapshot(self, session_id: str, at_s: float, component_ids: Optional[List[str]] = None) -> Dict[str, Any]:
        session = self.get_session(session_id)
        if not session:
            raise KeyError("Unknown session")
        selected = component_ids or list(session.get("componentIds") or [])
        session_start_s = _dt_to_epoch(datetime.datetime.fromisoformat(str(session["startTs"]))) or at_s
        component_map = {item.get("id"): item.get("type") or "robots" for item in session.get("components") or []}
        components = [
            self._component_snapshot(component_id, component_map.get(component_id, "robots"), at_s, session_start_s)
            for component_id in selected
        ]
        for component in components:
            component["video"]["displayed"] = True
            component["audio"]["selected"] = False
        return {
            "session": session,
            "at": _isoformat(at_s),
            "components": components,
        }

    def reconstruct_video_frame(self, component_id: str, at_s: float, session_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        if not self.is_available() or not VIDEO_SUPPORT:
            return None
        if session_id:
            with self._video_preload_lock:
                job = self._video_preload_jobs.get(session_id)
                if job:
                    info = (job.get("components") or {}).get(component_id) or {}
                    timestamps = list(info.get("timestamps") or [])
                    frames = list(info.get("frames") or [])
                    if timestamps and frames:
                        index = bisect.bisect_right(timestamps, at_s) - 1
                        if index >= 0 and (at_s - timestamps[index]) <= VIDEO_STALE_SECONDS:
                            return {
                                "jpeg": frames[index],
                                "metadata": {
                                    "sourceTs": _isoformat(timestamps[index]),
                                    "preloaded": True,
                                },
                            }
        topic_pattern = re.compile(rf"^pebble/[^/]+/{re.escape(component_id)}/outgoing/front-camera$")
        history = self._history()
        cursor = (
            history.find({"topic": {"$regex": topic_pattern.pattern}, "ts": {"$lte": _epoch_to_dt(at_s)}}, {"ts": 1, "payload": 1})
            .sort("ts", -1)
            .limit(2000)
        )
        docs = list(cursor)
        docs.reverse()
        last_keyframe_index = None
        decoded_packets: List[tuple[Dict[str, Any], Dict[str, Any]]] = []
        for index, doc in enumerate(docs):
            packet = _payload_json(doc)
            if isinstance(packet, dict):
                decoded_packets.append((doc, packet))
                if bool(packet.get("keyframe")):
                    last_keyframe_index = index
        if last_keyframe_index is None:
            return None
        frame = None
        metadata: Dict[str, Any] = {}
        latest_frame_ts: Optional[float] = None
        for doc, packet in decoded_packets[last_keyframe_index:]:
            encoded = packet.get("data")
            if not encoded:
                continue
            try:
                compressed = base64.b64decode(encoded)
                jpeg_bytes = zlib.decompress(compressed)
            except (base64.binascii.Error, zlib.error):
                continue
            frame_array = np.frombuffer(jpeg_bytes, dtype=np.uint8)
            packet_frame = cv2.imdecode(frame_array, cv2.IMREAD_COLOR)
            if packet_frame is None:
                continue
            if bool(packet.get("keyframe")) or frame is None:
                frame = packet_frame
            else:
                delta = packet_frame.astype(np.int16) - 128
                frame = np.clip(frame.astype(np.int16) + delta, 0, 255).astype(np.uint8)
            metadata = {
                "sourceTs": _isoformat(doc.get("ts")),
                "packetTimestamp": packet.get("timestamp"),
                "id": packet.get("id"),
                "keyframe": bool(packet.get("keyframe")),
            }
            latest_frame_ts = _dt_to_epoch(doc.get("ts"))
        if frame is None or latest_frame_ts is None or (at_s - latest_frame_ts) > VIDEO_STALE_SECONDS:
            return None
        ok, encoded_frame = cv2.imencode(".jpg", frame)
        if not ok:
            return None
        return {"jpeg": encoded_frame.tobytes(), "metadata": metadata}

    def _decode_replay_packet(
        self, packet: Dict[str, Any], last_frame: Optional[Any]
    ) -> Optional[tuple[bytes, Any, Dict[str, Any]]]:
        encoded = packet.get("data")
        if not encoded:
            return None
        try:
            compressed = base64.b64decode(encoded)
            jpeg_bytes = zlib.decompress(compressed)
        except (base64.binascii.Error, zlib.error):
            return None
        frame_array = np.frombuffer(jpeg_bytes, dtype=np.uint8)
        frame = cv2.imdecode(frame_array, cv2.IMREAD_COLOR)
        if frame is None:
            return None
        is_keyframe = bool(packet.get("keyframe"))
        if not is_keyframe and last_frame is None:
            return None
        if is_keyframe or last_frame is None:
            reconstructed = frame
        else:
            delta = frame.astype(np.int16) - 128
            reconstructed = np.clip(last_frame.astype(np.int16) + delta, 0, 255).astype(np.uint8)
        ok, encoded_frame = cv2.imencode(".jpg", reconstructed)
        if not ok:
            return None
        metadata = {
            "id": packet.get("id"),
            "keyframe": is_keyframe,
            "timestamp": packet.get("timestamp"),
        }
        return encoded_frame.tobytes(), reconstructed, metadata

    def replay_video_stream(self, session_id: str, component_id: str, start_s: float, token: str):
        if not self.is_available() or not VIDEO_SUPPORT:
            return
        session = self.get_session(session_id)
        if not session:
            return
        self.register_stream_token(session_id, component_id, token)
        component_map = {item.get("id"): item.get("type") or "robots" for item in session.get("components") or []}
        component_type = component_map.get(component_id, "robots")
        topic = f"pebble/{component_type}/{component_id}/outgoing/front-camera"
        end_s = _dt_to_epoch(datetime.datetime.fromisoformat(str(session["endTs"]))) or start_s
        history = self._history()

        previous_docs = list(
            history.find({"topic": topic, "ts": {"$lte": _epoch_to_dt(start_s)}}, {"ts": 1, "payload": 1})
            .sort("ts", -1)
            .limit(1000)
        )
        previous_docs.reverse()
        seed_docs: List[Dict[str, Any]] = []
        last_keyframe_index = None
        for index, doc in enumerate(previous_docs):
            packet = _payload_json(doc)
            if isinstance(packet, dict) and bool(packet.get("keyframe")):
                last_keyframe_index = index
        if last_keyframe_index is not None:
            seed_docs = previous_docs[last_keyframe_index:]

        boundary = b"--frame\r\n"
        last_frame = None
        seeded_frame: Optional[bytes] = None
        seeded_frame_ts: Optional[float] = None

        try:
            for doc in seed_docs:
                if not self.stream_token_active(session_id, component_id, token):
                    return
                packet = _payload_json(doc)
                if not isinstance(packet, dict):
                    continue
                decoded = self._decode_replay_packet(packet, last_frame)
                if decoded is None:
                    continue
                jpeg_bytes, last_frame, _metadata = decoded
                seeded_frame = jpeg_bytes
                seeded_frame_ts = _dt_to_epoch(doc.get("ts"))

            if seeded_frame is not None and seeded_frame_ts is not None and (start_s - seeded_frame_ts) <= VIDEO_STALE_SECONDS:
                if not self.stream_token_active(session_id, component_id, token):
                    return
                yield (
                    boundary
                    + b"Content-Type: image/jpeg\r\n"
                    + f"Content-Length: {len(seeded_frame)}\r\n\r\n".encode("ascii")
                    + seeded_frame
                    + b"\r\n"
                )

            future_docs = history.find(
                {"topic": topic, "ts": {"$gt": _epoch_to_dt(start_s), "$lte": _epoch_to_dt(end_s)}},
                {"ts": 1, "payload": 1},
            ).sort("ts", 1)

            wall_start = time.monotonic()
            for doc in future_docs:
                if not self.stream_token_active(session_id, component_id, token):
                    return
                frame_ts = _dt_to_epoch(doc.get("ts"))
                packet = _payload_json(doc)
                if frame_ts is None or not isinstance(packet, dict):
                    continue
                decoded = self._decode_replay_packet(packet, last_frame)
                if decoded is None:
                    continue
                jpeg_bytes, last_frame, _metadata = decoded
                target_offset = max(0.0, frame_ts - start_s)
                elapsed = time.monotonic() - wall_start
                delay = target_offset - elapsed
                if delay > 0:
                    time.sleep(min(delay, 0.1))
                    if not self.stream_token_active(session_id, component_id, token):
                        return
                yield (
                    boundary
                    + b"Content-Type: image/jpeg\r\n"
                    + f"Content-Length: {len(jpeg_bytes)}\r\n\r\n".encode("ascii")
                    + jpeg_bytes
                    + b"\r\n"
                )
        finally:
            self.clear_stream_token(session_id, component_id, token)

    def _stream_key(self, session_id: str, component_id: str) -> str:
        return f"{session_id}:{component_id}"

    def register_stream_token(self, session_id: str, component_id: str, token: str) -> None:
        with self._stream_lock:
            self._active_stream_tokens[self._stream_key(session_id, component_id)] = token

    def stream_token_active(self, session_id: str, component_id: str, token: str) -> bool:
        with self._stream_lock:
            return self._active_stream_tokens.get(self._stream_key(session_id, component_id)) == token

    def clear_stream_token(self, session_id: str, component_id: str, token: str) -> None:
        with self._stream_lock:
            key = self._stream_key(session_id, component_id)
            if self._active_stream_tokens.get(key) == token:
                self._active_stream_tokens.pop(key, None)

    def audio_chunks(self, component_id: str, start_s: float, end_s: float, limit: int = 150) -> List[Dict[str, Any]]:
        topic_pattern = re.compile(rf"^pebble/[^/]+/{re.escape(component_id)}/outgoing/audio$")
        cursor = (
            self._history()
            .find({"topic": {"$regex": topic_pattern.pattern}, "ts": {"$gte": _epoch_to_dt(start_s), "$lte": _epoch_to_dt(end_s)}})
            .sort("ts", 1)
            .limit(max(1, min(limit, 500)))
        )
        chunks: List[Dict[str, Any]] = []
        for doc in cursor:
            payload = doc.get("payload")
            if not isinstance(payload, bytes) or len(payload) < AUDIO_PACKET_HEADER_SIZE:
                continue
            header = payload[:AUDIO_PACKET_HEADER_SIZE]
            body = payload[AUDIO_PACKET_HEADER_SIZE:]
            try:
                magic = header[:4]
                version = header[4]
                codec = header[5]
                channels = header[6]
                rate = int.from_bytes(header[8:10], "big")
                frame_samples = int.from_bytes(header[10:12], "big")
                seq = int.from_bytes(header[12:16], "big")
                timestamp_ms = int.from_bytes(header[16:24], "big")
            except Exception:
                continue
            if magic != AUDIO_MAGIC or version != 1 or codec != 1:
                continue
            chunks.append(
                {
                    "ts": _isoformat(doc.get("ts")),
                    "timestampMs": timestamp_ms,
                    "seq": seq,
                    "rate": rate,
                    "channels": channels,
                    "frameSamples": frame_samples,
                    "data": base64.b64encode(body).decode("ascii"),
                }
            )
        return chunks
