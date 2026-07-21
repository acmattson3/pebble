from __future__ import annotations

import datetime
import json
import logging
import re
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional

try:
    from pymongo import ASCENDING, DESCENDING, MongoClient
except ImportError:  # pragma: no cover - optional dependency
    ASCENDING = None  # type: ignore[assignment]
    DESCENDING = None  # type: ignore[assignment]
    MongoClient = None


SESSION_GAP_SECONDS = 120.0
SESSION_PAD_SECONDS = 5.0
DISCOVERY_BATCH_SIZE = 5000
DISCOVERY_STATE_ID = "default"
REPLAY_PAGE_PATH = Path(__file__).resolve().parent / "replay_page.html"
SESSION_EVENT_LIMIT = 500
REPLAY_SYSTEM_ID = "replay"

SESSION_ACTIVITY_TOPIC_QUERY: Dict[str, Any] = {
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
        {"topic": {"$regex": r"/incoming/front-camera$"}},
        {"topic": {"$regex": r"/incoming/audio$"}},
        {"topic": {"$regex": r"/incoming/flags/reboot$"}},
        {"topic": {"$regex": r"/incoming/flags/service-restart$"}},
        {"topic": {"$regex": r"/incoming/flags/git-pull$"}},
    ]
}
SESSION_ACTIVITY_INCOMING_METRICS = {
    "flags/mqtt-video",
    "flags/mqtt-audio",
    "drive-values",
    "lights-solid",
    "lights-flash",
    "soundboard-command",
    "autonomy-command",
    "front-camera",
    "audio",
    "flags/reboot",
    "flags/service-restart",
    "flags/git-pull",
}
SESSION_ACTIVITY_OUTGOING_METRICS = {
    "front-camera",
    "audio",
    "soundboard-status",
    "autonomy-status",
}
MEDIA_ACTIVITY_TAGS = {"front-camera", "audio"}


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
    if len(parts) < 5:
        return None
    return {
        "system": parts[0],
        "component_type": parts[1],
        "component_id": parts[2],
        "direction": parts[3],
        "metric": "/".join(parts[4:]),
    }


def _rewrite_system_id(topic: str, system_id: str = REPLAY_SYSTEM_ID) -> Optional[str]:
    parts = topic.split("/")
    if len(parts) < 5:
        return None
    parts[0] = system_id
    return "/".join(parts)


def _is_session_activity_topic(topic: str) -> bool:
    parsed = _parse_component_topic(topic)
    if not parsed:
        return False
    metric = parsed["metric"]
    if parsed["direction"] == "incoming":
        return metric in SESSION_ACTIVITY_INCOMING_METRICS
    if parsed["direction"] == "outgoing":
        return metric in SESSION_ACTIVITY_OUTGOING_METRICS
    return False


def _is_meaningful_drive(payload: Any) -> bool:
    values = payload.get("value") if isinstance(payload, dict) and isinstance(payload.get("value"), dict) else payload
    if not isinstance(values, dict):
        return False
    for key in ("x", "z", "y", "l", "r"):
        value = _coerce_float(values.get(key))
        if value is not None and abs(value) > 0.02:
            return True
    return False


def _action_from_payload(payload: Any) -> str:
    if isinstance(payload, dict):
        action = payload.get("action")
        if isinstance(action, str) and action.strip():
            return action.strip().lower()
    return ""


def _event_state_text(value: Optional[bool]) -> str:
    if value is True:
        return "on"
    if value is False:
        return "off"
    return ""


def _event_from_activity(doc: Dict[str, Any], tags: Iterable[str]) -> Optional[Dict[str, Any]]:
    topic = str(doc.get("topic") or "")
    parsed = _parse_component_topic(topic)
    ts_epoch = _dt_to_epoch(doc.get("ts"))
    if not parsed or ts_epoch is None:
        return None
    payload = _payload_json(doc)
    metric = parsed["metric"]
    direction = parsed["direction"]
    tag_list = sorted(tags)
    primary = tag_list[0] if tag_list else metric.replace("/", "-")
    state = ""
    detail = ""

    if metric in {"flags/mqtt-video", "flags/mqtt-audio", "front-camera", "audio"}:
        state = _event_state_text(_normalize_bool_payload(payload))
    if isinstance(payload, dict):
        action = _action_from_payload(payload)
        if action:
            state = action
        file_value = payload.get("file")
        if isinstance(file_value, str) and file_value.strip():
            detail = file_value.strip()
        elif isinstance(payload.get("error"), str):
            detail = str(payload["error"]).strip()

    label_parts = [parsed["component_id"], primary.replace("-", " ")]
    if state:
        label_parts.append(state)
    if detail:
        label_parts.append(detail)
    if direction == "incoming" and primary not in {"drive", "lights"}:
        label_parts.insert(1, "command")

    return {
        "ts": ts_epoch,
        "component_id": parsed["component_id"],
        "component_type": parsed["component_type"],
        "topic": topic,
        "kind": primary,
        "state": state,
        "label": " ".join(label_parts),
    }


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
        if state is not None:
            tags.add(f"{metric}-command")
        else:
            return None
    elif metric in {"lights-solid", "lights-flash"}:
        tags.add("lights")
    elif metric == "soundboard-command":
        tags.add("soundboard")
    elif metric == "soundboard-status":
        state = _normalize_bool_payload(payload)
        if state is not None or (isinstance(payload, dict) and payload.get("error")):
            tags.add("soundboard")
        else:
            return None
    elif metric == "autonomy-command":
        tags.add("autonomy")
    elif metric == "autonomy-status":
        state = _normalize_bool_payload(payload)
        if state is not None or (isinstance(payload, dict) and payload.get("error")):
            tags.add("autonomy")
        else:
            return None
    elif metric in {"flags/reboot", "flags/service-restart", "flags/git-pull"}:
        state = _normalize_bool_payload(payload)
        if state:
            tags.add(metric.split("/")[-1])
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


def _public_event_from_doc(doc: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    activity = _classify_activity(doc)
    if activity is None:
        return None
    event = _event_from_activity(doc, activity["tags"])
    if event is None:
        return None
    return {
        "ts": _isoformat(event["ts"]),
        "componentId": event["component_id"],
        "componentType": event["component_type"],
        "topic": event["topic"],
        "kind": event["kind"],
        "state": event["state"],
        "label": event["label"],
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
    pending["component_ids"].add(component_id)
    pending["component_types"][component_id] = str(activity["component_type"])
    counts = pending["activity_counts"]
    media_tags = set(activity["tags"]) & MEDIA_ACTIVITY_TAGS
    count_event = not (media_tags and all(int(counts.get(tag, 0)) > 0 for tag in media_tags))
    if count_event:
        pending["event_count"] = int(pending["event_count"]) + 1
    for tag in activity["tags"]:
        if count_event or tag not in MEDIA_ACTIVITY_TAGS:
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
        self._republish_lock = threading.Lock()
        self._republish_job: Optional[Dict[str, Any]] = None

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

    def _systems_for_session_doc(self, doc: Dict[str, Any]) -> List[str]:
        start_s = _dt_to_epoch(doc.get("start_ts"))
        end_s = _dt_to_epoch(doc.get("end_ts"))
        if start_s is None or end_s is None:
            return []
        components = list(doc.get("components") or [])
        if not components:
            for component_id in doc.get("component_ids") or []:
                components.append({"id": component_id, "type": "robots"})
        topics = []
        for component in components:
            component_id = str(component.get("id") or "")
            component_type = str(component.get("type") or "robots")
            if not component_id:
                continue
            topics.append(
                {
                    "topic": {
                        "$regex": rf"^[^/]+/{re.escape(component_type)}/{re.escape(component_id)}/"
                    }
                }
            )
        if not topics:
            return []
        systems: set[str] = set()
        cursor = self._history().find(
            {
                "$or": topics,
                "ts": {"$gte": _epoch_to_dt(start_s), "$lte": _epoch_to_dt(end_s)},
            },
            {"topic": 1},
        ).limit(2000)
        for item in cursor:
            parsed = _parse_component_topic(str(item.get("topic") or ""))
            if parsed:
                systems.add(parsed["system"])
        return sorted(systems)

    def _default_system_for_session(self, session: Dict[str, Any]) -> str:
        systems = list(session.get("sourceSystems") or [])
        if systems:
            return str(systems[0])
        return "pebble"

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

    def _activity_topic_query(self, history: Any) -> Dict[str, Any]:
        try:
            topics = history.distinct("topic")
        except Exception:
            self.logger.exception("Failed to list replay activity topics; falling back to regex discovery query")
            return dict(SESSION_ACTIVITY_TOPIC_QUERY)
        exact_topics = sorted(str(topic) for topic in topics if _is_session_activity_topic(str(topic)))
        if not exact_topics:
            return dict(SESSION_ACTIVITY_TOPIC_QUERY)
        return {"topic": {"$in": exact_topics}}

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
            query = self._activity_topic_query(history)
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
                    "firstEventTs": _isoformat(doc.get("first_event_ts")),
                    "lastEventTs": _isoformat(doc.get("last_event_ts")),
                    "durationS": float(doc.get("duration_s") or 0.0),
                    "eventCount": int(doc.get("event_count") or 0),
                    "componentIds": list(doc.get("component_ids") or []),
                    "components": list(doc.get("components") or []),
                    "sourceSystems": self._systems_for_session_doc(doc),
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
        source_systems = self._systems_for_session_doc(doc)
        return {
            "id": str(doc.get("_id")),
            "startTs": _isoformat(doc.get("start_ts")),
            "endTs": _isoformat(doc.get("end_ts")),
            "firstEventTs": _isoformat(doc.get("first_event_ts")),
            "lastEventTs": _isoformat(doc.get("last_event_ts")),
            "durationS": float(doc.get("duration_s") or 0.0),
            "eventCount": int(doc.get("event_count") or 0),
            "componentIds": list(doc.get("component_ids") or []),
            "components": list(doc.get("components") or []),
            "sourceSystems": source_systems,
            "activityCounts": dict(doc.get("activity_counts") or {}),
        }

    def _synthetic_session_events(self, session: Dict[str, Any]) -> List[Dict[str, Any]]:
        first_ts = session.get("firstEventTs") or session.get("startTs")
        last_ts = session.get("lastEventTs") or session.get("endTs")
        events: List[Dict[str, Any]] = []
        counts = dict(session.get("activityCounts") or {})
        for component in session.get("components") or []:
            component_id = str(component.get("id") or "")
            component_type = str(component.get("type") or "robots")
            for kind in ("audio", "front-camera"):
                if int(counts.get(kind) or 0) <= 0:
                    continue
                name = "video" if kind == "front-camera" else kind
                events.append(
                    {
                        "ts": first_ts,
                        "componentId": component_id,
                        "componentType": component_type,
                        "topic": "",
                        "kind": kind,
                        "state": "start",
                        "label": f"{component_id} {name} start",
                    }
                )
                events.append(
                    {
                        "ts": last_ts,
                        "componentId": component_id,
                        "componentType": component_type,
                        "topic": "",
                        "kind": kind,
                        "state": "stop",
                        "label": f"{component_id} {name} stop",
                    }
                )
        return events

    def session_events(self, session_id: str, limit: int = SESSION_EVENT_LIMIT) -> List[Dict[str, Any]]:
        session = self.get_session(session_id)
        if not session:
            raise KeyError("Unknown session")
        start_s = _dt_to_epoch(datetime.datetime.fromisoformat(str(session["startTs"])))
        end_s = _dt_to_epoch(datetime.datetime.fromisoformat(str(session["endTs"])))
        if start_s is None or end_s is None:
            return []
        component_map = {item.get("id"): item.get("type") or "robots" for item in session.get("components") or []}
        topics = []
        for component_id, component_type in component_map.items():
            prefix = rf"^[^/]+/{re.escape(str(component_type))}/{re.escape(str(component_id))}"
            topics.extend(
                [
                    {"topic": {"$regex": f"{prefix}/incoming/flags/mqtt-video$"}},
                    {"topic": {"$regex": f"{prefix}/incoming/flags/mqtt-audio$"}},
                    {"topic": {"$regex": f"{prefix}/incoming/front-camera$"}},
                    {"topic": {"$regex": f"{prefix}/incoming/audio$"}},
                    {"topic": {"$regex": f"{prefix}/incoming/soundboard-command$"}},
                    {"topic": {"$regex": f"{prefix}/outgoing/soundboard-status$"}},
                    {"topic": {"$regex": f"{prefix}/incoming/autonomy-command$"}},
                    {"topic": {"$regex": f"{prefix}/outgoing/autonomy-status$"}},
                    {"topic": {"$regex": f"{prefix}/incoming/flags/reboot$"}},
                    {"topic": {"$regex": f"{prefix}/incoming/flags/service-restart$"}},
                    {"topic": {"$regex": f"{prefix}/incoming/flags/git-pull$"}},
                    {"topic": {"$regex": f"{prefix}/incoming/lights-solid$"}},
                    {"topic": {"$regex": f"{prefix}/incoming/lights-flash$"}},
                ]
            )
        if not topics:
            return []
        cursor = (
            self._history()
            .find(
                {
                    "$or": topics,
                    "ts": {"$gte": _epoch_to_dt(start_s), "$lte": _epoch_to_dt(end_s)},
                },
                {"ts": 1, "topic": 1, "payload": 1},
            )
            .sort("ts", 1)
            .limit(max(1, min(limit, 2000)))
        )
        events = self._synthetic_session_events(session)
        for doc in cursor:
            event = _public_event_from_doc(doc)
            if event is not None:
                events.append(event)
        return sorted(events, key=lambda event: str(event.get("ts") or ""))[: max(1, min(limit, 2000))]

    def republish_status(self) -> Dict[str, Any]:
        with self._republish_lock:
            job = self._republish_job
            if not job:
                return {"running": False}
            thread = job.get("thread")
            running = bool(thread and thread.is_alive() and not job["stop"].is_set())
            return {
                "running": running,
                "sessionId": job.get("session_id"),
                "startedAt": job.get("started_at"),
                "sourceAt": _isoformat(job.get("source_at")),
                "sourceSystem": job.get("source_system"),
                "published": int(job.get("published") or 0),
                "lastTopic": job.get("last_topic"),
                "error": job.get("error"),
            }

    def stop_republish(self) -> Dict[str, Any]:
        with self._republish_lock:
            job = self._republish_job
            if not job:
                return {"running": False}
            job["stop"].set()
        return self.republish_status()

    def start_republish(
        self,
        session_id: str,
        start_s: float,
        publish: Callable[[str, bytes, int, bool], bool],
        component_ids: Optional[List[str]] = None,
        source_system: Optional[str] = None,
    ) -> Dict[str, Any]:
        session = self.get_session(session_id)
        if not session:
            raise KeyError("Unknown session")
        selected_system = str(source_system or self._default_system_for_session(session)).strip()
        if selected_system not in set(session.get("sourceSystems") or [selected_system]):
            raise ValueError("Selected system is not present in this session")
        self.stop_republish()
        stop_event = threading.Event()
        job: Dict[str, Any] = {
            "session_id": session_id,
            "session": session,
            "source_at": _epoch_to_dt(start_s),
            "started_at": _isoformat(_utc_now()),
            "published": 0,
            "last_topic": None,
            "error": None,
            "stop": stop_event,
            "components": list(component_ids or []),
            "source_system": selected_system,
        }
        thread = threading.Thread(
            target=self._run_republish,
            args=(job, publish),
            name=f"replay-republish-{session_id}",
            daemon=True,
        )
        job["thread"] = thread
        with self._republish_lock:
            self._republish_job = job
        thread.start()
        return self.republish_status()

    def _run_republish(self, job: Dict[str, Any], publish: Callable[[str, bytes, int, bool], bool]) -> None:
        session = job["session"]
        try:
            start_s = _dt_to_epoch(job.get("source_at")) or 0.0
            session_end_s = _dt_to_epoch(datetime.datetime.fromisoformat(str(session["endTs"]))) or start_s
            component_map = {item.get("id"): item.get("type") or "robots" for item in session.get("components") or []}
            selected = list(job.get("components") or []) or list(component_map.keys())
            source_system = str(job.get("source_system") or self._default_system_for_session(session)).strip()
            topics = []
            for component_id in selected:
                component_type = component_map.get(component_id, "robots")
                topics.append(
                    {
                        "topic": {
                            "$regex": rf"^{re.escape(source_system)}/{re.escape(component_type)}/{re.escape(component_id)}/"
                        }
                    }
                )
            if not topics:
                return
            query = {
                "$or": topics,
                "ts": {"$gte": _epoch_to_dt(start_s), "$lte": _epoch_to_dt(session_end_s)},
            }
            cursor = self._history().find(query, {"ts": 1, "topic": 1, "payload": 1, "qos": 1}).sort("ts", 1)
            wall_start = time.monotonic()
            for doc in cursor:
                if job["stop"].is_set():
                    break
                source_ts = _dt_to_epoch(doc.get("ts"))
                if source_ts is None:
                    continue
                delay = (source_ts - start_s) - (time.monotonic() - wall_start)
                if delay > 0 and job["stop"].wait(delay):
                    break
                rewritten = _rewrite_system_id(str(doc.get("topic") or ""))
                payload = doc.get("payload")
                if rewritten is None or payload is None:
                    continue
                if not isinstance(payload, bytes):
                    payload = bytes(payload)
                ok = publish(rewritten, payload, int(doc.get("qos") or 0), False)
                if not ok:
                    job["error"] = f"publish failed for {rewritten}"
                    break
                job["published"] = int(job.get("published") or 0) + 1
                job["last_topic"] = rewritten
        except Exception as exc:  # pragma: no cover - runtime integration
            self.logger.exception("Replay republish failed")
            job["error"] = str(exc)
        finally:
            job["stop"].set()
