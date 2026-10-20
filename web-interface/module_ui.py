"""Generic, schema-validated module discovery and command gateway."""
from __future__ import annotations

from copy import deepcopy
import json
import logging
import hashlib
import re
from pathlib import Path
import threading
import time

from flask import Blueprint, jsonify, request, send_from_directory
from jsonschema import ValidationError, SchemaError

from control.common.modules import MAX_MESSAGE, validate_manifest, validate_payload


class ModuleUI:
    def __init__(self, app, publish, connected):
        self.publish, self.connected = publish, connected
        self.lock = threading.RLock()
        self.robots, self.routes = {}, {}
        assets = Path(__file__).resolve().parent / "module-assets"
        bp = Blueprint("modules", __name__)
        from module_media import ModuleMedia
        self.media = ModuleMedia(self, bp)

        @bp.get("/modules")
        def page():
            return send_from_directory(assets, "index.html")

        @bp.get("/modules/assets/<path:name>")
        def asset(name):
            return send_from_directory(assets, name)

        @bp.get("/modules/api/robots")
        def robots():
            with self.lock:
                return jsonify(connected=bool(self.connected()), robots=[
                    {"base": base, "identity": r["manifest"]["identity"],
                     "revision": r["manifest"]["revision"], "online": self.online(r)}
                    for base, r in sorted(self.robots.items())])

        @bp.get("/modules/api/state")
        def state():
            with self.lock:
                robot = self.robots.get(request.args.get("robot"))
                if not robot:
                    return jsonify(error="Robot has not advertised a module manifest"), 404
                body = {"online": self.online(robot), "revision": robot["manifest"]["revision"],
                        "states": deepcopy(robot["states"])}
                if request.args.get("revision") != body["revision"]:
                    body["manifest"] = deepcopy(robot["manifest"])
                return jsonify(body)

        @bp.post("/modules/api/command")
        def command():
            if request.content_length is None or request.content_length > MAX_MESSAGE:
                return jsonify(error="Command body is missing or too large"), 400
            body = request.get_json(silent=True)
            if not isinstance(body, dict) or set(body) != {"robot", "module", "endpoint", "payload", "revision"}:
                return jsonify(error="Expected robot, module, endpoint, payload and revision"), 400
            with self.lock:
                robot = self.robots.get(body["robot"]) if isinstance(body["robot"], str) else None
                if not robot:
                    return jsonify(error="Unknown robot"), 404
                if not self.online(robot):
                    return jsonify(error="Robot modules are offline"), 503
                if body["revision"] != robot["manifest"]["revision"]:
                    return jsonify(error="Module definitions changed; refresh controls"), 409
                module = next((m for m in robot["manifest"]["modules"] if m["id"] == body["module"]), None)
                endpoint = module["endpoints"].get(body["endpoint"]) if module and isinstance(body["endpoint"], str) else None
                if not endpoint or endpoint["kind"] != "command":
                    return jsonify(error="Unknown command endpoint"), 404
                try:
                    validate_payload(endpoint, body["payload"])
                except (ValueError, TypeError, ValidationError):
                    return jsonify(error="Payload does not satisfy the advertised command schema"), 400
                payload = body["payload"]
                if endpoint.get("resource"):
                    session = request.headers.get("X-Pebble-Session", "")
                    if not re.fullmatch(r"[a-zA-Z0-9-]{16,64}", session):
                        return jsonify(error="Media controls require a session identifier"), 400
                    consumer = "web:"+hashlib.sha256((session+body["module"]+body["endpoint"]).encode()).hexdigest()[:40]
                    payload = {**payload, "consumer": consumer, "ttl_seconds": 8}
                    try:
                        validate_payload(endpoint, payload)
                    except (ValueError, TypeError, ValidationError):
                        return jsonify(error="Media endpoint lacks lease fields"), 400
                accepted = self.publish(endpoint["topic"], payload, qos=endpoint["qos"], retain=False)
                if not accepted:
                    return jsonify(error="MQTT connection unavailable"), 503
                # A successful gateway publish is not an actuator acknowledgement.
                return jsonify(status="published", topic=endpoint["topic"])

        app.register_blueprint(bp)

    def online(self, robot):
        now = time.monotonic()
        module_host_online = robot["online"] and now - robot["status_seen"] < 20
        # Older robots may only advertise module-host availability. When the
        # robot-wide lifecycle topic is present, honor its LWT immediately.
        presence_seen = robot.get("presence_seen", 0.)
        presence_online = (
            not presence_seen
            or (robot.get("presence_online", False) and now - presence_seen < 120)
        )
        return bool(self.connected() and module_host_online and presence_online)

    def state_topics(self):
        with self.lock:
            return list(self.routes)

    def subscribe(self, client):
        client.subscribe("+/+/+/o/manifest", qos=1)
        for topic in self.state_topics():
            client.subscribe(topic, qos=1)

    def handle_message(self, client, msg):
        """Return True for module-owned topics, including rejected media packets.

        Binary stream messages must never fall through to the legacy JSON
        dispatcher. Keep ownership even if a decoder rejects a packet.
        """
        owned = False
        try:
            with self.lock:
                owned = msg.topic in self.routes or msg.topic.endswith("/o/manifest")
            added, removed = self.consume(msg.topic, bytes(msg.payload))
            for topic in removed:
                client.unsubscribe(topic)
            for topic in added:
                client.subscribe(topic, qos=1)
        except Exception:
            logging.exception("Module message processing failed on %s", msg.topic)
        return owned

    def consume(self, topic, payload):
        """Return added/removed subscriptions. All parsing is isolated from legacy UI."""
        with self.lock:
            route = self.routes.get(topic)
            if route and route[1] not in (None, "__presence__"):
                base, mid, eid = route
                module = next(m for m in self.robots[base]["manifest"]["modules"] if m["id"] == mid)
                endpoint = module["endpoints"][eid]
                if endpoint["kind"] in {"audio", "video"}:
                    self.media.consume(endpoint, payload)
                    return [], []
        if len(payload) > MAX_MESSAGE:
            return [], []
        with self.lock:
            if topic.endswith("/o/manifest"):
                base = topic[:-len("/o/manifest")]
                try:
                    if not payload:
                        old = self.robots.pop(base, None)
                        removed = [t for t, route in self.routes.items() if route[0] == base]
                        self.media.forget(removed + ([e["topic"] for m in old["manifest"]["modules"] for e in m["endpoints"].values()] if old else []))
                        for t in removed:
                            del self.routes[t]
                        return [], removed
                    manifest = validate_manifest(json.loads(payload))
                    if manifest["base"] != base:
                        raise ValueError("Manifest publication topic does not match its identity")
                    old = self.robots.get(base)
                    if old and old["manifest"] == manifest:
                        return [], []
                    if not old and len(self.robots) >= 128:
                        raise ValueError("Discovery limit reached")
                    removed = [t for t, route in self.routes.items() if route[0] == base]
                    self.media.forget(removed + ([e["topic"] for m in old["manifest"]["modules"] for e in m["endpoints"].values()] if old else []))
                    for t in removed:
                        del self.routes[t]
                    self.robots[base] = {"manifest": manifest, "states": {}, "online": False, "status_seen": 0.}
                    self.routes[manifest["availability_topic"]] = (base, None, None)
                    self.routes[f"{base}/o/online"] = (base, "__presence__", None)
                    for module in manifest["modules"]:
                        for eid, endpoint in module["endpoints"].items():
                            if endpoint["kind"] in {"state", "video", "audio"}:
                                self.routes[endpoint["topic"]] = (base, module["id"], eid)
                    added = [t for t, route in self.routes.items() if route[0] == base]
                    return added, [t for t in removed if t not in added]
                except (ValueError, TypeError, KeyError, ValidationError, SchemaError):
                    logging.warning("Rejected invalid module manifest on %s", topic)
                    return [], []
            route = self.routes.get(topic)
            if not route:
                return [], []
            base, mid, eid = route
            robot = self.robots[base]
            try:
                body = json.loads(payload)
                if mid == "__presence__":
                    if not isinstance(body, dict) or not isinstance(body.get("online"), bool):
                        return [], []
                    robot["presence_online"] = body["online"]
                    robot["presence_seen"] = time.monotonic()
                    return [], []
                if mid is None:
                    if not isinstance(body, dict) or not isinstance(body.get("online"), bool):
                        return [], []
                    robot["online"], robot["status_seen"] = body["online"], time.monotonic()
                else:
                    module = next(m for m in robot["manifest"]["modules"] if m["id"] == mid)
                    validate_payload(module["endpoints"][eid], body)
                    robot["states"].setdefault(mid, {})[eid] = {"value": body, "received_ms": int(time.time()*1000)}
            except (ValueError, TypeError, ValidationError):
                logging.warning("Rejected invalid module state on %s", topic)
            return [], []
