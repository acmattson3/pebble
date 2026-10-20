"""Versioned, data-only module contracts shared by robot and web runtimes."""
from __future__ import annotations

from copy import deepcopy
import hashlib
from functools import lru_cache
import json
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

from control.common.topics import RobotIdentity, endpoint_topic, identity_from_config

SCHEMA = "pebble-manifest/v2"
ROOT = Path(__file__).resolve().parents[2]
MAX_MESSAGE = 262144
MAX_EVENT_ENDPOINTS = 8
CONTRACT = json.loads((ROOT / "modules" / "manifest.schema.json").read_text())
VALIDATOR = Draft202012Validator(CONTRACT)


def payload_validator(schema: dict) -> Draft202012Validator:
    return _payload_validator_cached(json.dumps(schema, sort_keys=True, allow_nan=False))


@lru_cache(maxsize=256)
def _payload_validator_cached(serialized: str) -> Draft202012Validator:
    schema = json.loads(serialized)
    Draft202012Validator.check_schema(schema)
    # Descriptors arrive through MQTT. Keep validation local and bounded: no
    # remote references, executable expressions, or attacker-supplied regexes.
    allowed = {"type", "properties", "required", "additionalProperties", "minimum",
               "maximum", "enum", "const", "items", "minItems", "maxItems", "maxProperties",
               "minLength", "maxLength", "title", "description", "default"}
    def check(node, depth=0):
        if not isinstance(node, dict) or depth > 12 or set(node) - allowed:
            raise ValueError("Unsupported endpoint schema keyword or nesting")
        for child in node.get("properties", {}).values():
            check(child, depth+1)
        if isinstance(node.get("items"), dict):
            check(node["items"], depth+1)
        if isinstance(node.get("additionalProperties"), dict):
            check(node["additionalProperties"], depth+1)
    check(schema)
    return Draft202012Validator(schema)


def validate_payload(endpoint: dict, payload: Any) -> None:
    # JSON permits neither NaN nor infinity, despite Python's permissive parser.
    body = json.dumps(payload, allow_nan=False).encode()
    if len(body) > MAX_MESSAGE:
        raise ValueError("Command is too large")
    payload_validator(endpoint["schema"]).validate(payload)


def validate_manifest(manifest: dict) -> dict:
    if len(json.dumps(manifest, allow_nan=False).encode()) > MAX_MESSAGE:
        raise ValueError("Manifest is too large")
    VALIDATOR.validate(manifest)
    identity = RobotIdentity(manifest["identity"]["system"], manifest["identity"]["type"], manifest["identity"]["id"])
    if manifest["base"] != identity.base:
        raise ValueError("Manifest identity does not match its base")
    if manifest["availability_topic"] != endpoint_topic(identity, "o/modules/status"):
        raise ValueError("Invalid availability topic")
    ids, topics = set(), set()
    event_endpoint_count = 0
    for module in manifest["modules"]:
        if module["id"] in ids:
            raise ValueError("Duplicate module instance")
        ids.add(module["id"])
        actions, endpoints = module["actions"], module["endpoints"]
        for action in actions.values():
            if not action["min"] <= action["neutral"] <= action["max"] or action["min"] >= action["max"]:
                raise ValueError("Invalid action range or neutral")
        for endpoint in endpoints.values():
            if endpoint["kind"] == "event":
                event_endpoint_count += 1
                if event_endpoint_count > MAX_EVENT_ENDPOINTS:
                    raise ValueError("Manifest has too many event endpoints")
            topic = endpoint["topic"]
            relative = topic.removeprefix(identity.base + "/")
            if topic != endpoint_topic(identity, relative) or topic in topics:
                raise ValueError("Endpoint escapes its robot or duplicates another endpoint")
            if relative in {"o/manifest", "o/online", "o/modules/status"}:
                raise ValueError("Reserved module topic")
            topics.add(topic)
            if (endpoint["kind"] in {"command", "uplink"}) != relative.startswith("i/"):
                raise ValueError("Endpoint direction disagrees with its kind")
            if endpoint["kind"] == "command" and endpoint["retain"]:
                raise ValueError("Commands must not be retained")
            if endpoint["kind"] == "event" and endpoint["retain"]:
                raise ValueError("Event endpoints must not be retained")
            if endpoint["kind"] in {"video", "audio", "uplink"}:
                expected = {"pbvd", "h264"} if endpoint["kind"] == "video" else {"opus"}
                if endpoint.get("codec") not in expected or endpoint["retain"] or endpoint["qos"] != 0:
                    raise ValueError("Invalid media stream codec or delivery policy")
                if "opus" in expected and ("rate" not in endpoint or "channels" not in endpoint):
                    raise ValueError("Audio stream requires its sample format")
            if "resource" in endpoint and endpoint["kind"] != "command":
                raise ValueError("Resource requests require a command endpoint")
            payload_validator(endpoint["schema"])
        command_endpoints = set()
        for command in module["commands"]:
            endpoint = endpoints.get(command["endpoint"])
            if not endpoint or endpoint["kind"] != "command" or command["endpoint"] in command_endpoints:
                raise ValueError("Invalid or duplicate command binding")
            command_endpoints.add(command["endpoint"])
            if any(a not in actions for a in command["fields"].values()):
                raise ValueError("Command references an unknown action")
            # Every value permitted by an action must fit its command field.
            neutral = {field: actions[a]["neutral"] for field, a in command["fields"].items()}
            validate_payload(endpoint, neutral)
            for field, action_id in command["fields"].items():
                for value in (actions[action_id]["min"], actions[action_id]["max"]):
                    validate_payload(endpoint, {**neutral, field: value})
        binding_ids = set()
        for binding in module["bindings"]:
            if binding["id"] in binding_ids:
                raise ValueError("Duplicate binding ID")
            binding_ids.add(binding["id"])
            if not binding.get("reset") and binding.get("action") not in actions:
                raise ValueError("Input references an unknown action")
            if binding["input"] == "keyboard" and not binding.get("code"):
                raise ValueError("Keyboard binding needs a code")
            if binding["input"] != "keyboard" and "index" not in binding:
                raise ValueError("Controller binding needs an index")
        for widget in module["ui"]["controls"]:
            kind = widget["kind"]
            required = {"joystick": ("x", "y"), "range": ("action",), "button": ("action",)}.get(kind, ())
            if any(widget.get(key) not in actions for key in required):
                raise ValueError("Widget references an unknown action")
            if kind in {"video", "audio"}:
                if widget.get("endpoint") not in endpoints or endpoints[widget["endpoint"]]["kind"] != kind or widget.get("action") not in actions:
                    raise ValueError("Media widget requires a matching stream and action")
                if "uplink" in widget and (widget["uplink"] not in endpoints or endpoints[widget["uplink"]]["kind"] != "uplink" or widget.get("talk_action") not in actions):
                    raise ValueError("Audio talk control requires an uplink and action")
            if kind == "soundboard":
                command = endpoints.get(widget.get("command_endpoint"))
                files = endpoints.get(widget.get("files_endpoint"))
                status = endpoints.get(widget.get("status_endpoint"))
                if not command or command["kind"] != "command" or not files or files["kind"] != "state" or not status or status["kind"] != "state":
                    raise ValueError("Soundboard widget requires command, files, and status endpoints")
                command_schema = command["schema"]
                properties = command_schema.get("properties", {})
                if (command_schema.get("type") != "object"
                        or properties.get("action", {}).get("enum") != ["play", "stop"]
                        or properties.get("file", {}).get("type") != "string"
                        or not {"action", "file"}.issubset(set(command_schema.get("required", [])))):
                    raise ValueError("Soundboard command needs play/stop action and file fields")
                files_schema = files["schema"].get("properties", {}).get("files", {})
                if (files["schema"].get("type") != "object" or files_schema.get("type") != "array"
                        or files_schema.get("items", {}).get("type") != "string"):
                    raise ValueError("Soundboard files endpoint must provide a string files array")
                status_schema = status["schema"].get("properties", {})
                if status["schema"].get("type") != "object" or status_schema.get("playing", {}).get("type") != "boolean":
                    raise ValueError("Soundboard status endpoint must provide a playing flag")
            if kind == "autonomy":
                command = endpoints.get(widget.get("command_endpoint"))
                scripts = endpoints.get(widget.get("scripts_endpoint"))
                status = endpoints.get(widget.get("status_endpoint"))
                if (not command or command["kind"] != "command"
                        or not scripts or scripts["kind"] != "state"
                        or not status or status["kind"] != "state"):
                    raise ValueError("Autonomy widget requires command, scripts, and status endpoints")
                command_schema = command["schema"]
                if (command_schema.get("type") != "object"
                        or command_schema.get("properties", {}).get("action", {}).get("enum") != ["start", "stop"]):
                    raise ValueError("Autonomy command must declare start/stop actions")
                scripts_schema = scripts["schema"].get("properties", {}).get("scripts", {})
                if (scripts["schema"].get("type") != "object"
                        or scripts_schema.get("type") != "array"
                        or scripts_schema.get("items", {}).get("type") != "object"):
                    raise ValueError("Autonomy scripts endpoint must provide script descriptors")
                status_properties = status["schema"].get("properties", {})
                if (status["schema"].get("type") != "object"
                        or status_properties.get("running", {}).get("type") != "boolean"):
                    raise ValueError("Autonomy status endpoint must provide a running flag")
            if kind == "lights":
                solid = endpoints.get(widget.get("solid_endpoint"))
                flash = endpoints.get(widget.get("flash_endpoint"))
                if not solid or solid["kind"] != "command" or not flash or flash["kind"] != "command":
                    raise ValueError("Lights widget requires solid and flash command endpoints")
                validate_payload(solid, {"b": 0, "g": 0, "r": 0})
                validate_payload(flash, {"b": 0, "g": 0, "r": 0, "period": 1})
            if kind == "telemetry":
                metrics = widget.get("metrics", [])
                if not metrics:
                    raise ValueError("Telemetry widget needs at least one metric")
                for metric in metrics:
                    endpoint = endpoints.get(metric["endpoint"])
                    if not endpoint or endpoint["kind"] != "state":
                        raise ValueError("Telemetry metrics must reference state endpoints")
                    properties = endpoint["schema"].get("properties", {})
                    if metric["field"] not in properties:
                        raise ValueError("Telemetry metric field is not declared by its endpoint schema")
            if kind == "logs":
                endpoint = endpoints.get(widget.get("endpoint"))
                if not endpoint or endpoint["kind"] != "event":
                    raise ValueError("Logs widget needs an event endpoint")
            if kind == "admin":
                seen_admin = set()
                if not widget.get("actions"):
                    raise ValueError("Admin widget must advertise at least one configured action")
                for action in widget["actions"]:
                    endpoint = endpoints.get(action["endpoint"])
                    if action["id"] in seen_admin or not endpoint or endpoint["kind"] != "command":
                        raise ValueError("Admin actions need unique IDs and declared command endpoints")
                    seen_admin.add(action["id"])
                    if endpoint["schema"].get("type") != "object" or endpoint["schema"].get("properties", {}).get("enabled", {}).get("const") is not True:
                        raise ValueError("Admin endpoints must require a fixed enabled=true payload")
            if kind == "button" and "value" not in widget:
                raise ValueError("Button needs a value")
            if kind == "state" and (widget.get("endpoint") not in endpoints or endpoints[widget["endpoint"]]["kind"] != "state"):
                raise ValueError("State widget needs a state endpoint")
    return manifest


def build_manifest(config: dict, config_path: Path) -> tuple[dict, dict]:
    identity = identity_from_config(config)
    instances = config.get("services", {}).get("module_host", {}).get("modules", [])
    modules, adapters = [], {}
    for instance in instances:
        path = Path(instance["use"]).expanduser()
        if not path.is_absolute():
            path = ROOT / path
        module = json.loads(path.read_text())
        module["id"] = instance["id"]
        for key in ("title", "description", "bindings", "ui"):
            if key in instance:
                module[key] = deepcopy(instance[key])
        if "endpoints" in instance:
            selected = instance["endpoints"]
            if not isinstance(selected, list) or len(set(selected)) != len(selected) or set(selected) - set(module["endpoints"]):
                raise ValueError("Module endpoint selection must name unique declared endpoints")
            module["endpoints"] = {key: value for key, value in module["endpoints"].items() if key in selected}
            module["commands"] = [command for command in module["commands"] if command["endpoint"] in module["endpoints"]]
        for eid, attributes in instance.get("formats", {}).items():
            if eid not in module["endpoints"] or not isinstance(attributes, dict) or set(attributes)-{"codec","rate","channels"}:
                raise ValueError("Invalid module media format override")
            if module["endpoints"][eid]["kind"] not in {"video","audio","uplink"}:
                raise ValueError("Format overrides require a media endpoint")
            module["endpoints"][eid].update(deepcopy(attributes))
        if "topics" in instance and "subtopics" in instance:
            raise ValueError("Module instance accepts either subtopics or legacy topics")
        overrides = instance.get("subtopics", instance.get("topics", {}))
        for key, endpoint in module["endpoints"].items():
            endpoint["topic"] = endpoint_topic(identity, overrides.get(key, endpoint["topic"]))
        for key, adapter in instance.get("adapters", {}).items():
            if key not in module["endpoints"] or module["endpoints"][key]["kind"] != "command":
                raise ValueError("Adapter references an unknown command endpoint")
            if adapter["kind"] == "forward":
                if "topic" in adapter and "subtopic" in adapter:
                    raise ValueError("Compatibility adapter accepts either subtopic or legacy topic")
                relative = adapter.get("subtopic", adapter.get("topic"))
                # One explicitly declared, local compatibility destination.
                if not isinstance(relative, str) or not relative.startswith("incoming/") or any(
                    p in {"", ".", ".."} or any(c in p for c in "+#\x00") for p in relative.split("/")
                ):
                    raise ValueError("Compatibility adapter requires an incoming/... relative topic")
                resolved = {"kind": "forward", "topic": identity.base + "/" + relative}
            elif adapter["kind"] == "echo":
                target = module["endpoints"].get(adapter["state_endpoint"])
                if not target or target["kind"] != "state":
                    raise ValueError("Echo adapter requires a state endpoint")
                resolved = {"kind": "echo", "endpoint": target}
            else:
                raise ValueError("Unknown adapter kind")
            adapters[module["endpoints"][key]["topic"]] = resolved
        modules.append(module)
    manifest = {"schema": SCHEMA, "identity": {"system": identity.system, "type": identity.type, "id": identity.robot_id},
                "base": identity.base, "availability_topic": endpoint_topic(identity, "o/modules/status"),
                "modules": modules, "revision": "pending"}
    manifest["revision"] = hashlib.sha256(json.dumps(manifest, sort_keys=True).encode()).hexdigest()[:16]
    return validate_manifest(manifest), adapters
