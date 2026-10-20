#!/usr/bin/env python3
"""Publish robot-owned module discovery and run declared compatibility adapters."""
from __future__ import annotations
import argparse
import json
import logging
from pathlib import Path
import signal
import threading
import time
import uuid
from jsonschema import ValidationError

from control.common.config import load_config
from control.common.modules import MAX_MESSAGE, build_manifest, validate_payload
from control.common.mqtt import create_client, mqtt_auth_and_tls


class ModuleHost:
    def __init__(self, config: dict, config_path: Path):
        self.manifest, self.adapters = build_manifest(config, config_path)
        self.config, self.config_path = config, config_path
        self.stop_event = threading.Event()
        self.boot_id = uuid.uuid4().hex
        self.manifest_topic = self.manifest["base"] + "/o/manifest"
        self.status_topic = self.manifest["availability_topic"]
        self.commands = {e["topic"]: e for m in self.manifest["modules"] for e in m["endpoints"].values() if e["kind"] == "command"}
        self.echo_states = {}
        for module in self.manifest["modules"]:
            for command in module["commands"]:
                adapter = self.adapters.get(module["endpoints"][command["endpoint"]]["topic"])
                if adapter and adapter["kind"] == "echo":
                    initial = {field: module["actions"][action]["neutral"] for field, action in command["fields"].items()}
                    validate_payload(adapter["endpoint"], initial)
                    self.echo_states[adapter["endpoint"]["topic"]] = initial
        self.client = None
        self.last_manifest = 0.

    def status(self, online):
        return json.dumps({"online": online, "boot_id": self.boot_id, "t": int(time.time()*1000)})

    def on_connect(self, client, _userdata, _flags, rc):
        if rc != 0:
            return
        client.publish(self.manifest_topic, json.dumps(self.manifest), qos=1, retain=True)
        self.last_manifest = time.monotonic()
        client.publish(self.status_topic, self.status(True), qos=1, retain=True)
        for topic, value in self.echo_states.items():
            client.publish(topic, json.dumps(value), qos=1, retain=True)
        for topic in self.adapters:
            client.subscribe(topic, qos=self.commands[topic]["qos"])
        logging.info("Published modules for %s", self.manifest["base"])

    def on_message(self, client, _userdata, msg):
        # No retained commands, reverse aliases, or wildcard translations.
        adapter = self.adapters.get(msg.topic)
        if not adapter or msg.retain or len(msg.payload) > MAX_MESSAGE:
            return
        try:
            payload = json.loads(msg.payload)
            validate_payload(self.commands[msg.topic], payload)
            if adapter["kind"] == "forward":
                client.publish(adapter["topic"], json.dumps(payload), qos=0, retain=False)
            else:
                endpoint = adapter["endpoint"]
                validate_payload(endpoint, payload)
                self.echo_states[endpoint["topic"]] = payload
                client.publish(endpoint["topic"], json.dumps(payload), qos=endpoint["qos"], retain=endpoint["retain"])
        except (ValueError, TypeError, UnicodeError, ValidationError) as exc:
            logging.warning("Rejected module command on %s: %s", msg.topic, type(exc).__name__)
        except Exception:
            logging.exception("Module adapter failed on %s", msg.topic)

    def start(self):
        cfg = self.config.get("local_mqtt", {})
        self.client = create_client(client_id=self.manifest["base"]+"-modules")
        mqtt_auth_and_tls(self.client, cfg, self.config_path.parent)
        self.client.will_set(self.status_topic, self.status(False), qos=1, retain=True)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.reconnect_delay_set(1, 30)
        self.client.connect_async(cfg.get("host", "127.0.0.1"), int(cfg.get("port", 1883)), 15)
        self.client.loop_start()
        try:
            while not self.stop_event.wait(5):
                if self.client.is_connected():
                    self.client.publish(self.status_topic, self.status(True), qos=1, retain=True)
                    if time.monotonic()-self.last_manifest > 60:
                        self.client.publish(self.manifest_topic, json.dumps(self.manifest), qos=1, retain=True)
                        self.last_manifest = time.monotonic()
        finally:
            if self.client.is_connected():
                info = self.client.publish(self.status_topic, self.status(False), qos=1, retain=True)
                info.wait_for_publish(timeout=2)
            self.client.disconnect()
            self.client.loop_stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config")
    args = parser.parse_args()
    config, path = load_config(args.config)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    host = ModuleHost(config, path)
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: host.stop_event.set())
    host.start()


if __name__ == "__main__":
    main()
