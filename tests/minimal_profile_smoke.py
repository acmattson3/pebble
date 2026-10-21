#!/usr/bin/env python3
"""Broker-backed smoke test for the minimal MQTT component profile.

Requires Docker, eclipse-mosquitto:2.0.22, and paho-mqtt. It starts an isolated
broker and exercises a simulated MCU-style component using MQTT 3.1.1.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import tempfile
import threading
import time

import paho.mqtt.client as mqtt


def wait_until(check, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if check():
            return
        time.sleep(0.03)
    raise AssertionError("Timed out waiting for MQTT condition")


def make_client(client_id: str) -> mqtt.Client:
    # Explicit MQTT 3.1.1 mirrors the profile's minimum implementation target.
    if hasattr(mqtt, "CallbackAPIVersion"):
        return mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id,
            protocol=mqtt.MQTTv311,
        )
    return mqtt.Client(client_id=client_id, protocol=mqtt.MQTTv311)


def main() -> None:
    name = f"pebble-minimal-profile-{os.getpid()}"
    clients: list[mqtt.Client] = []
    container_started = False
    try:
        with tempfile.TemporaryDirectory(prefix="pebble-minimal-profile-") as tmp:
            config = Path(tmp) / "mosquitto.conf"
            config.write_text("listener 1883\nallow_anonymous true\npersistence false\n")
            config.chmod(0o644)
            subprocess.run(
                ["docker", "run", "--rm", "-d", "--name", name,
                 "-p", "127.0.0.1::1883", "-v",
                 f"{config}:/mosquitto/config/mosquitto.conf:ro",
                 "eclipse-mosquitto:2.0.22"],
                check=True, capture_output=True, text=True,
            )
            container_started = True
            mapping = subprocess.run(
                ["docker", "port", name, "1883/tcp"], check=True,
                capture_output=True, text=True,
            ).stdout.strip().splitlines()[0]
            port = int(mapping.rsplit(":", 1)[1])

            base = "pebble/robots/minilamp-simulator"
            drive_topic = f"{base}/i/drive"
            online_topic = f"{base}/o/online"
            battery_topic = f"{base}/o/telemetry/battery"
            live: list[tuple[str, dict, bool]] = []
            drives: list[dict] = []
            connected = threading.Event()
            component = make_client("minimal-profile-component")
            clients.append(component)
            component.will_set(online_topic, '{"online":false}', qos=1, retain=True)
            def on_component_connect(client, *_args):
                client.subscribe(drive_topic, qos=1)
                connected.set()

            component.on_connect = on_component_connect

            def on_component_message(_client, _userdata, message):
                drives.append(json.loads(message.payload))

            component.on_message = on_component_message
            component.connect("127.0.0.1", port, keepalive=10)
            component.loop_start()
            wait_until(connected.is_set)
            component.publish(online_topic, '{"online":true}', qos=0, retain=True).wait_for_publish()
            component.publish(battery_topic, '{"value":3.91,"unit":"v"}', qos=0, retain=True).wait_for_publish()

            observer = make_client("minimal-profile-observer")
            clients.append(observer)
            observer.on_message = lambda _c, _u, m: live.append(
                (m.topic, json.loads(m.payload), bool(m.retain))
            )
            observer.connect("127.0.0.1", port)
            observer.loop_start()
            observer.subscribe(f"{base}/#", qos=1)
            wait_until(lambda: any(t == online_topic and p == {"online": True} for t, p, _ in live))
            wait_until(lambda: any(t == battery_topic and p == {"value": 3.91, "unit": "v"} for t, p, _ in live))

            publisher = make_client("minimal-profile-command-source")
            clients.append(publisher)
            publisher.connect("127.0.0.1", port)
            publisher.loop_start()
            publisher.publish(drive_topic, json.dumps({"x": 0.25, "z": 0.5}), qos=1, retain=False).wait_for_publish()
            wait_until(lambda: bool(drives))
            assert drives[-1] == {"x": 0.25, "z": 0.5}

            late = make_client("minimal-profile-late-observer")
            clients.append(late)
            retained: list[tuple[str, dict, bool]] = []
            late.on_message = lambda _c, _u, m: retained.append(
                (m.topic, json.loads(m.payload), bool(m.retain))
            )
            late.connect("127.0.0.1", port)
            late.loop_start()
            late.subscribe([(f"{base}/o/#", 1), (f"{base}/i/#", 1)])
            wait_until(lambda: len(retained) >= 2)
            time.sleep(0.1)
            assert len(retained) == 2
            assert not any(topic == drive_topic for topic, _, _ in retained)
            assert any(t == online_topic and p == {"online": True} for t, p, _ in retained)
            assert any(t == battery_topic and p == {"value": 3.91, "unit": "v"} for t, p, _ in retained)
            assert all(is_retained for _, _, is_retained in retained)

            # Close only the transport, without MQTT DISCONNECT, to exercise LWT.
            component.loop_stop()
            if component._sock is not None:  # paho has no public abrupt-close API
                component._sock.close()
            wait_until(lambda: any(t == online_topic and p == {"online": False} for t, p, _ in live), timeout=12)

            final_observer = make_client("minimal-profile-final-observer")
            clients.append(final_observer)
            final_state: list[dict] = []
            final_observer.on_message = lambda _c, _u, m: final_state.append(json.loads(m.payload))
            final_observer.connect("127.0.0.1", port)
            final_observer.loop_start()
            final_observer.subscribe(online_topic, qos=1)
            wait_until(lambda: bool(final_state))
            assert final_state[-1] == {"online": False}

            print(json.dumps({
                "result": "passed",
                "checks": [
                    "MQTT 3.1.1 identity-scoped compact topics",
                    "component consumes non-retained JSON command",
                    "retained online and battery state for late subscriber",
                    "retained offline Last Will after abrupt transport loss",
                ],
            }))
    finally:
        for client in clients:
            try:
                client.disconnect()
                client.loop_stop()
            except Exception:
                pass
        if container_started:
            subprocess.run(["docker", "stop", "--time", "1", name], capture_output=True)


if __name__ == "__main__":
    main()
