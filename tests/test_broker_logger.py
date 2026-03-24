from __future__ import annotations

import argparse
import importlib.util
import json
import signal
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from tests.helpers import FakeMqttMessage


def _load_broker_logger_module():
    module_path = Path("web-interface/tools/mqtt-broker-logger.py").resolve()
    spec = importlib.util.spec_from_file_location("mqtt_broker_logger_under_test", module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _FakeLoggerClient:
    def __init__(self, client_id: str, handlers: dict[int, object]):
        self.client_id = client_id
        self.handlers = handlers
        self.on_connect = None
        self.on_message = None
        self.username_pw_set_calls: list[tuple[str, str | None]] = []
        self.tls_set_calls: list[dict[str, object]] = []
        self.tls_insecure_calls: list[bool] = []
        self.connect_calls: list[tuple[str, int, int]] = []
        self.subscribe_calls: list[tuple[str, int]] = []
        self.loop_started = False
        self.loop_stopped = False
        self.disconnected = False

    def username_pw_set(self, username: str, password: str | None = None) -> None:
        self.username_pw_set_calls.append((username, password))

    def tls_set(self, **kwargs) -> None:
        self.tls_set_calls.append(kwargs)

    def tls_insecure_set(self, value: bool) -> None:
        self.tls_insecure_calls.append(bool(value))

    def connect(self, host: str, port: int, keepalive: int) -> None:
        self.connect_calls.append((host, int(port), int(keepalive)))

    def subscribe(self, topic: str, qos: int = 0):
        self.subscribe_calls.append((topic, int(qos)))
        return (0, 1)

    def loop_start(self) -> None:
        self.loop_started = True
        if self.on_connect is not None:
            self.on_connect(self, None, {}, 0)
        if self.on_message is not None:
            self.on_message(
                self,
                None,
                FakeMqttMessage(
                    topic="pebble/robots/unit-1/outgoing/status",
                    payload=b'{"ok":true}',
                    qos=1,
                    retain=False,
                ),
            )
        handler = self.handlers.get(signal.SIGTERM)
        if handler is not None:
            handler(signal.SIGTERM, None)

    def loop_stop(self) -> None:
        self.loop_stopped = True

    def disconnect(self) -> None:
        self.disconnected = True


class BrokerLoggerTests(unittest.TestCase):
    def test_broker_cfg_prefers_web_then_remote_then_local(self):
        module = _load_broker_logger_module()

        self.assertEqual(
            module._broker_cfg(
                {
                    "web_interface": {"mqtt": {"host": "web-host"}},
                    "services": {"mqtt_bridge": {"remote_mqtt": {"host": "remote-host"}}},
                    "local_mqtt": {"host": "local-host"},
                }
            )["host"],
            "web-host",
        )
        self.assertEqual(
            module._broker_cfg(
                {
                    "services": {"mqtt_bridge": {"remote_mqtt": {"host": "remote-host"}}},
                    "local_mqtt": {"host": "local-host"},
                }
            )["host"],
            "remote-host",
        )
        self.assertEqual(module._broker_cfg({"local_mqtt": {"host": "local-host"}})["host"], "local-host")

    def test_resolve_tls_paths_resolves_relative_files(self):
        module = _load_broker_logger_module()

        with tempfile.TemporaryDirectory() as td:
            base_dir = Path(td)
            ca = base_dir / "ca.crt"
            cert = base_dir / "client.crt"
            key = base_dir / "client.key"
            ca.write_text("ca")
            cert.write_text("cert")
            key.write_text("key")

            resolved = module._resolve_tls_paths(
                {
                    "enabled": True,
                    "ca_cert": "ca.crt",
                    "client_cert": "client.crt",
                    "client_key": "client.key",
                    "insecure": True,
                    "ciphers": "ECDHE",
                },
                base_dir,
            )

        self.assertEqual(resolved["ca_cert"], str(ca))
        self.assertEqual(resolved["client_cert"], str(cert))
        self.assertEqual(resolved["client_key"], str(key))
        self.assertTrue(resolved["insecure"])
        self.assertEqual(resolved["ciphers"], "ECDHE")

    def test_main_writes_jsonl_and_uses_selected_broker(self):
        module = _load_broker_logger_module()

        with tempfile.TemporaryDirectory() as td:
            temp_dir = Path(td)
            cfg_path = temp_dir / "config.json"
            log_path = temp_dir / "broker-log.jsonl"
            cfg_path.write_text(
                json.dumps(
                    {
                        "web_interface": {
                            "mqtt": {
                                "host": "web-host",
                                "port": 1884,
                                "keepalive": 45,
                                "username": "logger-user",
                                "password": "logger-pass",
                                "tls": {"enabled": False},
                            }
                        },
                        "services": {
                            "mqtt_bridge": {
                                "remote_mqtt": {
                                    "host": "remote-host",
                                    "port": 1883,
                                    "keepalive": 60,
                                    "username": "",
                                    "password": "",
                                }
                            }
                        },
                        "local_mqtt": {"host": "local-host", "port": 1883},
                    }
                )
            )

            handlers: dict[int, object] = {}
            created: list[_FakeLoggerClient] = []

            def _register_signal(sig: int, handler):
                handlers[sig] = handler
                return handler

            def _client_factory(*_args, **kwargs):
                client = _FakeLoggerClient(str(kwargs.get("client_id") or ""), handlers)
                created.append(client)
                return client

            args = argparse.Namespace(
                config=str(cfg_path),
                host="",
                port=0,
                username="",
                password="",
                keepalive=0,
                topic=["pebble/#", "pebble/infrastructure"],
                qos=1,
                log_file=str(log_path),
                log_dir="web-interface/logs",
                flush_lines=1,
                max_payload_bytes=8192,
                decode_utf8=True,
                parse_json=True,
            )

            with mock.patch.object(module, "_parse_args", return_value=args), mock.patch.object(
                module.mqtt, "Client", side_effect=_client_factory
            ), mock.patch.object(module.signal, "signal", side_effect=_register_signal), mock.patch.object(
                module.time, "time", return_value=1234.5
            ):
                rc = module.main()

            self.assertEqual(rc, 0)
            self.assertEqual(len(created), 1)
            client = created[0]
            self.assertEqual(client.client_id, "pebble-broker-logger-1234")
            self.assertEqual(client.connect_calls, [("web-host", 1884, 45)])
            self.assertEqual(client.username_pw_set_calls, [("logger-user", "logger-pass")])
            self.assertEqual(client.subscribe_calls, [("pebble/#", 1), ("pebble/infrastructure", 1)])
            self.assertTrue(client.loop_started)
            self.assertTrue(client.loop_stopped)
            self.assertTrue(client.disconnected)

            lines = log_path.read_text(encoding="utf-8").strip().splitlines()
            self.assertEqual(len(lines), 1)
            entry = json.loads(lines[0])
            self.assertEqual(entry["topic"], "pebble/robots/unit-1/outgoing/status")
            self.assertEqual(entry["qos"], 1)
            self.assertFalse(entry["retain"])
            self.assertEqual(entry["payload_text"], '{"ok":true}')
            self.assertEqual(entry["payload_json"], {"ok": True})
            self.assertEqual(entry["payload_size_bytes"], len(b'{"ok":true}'))


if __name__ == "__main__":
    unittest.main()
