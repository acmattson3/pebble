import unittest
from copy import deepcopy
import importlib.util
import json
from pathlib import Path
import sys
from flask import Flask
from jsonschema import ValidationError
from control.common.modules import build_manifest, validate_manifest, validate_payload
from control.common.topics import RobotIdentity, endpoint_topic
from control.services.module_host import ModuleHost
from control.services.mqtt_bridge import MqttBridge
from tests.helpers import FakeMqttClient, FakeMqttMessage, make_base_config
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'web-interface'))
spec = importlib.util.spec_from_file_location('module_ui', ROOT / 'web-interface/module_ui.py')
web = importlib.util.module_from_spec(spec)
spec.loader.exec_module(web)

def make_configured():
    config = json.loads((ROOT / 'robots/goob/modules.example.json').read_text())
    config['local_mqtt'] = {'host': 'localhost', 'port': 1883}
    return config

def make_built():
    configured = make_configured()
    return build_manifest(configured, ROOT / 'control/configs/config.json')

class ModuleTests(unittest.TestCase):

    def test_compact_topics_cannot_escape_identity(self):
        identity = RobotIdentity('pebble', 'robots', 'goob')
        assert endpoint_topic(identity, 'i/drive') == 'pebble/robots/goob/i/drive'
        for bad in ('pebble/robots/other/i/drive', 'i/#', 'i/+', 'i/../drive', 'i//drive', 'incoming/drive', 'i/drive\x00'):
            with self.assertRaises(ValueError):
                endpoint_topic(identity, bad)

    def test_shared_and_robot_local_modules_are_data_only(self):
        built = make_built()
        manifest, adapters = built
        assert [m['type'] for m in manifest['modules']] == ['pebble.drive', 'pebble.soundboard', 'pebble.lights', 'pebble.telemetry', 'pebble.logs', 'goob.gripper-demo']
        assert 'adapters' not in json.dumps(manifest)
        assert adapters[manifest['base'] + '/i/drive']['topic'].endswith('/incoming/drive-values')
        assert adapters[manifest['base'] + '/i/soundboard/command']['topic'].endswith('/incoming/soundboard-command')
        drive = manifest['modules'][0]
        validate_payload(drive['endpoints']['command'], {'x': 0.5, 'z': -0.2})
        for payload in ({'x': 2, 'z': 0}, {'x': 0}, {'x': 0, 'z': 0, 'extra': 1}, {'x': float('nan'), 'z': 0}):
            with self.assertRaises((ValueError, ValidationError)):
                validate_payload(drive['endpoints']['command'], payload)
        soundboard = manifest['modules'][1]
        validate_payload(soundboard['endpoints']['command'], {'action': 'play', 'file': 'beep.wav'})
        for payload in ({'action': 'other', 'file': 'beep.wav'}, {'action': 'play', 'file': '', 'extra': 1}):
            with self.assertRaises((ValueError, ValidationError)):
                validate_payload(soundboard['endpoints']['command'], payload)

    def test_invalid_contract_is_rejected(self):
        built = make_built()
        for index, change in enumerate([lambda m: m['modules'][0]['endpoints']['command'].update(topic='other/robots/goob/i/drive'), lambda m: m['modules'][0]['endpoints']['command'].update(retain=True), lambda m: m['modules'][0]['endpoints']['command'].update(schema={'$ref': 'http://localhost/private'}), lambda m: m['modules'][0]['commands'][0]['fields'].update(x='missing-action'), lambda m: m['modules'][0]['actions']['forward'].update(max=3), lambda m: m['modules'][0]['bindings'][0].update(action='missing-action'), lambda m: m['modules'][0]['ui']['controls'][0].update(kind='html', html='<script>')]):
            with self.subTest(case=index):
                manifest = deepcopy(built[0])
                change(manifest)
                with self.assertRaises((ValueError, ValidationError)):
                    validate_manifest(manifest)

    def test_adapters_forward_once_and_echo_without_actuator(self):
        configured = make_configured()
        host = ModuleHost(configured, ROOT / 'control/configs/config.json')
        client = FakeMqttClient()
        soundboard = host.manifest['base'] + '/i/soundboard/command'
        host.on_message(client, None, FakeMqttMessage(soundboard, b'{"action":"play","file":"beep.wav"}', qos=0))
        assert client.publish_calls[-1][0] == host.manifest['base'] + '/incoming/soundboard-command'
        assert json.loads(client.publish_calls[-1][1]) == {'action': 'play', 'file': 'beep.wav'}
        assert client.publish_calls[-1][3] is False
        drive = host.manifest['base'] + '/i/drive'
        host.on_message(client, None, FakeMqttMessage(drive, b'{"x":0,"z":0.25}', qos=0))
        assert len(client.publish_calls) == 2
        assert client.publish_calls[-1][0] == host.manifest['base'] + '/incoming/drive-values'
        assert client.publish_calls[-1][3] is False
        host.on_message(client, None, FakeMqttMessage(drive, b'{"x":0,"z":1}', retain=True))
        host.on_message(client, None, FakeMqttMessage(drive, b'{"x":99,"z":1}'))
        assert len(client.publish_calls) == 2
        host.on_message(client, None, FakeMqttMessage(host.manifest['base'] + '/i/gripper-demo', b'{"position":0.7}'))
        assert len(client.publish_calls) == 3
        assert client.publish_calls[-1][0].endswith('/o/gripper-demo/state')
        assert client.publish_calls[-1][2:] == (1, True)
        before = len(client.publish_calls)
        host.on_connect(client, None, None, 0)
        assert [c[0] for c in client.publish_calls[before:]] == [host.manifest_topic, host.status_topic, host.manifest['base'] + '/o/gripper-demo/state']

    def test_module_adapter_subtopic_is_resolved_under_robot_identity(self):
        configured = make_configured()
        configured["services"]["module_host"]["modules"][0]["adapters"]["command"] = {
            "kind": "forward",
            "subtopic": "incoming/drive-values",
        }
        manifest, adapters = build_manifest(configured, ROOT / "control/configs/config.json")
        endpoint = manifest["base"] + "/i/drive"
        assert adapters[endpoint]["topic"] == manifest["base"] + "/incoming/drive-values"

        configured["services"]["module_host"]["modules"][0]["adapters"]["command"]["topic"] = "other/topic"
        with self.assertRaises(ValueError):
            build_manifest(configured, ROOT / "control/configs/config.json")

    def test_bridge_preserves_live_retained_module_states(self):
        built = make_built()
        manifest, _ = built
        cfg = make_base_config('goob')
        bridge = MqttBridge(cfg, ROOT / 'control/configs/config.json')
        bridge.local_client = FakeMqttClient()
        bridge.remote_client = FakeMqttClient()
        topic = manifest['base'] + '/o/manifest'
        bridge._on_local_message(None, None, FakeMqttMessage(topic, json.dumps(manifest).encode(), retain=False))
        assert bridge.remote_client.publish_calls[-1][3] is True
        state = manifest['base'] + '/o/gripper-demo/state'
        bridge._on_local_message(None, None, FakeMqttMessage(state, b'{"position":.5}'.replace(b':.', b':0.'), retain=False))
        assert bridge.remote_client.publish_calls[-1][3] is True
        command = manifest['base'] + '/i/drive'
        bridge._on_remote_message(None, None, FakeMqttMessage(command, b'{"x":0,"z":0}'))
        assert bridge.local_client.publish_calls[-1][0] == command
        before = len(bridge.remote_client.publish_calls)
        bridge._on_local_message(None, None, FakeMqttMessage(command, b'{"x":0,"z":0}'))
        assert len(bridge.remote_client.publish_calls) == before

    def test_generic_gateway_validation_discovery_state_and_offline(self):
        built = make_built()
        manifest, _ = built
        calls = []
        connected = [True]
        app = Flask(__name__)
        ui = web.ModuleUI(app, lambda *a, **k: calls.append((a, k)) or True, lambda: connected[0])
        topics, _ = ui.consume(manifest['base'] + '/o/manifest', json.dumps(manifest).encode())
        assert manifest['availability_topic'] in topics
        presence_topic = manifest['base'] + '/o/online'
        assert presence_topic in topics
        ui.consume(manifest['availability_topic'], b'{"online":true}')
        ui.consume(manifest['base'] + '/o/soundboard/files', json.dumps({
            'files': ['beep.wav'], 'controls': True, 'timestamp': 1.0,
        }).encode())
        ui.consume(manifest['base'] + '/o/gripper-demo/state', b'{"position":0.7}')
        log_topic = manifest['base'] + '/o/logs'
        for index in range(105):
            ui.consume(log_topic, json.dumps({
                't': index, 'level': 'INFO', 'service': 'test',
                'message': f'entry {index}', 'logger': 'test.logger', 'pid': 1,
            }).encode())
        client = app.test_client()
        with client.get('/modules') as response:
            assert response.status_code == 200
        with client.get('/modules/assets/app.mjs') as response:
            assert response.status_code == 200
        state = client.get('/modules/api/state', query_string={'robot': manifest['base']}).json
        assert state['states']['soundboard']['files']['value']['files'] == ['beep.wav']
        assert state['states']['gripper-demo']['state']['value'] == {'position': 0.7}
        log_entries = state['states']['logs']['entries']['value']
        assert len(log_entries) == 100
        assert log_entries[0]['message'] == 'entry 5'
        assert log_entries[-1]['message'] == 'entry 104'
        assert state['online']
        body = {'robot': manifest['base'], 'revision': manifest['revision'], 'module': 'drive', 'endpoint': 'command', 'payload': {'x': 0, 'z': 0}}
        assert client.post('/modules/api/command', json=body).status_code == 200
        assert len(calls) == 1
        ui.consume(presence_topic, b'{"online":false}')
        assert client.post('/modules/api/command', json=body).status_code == 503
        ui.consume(presence_topic, b'{"online":true}')
        assert client.post('/modules/api/command', json=body).status_code == 200
        assert len(calls) == 2
        for invalid in ({**body, 'payload': {'x': 0, 'z': 2}}, {**body, 'topic': 'unrelated'}, {**body, 'endpoint': 'state'}):
            assert client.post('/modules/api/command', json=invalid).status_code in (400, 404)
        assert client.post('/modules/api/command', json={**body, 'revision': 'stale'}).status_code == 409
        assert len(calls) == 2
        connected[0] = False
        assert client.post('/modules/api/command', json=body).status_code == 503
        connected[0] = True
        ui.robots[manifest['base']]['status_seen'] -= 21
        assert client.post('/modules/api/command', json=body).status_code == 503
        _, removed = ui.consume(manifest['base'] + '/o/manifest', b'')
        assert manifest['availability_topic'] in removed
        assert presence_topic in removed
        assert client.get('/modules/api/robots').json['robots'] == []
if __name__ == '__main__':
    unittest.main()
