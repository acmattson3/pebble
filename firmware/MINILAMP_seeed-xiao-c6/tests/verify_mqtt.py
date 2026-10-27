#!/usr/bin/env python3
"""Verify MiniLAMP discovery, telemetry and command rejection over verified WSS.

Valid commands contain only zero motor values. Requires paho-mqtt and the
repository's jsonschema dependency. Never prints or records credentials.
"""
import argparse
from collections import defaultdict
import json
from pathlib import Path
import ssl
import sys
import threading
import time
import paho.mqtt.client as mqtt

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from control.common.modules import validate_manifest, build_manifest

ap = argparse.ArgumentParser()
ap.add_argument('--host', default='mqtt.sssn.us')
ap.add_argument('--username', required=True)
ap.add_argument('--password-file', type=Path, required=True)
ap.add_argument('--base', default='pebble/robots/minilamp')
ap.add_argument('--seconds', type=float, default=60)
ap.add_argument('--output', type=Path, default=Path(__file__).with_name('live-validation.json'))
args = ap.parse_args()
config_path = ROOT/'robots/minilamp/modules.example.json'
expected_manifest, _ = build_manifest(json.loads(config_path.read_text()), config_path)
condition = threading.Condition()
latest, counts, statuses, batteries = {}, defaultdict(int), [], []
started = time.monotonic()
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id='minilamp-check-'+str(time.time_ns()),
                     transport='websockets', protocol=mqtt.MQTTv311)
client.ws_set_options(path='/')
client.tls_set(cert_reqs=ssl.CERT_REQUIRED)
client.username_pw_set(args.username, args.password_file.read_text().strip())

def on_connect(c, _userdata, _flags, reason, _properties):
    if reason == 0:
        c.subscribe(args.base+'/o/#', qos=1)

def on_message(_client, _userdata, msg):
    try:
        value = json.loads(msg.payload)
    except (ValueError, UnicodeError):
        return
    with condition:
        latest[msg.topic] = value
        counts[msg.topic] += 1
        if msg.topic.endswith('/o/status') and value.get('firmware') == 'minilamp-c6-onoff-wss-v2':
            statuses.append(value)
        if msg.topic.endswith('/o/telemetry/battery'):
            batteries.append(value)
        condition.notify_all()

client.on_connect = on_connect
client.on_message = on_message

def wait_for(predicate, timeout=15):
    end = time.monotonic()+timeout
    with condition:
        while not predicate():
            remaining = end-time.monotonic()
            assert remaining > 0, 'Timed out waiting for expected robot state'
            condition.wait(remaining)

def send(suffix, body):
    client.publish(args.base+'/i/'+suffix, body, qos=0, retain=False).wait_for_publish(5)

try:
    client.connect(args.host,443,10)
    client.loop_start()
    wait_for(lambda: statuses and statuses[-1].get('firmware') == 'minilamp-c6-onoff-wss-v2', 90)
    initial = statuses[-1].copy()
    assert initial['motors_ready'] and initial['mqtt_connected'] and initial['wifi_connected']
    assert initial['motor_values'] == [0,0,0,0], 'Robot is receiving another drive source'
    wait_for(lambda: latest.get(args.base+'/o/manifest', {}).get('revision') == expected_manifest['revision'] and batteries)
    manifest = validate_manifest(latest[args.base+'/o/manifest'])
    assert manifest['base'] == args.base
    assert {m['id'] for m in manifest['modules']} == {'drive','grouser','telemetry'}
    before = initial['accepted_commands']
    send('drive', json.dumps({'x':0,'z':0}))
    send('grouser', json.dumps({'value':0}))
    send('drive', json.dumps({'value':{'x':0,'z':0,'y':0}}))
    wait_for(lambda: statuses[-1]['accepted_commands'] >= before+3)
    wait_for(lambda: not statuses[-1]['drive_active'] and not statuses[-1]['grouser_active'])
    settled = statuses[-1].copy()
    bad = ['{"x":"bad","z":0}', '{"x":0}', '{"x":false,"z":0}',
           '{"x":NaN,"z":0}', '{"x":0,"z":0,"padding":"'+'a'*270+'"}',
           b'{"x":0,"z":0}\x00']
    for payload in bad:
        send('drive', payload)
    wait_for(lambda: statuses[-1]['rejected_commands'] >= settled['rejected_commands']+len(bad))
    final = statuses[-1].copy()
    assert final['drive_at_ms'] == settled['drive_at_ms']
    assert final['grouser_at_ms'] == settled['grouser_at_ms']
    assert final['motor_values'] == [0,0,0,0]
    end = time.monotonic()+args.seconds
    # Soak after command checks, collecting fresh heartbeats and voltage.
    while time.monotonic() < end:
        time.sleep(min(1, end-time.monotonic()))
    with condition:
        assert len(statuses) >= 6 and len(batteries) >= 2
        assert all(b['uptime_ms'] > a['uptime_ms'] for a,b in zip(statuses,statuses[1:]))
        assert all(s['boot_id'] == initial['boot_id'] and s['motors_ready'] for s in statuses)
        assert all(b['unit']=='v' and isinstance(b['value'], (int,float)) and 0 <= b['value'] <= 6.6 for b in batteries)
        result={'pass':True,'host':args.host,'transport':'wss','tls_verified':True,
                'elapsed_seconds':round(time.monotonic()-started,3),'status_messages':len(statuses),
                'battery_messages':len(batteries),'battery_min_v':min(b['value'] for b in batteries),
                'battery_max_v':max(b['value'] for b in batteries),'manifest_revision':manifest['revision'],
                'commands':'zero valid drive/grouser/legacy-wrapper and six invalid drive payloads',
                'first_status':initial,'last_status':statuses[-1]}
    args.output.write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result),flush=True)
finally:
    if client.is_connected():
        send('drive', '{"x":0,"z":0,"y":0}')
        send('grouser','{"value":0}')
        client.disconnect()
    client.loop_stop()
