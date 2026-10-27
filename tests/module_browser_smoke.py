"""Real two-broker + browser smoke test; all commands stay in isolated containers.

Requires Docker's cached eclipse-mosquitto:2.0.22 image and Playwright Chromium.
Run from the repo root with the normal Python dependencies plus Playwright.
"""
from __future__ import annotations
import importlib.util
import json
import os
from pathlib import Path
import struct
import subprocess
import sys
import tempfile
import threading
import time

from flask import Flask, Response
import paho.mqtt.client as mqtt
from playwright.sync_api import sync_playwright
from werkzeug.serving import make_server

from control.services.module_host import ModuleHost
from control.services.mqtt_bridge import MqttBridge

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'web-interface'))
spec = importlib.util.spec_from_file_location("module_ui", ROOT/"web-interface/module_ui.py")
web = importlib.util.module_from_spec(spec); spec.loader.exec_module(web)


def orientation_test_glb():
    """Build a tiny self-contained rectangular robot-block GLB fixture."""
    faces = [
        ((1, 0, 0), ((1, -0.5, -0.35), (1, 0.5, -0.35), (1, 0.5, 0.35), (1, -0.5, 0.35))),
        ((-1, 0, 0), ((-1, -0.5, 0.35), (-1, 0.5, 0.35), (-1, 0.5, -0.35), (-1, -0.5, -0.35))),
        ((0, 1, 0), ((-1, 0.5, -0.35), (-1, 0.5, 0.35), (1, 0.5, 0.35), (1, 0.5, -0.35))),
        ((0, -1, 0), ((-1, -0.5, 0.35), (-1, -0.5, -0.35), (1, -0.5, -0.35), (1, -0.5, 0.35))),
        ((0, 0, 1), ((-1, -0.5, 0.35), (1, -0.5, 0.35), (1, 0.5, 0.35), (-1, 0.5, 0.35))),
        ((0, 0, -1), ((1, -0.5, -0.35), (-1, -0.5, -0.35), (-1, 0.5, -0.35), (1, 0.5, -0.35))),
    ]
    positions, normals, indices = [], [], []
    for normal, vertices in faces:
        start = len(positions) // 3
        for vertex in vertices:
            positions.extend(vertex)
            normals.extend(normal)
        indices.extend((start, start + 1, start + 2, start, start + 2, start + 3))
    chunks = []
    offsets = []
    for values, fmt in ((positions, '<f'), (normals, '<f'), (indices, '<H')):
        raw = struct.pack('<' + fmt[-1] * len(values), *values)
        while sum(map(len, chunks)) % 4:
            chunks.append(b'\0')
        offset = sum(map(len, chunks))
        chunks.append(raw)
        offsets.append((offset, len(raw)))
    binary = b''.join(chunks)
    min_pos = [min(positions[i::3]) for i in range(3)]
    max_pos = [max(positions[i::3]) for i in range(3)]
    doc = {
        'asset': {'version': '2.0', 'generator': 'Pebble browser smoke fixture'},
        'scene': 0, 'scenes': [{'nodes': [0]}],
        'nodes': [{'mesh': 0}],
        'meshes': [{'primitives': [{'attributes': {'POSITION': 0, 'NORMAL': 1}, 'indices': 2, 'material': 0}]}],
        'materials': [{'pbrMetallicRoughness': {'baseColorFactor': [0.24, 0.72, 0.58, 1], 'metallicFactor': 0, 'roughnessFactor': 0.8}}],
        'buffers': [{'byteLength': len(binary)}],
        'bufferViews': [
            {'buffer': 0, 'byteOffset': offsets[0][0], 'byteLength': offsets[0][1], 'target': 34962},
            {'buffer': 0, 'byteOffset': offsets[1][0], 'byteLength': offsets[1][1], 'target': 34962},
            {'buffer': 0, 'byteOffset': offsets[2][0], 'byteLength': offsets[2][1], 'target': 34963},
        ],
        'accessors': [
            {'bufferView': 0, 'componentType': 5126, 'count': len(positions) // 3, 'type': 'VEC3', 'min': min_pos, 'max': max_pos},
            {'bufferView': 1, 'componentType': 5126, 'count': len(normals) // 3, 'type': 'VEC3'},
            {'bufferView': 2, 'componentType': 5123, 'count': len(indices), 'type': 'SCALAR', 'min': [min(indices)], 'max': [max(indices)]},
        ],
    }
    json_chunk = json.dumps(doc, separators=(',', ':')).encode()
    json_chunk += b' ' * (-len(json_chunk) % 4)
    binary += b'\0' * (-len(binary) % 4)
    total = 12 + 8 + len(json_chunk) + 8 + len(binary)
    return (struct.pack('<III', 0x46546C67, 2, total)
            + struct.pack('<II', len(json_chunk), 0x4E4F534A) + json_chunk
            + struct.pack('<II', len(binary), 0x004E4942) + binary)


def wait_until(check, timeout=12):
    end = time.monotonic()+timeout
    while time.monotonic()<end:
        if check(): return
        time.sleep(.04)
    raise AssertionError("Timed out waiting for test condition")


def main():
    containers=[]; clients=[]; bridge=host=server=None
    try:
        with tempfile.TemporaryDirectory(prefix="pebble-module-test-") as temp:
            conf=Path(temp)/"mosquitto.conf"
            conf.write_text("listener 1883\nallow_anonymous true\npersistence false\n")
            conf.chmod(0o644)
            for port,label in ((18884,"local"),(18885,"remote")):
                name=f"pebble-module-test-{os.getpid()}-{label}"
                subprocess.run(["docker","run","--rm","-d","--name",name,
                    "-p",f"127.0.0.1:{port}:1883","-v",f"{conf}:/mosquitto/config/mosquitto.conf:ro",
                    "eclipse-mosquitto:2.0.22"],check=True,capture_output=True)
                containers.append(name)
            time.sleep(.5)
            config=json.loads((ROOT/"robots/goob/modules.example.json").read_text())
            config['robot']['id']='module-integration'
            config['services']['module_host']['modules'].append({
                'id': 'orientation-preview',
                'use': 'modules/telemetry/module.json',
                'endpoints': ['imu'],
                'ui': {'columns': 1, 'controls': [{
                    'kind': 'orientation-3d', 'label': '3D orientation preview',
                    'endpoint': 'imu', 'model_url': '/test-assets/orientation-block.glb',
                    'model_rotation_deg': {'x': 90, 'y': 0, 'z': -90},
                }]},
            })
            config['services']['module_host']['modules'].append({
                'id': 'gripper-demo',
                'use': 'robots/goob/modules/gripper-demo/module.json',
                'adapters': {'command': {'kind': 'echo', 'state_endpoint': 'state'}},
            })
            config['local_mqtt']={'host':'127.0.0.1','port':18884}
            config['services']['mqtt_bridge']={'enabled':True,'remote_mqtt':{'host':'127.0.0.1','port':18885}}
            host=ModuleHost(config,ROOT/"control/configs/config.json")
            bridge=MqttBridge(config,ROOT/"control/configs/config.json")
            for worker in (host,bridge):threading.Thread(target=worker.start,daemon=True).start()
            base=host.manifest['base']
            local_messages=[]
            observer=mqtt.Client(client_id='module-test-observer')
            observer.on_connect=lambda c,*_:c.subscribe(base+'/#')
            observer.on_message=lambda c,u,m:local_messages.append((m.topic,json.loads(m.payload)))
            observer.connect('127.0.0.1',18884);observer.loop_start();clients.append(observer)
            local_publisher=mqtt.Client(client_id='module-test-local-publisher')
            local_publisher.connect('127.0.0.1',18884);local_publisher.loop_start();clients.append(local_publisher)
            local_publisher.publish(base+'/o/online',json.dumps({'online':True}),qos=1,retain=True).wait_for_publish()
            local_publisher.publish(base+'/o/soundboard/files',json.dumps({'files':['demo.wav'],'controls':True,'timestamp':time.time()}),qos=1,retain=True).wait_for_publish()
            local_publisher.publish(base+'/o/soundboard/status',json.dumps({'playing':False,'file':None,'error':None,'controls':True,'timestamp':time.time()}),qos=1,retain=True).wait_for_publish()
            local_publisher.publish(base+'/o/telemetry/imu',json.dumps({
                'orientation_deg': {'roll': 18.0, 'pitch': -12.0},
                'accel_norm_g': 1.0, 'temperature_c': 24.5,
                'health': {'device': 'fixture', 'protocol': 'test', 'samples_ok': 20,
                           'samples_error': 0, 'calibration_samples': 100},
            }),qos=1,retain=True).wait_for_publish()
            app=Flask(__name__)
            @app.get('/test-assets/orientation-block.glb')
            def orientation_model():
                return Response(orientation_test_glb(), mimetype='model/gltf-binary')
            gateway=mqtt.Client(client_id='module-test-web')
            clients.append(gateway)
            def publish(topic,payload,qos=0,retain=False):
                return gateway.publish(topic,json.dumps(payload),qos=qos,retain=retain).rc==0
            ui=web.ModuleUI(app,publish,gateway.is_connected)
            def consume(client,userdata,msg):
                ui.handle_message(client,msg)
            def connected(client,*_):
                ui.subscribe(client)
            gateway.on_connect=connected;gateway.on_message=consume
            gateway.connect('127.0.0.1',18885);gateway.loop_start()
            wait_until(lambda:base in ui.robots and ui.online(ui.robots[base]))
            server=make_server('127.0.0.1',18086,app,threaded=True)
            threading.Thread(target=server.serve_forever,daemon=True).start()
            drive=lambda:[p for t,p in local_messages if t==base+'/incoming/drive-values']
            sound_commands=lambda:[p for t,p in local_messages if t==base+'/incoming/soundboard-command']
            with sync_playwright() as pw:
                browser=pw.chromium.launch(headless=True)
                page=browser.new_page(viewport={'width':1200,'height':1000})
                errors=[];page.on('pageerror',lambda e:errors.append(str(e)))
                page.add_init_script("Object.defineProperty(navigator,'getGamepads',{value:()=>window.testPads||[]});")
                page.goto('http://127.0.0.1:18086/modules')
                page.locator('[data-module="drive"]').wait_for()
                page.locator('[data-module="soundboard"]').wait_for()
                orientation=page.locator('[data-module="orientation-preview"] .orientation-widget')
                orientation.wait_for()
                canvas=orientation.locator('.orientation-canvas')
                page.wait_for_function("document.querySelector('[data-module=\\\"orientation-preview\\\"] .orientation-canvas')?.dataset.modelLoaded==='true'")
                assert 'Live · roll 18.0° · pitch -12.0°' in orientation.locator('.orientation-status').text_content()
                assert canvas.evaluate('(node) => node.width > 0 && node.height > 0')
                telemetry=page.locator('[data-module="telemetry"] .telemetry-widget')
                telemetry.wait_for()
                online_value=telemetry.locator('.telemetry-metric').nth(1).locator('strong')
                last_seen_value=telemetry.locator('.telemetry-metric').nth(2).locator('strong')
                wait_until(lambda:online_value.text_content()=='Yes')
                assert last_seen_value.text_content() not in ('—','')
                page.wait_for_function("document.querySelector('#connection').textContent.startsWith('Online')")
                soundboard=page.locator('[data-module="soundboard"]')
                soundboard.get_by_label('Soundboard sound file').select_option('demo.wav')
                soundboard.get_by_role('button',name='Play selected Soundboard sound').click()
                wait_until(lambda:len(sound_commands())==1)
                assert sound_commands()[-1]=={'action':'play','file':'demo.wav'}
                soundboard.get_by_role('button',name='Stop Soundboard').click()
                wait_until(lambda:len(sound_commands())==2)
                assert sound_commands()[-1]=={'action':'stop','file':''}
                assert not drive(), 'Opening controls must not send drive commands'
                gripper=page.locator('[data-module="gripper-demo"]')
                gripper.get_by_role('button',name='Open',exact=True).click()
                page.wait_for_function("document.querySelector('[data-module=\"gripper-demo\"] .state strong').textContent==='1'")
                page.keyboard.press('h')
                page.wait_for_function("document.querySelector('[data-module=\"gripper-demo\"] .state strong').textContent==='0'")
                # Toggling a non-text form control must not swallow letter shortcuts.
                page.locator('#keyboard').uncheck()
                page.locator('#keyboard').check()
                before=len(drive())
                page.keyboard.down('w');wait_until(lambda:len(drive())>before and drive()[-1]['z']==1)
                page.keyboard.up('w');wait_until(lambda:drive()[-1]['z']==0)
                nonzero=[p for p in drive()[before:] if p['z']!=0]
                assert len(nonzero)==1,'One command must traverse the compatibility adapter once'
                card=page.locator('[data-module="drive"]')
                card.locator('summary').click()
                key=card.locator('.mapping').nth(0).locator('input[type=text]')
                key.focus();key.press('t');assert key.input_value()=='KeyT'
                page.locator('h1').click();before=len(drive());page.keyboard.press('w');page.wait_for_timeout(100)
                assert len(drive())==before,'Replaced keyboard binding must be inactive'
                page.keyboard.down('t');wait_until(lambda:drive()[-1]['z']==1)
                page.evaluate("window.dispatchEvent(new Event('blur'))")
                wait_until(lambda:drive()[-1]['z']==0);page.keyboard.up('t')
                page.reload();page.locator('[data-module="drive"]').wait_for();card=page.locator('[data-module="drive"]');card.locator('summary').click()
                assert card.locator('.mapping').nth(0).locator('input[type=text]').input_value()=='KeyT'
                page.evaluate("window.testPads=[{index:0,id:'Integration controller',axes:[0,0],buttons:[{pressed:false},{pressed:false}]}];window.dispatchEvent(new Event('gamepadconnected'))")
                page.locator('#gamepad').check()
                page.evaluate("window.testPads[0].axes[1]=-.6")
                wait_until(lambda:drive()[-1]['z']>.5)
                page.evaluate("window.testPads=[];window.dispatchEvent(new Event('gamepaddisconnected'))")
                wait_until(lambda:drive()[-1]['z']==0)
                stick=page.locator('.stick');box=stick.bounding_box()
                page.mouse.move(box['x']+box['width']/2,box['y']+box['height']/2)
                page.mouse.down();page.mouse.move(box['x']+box['width']/2,box['y']+20)
                wait_until(lambda:drive()[-1]['z']>.5)
                page.mouse.up();wait_until(lambda:drive()[-1]['z']==0)
                page.screenshot(path='/tmp/pebble-modules-browser.png',full_page=True)
                assert not errors,errors
                browser.close()
            # A fresh subscriber must get live-published discovery AND state retained
            # through the remote broker, rather than wait for a republish interval.
            retained=[]
            late=mqtt.Client(client_id='module-test-late')
            late.on_connect=lambda c,*_:c.subscribe([(base+'/o/manifest',1),(base+'/o/gripper-demo/state',1),(base+'/o/soundboard/files',1)])
            late.on_message=lambda c,u,m:retained.append((m.topic,m.retain))
            late.connect('127.0.0.1',18885);late.loop_start();clients.append(late)
            wait_until(lambda:len(retained)>=3)
            assert all(flag for _,flag in retained)
            print(json.dumps({'result':'passed','checks':['two-broker discovery','generic custom controls','robot-advertised soundboard widget','3D orientation widget loads a self-contained GLB and applies IMU telemetry','telemetry online and last-seen display','soundboard file state and play/stop commands','custom keyboard actions','drive forwarding once','keyboard remapping','mapping persistence','focus-loss neutral','controller axis','controller disconnect neutral','pointer stick','retained remote manifest/state','no browser exceptions'],'drive_messages':len(drive())}))
    finally:
        if server:server.shutdown()
        if bridge:bridge.stop()
        if host:host.stop_event.set()
        for client in clients:client.disconnect();client.loop_stop()
        for name in containers:subprocess.run(['docker','stop','--time','1',name],capture_output=True)


if __name__=='__main__':main()
