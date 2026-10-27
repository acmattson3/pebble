#!/usr/bin/env python3
"""Short supervised motor pulse. Requires a currently confirmed lifted robot.

Software observations are not physical measurements. Default pulse: full forward
for 0.45 seconds, then zero. Stop is also sent from finally; device deadman is 1s.
"""
import argparse,json,time
from pathlib import Path
import threading
import paho.mqtt.client as mqtt
import serial

ap=argparse.ArgumentParser()
ap.add_argument('--port',required=True)
ap.add_argument('--password-file',type=Path,required=True)
ap.add_argument('--username',required=True)
ap.add_argument('--host',default='mqtt.sssn.us')
ap.add_argument('--base',default='pebble/robots/minilamp')
ap.add_argument('--lifted',action='store_true',help='Robot is currently lifted with all wheels/grousers clear')
ap.add_argument('--mode',choices=['forward','reverse','turn','grouser'],default='forward')
ap.add_argument('--output',type=Path,default=Path(__file__).with_name('motor-pulse-validation.json'))
args=ap.parse_args()
assert args.lifted,'Confirm current safe physical setup before this motion test'
base=args.base
ready=threading.Event()
c=mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,client_id='minilamp-pulse-'+str(time.time_ns()),transport='websockets')
c.ws_set_options(path='/');c.tls_set();c.username_pw_set(args.username,args.password_file.read_text().strip())
def connected(client,userdata,flags,reason,props):
    if reason==0:ready.set()
c.on_connect=connected
port=serial.Serial(args.port,115200,timeout=0.05,write_timeout=1)
def status():
    port.reset_input_buffer();port.write(b'V\n')
    end=time.monotonic()+1
    while time.monotonic()<end:
        line=port.readline().decode(errors='replace').strip()
        if line.startswith('{'):
            value=json.loads(line)
            if 'firmware' in value:return value
    raise AssertionError('No fresh serial status')
def send(suffix,value):
    c.publish(base+'/i/'+suffix,json.dumps(value),qos=0,retain=False)
def stop():
    if c.is_connected():
        send('drive',{'x':0,'z':0,'y':0});send('grouser',{'value':0})

try:
    c.connect(args.host,443,10);c.loop_start();assert ready.wait(10)
    initial=status()
    assert initial['firmware']=='minilamp-c6-onoff-wss-v2'
    assert initial['mqtt_connected'] and initial['motors_ready']
    assert initial['motor_values']==[0,0,0,0]
    assert initial['battery_v']>1,'Connect battery first'
    command={'x':0,'z':0,'y':0}
    if args.mode=='forward':command['z']=1.0
    if args.mode=='reverse':command['z']=-1.0
    if args.mode=='turn':command['x']=1.0
    began=time.monotonic();samples=[]
    if args.mode=='grouser':send('grouser',{'value':1.0})
    else:send('drive',command)
    while time.monotonic()-began < 0.45:
        samples.append(status());time.sleep(0.05)
    stop()
    end=time.monotonic()+2
    while True:
        final=status()
        if final['motor_values']==[0,0,0,0]:break
        assert time.monotonic()<end,'Motor state did not return to zero'
        time.sleep(0.05)
    assert all(s['motors_ready'] and s['boot_id']==initial['boot_id'] for s in samples+[final])
    assert any(any(abs(v)>0 for v in s['motor_values']) for s in samples),'No pulse observed in software'
    result={'pass':True,'mode':args.mode,'requested_power':1.0,'requested_seconds':0.45,
            'software_only':True,'initial':initial,'during':samples,'final':final}
    args.output.write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps({'pass':True,'mode':args.mode,'battery_v':final['battery_v'],
          'during':[s['motor_values'] for s in samples],'final_motor_values':final['motor_values'],
          'motors_ready':final['motors_ready']}),flush=True)
finally:
    stop();time.sleep(0.15)
    c.disconnect();c.loop_stop();port.close()
