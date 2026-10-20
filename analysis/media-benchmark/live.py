#!/usr/bin/env python3
"""Observe the live module video path via the HTTP gateway (no drive commands).

Run on the gateway host; use --output to keep JSON metrics and a JPEG sample.
Camera demand is renewed and released in finally. Other consumers are untouched.
"""
import argparse
import hashlib
import json
from pathlib import Path
import statistics
import time
import urllib.parse
import urllib.request
import uuid


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url',default='http://127.0.0.1:8080')
    parser.add_argument('--robot',default='pebble/robots/goob')
    parser.add_argument('--module',default='camera')
    parser.add_argument('--seconds',type=float,default=35)
    parser.add_argument('--warmup',type=float,default=8)
    parser.add_argument('--output',required=True)
    args=parser.parse_args()
    query=urllib.parse.urlencode({'robot':args.robot})
    session=uuid.uuid4().hex
    def fetch(path):
        with urllib.request.urlopen(args.url+path,timeout=5) as response:
            return response.status,response.read(),response.headers
    def state():return json.loads(fetch('/modules/api/state?'+query)[1])
    for _ in range(30):
        initial=state()
        if initial['online']:break
        time.sleep(.5)
    else:raise RuntimeError('Robot did not come online')
    revision=initial['revision']
    media='/modules/api/media/'+urllib.parse.quote(args.module)+'/stream?'+query+'&revision='+revision
    def command(value):
        data={'robot':args.robot,'module':args.module,'endpoint':'request','revision':revision,'payload':{'enabled':value}}
        req=urllib.request.Request(args.url+'/modules/api/command',data=json.dumps(data).encode(),headers={'Content-Type':'application/json','X-Pebble-Session':session})
        with urllib.request.urlopen(req,timeout=5) as response:return json.load(response)
    started=time.monotonic();renew_at=started;sample_at=started;observations=[];hashes=set();jpeg=None;frames=0;offline=0;first_frame=None
    print('Observing live camera for',args.seconds,'seconds',flush=True)
    try:
        while time.monotonic()-started<args.seconds:
            now=time.monotonic()
            if now>=renew_at:
                command(1);renew_at=now+2
            status,body,headers=fetch(media)
            if status==200:
                frames+=1;jpeg=body;hashes.add(hashlib.sha256(body).hexdigest())
                if first_frame is None:first_frame=now-started
            if now>=sample_at:
                current=state();offline+=int(not current['online'])
                stats=json.loads(fetch(media+'&stats=1')[1]);stats['elapsed_seconds']=round(now-started,2)
                if now-started>=args.warmup:observations.append(stats)
                if int(now-started)%10==0:print(json.dumps(stats),flush=True)
                sample_at=now+1
            time.sleep(max(0,1/15-(time.monotonic()-now)))
    finally:
        command(0)
    live=[s for s in observations if s['live']]
    summary={'codec':live[-1]['codec'] if live else None,'seconds':args.seconds,'warmup_seconds':args.warmup,
        'http_frames':frames,'distinct_jpegs':len(hashes),'offline_observations':offline,'first_frame_seconds':first_frame,
        'mean_mqtt_kbps':round(statistics.mean(s['mqtt_kbps'] for s in live),1) if live else None,
        'mean_fps':round(statistics.mean(s['fps'] for s in live),1) if live else None,
        'mean_gateway_delay_ms':round(statistics.mean(s['gateway_delay_ms'] for s in live),1) if live else None,
        'decoder_resets':live[-1]['decoder_resets'] if live else None,'packets_dropped':live[-1]['packets_dropped'] if live else None}
    out=Path(args.output);out.parent.mkdir(parents=True,exist_ok=True);out.write_text(json.dumps({'summary':summary,'samples':observations},indent=2)+'\n')
    if jpeg:out.with_suffix('.jpg').write_bytes(jpeg)
    print(json.dumps(summary),flush=True)
    if not live or offline:raise SystemExit('Stream was unavailable or went offline')


if __name__=='__main__':main()
