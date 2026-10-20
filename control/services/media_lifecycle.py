"""Demand and process lifecycle for AV capture, publishing, and speaker playback."""
from __future__ import annotations
import json
import logging
import os
from pathlib import Path
import shlex
import threading
import time

from control.common.modules import build_manifest
from control.common.mqtt import create_client, mqtt_auth_and_tls
from control.common.topics import identity_from_config

RESOURCES = {'video', 'microphone', 'video-stream', 'audio-stream', 'speaker'}


class MediaLifecycle:
    def __init__(self, config, path, capture, pipeline_class):
        self.config, self.path, self.capture = config, path, capture
        self.base = identity_from_config(config).base
        self.lock = threading.Lock()
        self.leases = {}
        self.routes = {}
        self.state_routes = []
        manifest, _ = build_manifest(config, path)
        for module in manifest['modules']:
            resources = {e['resource'] for e in module['endpoints'].values() if e.get('resource')}
            if resources and 'state' in module['endpoints']:
                self.state_routes.append((module['endpoints']['state']['topic'],resources))
            for endpoint in module['endpoints'].values():
                if endpoint.get('resource'):
                    self.routes[endpoint['topic']] = endpoint['resource']
        self.jobs = {}
        bridge = config.get('services', {}).get('mqtt_bridge', {})
        av = config['services']['av_daemon']
        specs = [('video-stream', bridge.get('video_control', {}), 'command', 'cwd', 'env'),
                 ('audio-stream', bridge.get('audio_control', {}), 'publisher_command', 'publisher_cwd', 'publisher_env'),
                 ('speaker', bridge.get('audio_control', {}), 'receiver_command', 'receiver_cwd', 'receiver_env')]
        for resource, cfg, command_key, cwd_key, env_key in specs:
            command = cfg.get(command_key)
            if not command:
                continue
            command = shlex.split(command) if isinstance(command, str) else list(command)
            cwd = Path(cfg.get(cwd_key) or cfg.get('cwd') or '../..')
            if not cwd.is_absolute():
                cwd = (path.parent/cwd).resolve()
            env = os.environ.copy()
            env.update({str(k):str(v) for k,v in cfg.get('env', {}).items()})
            env.update({str(k):str(v) for k,v in cfg.get(env_key, {}).items()})
            self.jobs[resource] = pipeline_class(resource, lambda c=command:(c, None),
                av.get('restart_delay_seconds', 2), min(float(av.get('stop_timeout_seconds', 3)), 3), env, str(cwd))
        self.client = create_client(client_id=self.base+'-av')
        local = config.get('local_mqtt', {})
        mqtt_auth_and_tls(self.client, local, path.parent)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.reconnect_delay_set(1, 15)
        self.client.will_set(self.base+'/o/media/state', json.dumps({'online':False}), qos=1, retain=True)
        self.last_state = 0
        self.last_body = None
        # Transitional classic controls remain usable, scoped to a separate consumer.
        topics = bridge.get('topics', {})
        self.legacy = {topics.get('video_control', self.base+'/incoming/front-camera'): {'video-stream'},
                       topics.get('audio_control', self.base+'/incoming/audio'): {'audio-stream','speaker'}}

    def start(self):
        local = self.config.get('local_mqtt', {})
        self.client.connect_async(local.get('host', '127.0.0.1'), int(local.get('port',1883)), 15)
        self.client.loop_start()

    def on_connect(self, client, _userdata, _flags, rc):
        if rc == 0:
            for topic in [self.base+'/i/media/lease', *self.routes, *self.legacy]:
                client.subscribe(topic, qos=1)

    def on_message(self, _client, _userdata, msg):
        if msg.retain or len(msg.payload) > 4096:
            return
        try:
            body = json.loads(msg.payload)
            if msg.topic in self.legacy:
                enabled = body.get('enabled', body.get('value')) if isinstance(body, dict) else body
                if not isinstance(enabled, bool):
                    return
                # Classic controls retain explicit on/off semantics; browser modules use expiring leases.
                self.request('classic:'+msg.topic, self.legacy[msg.topic] if enabled else set(), 86400, legacy=True)
                return
            if msg.topic in self.routes:
                if not isinstance(body, dict) or set(body)-{'enabled','consumer','ttl_seconds'} or not isinstance(body.get('enabled'),(int,float)) or not 0 <= body['enabled'] <= 1:
                    return
                resource = self.routes[msg.topic]
                consumer = body.get('consumer', 'direct:'+msg.topic)
                self.request(consumer, {resource} if body['enabled'] >= .5 else set(), body.get('ttl_seconds', 8))
            elif msg.topic == self.base+'/i/media/lease':
                self.request(body['consumer'], set(body['resources']), body.get('ttl_seconds',8))
        except (ValueError, TypeError, KeyError):
            logging.warning('Rejected media demand on %s', msg.topic)

    def request(self, consumer, resources, ttl, legacy=False):
        if not isinstance(consumer, str) or not 1 <= len(consumer) <= 160 or not resources <= RESOURCES:
            raise ValueError('Invalid resource lease')
        if not isinstance(ttl, (int,float)) or not 1 <= ttl <= (86400 if legacy else 15):
            raise ValueError('Invalid lease lifetime')
        with self.lock:
            if resources:
                if consumer not in self.leases and len(self.leases) >= 256:
                    raise ValueError('Media consumer limit reached')
                self.leases[consumer] = (resources, time.monotonic()+ttl)
            else:
                self.leases.pop(consumer, None)

    def tick(self, now):
        with self.lock:
            self.leases = {key:value for key,value in self.leases.items() if value[1] > now}
            wanted = set().union(*(r for r,_ in self.leases.values())) if self.leases else set()
            consumers = len(self.leases)
        capture_wanted = {'video': bool(wanted & {'video','video-stream'}),
                          'audio': bool(wanted & {'microphone','audio-stream'})}
        # Stop readers before releasing devices; start devices before readers.
        for resource, job in self.jobs.items():
            if resource not in wanted:
                job.stop()
        for pipeline in self.capture:
            if capture_wanted[pipeline.name]:
                pipeline.tick(now)
            else:
                pipeline.stop()
        for resource, job in self.jobs.items():
            dependency = 'video' if resource == 'video-stream' else 'audio' if resource == 'audio-stream' else None
            ready = dependency is None or any(p.name == dependency and p.process and p.process.poll() is None and p.socket_path and p.socket_path.exists() for p in self.capture)
            if resource in wanted and ready:
                job.tick(now)
            elif resource in wanted:
                job.stop()
        body = {'online':True,'requested':sorted(wanted),'consumers':consumers,
                'capture':{p.name:bool(p.process and p.process.poll() is None) for p in self.capture},
                'streams':{r:bool(p.process and p.process.poll() is None) for r,p in self.jobs.items()}}
        if self.client.is_connected() and (body != self.last_body or now-self.last_state > 3):
            self.client.publish(self.base+'/o/media/state', json.dumps(body), qos=1, retain=True)
            for topic, resources in self.state_routes:
                state = {'capture':body['capture'].get('video' if 'video-stream' in resources else 'audio',False),
                         'publishing':any(body['streams'].get(r,False) for r in resources-{'speaker'}),
                         'speaker':body['streams'].get('speaker',False) if 'speaker' in resources else False,
                         'requested':sorted(wanted & resources)}
                self.client.publish(topic,json.dumps(state),qos=1,retain=True)
            self.last_body, self.last_state = body, now

    def stop(self):
        for job in self.jobs.values():
            job.stop()
        if self.client.is_connected():
            info = self.client.publish(self.base+'/o/media/state', json.dumps({'online':False}), qos=1, retain=True)
            info.wait_for_publish(timeout=1)
        self.client.disconnect()
        self.client.loop_stop()
