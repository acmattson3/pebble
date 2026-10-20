"""Renewable local capture leases for standalone consumers and autonomy code."""
import json
import threading
import uuid
from control.common.mqtt import create_client, mqtt_auth_and_tls
from control.common.topics import identity_from_config


class CaptureLease:
    """Use `with CaptureLease(config, path, ['video']): ...` around a reader.

    Raw capture does not start network publishing. A killed process loses its
    lease within eight seconds; other consumers retain their own leases.
    """
    def __init__(self, config, path, resources):
        self.config, self.path, self.resources = config, path, list(resources)
        if not set(resources) <= {'video','microphone'}:
            raise ValueError('CaptureLease supports video and microphone resources')
        self.consumer='local:'+uuid.uuid4().hex
        self.topic=identity_from_config(config).base+'/i/media/lease'
        self.stopped=threading.Event()
        self.client=create_client(client_id=self.consumer)
        mqtt_auth_and_tls(self.client,config.get('local_mqtt',{}),path.parent)
        self.client.on_connect=lambda c,u,f,rc:self.publish(self.resources) if rc==0 else None
        self.client.will_set(self.topic,json.dumps(self.body([])),qos=1,retain=False)

    def body(self, resources):
        return {'consumer':self.consumer,'resources':resources,'ttl_seconds':8}

    def publish(self, resources):
        return self.client.publish(self.topic,json.dumps(self.body(resources)),qos=0,retain=False)

    def __enter__(self):
        cfg=self.config.get('local_mqtt',{})
        self.client.connect_async(cfg.get('host','127.0.0.1'),int(cfg.get('port',1883)),15)
        self.client.loop_start()
        self.thread=threading.Thread(target=self.renew,daemon=True);self.thread.start()
        return self

    def renew(self):
        while not self.stopped.wait(2):
            if self.client.is_connected():self.publish(self.resources)

    def __exit__(self,*_):
        self.stopped.set();self.thread.join(timeout=3)
        if self.client.is_connected():self.publish([]).wait_for_publish(timeout=1)
        self.client.disconnect();self.client.loop_stop()
