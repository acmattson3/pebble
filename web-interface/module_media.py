"""Binary media gateway for robot-declared endpoints; no robot-specific routes."""
from collections import deque
import struct
import logging
import threading
import time

from flask import Response, jsonify, request
from control.common.opus import AudioDecoder, Opus, packet
from control.common.h264 import Decoder as H264Decoder

VIDEO_HEADER = struct.Struct('!4sBBBIQ')


class ModuleMedia:
    def __init__(self, owner, bp):
        self.owner = owner
        self.lock = threading.RLock()
        self.streams = {}

        @bp.route('/modules/api/media/<module_id>/<endpoint_id>', methods=['GET','POST'])
        def media(module_id, endpoint_id):
            with owner.lock:
                robot = owner.robots.get(request.args.get('robot'))
                if not robot or not owner.online(robot):
                    return jsonify(error='Robot media is offline'), 503
                if request.args.get('revision') != robot['manifest']['revision']:
                    return jsonify(error='Module definitions changed; refresh controls'), 409
                module = next((m for m in robot['manifest']['modules'] if m['id']==module_id), None)
                endpoint = module['endpoints'].get(endpoint_id) if module else None
                if not endpoint or endpoint['kind'] not in {'video','audio','uplink'}:
                    return jsonify(error='Unknown media endpoint'), 404
                topic = endpoint['topic']
            with self.lock:
                entry = self.streams.setdefault(topic, {})
                if request.method == 'POST':
                    if endpoint['kind'] != 'uplink' or request.mimetype != 'application/octet-stream':
                        return jsonify(error='Expected PCM16LE audio uplink'), 400
                    if request.content_length is None or not 0 < request.content_length <= 32000:
                        return jsonify(error='Audio upload must be bounded'), 400
                    rate, channels = endpoint['rate'], endpoint['channels']
                    size = rate*channels*2//50
                    pcm = request.get_data()
                    if len(pcm) % size:
                        return jsonify(error='Audio must contain complete 20 ms frames'), 400
                    try:
                        if 'encoder' not in entry:
                            entry['encoder'] = Opus(rate, channels, 24000)
                            entry['seq'] = 0
                        for offset in range(0,len(pcm),size):
                            entry['seq'] += 1
                            data = packet(pcm[offset:offset+size],entry['seq'],rate,channels,entry['encoder'])
                            if not owner.publish(topic, data, qos=0, retain=False):
                                return jsonify(error='MQTT unavailable'), 503
                    except (ValueError, OSError) as exc:
                        return jsonify(error=str(exc)), 503
                    return jsonify(status='published')
                if endpoint['kind'] == 'video':
                    if request.args.get('stats') == '1':
                        return jsonify(self.video_stats(entry,endpoint))
                    if time.monotonic()-entry.get('received',0) > 2 or 'jpeg' not in entry:
                        return Response(status=204)
                    stats = self.video_stats(entry,endpoint)
                    return Response(entry['jpeg'], mimetype='image/jpeg', headers={
                        'Cache-Control':'no-store','X-Video-Codec':endpoint['codec'],
                        'X-Video-Sequence':str(stats.get('sequence',0)),
                        'X-Video-Kbps':str(stats.get('mqtt_kbps',0)),
                        'X-Video-Fps':str(stats.get('fps',0)),
                        'X-Video-Age-Ms':str(round((time.monotonic()-entry['received'])*1000))})
                if endpoint['kind'] != 'audio':
                    return jsonify(error='Uplink is write-only'), 405
                try:
                    after = int(request.args.get('after','0'))
                except ValueError:
                    return jsonify(error='Invalid audio cursor'),400
                frames = [f for f in entry.get('frames',[]) if f['cursor'] > after and time.monotonic()-f['received'] < 1]
                if not frames:
                    return Response(status=204)
                return Response(b''.join(f['data'] for f in frames), mimetype='application/octet-stream', headers={
                    'X-Audio-Cursor':str(frames[-1]['cursor']), 'X-Audio-Rate':str(frames[-1]['rate']),
                    'X-Audio-Channels':str(frames[-1]['channels']), 'Cache-Control':'no-store'})

    def forget(self, topics):
        with self.lock:
            for topic in topics:
                self.streams.pop(topic, None)

    def consume(self, endpoint, payload):
        if len(payload) > 4*1024*1024 or endpoint['kind'] not in {'video','audio'}:
            return
        with self.lock:
            entry = self.streams.setdefault(endpoint['topic'], {})
            now = time.monotonic()
            try:
                if endpoint['kind'] == 'audio':
                    decoder = entry.setdefault('decoder', AudioDecoder())
                    frame = decoder.parse(payload)
                    if frame:
                        entry['cursor'] = entry.get('cursor',0)+1
                        frame.update(cursor=entry['cursor'],received=now)
                        entry.setdefault('frames',deque(maxlen=50)).append(frame)
                else:
                    self.record_packet(entry,len(payload),now)
                    import cv2
                    if endpoint['codec'] == 'h264':
                        decoder = entry.setdefault('h264_decoder',H264Decoder())
                        decoded = decoder.decode(payload)
                        if decoded is None:
                            return
                        frame,info = decoded
                        ok,jpeg = cv2.imencode('.jpg',frame)
                        if ok:
                            entry['jpeg'],entry['received'] = jpeg.tobytes(),now
                            self.record_video(entry,len(payload),info['seq'],info['timestamp_ms'],now)
                        return
                    import cv2
                    import numpy as np
                    if len(payload) < VIDEO_HEADER.size:
                        return
                    magic, version, kind, flags, seq, timestamp = VIDEO_HEADER.unpack_from(payload)
                    if magic != b'PBVD' or version != 1 or kind not in (1,2):
                        return
                    if kind == 2 and ('reference' not in entry or seq != (entry.get('seq',-1)+1)&0xffffffff):
                        entry.pop('reference',None)
                        return
                    frame = cv2.imdecode(np.frombuffer(payload[VIDEO_HEADER.size:],dtype=np.uint8),cv2.IMREAD_COLOR)
                    if frame is None or frame.shape[0]*frame.shape[1] > 4096*2160:
                        return
                    if kind == 2:
                        if frame.shape != entry['reference'].shape:
                            entry.pop('reference',None)
                            return
                        frame = np.clip(entry['reference'].astype(np.int16)+frame.astype(np.int16)-128,0,255).astype(np.uint8)
                    entry['reference'],entry['seq'] = frame,seq
                    ok,jpeg = cv2.imencode('.jpg',frame)
                    if ok:
                        entry['jpeg'],entry['received'] = jpeg.tobytes(),now
                        self.record_video(entry,len(payload),seq,timestamp,now)
            except Exception as exc:
                # FFmpeg errors must remain inside the media handler. A corrupt
                # AU invalidates its references; the next IDR starts decoding.
                if 'h264_decoder' in entry:
                    entry['h264_decoder'].reset()
                if now-entry.get('last_error_at',0) > 5:
                    logging.warning('Rejected %s media on %s: %s',endpoint.get('codec'),endpoint['topic'],exc)
                    entry['last_error_at'] = now
                return

    @staticmethod
    def record_packet(entry, size, now):
        samples = entry.setdefault('video_samples',deque(maxlen=300))
        samples.append((now,size))
        while samples and now-samples[0][0] > 5:
            samples.popleft()

    @staticmethod
    def record_video(entry, size, sequence, timestamp, now):
        entry['frames_decoded'] = entry.get('frames_decoded',0)+1
        entry['video_seq'],entry['source_timestamp_ms'] = sequence,timestamp
        entry['gateway_delay_ms'] = max(0,round(time.time()*1000-timestamp))

    @staticmethod
    def video_stats(entry, endpoint):
        samples = entry.get('video_samples',[])
        span = samples[-1][0]-samples[0][0] if len(samples)>1 else 0
        live = time.monotonic()-entry.get('received',0) < 2
        decoder = entry.get('h264_decoder')
        return {'codec':endpoint['codec'],'live':live,'frames_decoded':entry.get('frames_decoded',0),
                'sequence':entry.get('video_seq'), 'source_timestamp_ms':entry.get('source_timestamp_ms'),
                'gateway_delay_ms':entry.get('gateway_delay_ms'),
                'fps':round((len(samples)-1)/span,1) if live and span else 0,
                'mqtt_kbps':round(sum(s[1] for s in list(samples)[1:])*8/span/1000,1) if live and span else 0,
                'packets_dropped':decoder.dropped if decoder else 0,
                'decoder_resets':decoder.resets if decoder else 0,
                'waiting_for_keyframe':decoder.waiting_for_keyframe if decoder else False}
