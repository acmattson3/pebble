"""Low-delay H.264 access units carried in independent MQTT messages.

PBVH v1: magic, version, flags (bit 0 IDR), reserved, stream UUID, sequence,
wall-clock capture milliseconds, width, height; followed by one Annex B AU.
Every IDR repeats SPS/PPS. No B frames or delayed frame threading.
"""
from fractions import Fraction
import re
import struct
import time
import uuid

HEADER = struct.Struct('!4sBBH16sIQHH')
MAX_PACKET = 2*1024*1024


def dimensions(width, height):
    if not (16 <= width <= 1920 and 16 <= height <= 1080 and width%2 == height%2 == 0):
        raise ValueError('H.264 requires even dimensions between 16x16 and 1920x1080')


def unpack(payload):
    if not HEADER.size < len(payload) <= MAX_PACKET:
        raise ValueError('Invalid H.264 message size')
    magic, version, flags, reserved, stream, seq, timestamp, width, height = HEADER.unpack_from(payload)
    if magic != b'PBVH' or version != 1 or flags & ~1 or reserved:
        raise ValueError('Invalid H.264 header')
    dimensions(width, height)
    body = payload[HEADER.size:]
    nals = {m.group(1)[0]&31 for m in re.finditer(rb'\x00\x00(?:\x00)?\x01([\s\S])', body)}
    key = bool(flags & 1)
    if not nals & {1,5} or key != (5 in nals) or (key and not {7,8} <= nals):
        raise ValueError('Expected one Annex B AU; IDR must include SPS/PPS')
    return {'stream':stream, 'seq':seq, 'timestamp_ms':timestamp, 'width':width,
            'height':height, 'keyframe':key, 'data':body}


class Encoder:
    def __init__(self, width, height, fps=15, bitrate=400000, keyframe_interval=5, preset='ultrafast'):
        import av
        dimensions(width,height)
        if not 1 <= fps <= 60 or not 16000 <= bitrate <= 10000000 or not 1 <= keyframe_interval <= 120:
            raise ValueError('Invalid H.264 rate, bitrate or keyframe interval')
        if preset not in {'ultrafast','superfast','veryfast','faster','fast','medium'}:
            raise ValueError('Unsupported x264 preset')
        self.av, self.width, self.height = av, width, height
        self.stream, self.seq = uuid.uuid4().bytes, 0
        self.codec = av.CodecContext.create('libx264','w')
        self.codec.width, self.codec.height = width,height
        self.codec.pix_fmt = 'yuv420p'
        self.codec.time_base = Fraction(1,fps)
        self.codec.framerate = Fraction(fps,1)
        self.codec.bit_rate = bitrate
        self.codec.gop_size = keyframe_interval
        self.codec.thread_count = 1
        kbps = max(16,bitrate//1000)
        self.codec.options = {'preset':preset,'tune':'zerolatency','profile':'baseline','bf':'0',
            'x264-params':f'keyint={keyframe_interval}:min-keyint={keyframe_interval}:scenecut=0:repeat-headers=1:annexb=1:aud=1:vbv-maxrate={kbps}:vbv-bufsize={max(16,kbps//2)}'}
        self.codec.open()

    def encode(self, bgr, timestamp_ms=None):
        frame = self.av.VideoFrame.from_ndarray(bgr,format='bgr24')
        frame.pts, frame.time_base = self.seq,self.codec.time_base
        packets = self.codec.encode(frame)
        if len(packets) != 1:
            raise ValueError('Low-delay encoder must emit exactly one access unit per frame')
        encoded = packets[0]
        header = HEADER.pack(b'PBVH',1,int(encoded.is_keyframe),0,self.stream,self.seq,
            int(timestamp_ms if timestamp_ms is not None else time.time()*1000),self.width,self.height)
        self.seq = (self.seq+1)&0xffffffff
        result = header+bytes(encoded)
        if len(result) > MAX_PACKET:
            raise ValueError('H.264 access unit exceeds MQTT media limit')
        return result


class Decoder:
    def __init__(self):
        self.codec = None
        self.stream = None
        self.seq = None
        self.size = None
        self.waiting_for_keyframe = True
        self.dropped = 0
        self.resets = 0

    def reset(self):
        self.codec = None
        self.waiting_for_keyframe = True
        self.resets += 1

    def decode(self, payload):
        import av
        info = unpack(payload)
        stream, seq = info['stream'],info['seq']
        size = (info['width'],info['height'])
        if stream != self.stream or size != self.size:
            self.reset()
            self.stream, self.size, self.seq = stream,size,None
        if self.seq is not None:
            if seq == self.seq or (seq < self.seq and self.seq-seq < 0x80000000):
                self.dropped += 1
                return None
            if seq != (self.seq+1)&0xffffffff:
                self.reset()
        self.seq = seq
        if self.waiting_for_keyframe and not info['keyframe']:
            self.dropped += 1
            return None
        try:
            if self.codec is None:
                self.codec = av.CodecContext.create('h264','r')
                self.codec.thread_count = 1
                self.codec.open()
            frames = self.codec.decode(av.Packet(info['data']))
            if len(frames) != 1 or (frames[0].width,frames[0].height) != size:
                raise ValueError('Invalid H.264 frame dimensions or delayed output')
            self.waiting_for_keyframe = False
            return frames[0].to_ndarray(format='bgr24'),info
        except Exception:
            self.reset()
            raise
