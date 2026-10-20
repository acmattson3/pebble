"""Small libopus binding and Pebble audio framing (PBAT codec 1 PCM, 2 Opus).

MQTT packets are independently framed. Opus state is scoped to a stream; a
sequence restart or format change creates a fresh decoder. libopus0 is required.
"""
import ctypes as C
import ctypes.util
import struct
import time

HEADER = struct.Struct('!4sBBBBHHIQ')
RATES = (8000, 12000, 16000, 24000, 48000)


def library():
    lib = C.CDLL(ctypes.util.find_library('opus') or 'libopus.so.0')
    lib.opus_encoder_create.argtypes = [C.c_int, C.c_int, C.c_int, C.POINTER(C.c_int)]
    lib.opus_encoder_create.restype = C.c_void_p
    lib.opus_decoder_create.argtypes = [C.c_int, C.c_int, C.POINTER(C.c_int)]
    lib.opus_decoder_create.restype = C.c_void_p
    lib.opus_encoder_destroy.argtypes = [C.c_void_p]
    lib.opus_decoder_destroy.argtypes = [C.c_void_p]
    lib.opus_encode.argtypes = [C.c_void_p, C.c_void_p, C.c_int, C.c_void_p, C.c_int]
    lib.opus_decode.argtypes = [C.c_void_p, C.c_void_p, C.c_int, C.c_void_p, C.c_int, C.c_int]
    # ctl is variadic: declare its fixed arguments and type additional arguments.
    lib.opus_encoder_ctl.argtypes = [C.c_void_p, C.c_int]
    return lib


class Opus:
    def __init__(self, rate=16000, channels=1, bitrate=24000, decode=False):
        if rate not in RATES or channels not in (1, 2):
            raise ValueError('Opus requires 8/12/16/24/48 kHz and one or two channels')
        self.lib, self.rate, self.channels, self.decode_mode = library(), rate, channels, decode
        err = C.c_int()
        if decode:
            self.handle = self.lib.opus_decoder_create(rate, channels, C.byref(err))
        else:
            self.handle = self.lib.opus_encoder_create(rate, channels, 2048, C.byref(err))
        if err.value or not self.handle:
            raise ValueError(f'Opus create failed: {err.value}')
        if not decode:
            for request, value in ((4002, bitrate), (4010, 3)):
                if self.lib.opus_encoder_ctl(self.handle, request, C.c_int(value)) < 0:
                    self.close()
                    raise ValueError('Opus configuration failed')

    def encode(self, pcm):
        samples = len(pcm)//(2*self.channels)
        if samples not in [self.rate*n//1000 for n in (5, 10, 20, 40, 60)] or len(pcm) != samples*2*self.channels:
            raise ValueError('Invalid Opus frame duration or PCM alignment')
        output = C.create_string_buffer(4000)
        count = self.lib.opus_encode(self.handle, pcm, samples, output, len(output))
        if count < 0:
            raise ValueError(f'Opus encode failed: {count}')
        return output.raw[:count]

    def decode(self, packet, samples):
        if not 0 < samples <= self.rate*120//1000 or len(packet) > 4000:
            raise ValueError('Invalid Opus packet size')
        output = C.create_string_buffer(samples*self.channels*2)
        count = self.lib.opus_decode(self.handle, packet, len(packet), output, samples, 0)
        if count != samples:
            raise ValueError(f'Opus sample count mismatch: {count}')
        return output.raw[:count*self.channels*2]

    def close(self):
        if getattr(self, 'handle', None):
            destroy = self.lib.opus_decoder_destroy if self.decode_mode else self.lib.opus_encoder_destroy
            destroy(self.handle)
            self.handle = None

    def __del__(self):
        self.close()


def packet(pcm, seq, rate, channels, encoder=None):
    samples = len(pcm)//(channels*2)
    body = encoder.encode(pcm) if encoder else pcm
    return HEADER.pack(b'PBAT', 1, 2 if encoder else 1, channels, 0, rate,
                       samples, seq & 0xffffffff, int(time.time()*1000)) + body


class AudioDecoder:
    def __init__(self):
        self.decoder = None
        self.format = None
        self.last_seq = None
        self.last_timestamp = None
        self.generation = 0

    def parse(self, payload):
        if len(payload) < HEADER.size:
            return None
        magic, version, codec, channels, flags, rate, samples, seq, timestamp = HEADER.unpack_from(payload)
        if magic != b'PBAT' or version != 1 or codec not in (1, 2) or rate not in RATES or channels not in (1, 2) or not 0 < samples <= rate*120//1000:
            return None
        body = payload[HEADER.size:]
        fmt = (codec, rate, channels)
        # A stream can restart within the same millisecond as its previous
        # packet. A strictly lower sequence with a non-older timestamp is
        # sufficient to distinguish that restart from stale packets; an
        # equal sequence remains a duplicate and is discarded below.
        restart = (self.last_seq is not None and seq < self.last_seq
                   and timestamp >= (self.last_timestamp or 0))
        changed = self.format != fmt
        if self.last_seq is not None and seq <= self.last_seq and not restart and not changed:
            return None
        if changed or restart:
            if self.decoder:
                self.decoder.close()
            self.decoder = None
            self.format = fmt
            self.generation += 1
        if codec == 2:
            if self.decoder is None:
                self.decoder = Opus(rate, channels, decode=True)
            body = self.decoder.decode(body, samples)
        elif len(body) != samples*channels*2:
            return None
        self.last_seq, self.last_timestamp = seq, timestamp
        return {'seq':seq, 'timestamp':timestamp/1000, 'rate':rate, 'channels':channels,
                'frame_samples':samples, 'data':body}
