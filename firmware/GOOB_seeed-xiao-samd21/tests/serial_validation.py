"""Bare-board serial regression + soak test. Requires pyserial 3.5.

Run: python tests/serial_validation.py --port COM3 --seconds 600
Uses zero motor commands only. Save results next to this script. Keep other
serial monitors closed. This does not measure electrical motor behavior.
"""
import argparse
import json
import re
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
import serial

parser = argparse.ArgumentParser()
parser.add_argument('--port', required=True)
parser.add_argument('--seconds', type=float, default=600)
args = parser.parse_args()
root = Path(__file__).resolve().parent
logfile = root / 'serial-validation.log'
resultfile = root / 'serial-validation.json'
events = []
started = time.monotonic()
last_uptime = -1
v_count = 0
log_lock = threading.Lock()
capture_lock = threading.Lock()
lines = []
stop_reader = threading.Event()
reader_errors = []
port = serial.Serial(args.port, 460800, timeout=0.1, write_timeout=2)
port.dtr = True
log = logfile.open('w', encoding='utf-8', buffering=1)

def read_loop():
    pending = b''
    try:
        while not stop_reader.is_set():
            pending += port.read(max(1, port.in_waiting))
            while b'\n' in pending:
                line, pending = pending.split(b'\n', 1)
                text = line.decode('ascii', errors='replace').strip()
                with capture_lock:
                    lines.append(text)
                with log_lock:
                    log.write(f'{time.monotonic()-started:.3f} RX {text}\n')
    except Exception as exc:
        reader_errors.append(str(exc))

thread = threading.Thread(target=read_loop, daemon=True)
thread.start()

def capture(payload, wait=0.4):
    with capture_lock:
        offset = len(lines)
    with log_lock:
        log.write(f'{time.monotonic()-started:.3f} TX {payload[:160]!r} bytes={len(payload)}\n')
    port.write(payload)
    time.sleep(wait)
    with capture_lock:
        return lines[offset:]

def check(condition, label):
    events.append({'test': label, 'pass': bool(condition)})
    print(('PASS ' if condition else 'FAIL ') + label, flush=True)
    if not condition:
        raise AssertionError(label)

def status():
    global last_uptime, v_count
    response = capture(b'V\n', 0.5)
    matches = [line for line in response if line.startswith('INFO firmware=')]
    check(bool(matches), 'V responds')
    data = dict(re.findall(r'(\w+)=([^ ]+)', matches[-1]))
    check(data['firmware'] == 'goob-samd21-bounded-recovery-v2', 'expected production firmware')
    uptime = int(data['uptime_ms'])
    check(uptime > last_uptime, 'uptime increases without reset')
    last_uptime = uptime
    v_count += 1
    return data

outcome = {'started_utc': datetime.now(timezone.utc).isoformat(), 'port': args.port,
           'requested_seconds': args.seconds, 'hardware': 'user-confirmed bare board'}
try:
    initial = status()
    check(initial['imu_ready'] == '0', 'absent IMU unavailable')
    for payload, ack in [(b'L 0 0 0\r\n', 'OK L'), (b'F 0 0 0 2\n', 'OK F'),
                         (b'M 0 0\n', 'OK M'), (b' \tv\r\n', 'INFO firmware=')]:
        response = capture(payload)
        check(any(line.startswith(ack) for line in response), f'valid command {payload!r}')
    reference = int(status()['motor_cmd_ms'])
    for value in ('nan', 'NaN', 'inf', '-inf', '1e999', 'nope', '0oops', '1e-999'):
        response = capture(f'M {value} 0\n'.encode())
        check(any(line.startswith('ERR M') for line in response), f'reject invalid motor {value}')
        check(int(status()['motor_cmd_ms']) == reference, 'invalid motor does not renew timestamp')
    check(any('ERR M' in line for line in capture(b'M 0\n')), 'reject incomplete motor command')
    response = capture(b'V' + b' ' * 127 + b'\n')
    check(any(line.startswith('INFO firmware=') for line in response), '128-byte command accepted')
    response = capture(b'V' + b' ' * 128 + b'\nV\n')
    check(sum(line == 'ERR command too long or invalid byte' for line in response) == 1,
          '129-byte command discarded with one error')
    check(any(line.startswith('INFO firmware=') for line in response), 'recovery after overlong line')
    response = capture(b'x' * 8192, 1.0)
    check(not any(line.startswith('ERR command') for line in response),
          'missing newline produces no repeated errors')
    response = capture(b'\nV\n')
    check(sum(line == 'ERR command too long or invalid byte' for line in response) == 1,
          'missing-newline discard reports one error at newline')
    check(any(line.startswith('INFO firmware=') for line in response), 'parser recovers after missing newline')
    response = capture(b'V\x00M 0 0\nV\n')
    check(sum(line == 'ERR command too long or invalid byte' for line in response) == 1,
          'embedded NUL discarded')
    check(any(line.startswith('INFO firmware=') for line in response), 'parser recovers after NUL')
    flood_start = time.monotonic()
    with capture_lock:
        flood_offset = len(lines)
    flood_bytes = 0
    while time.monotonic() - flood_start < 20:
        payload = b' \n' * 120 + b'V\n'
        port.write(payload)
        flood_bytes += len(payload)
        time.sleep(0.025)
    time.sleep(1)
    with capture_lock:
        flood_lines = lines[flood_offset:]
    flood_v = [line for line in flood_lines if line.startswith('INFO firmware=')]
    check(len(flood_v) >= 100, 'sustained input keeps V responsive')
    flood_uptimes = [int(re.search(r'uptime_ms=(\d+)', line)[1]) for line in flood_v]
    check(all(b >= a for a, b in zip(flood_uptimes, flood_uptimes[1:])),
          'no reset during sustained input')
    outcome['flood_bytes'] = flood_bytes
    outcome['flood_v_responses'] = len(flood_v)
    outcome['flood_seconds'] = time.monotonic() - flood_start
    status()
    next_status = time.monotonic()
    while time.monotonic() - started < args.seconds:
        if reader_errors:
            raise RuntimeError(reader_errors)
        if time.monotonic() >= next_status:
            status()
            next_status = time.monotonic() + 10
        time.sleep(0.2)
    final = status()
    outcome['initial_status'] = initial
    outcome['final_status'] = final
    outcome['v_queries'] = v_count
    outcome['pass'] = True
except Exception as exc:
    outcome['pass'] = False
    outcome['error'] = repr(exc)
    raise
finally:
    outcome['actual_seconds'] = time.monotonic() - started
    outcome['tests'] = events
    outcome['reader_errors'] = reader_errors
    stop_reader.set()
    thread.join(2)
    port.close()
    log.close()
    resultfile.write_text(json.dumps(outcome, indent=2) + '\n')
    print(f'Results: {resultfile}; elapsed {outcome["actual_seconds"]:.1f}s', flush=True)
