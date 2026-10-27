"""Production firmware check with an acknowledging MPU-6050.

Requires pyserial 3.5. Motors must be disconnected/unpowered or the user must
confirm the current robot setup is safe (for example, lifted). Uses only
zero valid motor commands. Run: python tests/sensor_validation.py --port COM3
"""
import argparse
import json
import re
import time
from pathlib import Path
import serial

parser = argparse.ArgumentParser()
parser.add_argument('--port', required=True)
args = parser.parse_args()
root = Path(__file__).resolve().parent
started = time.monotonic()
lines = []
statuses = []
pending = b''
port = serial.Serial(args.port, 460800, timeout=0.05, write_timeout=2)
port.dtr = True
log = (root / 'sensor-validation.log').open('w', encoding='utf-8', buffering=1)

def collect(seconds):
    global pending
    until = time.monotonic() + seconds
    while time.monotonic() < until:
        pending += port.read(max(1, port.in_waiting))
        while b'\n' in pending:
            raw, pending = pending.split(b'\n', 1)
            text = raw.decode('ascii', errors='replace').strip()
            lines.append(text)
            log.write(f'{time.monotonic()-started:.3f} RX {text}\n')
            if text.startswith('INFO firmware='):
                statuses.append(dict(re.findall(r'(\w+)=([^ ]+)', text)))
                print(text, flush=True)

def send(text):
    log.write(f'{time.monotonic()-started:.3f} TX {text!r}\n')
    port.write(text.encode('ascii') + b'\n')

result = {'pass': False, 'motor_commands': 'zero only'}
try:
    send('V'); collect(3); send('V'); collect(0.5)
    if not statuses or statuses[-1]['imu_ready'] != '1':
        raise AssertionError('IMU not ready; inspect recorded diagnostics')
    send('M 0 0'); send('L 0 0 0'); collect(0.5)
    send('C')
    for _ in range(14):
        send('V'); collect(0.5)
    for _ in range(20):
        send('V'); collect(1)
    sample_lines = [line for line in lines if line.startswith('I n=')]
    check = {
        'production_id': all(s['firmware'] == 'goob-samd21-bounded-recovery-v2' for s in statuses),
        'uptime_increases': all(int(b['uptime_ms']) > int(a['uptime_ms'])
                                for a, b in zip(statuses, statuses[1:])),
        'samples_received': len(sample_lines) > 100,
        'calibrating_state_observed': any(s['imu_calibrating'] == '1' for s in statuses),
        'manual_calibration_completed': any(line.startswith('INFO IMU recalibration complete samples=400')
                                             for line in lines),
        'final_ready': statuses[-1]['imu_ready'] == '1' and statuses[-1]['imu_calibrating'] == '0',
        'zero_motor_ack': 'OK M' in lines,
        'light_ack': 'OK L' in lines,
        'no_sensor_fault': not any(line.startswith('WARN IMU unavailable') for line in lines),
    }
    result.update({'pass': all(check.values()), 'checks': check,
                   'sample_count': len(sample_lines), 'status_count': len(statuses),
                   'initial_status': statuses[0], 'final_status': statuses[-1]})
except Exception as exc:
    result['error'] = repr(exc)
finally:
    result['actual_seconds'] = time.monotonic() - started
    port.close(); log.close()
    (root / 'sensor-validation.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result), flush=True)
if not result['pass']:
    raise SystemExit(1)
