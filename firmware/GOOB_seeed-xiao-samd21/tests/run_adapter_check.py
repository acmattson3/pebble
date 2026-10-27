"""Collect the real-adapter diagnostic. Always restore production afterward."""
import argparse
import json
import time
from pathlib import Path
import serial

ap = argparse.ArgumentParser()
ap.add_argument('--port', required=True)
args = ap.parse_args()
root = Path(__file__).resolve().parent
lines = []
pending = b''
start = time.monotonic()
complete = False
with serial.Serial(args.port, 460800, timeout=0.1, write_timeout=1) as port:
    port.dtr = True
    while time.monotonic() - start < 20 and not complete:
        pending += port.read(max(1, port.in_waiting))
        while b'\n' in pending:
            raw, pending = pending.split(b'\n', 1)
            line = raw.decode('ascii', errors='replace').strip()
            lines.append(line)
            print(line, flush=True)
            if line.startswith('ADAPTER COMPLETE'):
                complete = True
result = {
    'pass': complete and 'ADAPTER COMPLETE failures=0' in lines
        and not any(line.startswith('ADAPTER FAIL') for line in lines),
    'actual_seconds': time.monotonic()-start,
    'checks_passed': sum(line.startswith('ADAPTER PASS') for line in lines),
    'details': [line for line in lines if line.startswith(('ADAPTER nack_us=',
                                                          'ADAPTER timeout_us=', 'ADAPTER who='))],
}
(root / 'adapter-validation.log').write_text('\n'.join(lines)+'\n')
(root / 'adapter-validation.json').write_text(json.dumps(result, indent=2)+'\n')
print(json.dumps(result), flush=True)
if not result['pass']: raise SystemExit(1)
