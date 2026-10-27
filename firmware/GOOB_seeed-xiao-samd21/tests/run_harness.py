"""Collect the bare-board harness and its intentional watchdog reboot.

Only use after uploading the generated GoobHarness sketch. Requires pyserial
3.5. Run: python tests/run_harness.py --port COM3
Always restore production firmware afterward.
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
port = None
pending = b''
triggered = False
complete = False
errors = []
try:
    while time.monotonic() - started < 45:
        if port is None:
            try:
                port = serial.Serial(args.port, 460800, timeout=0.1, write_timeout=1)
                port.dtr = True
                pending = b''
            except serial.SerialException:
                time.sleep(0.2)
                continue
        try:
            pending += port.read(max(1, port.in_waiting))
        except serial.SerialException as exc:
            errors.append(str(exc))
            port.close(); port = None
            time.sleep(0.3)
            continue
        while b'\n' in pending:
            raw, pending = pending.split(b'\n', 1)
            line = raw.decode('ascii', errors='replace').strip()
            lines.append(line)
            print(line, flush=True)
            if line.startswith('TEST TRIGGERING WATCHDOG'):
                triggered = True
                port.close(); port = None
                time.sleep(3)
                break
            if line == 'TEST WATCHDOG COMPLETE':
                complete = True
        if complete:
            break
finally:
    if port:
        port.close()
    passed = (triggered and complete and not any(line.startswith('TEST FAIL') for line in lines)
              and 'TEST HARNESS failures=0' in lines
              and any('TEST PASS watchdog reboot locks out sensor probing' == line for line in lines))
    result = {'pass': passed, 'actual_seconds': time.monotonic() - started,
              'intentional_watchdog_triggered': triggered,
              'watchdog_complete': complete, 'serial_reconnection_events': errors,
              'pass_checks': sum(line.startswith('TEST PASS') for line in lines)}
    (root / 'harness-validation.log').write_text('\n'.join(lines) + '\n')
    (root / 'harness-validation.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result), flush=True)
    if not passed:
        raise SystemExit(1)
