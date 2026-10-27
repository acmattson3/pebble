"""Read-only production capture: V commands only. Requires pyserial 3.5."""
import argparse
import json
import re
import time
from pathlib import Path
import serial

ap = argparse.ArgumentParser()
ap.add_argument('--port', required=True)
ap.add_argument('--seconds', type=float, default=60)
args = ap.parse_args()
root = Path(__file__).resolve().parent
start = time.monotonic()
statuses, samples, summaries, battery, warnings = [], [], [], [], []
pending = b''
def fields(line):
    return dict(re.findall(r'(\w+)=([^ ]+)', line))
with serial.Serial(args.port, 460800, timeout=0.05, write_timeout=1) as port, \
        (root / 'connected-monitor.log').open('w', encoding='utf-8', buffering=1) as log:
    port.dtr = True
    next_query = 0
    while time.monotonic() - start < args.seconds:
        if time.monotonic() >= next_query:
            port.write(b'V\n')
            next_query = time.monotonic() + 5
        pending += port.read(max(1, port.in_waiting))
        while b'\n' in pending:
            raw, pending = pending.split(b'\n', 1)
            line = raw.decode('ascii', errors='replace').strip()
            log.write(f'{time.monotonic()-start:.3f} {line}\n')
            if line.startswith('INFO firmware='):
                statuses.append(fields(line)); print(line, flush=True)
            elif line.startswith('I n='): samples.append(fields(line))
            elif line.startswith('S n='): summaries.append(fields(line))
            elif line.startswith('C chg='): battery.append(fields(line))
            elif line.startswith(('WARN ', 'ERR ')): warnings.append(line)
sample_times = [int(x['ms']) for x in samples]
gaps = [b-a for a,b in zip(sample_times, sample_times[1:])]
result = {
    'actual_seconds': time.monotonic()-start, 'commands': 'V only',
    'sample_count': len(samples), 'summary_count': len(summaries),
    'sample_rate_hz': (len(samples)-1)*1000/(sample_times[-1]-sample_times[0]) if len(samples)>1 else None,
    'max_sample_gap_ms': max(gaps) if gaps else None,
    'first_status': statuses[0] if statuses else None,
    'last_status': statuses[-1] if statuses else None,
    'first_summary': summaries[0] if summaries else None,
    'last_summary': summaries[-1] if summaries else None,
    'battery': battery, 'warnings': warnings,
    'pass': bool(statuses and samples and summaries) and not warnings
        and all(int(b['uptime_ms'])>int(a['uptime_ms']) for a,b in zip(statuses,statuses[1:]))
        and all(x['imu_ready']=='1' and x['imu_error']=='0' for x in statuses)
        and all(x['err']=='0' for x in summaries),
}
(root / 'connected-monitor.json').write_text(json.dumps(result, indent=2)+'\n')
print(json.dumps(result), flush=True)
if not result['pass']: raise SystemExit(1)
