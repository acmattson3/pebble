"""Generate a separate on-target test sketch from the CURRENT production source.

Only the I2C adapter is replaced with a simulated sensor; production scheduling,
parser, estimator and motor-timeout functions are exercised on the SAMD21.
Requires a bare board. It deliberately triggers one watchdog reset at the end.
Never upload this harness to an attached robot. Upload production afterward.
Usage: python tests/build_harness.py <temporary-output-directory>
"""
import sys
from pathlib import Path
root = Path(__file__).resolve().parents[1]
target = Path(sys.argv[1]).resolve() / 'GoobHarness'
target.mkdir(parents=True, exist_ok=True)
source = (root / 'GOOB_seeed-xiao-samd21.ino').read_text()
source = source.replace('#include "BoundedI2C.h"', '#include "SimulatedI2C.h"')
source = source.replace('void setup() {', 'void firmwareSetup() {')
source = source.replace('void loop() {', 'void firmwareLoop() {')
source += (root / 'tests' / 'HarnessBody.txt').read_text()
(target / 'GoobHarness.ino').write_text(source)
(target / 'SimulatedI2C.h').write_text((root / 'tests' / 'SimulatedI2C.txt').read_text())
print(target)
