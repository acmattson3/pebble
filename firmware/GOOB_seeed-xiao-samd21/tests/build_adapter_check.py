"""Generate a diagnostic using the real project-owned I2C adapter.

Requires MPU-6050 at 0x68 and external pull-ups. Motors must be unpowered or
disconnected, or the user must confirm the current robot setup is safe/lifted.
Tests real ID/read/write, address NACK, and a finite deadline by temporarily
gating the SERCOM2 peripheral clock internally (no shorted pins).
Restore production afterward. Usage: python tests/build_adapter_check.py <work>
"""
from pathlib import Path
import sys
root = Path(__file__).resolve().parents[1]
target = Path(sys.argv[1]).resolve() / 'AdapterCheck'
target.mkdir(parents=True, exist_ok=True)
(target / 'BoundedI2C.h').write_text((root / 'BoundedI2C.h').read_text())
(target / 'AdapterCheck.ino').write_text(r'''
#include "BoundedI2C.h"
#include <Adafruit_SleepyDog.h>
BoundedI2C bus;
bool done = false;
int failures = 0;
void check(bool ok, const char *what) {
  Serial.print(ok ? "ADAPTER PASS " : "ADAPTER FAIL "); Serial.println(what);
  if (!ok) ++failures;
  Watchdog.reset();
}
void setup() {
  for (uint8_t pin = 7; pin <= 10; ++pin) {
    digitalWrite(pin, LOW); pinMode(pin, OUTPUT);
  }
  Watchdog.enable(1000);
  Serial.begin(460800);
}
void loop() {
  Watchdog.reset();
  if (done || !Serial) return;
  done = true;
  uint8_t who = 0, raw[14];
  check(bus.begin(), "real bus initialization");
  check(bus.readRegister(0x68, 0x75, &who, 1) && (who == 0x68 || who == 0x69),
        "real MPU identity read");
  Serial.print("ADAPTER who="); Serial.println(who, HEX);
  check(bus.writeRegister(0x68, 0x6b, 0), "real register write");
  delay(100); Watchdog.reset();
  check(bus.readRegister(0x68, 0x3b, raw, sizeof(raw)) && bus.received == sizeof(raw),
        "real repeated-start 14-byte read");
  const uint32_t nackStart = micros();
  const bool nackOk = bus.readRegister(0x67, 0x75, &who, 1);
  const uint32_t nackUs = micros() - nackStart;
  check(!nackOk && bus.error == BoundedI2C::ADDRESS_NACK && nackUs < 12000,
        "address NACK returns within deadline");
  Serial.print("ADAPTER nack_us="); Serial.println(nackUs);
  check(bus.begin(), "reinitialize after NACK");

  // Gate only this peripheral's core clock, preserving SysTick and USB.
  GCLK->CLKCTRL.reg = GCLK_CLKCTRL_ID(SERCOM2_GCLK_ID_CORE) | GCLK_CLKCTRL_GEN_GCLK0;
  uint32_t waitStart = micros();
  while (GCLK->STATUS.bit.SYNCBUSY && micros() - waitStart < 10000) {}
  const uint32_t timeoutStart = micros();
  const bool timedOk = bus.readRegister(0x68, 0x75, &who, 1);
  const uint8_t timedError = bus.error;
  const uint32_t timeoutUs = micros() - timeoutStart;
  // Restore the channel before printing/checking or resetting the adapter.
  GCLK->CLKCTRL.reg = GCLK_CLKCTRL_ID(SERCOM2_GCLK_ID_CORE) |
                      GCLK_CLKCTRL_GEN_GCLK0 | GCLK_CLKCTRL_CLKEN;
  waitStart = micros();
  while (GCLK->STATUS.bit.SYNCBUSY && micros() - waitStart < 10000) {}
  check(!timedOk && timedError == BoundedI2C::TIMEOUT && timeoutUs >= 10000 && timeoutUs < 12000,
        "clock-gated transaction times out in 10 ms including cleanup");
  Serial.print("ADAPTER timeout_us="); Serial.print(timeoutUs);
  Serial.print(" error="); Serial.println(timedError);
  check(bus.begin() && bus.readRegister(0x68, 0x75, &who, 1) && (who == 0x68 || who == 0x69),
        "real sensor communication recovers after timeout");
  Serial.print("ADAPTER COMPLETE failures="); Serial.println(failures);
}
''')
print(target)
