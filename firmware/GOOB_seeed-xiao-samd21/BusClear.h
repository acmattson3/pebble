#ifndef GOOB_BUS_CLEAR_H
#define GOOB_BUS_CLEAR_H
#include <stdint.h>

// NXP UM10204 rev 7, 3.1.16: at most nine clocks for SDA stuck LOW;
// SCL stuck LOW needs device reset/power cycling, never a forced HIGH.
// IO offers ONLY release and pull-low operations. No delays or polling loops.
// service() performs one phase per main-loop pass; timing minima may stretch.
template <class IO> class GoobBusClear {
 public:
  enum Result : uint8_t { IDLE, RUNNING, CLEARED, SDA_HELD, SCL_HELD, DEADLINE };
  static const uint32_t TOTAL_US = 250000;
  static const uint32_t SCL_WAIT_US = 2000;
  static const uint32_t HOLD_US = 10;
  Result result = IDLE;
  uint8_t pulses = 0;
  bool initialSda = true, initialScl = true, finalSda = true, finalScl = true;

  explicit GoobBusClear(IO &pins) : io(pins) {}
  void start(uint32_t now) {
    started = phaseAt = now;
    pulses = 0;
    io.releaseSda(); io.releaseScl();
    phase = SETTLE;
    result = RUNNING;
  }
  Result service(uint32_t now) {
    if (result != RUNNING) return result;
    if (uint32_t(now - started) >= TOTAL_US) return finish(DEADLINE);
    switch (phase) {
      case SETTLE:
        if (!held(now)) break;
        initialSda = io.sda(); initialScl = io.scl();
        if (!initialScl) return finish(SCL_HELD);
        if (initialSda) beginStop(now);
        else { io.pullSclLow(); phase = CLOCK_LOW; phaseAt = now; }
        break;
      case CLOCK_LOW:
        if (!held(now)) break;
        io.releaseScl(); phase = WAIT_CLOCK; phaseAt = now;
        break;
      case WAIT_CLOCK:
        if (io.scl()) { ++pulses; phase = CLOCK_HIGH; phaseAt = now; }
        else if (uint32_t(now - phaseAt) >= SCL_WAIT_US) return finish(SCL_HELD);
        break;
      case CLOCK_HIGH:
        if (!held(now)) break;
        if (!io.scl()) return finish(SCL_HELD);
        if (io.sda() || pulses >= 9) beginStop(now);
        else { io.pullSclLow(); phase = CLOCK_LOW; phaseAt = now; }
        break;
      case STOP_LOW:
        if (!held(now)) break;
        io.releaseScl(); phase = STOP_WAIT_CLOCK; phaseAt = now;
        break;
      case STOP_WAIT_CLOCK:
        if (io.scl()) { phase = STOP_HIGH; phaseAt = now; }
        else if (uint32_t(now - phaseAt) >= SCL_WAIT_US) return finish(SCL_HELD);
        break;
      case STOP_HIGH:
        if (!held(now)) break;
        if (!io.scl()) return finish(SCL_HELD);
        io.releaseSda(); phase = STOP_RELEASED; phaseAt = now;
        break;
      case STOP_RELEASED:
        if (!held(now)) break;
        return finish(!io.scl() ? SCL_HELD : (!io.sda() ? SDA_HELD : CLEARED));
    }
    return result;
  }
 private:
  enum Phase : uint8_t { SETTLE, CLOCK_LOW, WAIT_CLOCK, CLOCK_HIGH,
    STOP_LOW, STOP_WAIT_CLOCK, STOP_HIGH, STOP_RELEASED } phase = SETTLE;
  IO &io;
  uint32_t started = 0, phaseAt = 0;
  bool held(uint32_t now) const { return uint32_t(now - phaseAt) >= HOLD_US; }
  void beginStop(uint32_t now) {
    // Pull SCL low before SDA, so setting up STOP cannot create a START.
    io.pullSclLow(); io.pullSdaLow();
    phase = STOP_LOW; phaseAt = now;
  }
  Result finish(Result value) {
    io.releaseSda(); io.releaseScl();
    finalSda = io.sda(); finalScl = io.scl();
    result = value;
    return result;
  }
};
#endif
