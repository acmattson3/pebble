#ifndef GOOB_BOUNDED_I2C_H
#define GOOB_BOUNDED_I2C_H

#include <Arduino.h>
#include <wiring_private.h>

// Project-owned SAMD21 SERCOM2 master, exclusively owning XIAO D4/D5.
// No Wire calls, interrupts, or installed-core changes. Manual ACK sequencing
// follows the SAMD21 I2CM peripheral and Seeed 1.8.6's master implementation.
// Each public operation includes cleanup in ONE 10 ms budget. Clock stretching,
// synchronization, and partial reads all share that budget. micros() requires
// interrupts to remain enabled; the independent watchdog is the final fallback.
class BoundedI2C {
 public:
  enum Error : uint8_t {
    OK = 0, ADDRESS_NACK = 2, DATA_NACK = 3, BUS_ERROR = 4,
    TIMEOUT = 5, LINE_LOW = 6, NOT_READY = 7
  };
  static const uint32_t DEADLINE_US = 10000;
  uint8_t error = OK;
  uint8_t received = 0;

  bool begin() {
    start();
    ready = false;
    PM->APBCMASK.reg |= PM_APBCMASK_SERCOM2;
    GCLK->CLKCTRL.reg = GCLK_CLKCTRL_ID(SERCOM2_GCLK_ID_CORE) |
                          GCLK_CLKCTRL_GEN_GCLK0 | GCLK_CLKCTRL_CLKEN;
    while (GCLK->STATUS.bit.SYNCBUSY) if (expired()) return fail(TIMEOUT);
    NVIC_DisableIRQ(SERCOM2_IRQn);
    SERCOM2->I2CM.CTRLA.bit.ENABLE = 0;
    if (!sync()) return fail(TIMEOUT);
    pinMode(SDA, INPUT_PULLUP);
    pinMode(SCL, INPUT_PULLUP);
    if (!linesHigh()) return fail(LINE_LOW);
    SERCOM2->I2CM.CTRLA.bit.SWRST = 1;
    while (SERCOM2->I2CM.CTRLA.bit.SWRST || SERCOM2->I2CM.SYNCBUSY.reg)
      if (expired()) return fail(TIMEOUT);
    SERCOM2->I2CM.CTRLA.reg = SERCOM_I2CM_CTRLA_MODE(5);
    SERCOM2->I2CM.CTRLB.reg = 0; // Manual ACK; no smart mode.
    SERCOM2->I2CM.BAUD.bit.BAUD = SystemCoreClock / 200000UL - 5 -
        (((SystemCoreClock / 1000000UL) * 125UL) / 2000UL);
    pinPeripheral(SDA, g_APinDescription[SDA].ulPinType);
    pinPeripheral(SCL, g_APinDescription[SCL].ulPinType);
    SERCOM2->I2CM.CTRLA.bit.ENABLE = 1;
    if (!sync()) return fail(TIMEOUT);
    SERCOM2->I2CM.STATUS.bit.BUSSTATE = 1;
    if (!sync()) return fail(TIMEOUT);
    ready = true;
    return true;
  }

  bool writeRegister(uint8_t address, uint8_t reg, uint8_t value) {
    start();
    if (!prepare() || !addressPhase(address, false) ||
        !put(reg) || !put(value)) return false;
    return stop();
  }

  bool readRegister(uint8_t address, uint8_t reg, uint8_t *out, size_t count) {
    start();
    if (!count || count > 32) return fail(BUS_ERROR);
    if (!prepare() || !addressPhase(address, false) || !put(reg) ||
        !addressPhase(address, true)) return false;
    for (size_t i = 0; i < count; ++i) {
      // addressPhase has awaited the first SB. Subsequent reads request a
      // byte with ACK + CMD=2, as in the installed Wire master implementation.
      if (i) {
        SERCOM2->I2CM.CTRLB.bit.ACKACT = 0;
        SERCOM2->I2CM.CTRLB.bit.CMD = 2;
        if (!sync()) return fail(TIMEOUT);
        if (!waitFlag(SERCOM_I2CM_INTFLAG_SB, true)) return false;
      }
      out[i] = SERCOM2->I2CM.DATA.bit.DATA;
      ++received;
    }
    SERCOM2->I2CM.CTRLB.bit.ACKACT = 1;
    return stop();
  }

 private:
  bool ready = false;
  uint32_t startedUs = 0;
  void start() { startedUs = micros(); error = OK; received = 0; }
  bool expired() const { return uint32_t(micros() - startedUs) >= DEADLINE_US; }
  bool linesHigh() const { return digitalRead(SDA) && digitalRead(SCL); }
  bool sync() {
    while (SERCOM2->I2CM.SYNCBUSY.reg) if (expired()) return false;
    return !expired();
  }
  bool fail(uint8_t why) {
    error = why;
    ready = false;
    // Best-effort STOP only while owning a synchronized bus. Never start a
    // second cleanup deadline. Release peripheral pins for the next retry.
    if (!SERCOM2->I2CM.SYNCBUSY.reg && SERCOM2->I2CM.STATUS.bit.BUSSTATE == 2) {
      SERCOM2->I2CM.CTRLB.bit.CMD = 3;
      sync();
    }
    SERCOM2->I2CM.CTRLA.bit.ENABLE = 0;
    sync();
    pinMode(SDA, INPUT_PULLUP);
    pinMode(SCL, INPUT_PULLUP);
    return false;
  }
  bool prepare() {
    if (!ready) return fail(NOT_READY);
    if (!linesHigh()) return fail(LINE_LOW);
    if (!sync()) return fail(TIMEOUT);
    if (SERCOM2->I2CM.STATUS.bit.BUSSTATE != 1) return fail(BUS_ERROR);
    SERCOM2->I2CM.CTRLB.bit.ACKACT = 0;
    return true;
  }
  bool waitFlag(uint8_t flag, bool reading = false) {
    while (true) {
      if (expired()) return fail(TIMEOUT);
      if (SERCOM2->I2CM.STATUS.reg &
          (SERCOM_I2CM_STATUS_BUSERR | SERCOM_I2CM_STATUS_ARBLOST)) return fail(BUS_ERROR);
      const uint8_t flags = SERCOM2->I2CM.INTFLAG.reg;
      if (flags & SERCOM_I2CM_INTFLAG_ERROR) return fail(BUS_ERROR);
      if (reading && (flags & SERCOM_I2CM_INTFLAG_MB) &&
          SERCOM2->I2CM.STATUS.bit.RXNACK) return fail(ADDRESS_NACK);
      if (flags & flag) return true;
    }
  }
  bool addressPhase(uint8_t address, bool reading) {
    SERCOM2->I2CM.ADDR.reg = (uint32_t(address) << 1) | (reading ? 1 : 0);
    if (!waitFlag(reading ? SERCOM_I2CM_INTFLAG_SB : SERCOM_I2CM_INTFLAG_MB,
                  reading)) return false;
    if (SERCOM2->I2CM.STATUS.bit.RXNACK) return fail(ADDRESS_NACK);
    return true;
  }
  bool put(uint8_t value) {
    SERCOM2->I2CM.DATA.reg = value;
    if (!waitFlag(SERCOM_I2CM_INTFLAG_MB)) return false;
    if (SERCOM2->I2CM.STATUS.bit.RXNACK) return fail(DATA_NACK);
    return true;
  }
  bool stop() {
    SERCOM2->I2CM.CTRLB.bit.CMD = 3;
    if (!sync()) return fail(TIMEOUT);
    while (SERCOM2->I2CM.STATUS.bit.BUSSTATE != 1) {
      if (expired()) return fail(TIMEOUT);
      if (SERCOM2->I2CM.STATUS.bit.BUSERR) return fail(BUS_ERROR);
    }
    return true;
  }
};
#endif
