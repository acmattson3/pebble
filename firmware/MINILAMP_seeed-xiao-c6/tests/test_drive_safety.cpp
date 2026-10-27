#include "../DriveSafety.h"
#include <assert.h>
#include <limits>
#include <stdio.h>

int main() {
  MiniLamp::DriveSafety state;
  float motors[4];
  assert(state.drive(0, 0.4f, 100, 100));
  state.expire(1099); assert(state.driveActive);
  state.expire(1100); assert(!state.driveActive && state.left == 0 && state.right == 0);
  // An unrelated drive refresh must not extend a grouser lease.
  assert(state.rotate(0.5f, 2000, 2000));
  assert(state.drive(0, 0.25f, 2900, 2900));
  state.expire(3000);
  assert(!state.grouserActive && state.grouser == 0 && state.driveActive);
  // Nor may grouser refresh extend the drive lease.
  assert(state.rotate(-0.5f, 3800, 3800));
  state.expire(3900); assert(!state.driveActive && state.grouserActive);
  state.stop(); state.mixed(motors);
  for (float value : motors) assert(value == 0);
  assert(!state.drive(0, 1, 4000, 5000)); // Delayed queued command expires.
  assert(!state.rotate(1, 4000, 5000));
  assert(!state.drive(std::numeric_limits<float>::quiet_NaN(), 0, 6000, 6000));
  assert(!state.drive(0, std::numeric_limits<float>::infinity(), 6000, 6000));
  assert(!state.rotate(1.1f, 6000, 6000));
  assert(state.drive(0, 0.5f, 7000, 7000));
  assert(state.rotate(0.25f, 7000, 7000));
  state.mixed(motors);
  assert(motors[0] == 1 && motors[1] == 0 && motors[2] == 0 && motors[3] == 1);
  state.stop();
  assert(state.drive(0, 0.249f, 7100, 7100));
  state.mixed(motors); for (float value : motors) assert(value == 0);
  assert(state.drive(0, 0.25f, 7100, 7100));
  state.mixed(motors); for (float value : motors) assert(value == 1);
  assert(state.rotate(-0.249f, 7100, 7100)); assert(state.grouser == 0);
  assert(state.rotate(-0.25f, 7100, 7100)); assert(state.grouser == -1);
  // Millis rollover must not defeat the deadman.
  assert(state.drive(0, -0.5f, UINT32_MAX - 499, UINT32_MAX - 499));
  state.expire(499); assert(state.driveActive);
  state.expire(500); assert(!state.driveActive);
  puts("PASS: deadlines, independent leases, stale/invalid input, on/off mixing/deadband, rollover");
}
