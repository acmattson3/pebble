# Proposed first implementation milestone

Start with a small, versioned module contract and a Goob demonstration. Keep video
codec selection independent of that contract.

October 19 update: the module foundation and Goob demonstration in sections 1–2
are implemented and running on Goob and ARL. See [module controls](../../modules/README.md).
Sections 3–5 remain the proposed next stages.

## 1. Define the minimum contract

- Identity stays `{system}/{type}/{id}`: Goob is `pebble/robots/goob`.
- Directions become `i` and `o`; endpoint suffixes remain readable.
- Every module declares relative endpoints, payload schemas, units, state,
  actions, layout, default input bindings, and optional media/resource needs.
- Public modules and robot-local modules use the same format and renderer.
- Module IDs and schemas have explicit versions. Transport topic generation
  happens in one shared helper; full topic strings are not duplicated per service.
- An instance binds a reusable module to a robot's endpoints. Local mapping
  overrides do not require copying the public module.

Keep the first schema small. A drive module, a simple custom module, and a camera
module should exercise it before adding a graphical layout designer.

## 2. Extract shared controls and input handling

Extract drive behavior from the large web-control application into a public
module. Define input adapters for keyboard, gamepad, and pointer/touch. They
update named actions; widgets use those same actions. A generic command binding
maps actions to endpoint fields, while the robot's adapter handles hardware units.

Support button edges and held actions, continuous axes, dead zones, inversion,
scaling, update cadence, and neutral values on release/focus loss/disconnection.
Provide per-device operator mapping overrides. Conflicting default shortcuts
should be visible to the operator. Explicit control ownership is outside scope.

Acceptance: drive works through keyboard, gamepad, and on-screen controls; a
robot-defined example gripper module adds its layout and mappings with no
gripper-specific web route or renderer changes. The example need not actuate
hardware. Promoting it to the public module library changes its reference, not
its module format.

## 3. Make AV capture depend on demand

Move media lifecycle management out of the MQTT bridge into the AV service.
Track independent requests for capture, encoding, transmission, and playback.
Local vision can request camera frames while remote streaming is disabled.
Release unused capture resources by destroying pipelines or setting them NULL.
Publish actual resource state and errors independently of supported capabilities.

Acceptance: no consumers means no camera/microphone capture; a viewer or local
vision task starts the required pipeline; the last consumer leaving stops it;
playback does not require microphone capture. Crashed consumers eventually lose
their requests through leases or equivalent process-lifetime tracking.

## 4. Add Opus in both directions

Extend the audio codec field and advertise supported formats. Start with mono
speech at 24-32 kbps and 20 ms packets. Use one Opus packet per MQTT message,
preserve sequence/timing information, and provide bounded receiver buffering.
Verify robot-to-browser and browser-to-robot paths independently, including
start/stop and reconnect. Keep PCM compatibility during migration.

## 5. Choose video using measurements

Preserve the current delta codec while comparing independent JPEG and H.264.
Prioritize visual usability at matched bitrate, then encoding CPU and recovery.
The generic camera module advertises codec choices rather than depending on one
codec. H618 hardware encoding is a separate backend investigation; current Goob
software encoding measurements cannot establish its eventual performance.

## Migration boundaries

Keep legacy discovery, controls, and topic translation in a compatibility adapter
while the Goob vertical slice matures. Choose one command ingress per endpoint
during migration to prevent duplicate command execution. Restrict translation to
declared mappings rather than bidirectional wildcard rewriting. Migrate other
robots and firmware after the common path works. Avoid combining this milestone
with a repository-wide rename, a new UI framework, a layout designer, or a kernel
replacement.
