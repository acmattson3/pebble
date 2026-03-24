# AprilTag Odom Follow

`run.py` is now a lightweight decision script.

It does not run camera or raw odometry processing directly. Instead it consumes:

- `.../outgoing/apriltag-locations` (from dependency script `apriltag-locations`)
- `.../outgoing/wheel-odometry` (from dependency script `wheel-odometry`)

and publishes drive commands to:

- `.../incoming/drive-values`

## Behavior

- When the target tag is visible, vision authority matches `apriltag-follow`.
- While visible, the script continuously remembers a world-space odometry goal point at stand-off distance.
- On tag loss, if a goal exists, it drives by odometry to that point.
- After the goal is reached (or no goal exists), it runs search pulses to reacquire.

## Dependency launch

`autonomy_manager` starts/stops dependency scripts automatically for this script:

- `apriltag-locations`
- `wheel-odometry` with `use_tune_state=false`

## Logs

Each run writes CSV decision logs in `autonomy/apriltag-odom-follow/logs` by default.
