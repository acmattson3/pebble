# AprilTag Locations

Publishes all visible AprilTag IDs and normalized corner points to MQTT.

Outputs:

- `.../outgoing/apriltag-locations`
- `.../outgoing/video-overlays`

Payload includes all detected marker IDs (`marker_id`) with `corners_norm` and `center_norm`.

This script is intended as a dependency for `apriltag-odom-follow`.
