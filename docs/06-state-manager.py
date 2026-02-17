## Sampler Thread

### `sampler.py` Responsibilities

Runs in a continuous loop. Each cycle: queries Kafka for current partition heads and committed offsets, decides whether to write each sample based on group status and elapsed time, writes to the database, and evaluates group state machine transitions.

### Cycle Logic

```
1. Record cycle start time
2. Get active consumer groups from kafka_client — filter excluded groups
3. Get all (topic, partition) pairs consumed by those groups — filter excluded topics
4. Bulk fetch latest produced offsets for all topic/partitions
5. For each group_id:
    a. Bulk fetch committed offsets for this group across all its topic/partitions
    b. For each (topic, partition) in this group:
        i.   Always write to consumer_commits (used for offline detection, not rate-limited)
        ii.  Determine write cadence based on group status (ONLINE=60s, OFFLINE/RECOVERING=1800s)
        iii. Get last write time for this (topic, partition) from partition_offsets
        iv.  If elapsed time >= cadence: write to partition_offsets (only if offset changed OR cadence elapsed regardless)
    c. Evaluate state machine for this (group_id, topic) — see below
6. Sleep for remainder of sample_interval_seconds
```

### State Machine Evaluation

Called per `(group_id, topic)` after processing all partitions for that combination.

```
1. Pull last N committed_offset values per partition for this group/topic from consumer_commits
   (N = offline_detection_consecutive_samples from config)
2. For each partition: check if all N values are identical (static)
3. If ALL partitions are static:
    - Increment consecutive_static counter
    - If consecutive_static >= threshold:
        - If status is ONLINE → transition to OFFLINE
        - If status is RECOVERING → transition back to OFFLINE
        - Update last_advancing_at is NOT updated
4. If ANY partition is advancing (not all static):
    - Reset consecutive_static = 0
    - Update last_advancing_at = now
    - If status is OFFLINE → transition to RECOVERING
    - If status is RECOVERING:
        - Calculate current max lag across partitions using interpolation
        - Check: lag < online_lag_threshold_seconds
          AND time_in_recovering >= recovering_minimum_duration_seconds
        - If both true → transition to ONLINE
5. Persist any status change via state_manager.set_group_status()
```

### Write Cadence Detail

`partition_offsets` is not group-scoped. If two groups consume the same partition, they share the same interpolation rows. The sampler should avoid writing duplicate rows: before inserting a new `partition_offsets` row, check if the latest stored offset for that `(topic, partition)` is already equal to the current offset AND the elapsed time is less than the coarse cadence. If so, skip the write. This prevents filling the table with identical rows during quiet periods while still maintaining the timestamp trail.
