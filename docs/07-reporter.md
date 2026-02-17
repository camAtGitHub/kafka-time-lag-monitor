## Reporter Thread

### `reporter.py` Responsibilities

Runs in a continuous loop at its own interval. Reads current state from the database, calculates lag for all monitored group/topic combinations, assembles a JSON blob, and writes it atomically to the configured output path.

### Cycle Logic

```
1. Get all distinct (group_id, topic, partition) triples from recent consumer_commits
2. Group by (group_id, topic) to get the list of partitions per combination
3. For each (group_id, topic):
    a. Get current status from state_manager
    b. Get most recent committed_offset per partition from consumer_commits
    c. For each partition:
        i.  Get interpolation_points from partition_offsets for this (topic, partition)
        ii. Call interpolation.calculate_lag_seconds(committed_offset, points, now)
    d. Call interpolation.aggregate_partition_lags() → (max_lag, worst_partition, method)
    e. Determine data_resolution: "fine" if ONLINE, "coarse" if OFFLINE or RECOVERING
    f. Build output record (see schema below)
4. Assemble full output dict with top-level "generated_at" and "consumers" list
5. Write to {json_path}.tmp
6. Call os.replace({json_path}.tmp, json_path) — atomic rename
7. Update state_manager with last output
8. Sleep for remainder of report_interval_seconds
```

### Output JSON Schema

```json
{
  "generated_at": 1718123456,
  "consumers": [
    {
      "group_id": "billing-consumer",
      "topic": "invoices",
      "lag_seconds": 147,
      "lag_display": "2 minutes",
      "worst_partition": 3,
      "status": "online",
      "data_resolution": "fine",
      "partitions_monitored": 6,
      "calculated_at": 1718123456
    },
    {
      "group_id": "audit-consumer",
      "topic": "invoices",
      "lag_seconds": 0,
      "lag_display": "< 1 minute",
      "worst_partition": null,
      "status": "online",
      "data_resolution": "fine",
      "partitions_monitored": 6,
      "calculated_at": 1718123456
    },
    {
      "group_id": "legacy-consumer",
      "topic": "invoices",
      "lag_seconds": 259200,
      "lag_display": "3 days",
      "worst_partition": 2,
      "status": "offline",
      "data_resolution": "coarse",
      "partitions_monitored": 6,
      "calculated_at": 1718123456
    }
  ]
}
```

Notes:
- `lag_seconds` is always an integer, never null. Use `0` when consumer is current or data is unavailable.
- `status` values are lowercase strings: `"online"`, `"offline"`, `"recovering"`
- `worst_partition` is null when lag is zero

