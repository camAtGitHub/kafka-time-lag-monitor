## Interpolation Engine

### `interpolation.py` Responsibilities

Pure functions only. No I/O, no database access, no Kafka calls, no side effects. Takes plain Python data structures as arguments, returns calculated values. Must be fully testable in isolation with no mocking required.

### Required Functions

**`calculate_lag_seconds(committed_offset, interpolation_points, current_time) -> tuple[int, str]`**

Returns `(lag_seconds, method)` where method is one of: `"exact"`, `"interpolated"`, `"extrapolated"`, `"current"`, `"no_data"`.

`interpolation_points` is a list of `(offset, timestamp)` tuples, sorted by timestamp descending (newest first).

Logic:
1. Empty list → return `(0, "no_data")`
2. `committed_offset >= newest offset in table` → return `(0, "current")` — consumer is at or ahead of last known sample
3. Scan for exact offset match → return `(current_time - matching_timestamp, "exact")`. If multiple rows have the same offset, use the one with the latest timestamp (most conservative lag estimate).
4. Find bracketing points where `lower.offset <= committed_offset <= upper.offset` → call `interpolate_timestamp()`, return `(current_time - result, "interpolated")`
5. `committed_offset < oldest offset in table` → return `(current_time - oldest_timestamp, "extrapolated")` — lag is at least this old

**`interpolate_timestamp(committed_offset, lower_point, upper_point) -> int`**

`lower_point` and `upper_point` are each `(offset, timestamp)` tuples.

Applies:
```
(committed - lower_offset) / (upper_offset - lower_offset)
    = (x - lower_ts) / (upper_ts - lower_ts)
```

Solving for x. If `upper_offset == lower_offset` (division by zero), return `upper_point[1]` (the later timestamp).

Worked example: `committed_offset=250`, `lower=(240, 1597304361)`, `upper=(260, 1597304421)` → must return `1597304391`.

**`aggregate_partition_lags(partition_lag_list) -> tuple[int, int | None, str]`**

`partition_lag_list` is a list of `(partition, lag_seconds, method)` tuples.

Returns `(max_lag_seconds, worst_partition, method_of_worst)`. If all lags are 0 returns `(0, None, "current")`.

**`format_lag_display(lag_seconds) -> str`**

Converts integer seconds to a human-readable string.

Examples:
- `0` → `"< 1 minute"`
- `45` → `"< 1 minute"`
- `90` → `"1 minute"`
- `150` → `"2 minutes"`
- `3750` → `"1 hour 2 minutes"`
- `259200` → `"3 days"`
- `277380` → `"3 days 5 hours"`

