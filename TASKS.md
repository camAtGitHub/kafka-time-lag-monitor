---

# Fix to be implemented. RE: `_handle_idle_group` Log Storm + Ghost Group Retirement

## Context and Scope

Two distinct problems, one coherent solution:

1. **Log storm** — `_handle_idle_group` logs WARNING and writes DB every cycle for groups that are already OFFLINE. Fix: only act on state transitions, not on steady-state OFFLINE.

2. **Ghost group accumulation** — Groups completely absent from Kafka (not in `list_consumer_groups` at all) accumulate in the DB forever with no cleanup path. Fix: retire them after a configurable retention period once they have been OFFLINE long enough.

Groups that *are* in Kafka but have no active members (e.g. `cambo` — `Empty` state, shows in `--describe`) are intentionally *not* retired, because an admin may have left them registered deliberately. They benefit from fix 1 (log storm) but not fix 2 (retirement). Retirement is scoped only to groups in `idle_groups_with_history` — the set of groups that do not appear in `get_active_consumer_groups()` at all.

---

## Files Modified

- `src/config.py` — add `absent_group_retention_seconds` config field
- `src/database.py` — add `delete_group_data()`
- `src/state_manager.py` — add `remove_group()`
- `src/sampler.py` — rewrite `_handle_idle_group()`, add `_retire_ghost_groups()`
- `config.yaml` and `test_config.yaml` — add new config key

---

## 1. Config — `src/config.py`

### Dataclass

In the `MonitoringConfig` dataclass, add one field after the existing integer fields:

```python
absent_group_retention_seconds: int = 604800  # 7 days
```

### Parsing

In the `load_config()` function, in the monitoring section block alongside the other `_get_nested` calls, add:

```python
absent_group_retention_seconds = _get_nested(
    monitoring_data,
    "absent_group_retention_seconds",
    required=False,
    default=604800,
)
_validate_type(
    absent_group_retention_seconds, int, "monitoring.absent_group_retention_seconds"
)
```

In the `MonitoringConfig(...)` constructor call, add:

```python
absent_group_retention_seconds=absent_group_retention_seconds,
```

---

## 2. Database — `src/database.py`

Add this new function. Place it near the other group-related functions (after `get_group_tracked_topics` or similar):

```python
def delete_group_data(conn: sqlite3.Connection, group_id: str) -> None:
    """Delete all database records for a consumer group.

    Removes rows from consumer_commits and group_status for the given group.
    Does NOT touch partition_offsets — that table is keyed by topic/partition
    only and is shared across groups; housekeeping prunes it by row count.

    Args:
        conn: Database connection
        group_id: Consumer group ID to remove
    """
    conn.execute(
        "DELETE FROM consumer_commits WHERE group_id = ?",
        (group_id,),
    )
    conn.execute(
        "DELETE FROM group_status WHERE group_id = ?",
        (group_id,),
    )
    # Note: caller is responsible for committing the transaction
```

No `conn.commit()` inside — the sampler calls `commit_batch()` at the end of the cycle, consistent with the rest of the write path.

---

## 3. StateManager — `src/state_manager.py`

Add this method to the `StateManager` class:

```python
def remove_group(self, group_id: str) -> None:
    """Remove all in-memory state for a consumer group.

    Called when a ghost group is retired from the database. Clears all
    (group_id, topic) keys from the in-memory status dict so the group
    is no longer iterated over or written by persist_group_statuses.

    Args:
        group_id: Consumer group ID to remove
    """
    with self._lock:
        keys_to_remove = [
            key for key in self._group_statuses if key[0] == group_id
        ]
        for key in keys_to_remove:
            del self._group_statuses[key]
        if keys_to_remove:
            logger.debug(
                f"Removed in-memory state for retired group {group_id} "
                f"({len(keys_to_remove)} topic(s))"
            )
```

---

## 4. Sampler — `src/sampler.py`

### 4a. Rewrite `_handle_idle_group`

Replace the entire existing method body. The new version checks current status before acting and only logs/writes on a genuine state transition:

```python
def _handle_idle_group(self, group_id: str, cycle_start: float) -> None:
    """Handle a group with no active partitions.

    Checks current state for each tracked topic. Only logs WARNING and
    writes OFFLINE status on the first cycle the group is detected as idle
    (i.e. a genuine state transition). Subsequent cycles where the group
    is already OFFLINE produce a DEBUG log only — no DB write.

    Args:
        group_id: The consumer group ID
        cycle_start: Timestamp when the cycle started
    """
    has_history = database.has_group_history(self._db_conn, group_id)

    if not has_history:
        logger.debug(f"No partitions assigned to group {group_id}")
        return

    tracked_topics = database.get_group_tracked_topics(self._db_conn, group_id)
    current_time = int(cycle_start)

    for topic in tracked_topics:
        existing_status = self._state_manager.get_group_status(group_id, topic)
        current_status = existing_status.get("status", "ONLINE")
        last_advancing_at = existing_status.get("last_advancing_at", 0)

        if current_status == "OFFLINE":
            # Already OFFLINE from a previous cycle — steady state, no action needed.
            # Retirement (if applicable) is handled separately by _retire_ghost_groups.
            logger.debug(
                f"Group {group_id}/{topic} remains OFFLINE (no active partitions)"
            )
            continue

        # This is a genuine transition: group was ONLINE or RECOVERING and has
        # now lost all partitions. Log once and write the status change.
        logger.warning(
            f"Group {group_id} has no active partitions but has DB history "
            f"for topic '{topic}'. Transitioning {current_status} -> OFFLINE."
        )
        self._state_manager.set_group_status(
            group_id,
            topic,
            "OFFLINE",
            current_time,
            last_advancing_at,
            self._config.monitoring.offline_detection_consecutive_samples,
        )
```

**Key behavioural changes from current:**
- Per-topic iteration with individual status checks (current code does one WARNING for all topics combined, then writes all — new code logs the transition per topic, which is more precise and consistent with how the state machine works everywhere else)
- Already-OFFLINE topics: no WARNING, no DB write, DEBUG only
- Transitioning topics: WARNING once, write once

### 4b. Add `_retire_ghost_groups`

Add this new method to the `Sampler` class:

```python
def _retire_ghost_groups(
    self, idle_groups_with_history: set, cycle_start: float
) -> None:
    """Retire groups that are absent from Kafka and have been OFFLINE long enough.

    Only called for groups in idle_groups_with_history — the set of groups
    that do not appear in get_active_consumer_groups() at all. Groups that
    ARE in Kafka (even with no active members / Empty state) are not retired
    here; their lifecycle is managed by the Kafka administrator.

    A group is retired when ALL of the following are true:
      - It is absent from Kafka entirely (in idle_groups_with_history)
      - Every tracked topic for the group has status OFFLINE
      - The earliest status_changed_at across all topics is older than
        absent_group_retention_seconds

    On retirement:
      - All consumer_commits rows for the group are deleted
      - All group_status rows for the group are deleted
      - In-memory state is cleared via state_manager.remove_group()
      - An INFO log is emitted with the group ID and how long it was OFFLINE

    partition_offsets is NOT cleaned up here — it is keyed by topic/partition
    only and shared across groups. Housekeeping prunes it by row count naturally.

    Args:
        idle_groups_with_history: Set of group IDs absent from Kafka but with
                                  historical data in the database
        cycle_start: Timestamp when the cycle started
    """
    retention = self._config.monitoring.absent_group_retention_seconds
    current_time = int(cycle_start)

    for group_id in idle_groups_with_history:
        tracked_topics = database.get_group_tracked_topics(
            self._db_conn, group_id
        )

        if not tracked_topics:
            continue

        # All topics must be OFFLINE, and we find the earliest OFFLINE timestamp
        should_retire = True
        earliest_offline_at = current_time

        for topic in tracked_topics:
            status = self._state_manager.get_group_status(group_id, topic)

            if status.get("status") != "OFFLINE":
                # Group has a topic not yet OFFLINE — not ready for retirement.
                # This can happen on the same cycle _handle_idle_group transitions
                # it, since retirement runs after idle handling.
                should_retire = False
                break

            topic_offline_since = status.get("status_changed_at", current_time)
            earliest_offline_at = min(earliest_offline_at, topic_offline_since)

        if not should_retire:
            continue

        time_offline_seconds = current_time - earliest_offline_at

        if time_offline_seconds < retention:
            logger.debug(
                f"Ghost group {group_id} has been OFFLINE for "
                f"{time_offline_seconds // 3600:.1f}h — "
                f"retention period is {retention // 3600:.1f}h, not retiring yet."
            )
            continue

        # Retention period exceeded — retire the group
        logger.info(
            f"Retiring ghost group '{group_id}': absent from Kafka, "
            f"OFFLINE for {time_offline_seconds // 3600:.1f}h "
            f"(retention threshold: {retention // 3600:.1f}h). "
            f"Removing from database. Topics were: {tracked_topics}"
        )

        database.delete_group_data(self._db_conn, group_id)
        self._state_manager.remove_group(group_id)
```

### 4c. Wire `_retire_ghost_groups` into the run loop

In `run()`, after the existing Step 3c block, add Step 3d immediately after:

```python
                # Step 3c: Mark idle groups as OFFLINE (transition only — no repeat writes)
                for group_id in idle_groups_with_history:
                    self._handle_idle_group(group_id, cycle_start)

                # Step 3d: Retire ghost groups (absent from Kafka past retention period)
                self._retire_ghost_groups(idle_groups_with_history, cycle_start)
```

No other changes to the run loop are needed.

---

## 5. Config Files

### `config.yaml`

Add under the `monitoring:` block, alongside the other interval settings:

```yaml
monitoring:
  # ... existing fields ...
  absent_group_retention_seconds: 604800   # 7 days. Ghost groups (absent from Kafka entirely)
                                            # are removed from the database after being OFFLINE
                                            # for this long. Set to 0 to disable retirement.
```

### `test_config.yaml`

Add the same key with a short value suitable for tests:

```yaml
monitoring:
  # ... existing fields ...
  absent_group_retention_seconds: 3600    # 1 hour for test config
```

---

## Interaction Between the Two Fixes

The two fixes are designed to chain correctly within a single cycle:

1. **Step 3c** calls `_handle_idle_group` for every group in `idle_groups_with_history`. For a brand-new ghost group, this transitions it to OFFLINE and logs WARNING. For an already-OFFLINE group, it logs DEBUG and does nothing.

2. **Step 3d** calls `_retire_ghost_groups` for the same set. For a group that was *just* transitioned to OFFLINE in 3c, `status_changed_at` is set to `current_time`, so `time_offline_seconds` will be ~0 and it will not be retired this cycle. The INFO log explaining retirement doesn't fire until the retention period has elapsed in a future cycle. There is no race.

3. Once `delete_group_data` and `remove_group` are called, the group disappears from `consumer_commits` and `group_status`. On the next cycle, `get_all_groups_with_history` will no longer return it (since that queries `consumer_commits`), so it won't appear in `idle_groups_with_history` and neither `_handle_idle_group` nor `_retire_ghost_groups` will be called for it again. Clean exit.

---

## Behaviour Summary After Fix

| Scenario | Before fix | After fix |
|---|---|---|
| Group transitions to OFFLINE (first idle cycle) | WARNING + DB write | WARNING + DB write (unchanged) |
| Group already OFFLINE, subsequent cycles | WARNING + DB write every 30s | DEBUG only, no DB write |
| Ghost group absent from Kafka < retention period | WARNING every 30s, forever | DEBUG only |
| Ghost group absent from Kafka ≥ retention period | WARNING every 30s, forever | INFO once, then deleted from DB — gone |
| Group in Kafka, Empty state (e.g. `cambo`) | WARNING every 30s | DEBUG only after first transition |
