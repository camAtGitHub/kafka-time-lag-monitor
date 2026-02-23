## Idle Group Handling and Ghost Group Retirement

### Overview

The sampler handles two distinct scenarios for consumer groups that have no active partitions assigned:

1. **Idle Group Transition** — Groups with database history that lose all partitions are transitioned to OFFLINE status
2. **Ghost Group Retirement** — Groups completely absent from Kafka are automatically removed from the database after a retention period

### Idle Group Detection (`_handle_idle_group`)

When a group has no active partitions but has historical data in the database, the sampler checks the current status for each tracked topic:

- **First detection (transition):** Logs WARNING and writes OFFLINE status to the database
- **Subsequent cycles (steady state):** Logs DEBUG only, no database write

This prevents log storms and unnecessary database writes for groups that remain OFFLINE.

**Key behavior:**
- Groups in Kafka but with no active members (e.g., `Empty` state) are intentionally handled this way
- Only logs/writes on genuine state transitions (ONLINE → OFFLINE or RECOVERING → OFFLINE)
- Already-OFFLINE groups produce DEBUG log only

### Ghost Group Retirement (`_retire_ghost_groups`)

Groups that are completely absent from Kafka (not returned by `list_consumer_groups`) are candidates for retirement after they've been OFFLINE long enough.

**Retirement criteria (all must be true):**
- Group is absent from Kafka entirely (in `idle_groups_with_history`)
- Every tracked topic for the group has status OFFLINE
- The earliest `status_changed_at` across all topics is older than `absent_group_retention_seconds`

**On retirement:**
- All `consumer_commits` rows for the group are deleted
- All `group_status` rows for the group are deleted
- In-memory state is cleared via `state_manager.remove_group()`
- INFO log is emitted with group ID and how long it was OFFLINE

**Important notes:**
- `partition_offsets` is NOT cleaned up (keyed by topic/partition only, shared across groups)
- Groups that ARE in Kafka but have no active members are NOT retired
- Housekeeping naturally prunes `partition_offsets` by row count

### Configuration

```yaml
monitoring:
  absent_group_retention_seconds: 604800   # 7 days (optional, defaults to 604800)
```

Set to a large value (e.g., 2592000 for 30 days) for long-lived environments. Set to a smaller value (e.g., 86400 for 1 day) for ephemeral test environments.

### Execution Flow

The sampler run loop executes these operations in sequence during each cycle:

1. **Step 3c:** Call `_handle_idle_group()` for each group in `idle_groups_with_history`
   - Transitions newly-idle groups to OFFLINE
   - Already-OFFLINE groups log DEBUG only

2. **Step 3d:** Call `_retire_ghost_groups()` with the same set
   - Groups just transitioned in Step 3c have `status_changed_at = current_time`, so `time_offline_seconds ≈ 0`
   - They won't be retired until a future cycle when the retention period elapses
   - Groups already OFFLINE for longer than retention are removed

3. **Step 5:** Call `commit_batch()` to atomically commit all sampler writes

This sequencing ensures no race conditions between transition and retirement.

### Behavior Summary

| Scenario | Before fix | After fix |
|---|---|---|
| Group transitions to OFFLINE (first idle cycle) | WARNING + DB write | WARNING + DB write (unchanged) |
| Group already OFFLINE, subsequent cycles | WARNING + DB write every 30s | DEBUG only, no DB write |
| Ghost group absent from Kafka < retention period | WARNING every 30s, forever | DEBUG only |
| Ghost group absent from Kafka ≥ retention period | WARNING every 30s, forever | INFO once, then deleted from DB — gone |
| Group in Kafka, Empty state (e.g., `cambo`) | WARNING every 30s | DEBUG only after first transition |

---
