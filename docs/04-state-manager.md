## State Manager

### `state_manager.py` Responsibilities

Owns the single shared in-memory state dictionary used for communication between threads. All access is protected by a `threading.RLock`. This is the only sanctioned way for threads to share data — threads do not call each other directly.

**Key architectural principle:** StateManager is purely in-memory during operation. Database persistence is deferred and folded into the sampler's batch transaction to maintain the single-writer pattern.

### In-Memory State Structure

```python
{
    "group_statuses": {
        (group_id, topic): {
            "status": "ONLINE",           # ONLINE | OFFLINE | RECOVERING
            "status_changed_at": 1718000000,
            "last_advancing_at": 1718000000,
            "consecutive_static": 0
        }
    },
    "last_json_output": {},               # Most recent reporter output dict
    "thread_last_run": {
        "sampler": 0,
        "reporter": 0,
        "housekeeping": 0
    }
}
```

### Required Functions

- `__init__(db_path, config)` — loads persisted group statuses from database into memory on construction. Opens connection inline, loads data, then closes it immediately. Does not maintain a persistent connection.
- `get_group_status(group_id, topic) -> dict` — returns status dict; defaults to ONLINE state if unknown
- `set_group_status(group_id, topic, status, **kwargs)` — **updates in-memory state only**. Does not write to database. Persistence is handled separately by `persist_group_statuses()`.
- `persist_group_statuses(conn)` — writes all current group statuses to database using the provided connection. Called by sampler at end of cycle as part of batch transaction. Does not commit; that's handled by `commit_batch()`.
- `get_all_group_statuses() -> dict` — returns a snapshot copy of the full status dict
- `update_thread_last_run(thread_name, timestamp)`
- `get_thread_last_run(thread_name) -> int`
- `set_last_json_output(output_dict)`
- `get_last_json_output() -> dict`
- `remove_group(group_id)` — remove all in-memory state for a consumer group. Called when a ghost group is retired from the database. Does not affect the database; that's handled separately by `delete_group_data()`.

### Transaction Model

StateManager does **not** write to the database during normal operation. The `set_group_status()` method only updates in-memory state. At the end of each sampler cycle, the sampler calls `persist_group_statuses(sampler_db_conn)` to fold all status updates into the sampler's open transaction. This ensures:

1. Single writer per cycle (no lock contention)
2. All writes (consumer_commits, partition_offsets, group_status) committed atomically
3. StateManager has no persistent database connection to manage
4. Clean transaction boundaries throughout the system

The `INSERT OR REPLACE` semantics of `upsert_group_status` make it safe to write all statuses on every cycle, even if they haven't changed.

