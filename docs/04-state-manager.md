## State Manager

### `state_manager.py` Responsibilities

Owns the single shared in-memory state dictionary used for communication between threads. All access is protected by a `threading.RLock`. This is the only sanctioned way for threads to share data — threads do not call each other directly.

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

- `__init__(db_conn)` — loads persisted group statuses from database into memory on construction
- `get_group_status(group_id, topic) -> dict` — returns status dict; defaults to ONLINE state if unknown
- `set_group_status(group_id, topic, status, **kwargs)` — updates memory and persists to database atomically
- `get_all_group_statuses() -> dict` — returns a snapshot copy of the full status dict
- `update_thread_last_run(thread_name, timestamp)`
- `get_thread_last_run(thread_name) -> int`
- `set_last_json_output(output_dict)`
- `get_last_json_output() -> dict`

