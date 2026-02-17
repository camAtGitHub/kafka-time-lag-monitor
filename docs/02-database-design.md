## Database Design

### `database.py` Responsibilities

All SQLite interaction lives here. No other module constructs SQL. All other modules call functions from this module.

### Connection Initialisation

On first connection, execute:
```sql
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA auto_vacuum=INCREMENTAL;
```

WAL mode allows concurrent reads and writes from multiple threads without blocking. `INCREMENTAL` auto_vacuum enables the housekeeping thread to reclaim disk space in small increments rather than requiring a full blocking VACUUM.

### Table Definitions

**`partition_offsets`** — the interpolation table. One row per sample per topic/partition. Not group-scoped — multiple groups consuming the same partition share this data.

| Column | Type | Notes |
|---|---|---|
| topic | TEXT | |
| partition | INTEGER | |
| offset | INTEGER | Latest produced offset at time of sample |
| sampled_at | INTEGER | Unix timestamp |

Primary key: `(topic, partition, sampled_at)`

**`consumer_commits`** — rolling history of committed offsets per consumer group. Used for offline detection and consumption rate calculation.

| Column | Type | Notes |
|---|---|---|
| group_id | TEXT | |
| topic | TEXT | |
| partition | INTEGER | |
| committed_offset | INTEGER | |
| recorded_at | INTEGER | Unix timestamp |

Primary key: `(group_id, topic, partition, recorded_at)`

**`group_status`** — persisted state machine status per group/topic combination.

| Column | Type | Notes |
|---|---|---|
| group_id | TEXT | |
| topic | TEXT | |
| status | TEXT | ONLINE, OFFLINE, or RECOVERING |
| status_changed_at | INTEGER | Unix timestamp of last transition |
| last_advancing_at | INTEGER | Unix timestamp when offset last moved |
| consecutive_static | INTEGER | Counter, reset to 0 when offset moves |

Primary key: `(group_id, topic)`

**`excluded_topics`** — runtime-configurable topic exclusion list.

| Column | Type |
|---|---|
| topic | TEXT PRIMARY KEY |

**`excluded_groups`** — runtime-configurable group exclusion list.

| Column | Type |
|---|---|
| group_id | TEXT PRIMARY KEY |

### Required Functions

- `init_db(path) -> connection` — create tables if not exist, set pragmas, return connection
- `insert_partition_offset(conn, topic, partition, offset, sampled_at)`
- `insert_consumer_commit(conn, group_id, topic, partition, committed_offset, recorded_at)`
- `get_interpolation_points(conn, topic, partition) -> list[tuple[int, int]]` — returns list of `(offset, sampled_at)` ordered by `sampled_at DESC`
- `get_recent_commits(conn, group_id, topic, partition, limit) -> list[tuple[int, int]]` — returns `(committed_offset, recorded_at)` rows
- `get_last_write_time(conn, topic, partition) -> int | None` — most recent `sampled_at` for this topic/partition
- `get_group_status(conn, group_id, topic) -> dict | None`
- `upsert_group_status(conn, group_id, topic, status, status_changed_at, last_advancing_at, consecutive_static)`
- `load_all_group_statuses(conn) -> dict` — used at startup to rehydrate state manager
- `is_topic_excluded(conn, topic, config_exclusions) -> bool`
- `is_group_excluded(conn, group_id, config_exclusions) -> bool`
- `prune_partition_offsets(conn, topic, partition, keep_n)` — delete all but most recent `keep_n` rows
- `prune_consumer_commits(conn, group_id, topic, partition, keep_n)` — same pattern
- `get_all_partition_keys(conn) -> list[tuple[str, int]]` — distinct `(topic, partition)` pairs
- `get_all_commit_keys(conn) -> list[tuple[str, str, int]]` — distinct `(group_id, topic, partition)` triples
- `run_incremental_vacuum(conn, pages=100)`

---
