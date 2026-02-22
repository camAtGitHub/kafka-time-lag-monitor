
### 🔴 HIGH — TASK 23: `_process_group` discards per-group partition assignments, queries everything

**File:** `sampler.py` lines 136–141

```python
group_partitions = [
    (t, p) for t, p in all_topic_partitions
]
```

This just copies **all** topic/partitions from across **all** groups for every group. It's not filtering to what *this* group actually consumes. So `get_committed_offsets()` is called for every group against every partition in the entire system. For an environment with 10 groups consuming 5 different topics each, you're asking each group for partitions it's never heard of. The Kafka response will give you back empty/negative offsets for those, which then get silently dropped at `tp.offset >= 0`. It works, but it's extremely wasteful and generates a lot of unnecessary Kafka traffic. More importantly: the `describe_consumer_groups` call that *was* supposed to give per-group partition assignments (in `get_all_consumed_topic_partitions`) is being completely ignored — its output is only used as a union set, but the per-group breakdown is thrown away. This is the root cause of the idle group reporting issue you were asking about too.

**Fix:** `get_all_consumed_topic_partitions` should return a `Dict[str, Set[Tuple[str,int]]]` keyed by group_id instead of a flat set, and `_process_group` should look up only its own partitions from that dict.

---

### 🔴 HIGH — TASK 24: SQLite connection shared across threads without serialisation

**File:** `database.py` / `main.py`

A single `sqlite3.Connection` is created in `main()` and passed to the Sampler, Reporter, and Housekeeping threads. `check_same_thread=False` is set, which suppresses the error, but SQLite connections are not thread-safe for concurrent writes. WAL mode helps readers not block writers, but it doesn't make the connection object itself thread-safe. You have three threads potentially calling `conn.execute()` + `conn.commit()` concurrently. This is a data corruption risk under load — you'll get "database is locked" errors or silent corruption depending on timing.

**Fix:** Either give each thread its own connection (each opening the same database file — perfectly fine with WAL), or wrap all `conn.execute` calls in a threading lock at the database layer. Separate connections per thread is the cleaner approach.

---

### 🟠 MEDIUM — TASK 25: State machine defaults unknown groups to ONLINE, masking startup lag

**File:** `state_manager.py` lines 70–76

```python
return {
    "status": "ONLINE",
    ...
    "consecutive_static": 0
}
```

A group/topic seen for the first time starts at `ONLINE` with `consecutive_static=0`. That's fine in steady state, but at startup the consecutive static counter needs to build up from scratch. If the monitor restarts mid-outage, a group that was already OFFLINE will be reported as ONLINE for `threshold` cycles (e.g. 5 × 30s = 2.5 minutes) before it's detected as OFFLINE again. The DB-persisted status is loaded at startup via `_load_group_statuses()`, so this only affects truly new groups — but it's worth documenting explicitly as a known startup blind spot.

---

### 🟠 MEDIUM — TASK 26: `verify_kafka_connectivity` builds a throwaway AdminClient

**File:** `main.py` lines 59–71

```python
admin_client = kafka_client.build_admin_client(config)
```

This builds a client for connectivity checking, then a *second* `admin_client` is built at line 157 for actual use. The first one is never closed and leaks connection resources. In practice confluent-kafka's AdminClient is robust about GC cleanup, but it's sloppy and could be confusing when debugging connection issues (two clients to the same broker from startup).

**Fix:** Return the verified client from `verify_kafka_connectivity` and reuse it.

---

### 🟠 MEDIUM — TASK 27: Idle/ghost group handling (your original question)

**File:** `kafka_client.py` + `sampler.py`

Confirming your analysis: because of the bug in `_process_group` above, the `describe_consumer_groups` per-group partition data is thrown away. So idle groups (no active member assignments) silently produce no data at all — they fall through `all_topic_partitions` being empty for that group, hit the `if not group_partitions: return` early exit at line 142 of `sampler.py`, and generate only a DEBUG log. Groups with DB history should be treated as your **Scenario A** — emit a WARNING and mark IDLE/OFFLINE. Groups with no DB history should be your **Scenario B** — DEBUG is fine. The fix is coupled to the HIGH bug above: once per-group partition lookups are correct, you can check against DB history and respond appropriately.

---

### 🟡 LOW — TASK 28: `_write_partition_offset_if_needed` queries interpolation points just to check the last offset

**File:** `sampler.py` lines 216–219

```python
interpolation_points = database.get_interpolation_points(
    self._db_conn, topic, partition
)
last_stored_offset = interpolation_points[0][0] if interpolation_points else None
```

`get_interpolation_points` fetches **all** historical rows for this partition (unbounded query), just to get the most recent offset. This gets progressively more expensive as data accumulates, even with housekeeping. You already have `get_last_write_time` which uses `MAX(sampled_at)` — you just need a `get_last_stored_offset` that does `SELECT offset FROM partition_offsets WHERE topic=? AND partition=? ORDER BY sampled_at DESC LIMIT 1`. Cheap and purpose-built.

---

### 🟡 LOW — TASK 29: `run_incremental_vacuum` uses f-string for SQL parameter

**File:** `database.py` line 440

```python
conn.execute(f"PRAGMA incremental_vacuum({pages})")
```

`PRAGMA` statements don't support bound parameters in SQLite, so this is technically correct — but if `pages` ever came from user input this would be an injection vector. Right now it's always called with a hardcoded `100`, so not exploitable, but worth changing to validate `pages` is an integer before interpolating.

---

### 🟡 LOW — TASK 30: Reporter `data_resolution` logic is inverted for RECOVERING state

**File:** `reporter.py` line 170

```python
data_resolution = "fine" if status == "online" else "coarse"
```

`status` here is the lowercased string. A group in `RECOVERING` state is actively consuming (offsets are advancing), so it should arguably be `"fine"` resolution too — not `"coarse"`. Whether that's intentional is a design decision, but it's potentially confusing to report `"coarse"` resolution for a group that's catching up and writing at full cadence.

---

### 🟡 LOW — TASK 31: Housekeeping `_run_cycle` ignores elapsed time in sleep calculation

**File:** `housekeeping.py` line 48

The `cycle_start` variable is captured at line 62 inside `_run_cycle` but the outer `run()` loop just sleeps for the full `housekeeping_interval_seconds` regardless of how long the cycle took. For a very large database with thousands of partition keys, housekeeping could take longer than the interval, resulting in back-to-back runs. Low risk but the pattern used in the Sampler (subtract elapsed from sleep) should be consistent here.

---

### ℹ️ NOTE — No TLS/SASL in `build_admin_client`

**File:** `kafka_client.py` lines 43–47

```python
conf = {
    'bootstrap.servers': config.kafka.bootstrap_servers,
    'security.protocol': config.kafka.security_protocol,
}
```

SASL credentials (username/password/mechanism) are not passed even though `security.protocol` is configurable. If you're running with `SASL_SSL` or `SASL_PLAINTEXT` this will silently fail to authenticate. Worth checking your config.py to see if SASL params are even parsed, and if so, that they're passed through here.

FIX: For now just log a one time ERROR log message on startup that SASL/TLS is not currently supported.

---

**Summary table:**

| Severity | Issue | File |
|---|---|---|
| 🔴 HIGH | Per-group partition lookup broken — queries all partitions for all groups | `sampler.py` |
| 🔴 HIGH | SQLite connection shared across threads without synchronisation | `database.py` / `main.py` |
| 🟠 MEDIUM | Startup re-detection delay for groups with no DB history | `state_manager.py` |
| 🟠 MEDIUM | Throwaway AdminClient leaks on startup | `main.py` |
| 🟠 MEDIUM | Idle group not flagged as OFFLINE (depends on HIGH fix) | `sampler.py` / `kafka_client.py` |
| 🟡 LOW | Full interpolation table scanned just to get last offset | `sampler.py` |
| 🟡 LOW | f-string SQL in vacuum pragma | `database.py` |
| 🟡 LOW | RECOVERING reported as coarse resolution | `reporter.py` |
| 🟡 LOW | Housekeeping doesn't subtract cycle time from sleep | `housekeeping.py` |
| ℹ️ NOTE | SASL credentials not forwarded to AdminClient | `kafka_client.py` |

T
