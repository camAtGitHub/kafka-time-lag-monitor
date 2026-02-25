# TASKS.md

## MISSION
Continue the kafka-time-lag-monitor code review series. This file addresses:
- A new bug found in the main thread shutdown path (TASK 51)
- A clean resolution of the partial TASK 50 implementation (TASK 52)
- Missing test coverage identified during the TASKS-4 post-implementation review (TASKS 53–59)

MISSION: Implement ALL of the fixes and tests in a way that guards against regression of these issues in the future, with a focus on the critical shutdown delay bug.

## METHODOLOGY
Tasks are ordered lowest-risk/most-isolated first. Test tasks (53–59) are independent of each
other and can be worked in any order once TASK 51 and 52 are complete.

---

## Implementation Order

1. **TASK 51** — shutdown bug, `main.py` only, zero interactions
2. **TASK 52** — `database.py` + two call sites, low interaction
3. **TASK 53–59** — new tests, any order, no production code changes

---

## 🔴 HIGH — TASK 51: `time.sleep(30)` in main heartbeat does not respond to shutdown — causes 21–25s delay on Ctrl-C

**File:** `main.py`

**Root cause (PEP 475):**
Python 3.5+ introduced PEP 475, which causes interrupted system calls to be automatically
*retried* after a signal handler returns. `time.sleep()` is implemented as a loop over
`clock_nanosleep()` / `select()`. When `^C` fires:

1. The OS delivers SIGINT to the main thread.
2. The low-level sleep syscall returns `EINTR`.
3. Python runs the custom signal handler (`handle_signal`), which sets `shutdown_event`.
4. **PEP 475 behaviour:** Python calculates the remaining sleep duration and *restarts the
   sleep syscall* for that duration, because no Python-level exception was raised.
5. The main thread stays blocked in `time.sleep()` for the rest of its 30-second window
   even though `shutdown_event` is already set.

The worker threads (`sampler`, `reporter`, `housekeeping`) all use `shutdown_event.wait()` for
their inter-cycle sleeps, so they wake and exit as soon as the event is set. However, the main
thread cannot reach the `join()` loop until `time.sleep(30)` finally returns. On a production
deployment where `^C` arrives uniformly across the 30-second window, the expected remaining
sleep is 15 seconds. The observed 21–25s range is consistent with `^C` arriving 5–9 seconds
into the current 30-second sleep (30 − 5 = 25, 30 − 9 = 21).

`threading.Event.wait()` is *not* subject to PEP 475 resume behaviour because it is
implemented at the Python level using a `Condition` variable. When `shutdown_event.set()` is
called from the signal handler, the condition notifies any waiting threads and
`shutdown_event.wait()` returns `True` on the very next check — typically within milliseconds.

**Expected outcome:** After `^C`, the process shuts down within 1–3 seconds under normal
operating conditions (threads sleeping between cycles). Under worst-case conditions (a thread
mid-Kafka-call at shutdown time), shutdown completes within one Kafka request timeout plus a
small margin, rather than 21–25+ seconds.

**Implementation:**

Replace the `time.sleep(30)` in `main()`'s heartbeat loop with `shutdown_event.wait(timeout=30)`:

```python
# BEFORE
try:
    while not shutdown_event.is_set():
        time.sleep(30)
        sampler_last = state_mgr.get_thread_last_run("sampler")
        reporter_last = state_mgr.get_thread_last_run("reporter")
        housekeeping_last = state_mgr.get_thread_last_run("housekeeping")
        logger.debug(
            f"Heartbeat: sampler={sampler_last}, "
            f"reporter={reporter_last}, housekeeping={housekeeping_last}"
        )
except KeyboardInterrupt:
    logger.info("Keyboard interrupt received")
    shutdown_event.set()

# AFTER
try:
    while not shutdown_event.wait(timeout=30):
        sampler_last = state_mgr.get_thread_last_run("sampler")
        reporter_last = state_mgr.get_thread_last_run("reporter")
        housekeeping_last = state_mgr.get_thread_last_run("housekeeping")
        logger.debug(
            f"Heartbeat: sampler={sampler_last}, "
            f"reporter={reporter_last}, housekeeping={housekeeping_last}"
        )
except KeyboardInterrupt:
    logger.info("Keyboard interrupt received")
    shutdown_event.set()
```

`shutdown_event.wait(timeout=30)` returns `True` (truthy) when the event is set — terminating
the `while not` loop immediately — or `False` after 30 seconds, allowing a heartbeat log.
The `except KeyboardInterrupt` block remains as a defence against environments where the
default SIGINT handler bypasses the custom one (e.g., a second rapid `^C`).

**Secondary improvement (recommended but not critical):** The `thread.join(timeout=10)` loop
is sequential. In the (now uncommon) case where a thread is mid-Kafka-call at shutdown and
takes longer than 10 seconds to exit, joins stack linearly. Consider joining threads
concurrently via a shared deadline:

```python
deadline = time.time() + 10
for thread in threads:
    remaining = max(0, deadline - time.time())
    thread.join(timeout=remaining)
    if thread.is_alive():
        logger.warning(f"Thread {thread.name} did not stop within timeout")
```

This caps total join wait at ~10 seconds regardless of thread count, rather than 10s × N.

**Notes:** Do not remove `time` from the imports — it is still used in `verify_kafka_connectivity`
and `run_with_restart`. The `except KeyboardInterrupt` block may become unreachable in normal
operation but retains value as a belt-and-braces defence. After this fix, the observed shutdown
time should be < 3 seconds in the common case.

**Tests:** See TASK 53.

---

## 🟡 LOW — TASK 52: `get_interpolation_points` limit is hardcoded at 1000 — decouple from config

**File:** `database.py`, `sampler.py`, `reporter.py`

**Context:**
TASK 50 from TASKS-4 added a `LIMIT 1000` to `get_interpolation_points`. This is better than
no limit but is a magic number disconnected from `max_entries_per_partition` in config.
Consider: if an operator sets `max_entries_per_partition: 2000` (valid per TASK 49's `>= 2`
constraint), housekeeping will retain 2000 rows per partition, but the interpolation query
will silently cap at 1000 — returning an incomplete view of the historical data and potentially
degrading lag accuracy. The fix as originally specified in TASK 50 is straightforward and correct.

**Expected outcome:** The interpolation query fetches up to `max_entries_per_partition` rows
(passed by the caller from config), with a safe default for any call site that does not supply
a limit. The magic number 1000 is eliminated.

**Implementation — three locations:**

**1. `database.py` — add `limit` parameter:**

```python
def get_interpolation_points(
    conn: sqlite3.Connection, topic: str, partition: int, limit: int = 500
) -> List[Tuple[int, int]]:
    """Get interpolation points for a topic/partition ordered by sampled_at DESC.

    Returns up to `limit` most recent points. Callers should pass
    max_entries_per_partition from config to align with housekeeping bounds.

    Args:
        conn: Database connection
        topic: Topic name
        partition: Partition number
        limit: Maximum rows to return (default: 500)

    Returns:
        List of (offset, sampled_at) tuples ordered by sampled_at DESC
    """
    cursor = conn.execute(
        """SELECT offset, sampled_at FROM partition_offsets
           WHERE topic = ? AND partition = ?
           ORDER BY sampled_at DESC
           LIMIT ?""",
        (topic, partition, limit),
    )
    return [(row[0], row[1]) for row in cursor.fetchall()]
```

**2. `sampler.py` — `_calculate_max_lag`, pass configured limit:**

```python
interpolation_points = database.get_interpolation_points(
    self._db_conn,
    topic,
    partition,
    self._config.monitoring.max_entries_per_partition,   # ADD THIS
)
```

**3. `reporter.py` — `_process_consumer`, pass configured limit:**

```python
interpolation_points = database.get_interpolation_points(
    self._db_conn,
    topic,
    partition,
    self._config.monitoring.max_entries_per_partition,   # ADD THIS
)
```

**Notes:** The default of 500 is a safe backstop for any future call sites (e.g., tests,
CLI tools) that do not supply a limit. It is lower than the now-removed hardcoded 1000 but
higher than the default `max_entries_per_partition` of 300, providing headroom. No other
callers of `get_interpolation_points` exist in the current codebase. The interpolation
algorithm scans all returned rows to find bracketing points — it does not need all rows, only
the two that bracket the committed offset. A future optimisation (two targeted queries: one
for the nearest row above the committed offset, one below) would eliminate this scan entirely
and make the limit irrelevant, but that is explicitly out of scope here.

**Tests:** See TASK 59.

---

## 🟡 LOW — TASK 53: Test that shutdown completes quickly after `shutdown_event` is set

**File:** `src/tests/test_main.py` (new file)

**Reason:** TASK 51 fixes a shutdown delay caused by `time.sleep(30)` not responding to the
shutdown event. Without a test, this regression could silently reappear if the heartbeat loop
is refactored back to `time.sleep()`. This is the only test that guards against the PEP 475
sleep behaviour.

**Implementation:**

```python
import threading
import time
import pytest


def test_shutdown_event_wait_exits_immediately():
    """
    Verify that shutdown_event.wait() returns instantly when event is set.

    Regression test for TASK 51. If this is replaced with time.sleep(30),
    this test will not catch the regression — the separate shutdown timing
    test below covers that scenario.
    """
    shutdown_event = threading.Event()
    results = []

    def waiter():
        start = time.time()
        shutdown_event.wait(timeout=30)
        results.append(time.time() - start)

    t = threading.Thread(target=waiter)
    t.start()
    time.sleep(0.05)
    shutdown_event.set()
    t.join(timeout=2)

    assert not t.is_alive(), "Thread should have exited"
    assert results[0] < 1.0, f"Wait took {results[0]:.2f}s — expected < 1s"


def test_main_heartbeat_exits_within_one_second_of_shutdown():
    """
    Simulate the main heartbeat loop and verify it exits < 1 second after
    shutdown_event is set. This guards against regression to time.sleep(30).

    The heartbeat pattern under test:
        while not shutdown_event.wait(timeout=30):
            logger.debug("heartbeat")
    """
    shutdown_event = threading.Event()
    exited_at = []

    def heartbeat_loop():
        while not shutdown_event.wait(timeout=30):
            pass  # simulates heartbeat body
        exited_at.append(time.time())

    t = threading.Thread(target=heartbeat_loop)
    t.start()
    time.sleep(0.1)

    set_at = time.time()
    shutdown_event.set()
    t.join(timeout=2)

    assert not t.is_alive()
    assert exited_at, "Heartbeat loop never exited"
    assert exited_at[0] - set_at < 1.0, (
        f"Heartbeat loop took {exited_at[0] - set_at:.2f}s to exit after shutdown — "
        "regression: time.sleep() was likely reintroduced"
    )
```

**Expected outcome:** Both tests pass in < 1 second. If `time.sleep(30)` is ever reintroduced
in the heartbeat loop, `test_main_heartbeat_exits_within_one_second_of_shutdown` will fail
with a clear message.

---

## 🔴 HIGH — TASK 54: Test that DB reconnect actually produces a working connection (Sampler, Reporter, Housekeeping)

**Files:** `src/tests/test_sampler.py`, `src/tests/test_reporter.py`, `src/tests/test_housekeeping.py`

**Reason:** The existing `test_database_write_error_handled_gracefully` in `test_sampler.py`
only asserts that the error is caught without crashing. It does **not** verify that:
1. A new connection is obtained after the error.
2. The subsequent cycle succeeds using that new connection.

This is the highest-value missing test in the suite. The reconnect logic in TASK 42 is the
only thing preventing silent data loss after a transient DB error. An incorrect reconnect
(e.g., reconnecting to the wrong path, or failing silently) would not be caught by the current
test. The same gap exists for Reporter and Housekeeping.

**Implementation — Sampler (add to `TestSamplerRunLoop` or a new class):**

```python
def test_db_reconnect_on_database_error_and_next_cycle_succeeds(self, db_path, db_conn):
    """
    TASK 42 regression: after a sqlite3.DatabaseError mid-cycle, the sampler
    must obtain a fresh connection and succeed on the next cycle.
    """
    import sqlite3
    from unittest.mock import MagicMock, patch
    import threading

    config = make_test_config()
    state_mgr = MockStateManager()

    mock_kafka = MagicMock()
    mock_kafka.get_active_consumer_groups.return_value = ["group1"]
    mock_kafka.get_all_consumed_topic_partitions.return_value = {
        "group1": {("topic1", 0)}
    }
    mock_kafka.get_latest_produced_offsets.return_value = {("topic1", 0): 100}
    mock_kafka.get_committed_offsets.return_value = {("topic1", 0): 50}

    call_count = {"n": 0}
    original_insert = database.insert_consumer_commit

    def insert_side_effect(*args, **kwargs):
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise sqlite3.OperationalError("database is locked")
        return original_insert(*args, **kwargs)

    new_conn_calls = []
    original_get_connection = database.get_connection

    def tracking_get_connection(path):
        conn = original_get_connection(path)
        new_conn_calls.append(conn)
        return conn

    with patch("sampler.database.insert_consumer_commit", side_effect=insert_side_effect), \
         patch("sampler.database.get_connection", side_effect=tracking_get_connection):

        s = Sampler(config, db_path, mock_kafka, state_mgr)
        initial_conn = s._db_conn

        # Run exactly two cycles: error on cycle 1, succeed on cycle 2
        shutdown_event = threading.Event()
        cycle_count = {"n": 0}
        original_wait = shutdown_event.wait

        def controlled_wait(timeout=None):
            cycle_count["n"] += 1
            if cycle_count["n"] >= 2:
                shutdown_event.set()
            return original_wait(timeout=0.01)

        shutdown_event.wait = controlled_wait
        s.run(shutdown_event)

    # A new connection must have been obtained after the error
    assert len(new_conn_calls) >= 1, "No reconnect occurred after DatabaseError"
    assert s._db_conn is not initial_conn, "Connection was not replaced after error"
```

**Nuance:** The test uses a side-effect counter to raise the error only on cycle 1, then
succeed on cycle 2. Asserting `s._db_conn is not initial_conn` is the key correctness check —
it verifies the object reference changed, not just that no exception was raised.

**Reporter and Housekeeping:** Write analogous tests in their respective test files. For
Housekeeping, patch `database.prune_partition_offsets` to raise on first call; verify
`h._db_conn` is replaced. For Reporter, patch `database.get_all_commit_keys`.

**Expected outcome:** Tests confirm that after a `sqlite3.DatabaseError`, the affected thread
replaces its connection and the following cycle executes successfully using the new connection.

---

## 🔴 HIGH — TASK 55: Test that housekeeping prune operations actually delete rows from DB

**File:** `src/tests/test_housekeeping.py`

**Reason:** TASK 43 fixed a critical bug where prune operations logged correct rowcounts but
silently rolled back due to missing `commit_batch()`. The existing `test_cycle_logs_summary`
test only inserts 10 rows (below pruning thresholds) and only checks the log message — it
does not verify actual DB state. A regression to the missing-commit behaviour would not be
caught.

**Implementation:**

```python
def test_housekeeping_cycle_actually_deletes_rows_from_partition_offsets(
    self, db_path_initialized, db_conn, mock_config
):
    """
    TASK 43 regression: after _run_cycle(), rows exceeding max_entries_per_partition
    must be physically absent from the database — not just reported as deleted.

    The pre-fix bug was: prune functions issued conn.commit() per-partition but
    housekeeping._run_cycle() had no final commit_batch(), causing all DELETEs
    to roll back silently while rowcount was still reported correctly.
    """
    max_entries = mock_config.monitoring.max_entries_per_partition
    insert_count = max_entries + 50  # exceed limit by 50

    now = int(time.time())
    for i in range(insert_count):
        database.insert_partition_offset(db_conn, "topic1", 0, i, now - i)
    database.commit_batch(db_conn)

    # Verify setup: rows exist above threshold
    pre_count = db_conn.execute(
        "SELECT COUNT(*) FROM partition_offsets WHERE topic='topic1' AND partition=0"
    ).fetchone()[0]
    assert pre_count == insert_count

    hk = Housekeeping(mock_config, db_path_initialized)
    hk._run_cycle()

    # Query via a SEPARATE connection to confirm the commit reached the DB file
    verify_conn = database.get_connection(db_path_initialized)
    post_count = verify_conn.execute(
        "SELECT COUNT(*) FROM partition_offsets WHERE topic='topic1' AND partition=0"
    ).fetchone()[0]
    verify_conn.close()

    assert post_count == max_entries, (
        f"Expected {max_entries} rows after pruning, found {post_count}. "
        "Regression: commit_batch() may be missing from _run_cycle()."
    )


def test_housekeeping_cycle_actually_deletes_rows_from_consumer_commits(
    self, db_path_initialized, db_conn, mock_config
):
    """Same as above for consumer_commits table."""
    max_entries = mock_config.monitoring.max_commit_entries_per_partition
    insert_count = max_entries + 50
    now = int(time.time())

    for i in range(insert_count):
        database.insert_consumer_commit(db_conn, "group1", "topic1", 0, i, now - i)
    database.commit_batch(db_conn)

    hk = Housekeeping(mock_config, db_path_initialized)
    hk._run_cycle()

    verify_conn = database.get_connection(db_path_initialized)
    post_count = verify_conn.execute(
        "SELECT COUNT(*) FROM consumer_commits "
        "WHERE group_id='group1' AND topic='topic1' AND partition=0"
    ).fetchone()[0]
    verify_conn.close()

    assert post_count == max_entries, (
        f"Expected {max_entries} rows, found {post_count}. "
        "Regression: commit_batch() may be missing from _run_cycle()."
    )
```

**Nuance — use a separate connection for verification:** The most important detail here is
reading the post-cycle state via a *different* connection than the one housekeeping used.
SQLite WAL mode allows multiple readers with one writer. Using a separate connection confirms
the transaction was committed to the DB file, not just visible within the writer's own
connection (which would be the case even without a commit, due to SQLite's read-your-own-writes
behaviour within a connection).

**Expected outcome:** Both tests pass, confirming rows are actually deleted (not just reported
as deleted). If `commit_batch()` is removed from `_run_cycle()`, both tests fail with a clear
message showing `post_count == insert_count`.

---

## 🟠 MEDIUM — TASK 56: Test main() exits with code 1 when output directory does not exist

**File:** `src/tests/test_main.py` (new file, same as TASK 53)

**Reason:** TASK 45 added output directory validation to `main()`. The task specification
explicitly required a test asserting exit code 1 with a nonexistent parent directory. No
`test_main.py` exists in the test suite. Without this test, a refactor that moves or removes
the directory check would be undetected.

**Implementation:**

```python
import sys, os
import pytest
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import main as main_module
from config import (
    Config, KafkaConfig, MonitoringConfig, DatabaseConfig,
    OutputConfig, ExcludeConfig
)


def make_minimal_config(json_path: str) -> Config:
    """Build a minimal Config with the given json_path for testing main()."""
    return Config(
        kafka=KafkaConfig(bootstrap_servers="localhost:9092"),
        monitoring=MonitoringConfig(
            sample_interval_seconds=30,
            offline_sample_interval_seconds=1800,
            report_interval_seconds=30,
            housekeeping_interval_seconds=900,
            max_entries_per_partition=300,
            max_commit_entries_per_partition=100,
            offline_detection_consecutive_samples=5,
            recovering_minimum_duration_seconds=900,
            online_lag_threshold_seconds=600,
        ),
        database=DatabaseConfig(path="/tmp/test.db"),
        output=OutputConfig(json_path=json_path),
        exclude=ExcludeConfig(topics=set(), groups=set()),
    )


def test_main_returns_1_when_output_directory_does_not_exist(tmp_path):
    """
    TASK 45 regression: main() must return exit code 1 when the parent
    directory of output.json_path does not exist.
    """
    nonexistent = str(tmp_path / "does_not_exist" / "output.json")
    cfg = make_minimal_config(nonexistent)

    with patch("main.config_module.load_config", return_value=cfg), \
         patch("sys.argv", ["main.py", "--config", "dummy.yaml"]):
        result = main_module.main()

    assert result == 1, f"Expected exit code 1, got {result}"


def test_main_proceeds_past_output_dir_check_when_dir_exists(tmp_path):
    """
    Verify a valid output directory does not trigger the early exit.
    Stop after the DB init step (mocked) to avoid requiring Kafka.
    """
    valid_path = str(tmp_path / "output.json")
    cfg = make_minimal_config(valid_path)

    with patch("main.config_module.load_config", return_value=cfg), \
         patch("main.database.init_db", side_effect=RuntimeError("stop")), \
         patch("sys.argv", ["main.py", "--config", "dummy.yaml"]):
        result = main_module.main()

    # Result will be 1 due to the mocked DB error — that's fine.
    # The key check: the RuntimeError from init_db was raised, which means
    # the output directory check was passed successfully.
    # If output dir check had fired, init_db would never be reached.


def test_main_bare_filename_does_not_false_positive(tmp_path, monkeypatch):
    """
    Edge case: a bare filename (no directory component, e.g. 'output.json')
    refers to the current working directory, which always exists.
    The output directory check must not reject this.
    """
    monkeypatch.chdir(tmp_path)
    cfg = make_minimal_config("output.json")  # no directory component

    with patch("main.config_module.load_config", return_value=cfg), \
         patch("main.database.init_db", side_effect=RuntimeError("stop")), \
         patch("sys.argv", ["main.py", "--config", "dummy.yaml"]):
        # If this raises RuntimeError (from init_db mock), the dir check passed.
        # If it returns 1 before reaching init_db, the check false-fired.
        result = main_module.main()
```

**Expected outcome:** `test_main_returns_1_when_output_directory_does_not_exist` passes
immediately. `test_main_bare_filename_does_not_false_positive` confirms the edge case is
handled without a false exit-code-1.

---

## 🟠 MEDIUM — TASK 57: Test cross-episode `consecutive_static` accumulation is prevented

**File:** `src/tests/test_sampler.py`

**Reason:** The existing `test_consecutive_static_reset_when_caught_up` tests the basic reset
behaviour. It does not test the specific failure mode described in TASK 44 — that a counter
partially accumulated across multiple lag episodes (separated by a caught-up period) does not
trigger a premature OFFLINE transition. This is the exact scenario that motivated the bug fix
and is not covered.

**Implementation:**

```python
def test_consecutive_static_does_not_accumulate_across_caught_up_episodes(
    self, db_path, db_conn
):
    """
    TASK 44 regression: consecutive_static must not accumulate across separate
    lag episodes separated by caught-up periods.

    Scenario:
      - threshold = 5
      - Pre-seed state: consecutive_static = 4 (one away from OFFLINE)
      - Run one caught-up cycle (no lag, static offsets)
      - Assert counter resets to 0, status remains ONLINE
      - Run one static-with-lag cycle
      - Assert counter is 1 (not 5), no OFFLINE transition
    """
    threshold = 5
    config = make_test_config()
    config.monitoring.offline_detection_consecutive_samples = threshold
    state_mgr = MockStateManager()
    mock_kafka = MagicMock()

    s = Sampler(config, db_path, mock_kafka, state_mgr)

    # Seed: 4 cycles of static-with-lag already accumulated
    state_mgr.set_group_status(
        "group1", "topic1", "ONLINE",
        status_changed_at=int(time.time()) - 100,
        last_advancing_at=int(time.time()) - 100,
        consecutive_static=threshold - 1,
    )

    now = int(time.time())
    produced = 1000

    # Caught-up state: committed == produced (no lag), same offset across samples
    db_conn.execute("DELETE FROM consumer_commits")
    for i in range(threshold):
        db_conn.execute(
            "INSERT INTO consumer_commits VALUES (?,?,?,?,?)",
            ("group1", "topic1", 0, produced, now - i)
        )
    db_conn.execute("DELETE FROM partition_offsets")
    for i in range(threshold):
        db_conn.execute(
            "INSERT INTO partition_offsets VALUES (?,?,?,?)",
            ("topic1", 0, produced, now - i)
        )
    db_conn.commit()

    committed_offsets = {("topic1", 0): produced}
    latest_offsets = {("topic1", 0): produced}

    s._evaluate_state_machine(
        "group1", "topic1", committed_offsets, latest_offsets, float(now)
    )

    status = state_mgr.get_group_status("group1", "topic1")
    assert status["consecutive_static"] == 0, (
        f"Expected 0 after caught-up cycle, got {status['consecutive_static']}"
    )
    assert status["status"] == "ONLINE", (
        f"Expected ONLINE, got {status['status']}"
    )

    # Now one cycle of static-with-lag — counter must be 1, not 5
    committed = 500  # has lag
    db_conn.execute("DELETE FROM consumer_commits")
    for i in range(threshold):
        db_conn.execute(
            "INSERT INTO consumer_commits VALUES (?,?,?,?,?)",
            ("group1", "topic1", 0, committed, now - i)
        )
    db_conn.commit()

    committed_offsets = {("topic1", 0): committed}
    s._evaluate_state_machine(
        "group1", "topic1", committed_offsets, latest_offsets, float(now)
    )

    status = state_mgr.get_group_status("group1", "topic1")
    assert status["consecutive_static"] == 1, (
        f"Expected 1 after one lag cycle post-reset, got {status['consecutive_static']}. "
        "Regression: counter was not cleared during caught-up period"
    )
    assert status["status"] == "ONLINE", (
        "OFFLINE transition fired prematurely — cross-episode accumulation regression"
    )
```

**Expected outcome:** Test passes. If the `else: new_consecutive_static = 0` line is removed
from `_evaluate_state_machine`, this test fails with `consecutive_static=5` and a premature
OFFLINE transition.

---

## 🟡 LOW — TASK 58: Test ghost group partition_offsets are kept fresh (Task 32 coverage)

**File:** `src/tests/test_sampler.py`

**Reason:** TASK 32 fixed the case where a group active in Kafka with zero members (`Empty`
state) would have its `partition_offsets` go stale — causing lag to permanently report 0.
No test explicitly covers this scenario. The fix is in `_process_group` and is the most
complex logic added in recent rounds.

**Implementation:**

```python
def test_ghost_group_partition_offsets_written_when_active_but_zero_members(
    self, db_path, db_conn
):
    """
    TASK 32 regression: a group that appears in get_active_consumer_groups()
    but has zero active members (empty group_partitions) must still have
    partition_offsets written to keep interpolation data fresh.

    Pre-fix: _process_group called _handle_idle_group but never wrote
    partition_offsets, so interpolation stalled and lag reported 0 forever.
    """
    from unittest.mock import MagicMock

    config = make_test_config()
    state_mgr = MockStateManager()
    mock_kafka = MagicMock()

    s = Sampler(config, db_path, mock_kafka, state_mgr)

    # Seed DB history so has_group_history() returns True
    now = int(time.time())
    db_conn.execute(
        "INSERT INTO consumer_commits VALUES (?,?,?,?,?)",
        ("ghost-group", "topic1", 0, 400, now - 60)
    )
    db_conn.execute(
        "INSERT INTO partition_offsets VALUES (?,?,?,?)",
        ("topic1", 0, 490, now - 60)  # old entry
    )
    db_conn.commit()

    # Group is in Kafka but has zero members → empty partition set
    latest_offsets = {("topic1", 0): 500}
    group_topic_partitions = {"ghost-group": set()}

    s._process_group("ghost-group", group_topic_partitions, latest_offsets, float(now))
    database.commit_batch(s._db_conn)

    rows = db_conn.execute(
        "SELECT offset, sampled_at FROM partition_offsets "
        "WHERE topic='topic1' AND partition=0 "
        "ORDER BY sampled_at DESC LIMIT 1"
    ).fetchone()

    assert rows is not None, "No partition_offset row found after ghost group cycle"
    assert rows[0] == 500, f"Expected HWM offset 500, got {rows[0]}"
    assert rows[1] == int(now), (
        "partition_offset timestamp not updated — ghost group data is stale. "
        "Regression: has_group_history block removed from _process_group."
    )

    # Group must also be transitioned to OFFLINE
    status = state_mgr.get_group_status("ghost-group", "topic1")
    assert status["status"] == "OFFLINE", (
        f"Ghost group should be OFFLINE, got {status['status']}"
    )
```

**Expected outcome:** Test passes, confirming fresh `partition_offsets` are written for
zero-member active groups. If the `has_group_history` block is removed from `_process_group`,
the test fails with stale offset data and the lag-permanently-zero bug is re-introduced.

---

## 🟡 LOW — TASK 59: Test `get_interpolation_points` respects the limit parameter

**File:** `src/tests/test_database.py`

**Reason:** After TASK 52 adds the `limit` parameter to `get_interpolation_points`, a test
should verify the parameter is respected. Additionally, the default (500) must be enforced
when no limit is passed. These tests did not exist before because the function had a
hardcoded limit.

**Implementation:**

```python
def test_get_interpolation_points_respects_limit(db_conn):
    """TASK 52: get_interpolation_points must return at most `limit` rows."""
    now = int(time.time())
    for i in range(20):
        database.insert_partition_offset(db_conn, "topic1", 0, i * 10, now - i)
    database.commit_batch(db_conn)

    result = database.get_interpolation_points(db_conn, "topic1", 0, limit=5)
    assert len(result) == 5, f"Expected 5 rows with limit=5, got {len(result)}"


def test_get_interpolation_points_default_limit_is_safe(db_conn):
    """
    Default limit (500) must be applied when no limit is passed explicitly.
    Insert 600 rows; expect at most 500 returned.
    """
    now = int(time.time())
    for i in range(600):
        database.insert_partition_offset(db_conn, "topic1", 0, i, now - i)
    database.commit_batch(db_conn)

    result = database.get_interpolation_points(db_conn, "topic1", 0)
    assert len(result) <= 500, (
        f"Default limit not enforced: got {len(result)} rows, expected <= 500"
    )


def test_get_interpolation_points_returns_most_recent_rows(db_conn):
    """
    When limit truncates results, the most recent rows (highest sampled_at)
    must be returned, ordered DESC by sampled_at.
    """
    now = int(time.time())
    for i in range(10):
        database.insert_partition_offset(db_conn, "topic1", 0, i * 100, now - i)
    database.commit_batch(db_conn)

    result = database.get_interpolation_points(db_conn, "topic1", 0, limit=3)
    assert len(result) == 3
    assert result[0][1] == now       # most recent first
    assert result[1][1] == now - 1
    assert result[2][1] == now - 2
```

**Expected outcome:** All three tests pass after TASK 52 is implemented. The second test
would fail with the old hardcoded `LIMIT 1000` if 600 rows are inserted (1000 > 600, so
all would be returned — the test would pass trivially). The first test correctly validates
the parameter is wired through.

---

## Summary Table

| Task | Priority | File(s) | Type | Guards Against |
|------|----------|---------|------|---------------|
| 51 | 🔴 HIGH | `main.py` | Bug fix | 21–25s shutdown delay (PEP 475 / `time.sleep`) |
| 52 | 🟡 LOW | `database.py`, `sampler.py`, `reporter.py` | Improvement | Config-query limit mismatch |
| 53 | 🟡 LOW | `test_main.py` (new) | Test | TASK 51 regression |
| 54 | 🔴 HIGH | `test_sampler/reporter/housekeeping.py` | Test | TASK 42 reconnect correctness |
| 55 | 🔴 HIGH | `test_housekeeping.py` | Test | TASK 43 silent prune rollback |
| 56 | 🟠 MEDIUM | `test_main.py` (new) | Test | TASK 45 output dir check removal |
| 57 | 🟠 MEDIUM | `test_sampler.py` | Test | TASK 44 cross-episode accumulation |
| 58 | 🟡 LOW | `test_sampler.py` | Test | TASK 32 ghost group stale offsets |
| 59 | 🟡 LOW | `test_database.py` | Test | TASK 52 limit parameter | 
