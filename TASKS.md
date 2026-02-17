# Kafka Consumer Lag Monitor — Architecture, Design & Implementation Plan

---

## Project Overview

This software is a self-contained Python daemon that monitors Kafka consumer group lag across multiple topics and consumer groups. Its primary purpose is to answer the question: **"How old is the data currently being processed by each consumer group?"** — expressed as a time value in seconds, not a raw offset count.

The daemon runs continuously on a Linux host, periodically sampling Kafka broker state, storing historical offset snapshots in a local SQLite database, and emitting a JSON file that downstream tooling (dashboards, alerting systems, etc.) can consume.

### Why Time-Based Lag, Not Offset-Based Lag

Raw offset lag (how many messages behind a consumer is) is a poor operational metric in isolation. A lag of 50,000 offsets is catastrophic on a high-throughput topic and completely irrelevant on a nearly-idle one. Time-based lag answers the question that actually matters operationally: **how stale is this consumer's view of the world right now?**

### Core Technique: Offset Interpolation

The system maintains a per-partition table of historical snapshots: at a given point in time, the partition's head was at a given offset. When the system needs to calculate the time lag for a consumer, it looks up the consumer's last committed offset in this table. If an exact match exists, it returns the timestamp for that entry. If the committed offset falls between two entries, it applies linear interpolation to estimate the timestamp. The lag is then `current_time - estimated_timestamp`.

### Consumer Group State Machine

Consumer groups are not always active. To handle groups that go offline (and may stay offline for days), the system implements a three-state machine per group/topic:

- **ONLINE** — offset is advancing and lag is below threshold. Fine-grained sampling (every 60 seconds).
- **OFFLINE** — offset has been static for N consecutive samples. Coarse sampling (every 30 minutes).
- **RECOVERING** — offset is advancing again after being offline, but lag is still above the online threshold. Coarse sampling maintained until lag drops sufficiently.

The write cadence change is the key design insight: rather than writing at full rate and performing complex downsampling during housekeeping, the system writes less frequently for groups in non-ONLINE states. Housekeeping is therefore a simple count-based operation — keep the last N rows per partition — and the coarser resolution for offline/recovering groups emerges naturally from the reduced write frequency.

### Technology Choices

- **Language:** Python 3.9+
- **Kafka client:** `confluent-kafka` (AdminClient API — no consumer group membership required, purely observational)
- **Database:** SQLite with WAL mode (single file, no server, concurrent thread-safe reads/writes)
- **Testing:** `pytest`
- **Configuration:** YAML via `pyyaml`
- **No other external dependencies**

---

## Repository Structure

```
kafka-lag-monitor/
|    docs/
|    ├── 00-summary.md
|    ├── 01-configuration-spec.md
|    ├── 02-database-design.md
|    ├── 03-kafka-client.md
|    ├── 04-state-manager.md
|    ├── 05-interpolation-engine.md
|    ├── 06-state-manager.py
|    ├── 07-reporter.md
|    ├── 08-house-keeping.md
|    └── 999-project-design-everything-kitchen-sink.md
└─── src/
     ├── config.yaml                  # User configuration
     ├── requirements.txt             # confluent-kafka, pyyaml, pytest
     ├── main.py                      # Entry point, thread lifecycle, signal handling
     ├── config.py                    # Config loading and validation
     ├── database.py                  # All SQLite interaction
     ├── kafka_client.py              # All confluent-kafka interaction
     ├── state_manager.py             # Shared in-memory state, thread-safe
     ├── interpolation.py             # Pure calculation functions, no I/O
     ├── sampler.py                   # Sampler thread
     ├── reporter.py                  # Reporter thread
     ├── housekeeping.py              # Housekeeping thread
     └── tests/
         ├── conftest.py              # Shared pytest fixtures
         ├── test_config.py
         ├── test_database.py
         ├── test_interpolation.py
         ├── test_state_manager.py
         ├── test_sampler.py
         ├── test_reporter.py
         ├── test_housekeeping.py
         └── test_integration.py      # Real Kafka — run separately
```
# Implementation Task List

> **Note on Testing:** All modules are built first in their entirety before any tests are written or run. Tests are written as a batch in a dedicated phase using `pytest`. Integration tests requiring a real Kafka server are isolated in `tests/test_integration.py` and are run as the final phase against a live cluster.

---

# Task 1 — Project Scaffold and Dependencies

**Goal / Why:**
Establish the repository structure, dependency management, and verify the Python environment is correctly configured before any implementation begins. This task has no logic — it is purely structural.

**Expected Outcome / Acceptance Criteria:**
- Repository directory structure matches the layout defined in this document exactly, with all module files created as empty stubs (containing only a module-level docstring)
- `requirements.txt` contains `confluent-kafka`, `pyyaml`, and `pytest`
- `tests/` directory exists with empty `conftest.py` and empty stub test files for each module
- A sample `config.yaml` is present and matches the schema defined in this document
- All empty stub files are importable without error (`python -c "import module_name"` succeeds for each)

**Status:** TODO

---

# Task 2 — Implement `config.py`

**Goal / Why:**
All modules receive a validated `Config` object rather than reading files or environment variables themselves. This module is the single point of configuration loading and must fail loudly and clearly if anything is wrong, before any threads start.

**Expected Outcome / Acceptance Criteria:**
- A `Config` dataclass is defined with typed fields matching every key in the `config.yaml` schema
- A `ConfigError` exception class is defined
- A `load_config(path: str) -> Config` function loads and parses the YAML file
- Missing required fields raise `ConfigError` with a message identifying the missing field by name
- Fields with incorrect types raise `ConfigError` with a descriptive message
- Optional fields (e.g. security_protocol) have sensible defaults applied if absent
- No other module reads `config.yaml` directly

**Status:** TODO

---

# Task 3 — Implement `database.py`

**Goal / Why:**
All SQLite interaction must be isolated in one module. This keeps SQL out of business logic and makes the data layer independently testable. All schema creation, all queries, and all maintenance operations live here.

**Expected Outcome / Acceptance Criteria:**
- `init_db(path)` creates all five tables if they do not exist, sets all three PRAGMAs, and returns a connection object
- All functions listed in the Database Design section are implemented with correct signatures
- `prune_partition_offsets` and `prune_consumer_commits` keep exactly `keep_n` rows and always retain the most recent rows (by timestamp), never the oldest
- `upsert_group_status` uses `INSERT OR REPLACE` semantics so it functions as both insert and update
- `run_incremental_vacuum` calls `PRAGMA incremental_vacuum(N)` with the given page count
- No function raises an unhandled exception — all database errors are propagated as standard Python exceptions for callers to handle
- The module has no imports from any other project module

**Status:** TODO

---

# Task 4 — Implement `interpolation.py`

**Goal / Why:**
The interpolation engine is the mathematical core of the system. It must be implemented as pure functions with no side effects so it can be tested exhaustively without any infrastructure. All lag calculation logic lives here and nowhere else.

**Expected Outcome / Acceptance Criteria:**
- All four functions defined in the Interpolation Engine section are implemented with correct signatures
- `calculate_lag_seconds` handles all five cases: `no_data`, `current`, `exact`, `interpolated`, `extrapolated`
- Duplicate offset entries (same offset, multiple timestamps) are handled by using the latest timestamp, producing the most conservative (largest) lag estimate
- `interpolate_timestamp` handles the division-by-zero case (identical offsets) by returning the later timestamp
- The worked example produces the correct result: `interpolate_timestamp(250, (240, 1597304361), (260, 1597304421))` returns `1597304391`
- `format_lag_display` returns `"< 1 minute"` for any value under 60 seconds including 0, and produces grammatically correct singular/plural output (e.g. `"1 minute"` not `"1 minutes"`)
- The module has no imports from any other project module and no I/O of any kind

**Status:** TODO

---

# Task 5 — Implement `kafka_client.py`

**Goal / Why:**
All `confluent-kafka` usage must be isolated here so the rest of the system has no direct dependency on the Kafka library. This also means all Kafka error handling is centralised and all other modules are shielded from Kafka exceptions.

**Expected Outcome / Acceptance Criteria:**
- All functions listed in the Kafka Client Abstraction section are implemented
- `build_admin_client(config)` constructs an `AdminClient` from the bootstrap servers and security protocol in the config
- Every function catches all exceptions, logs a warning with contextual information (which function, which group/topic if applicable), and returns the appropriate empty value
- No function raises any exception to its caller under any circumstances
- No other project module imports from `confluent-kafka`
- The module accepts a `Config` object and an `AdminClient` object as arguments — it does not construct its own config or client internally (except `build_admin_client`)

**Status:** TODO

---

# Task 6 — Implement `state_manager.py`

**Goal / Why:**
Threads must not share mutable state without synchronisation. The state manager is the single controlled access point for all inter-thread shared data. It owns the lock and is the only module that both reads and writes the in-memory state dict.

**Expected Outcome / Acceptance Criteria:**
- `StateManager.__init__(db_conn, config)` loads all persisted group statuses from the database into memory on construction
- All public methods acquire the `RLock` before accessing the state dict and release it on exit (use context manager)
- `set_group_status` both updates the in-memory dict and calls `database.upsert_group_status` within the same lock acquisition
- `get_group_status` returns a copy of the status dict (not a reference to the internal dict) so callers cannot accidentally mutate shared state
- `get_all_group_statuses` similarly returns a deep copy
- If a group/topic combination has no recorded status, `get_group_status` returns a default ONLINE state dict rather than None

**Status:** TODO

---

# Task 7 — Implement `sampler.py`

**Goal / Why:**
The sampler is the data collection engine. It drives all writes to `partition_offsets` and `consumer_commits`, and is responsible for evaluating and transitioning group state. Its write throttling behaviour for non-ONLINE groups is what makes the rest of the system's simplicity possible.

**Expected Outcome / Acceptance Criteria:**
- `Sampler.__init__(config, db_conn, kafka_client, state_manager)` stores all dependencies
- `Sampler.run(shutdown_event)` implements the cycle logic defined in the Sampler Thread section
- The sampler respects the configured write cadence: for ONLINE groups, writes to `partition_offsets` no more than once per `sample_interval_seconds`; for OFFLINE/RECOVERING groups, no more than once per `offline_sample_interval_seconds`
- The sampler always writes to `consumer_commits` regardless of group status and regardless of write cadence — this table is not throttled
- Duplicate `partition_offsets` writes are avoided: a new row is only written if the offset has changed since the last entry, OR the full cadence interval has elapsed (to maintain the timestamp trail even during quiet periods)
- All five state machine transitions are implemented: ONLINE→OFFLINE, OFFLINE→RECOVERING, RECOVERING→OFFLINE, RECOVERING→ONLINE, and the ONLINE→ONLINE (no transition, counter increment) path
- All transitions are logged at INFO level with the group_id, topic, old status, and new status
- The thread checks `shutdown_event.is_set()` at the top of every cycle and exits cleanly
- Exceptions from kafka_client calls do not crash the sampler — the cycle is skipped and the thread sleeps normally

**Status:** TODO

---

# Task 8 — Implement `reporter.py`

**Goal / Why:**
The reporter transforms raw database state into the final consumer-facing JSON output. It must produce a valid, complete JSON file on every cycle, and must never produce a partial or corrupt file that a downstream reader could observe.

**Expected Outcome / Acceptance Criteria:**
- `Reporter.__init__(config, db_conn, state_manager)` stores all dependencies
- `Reporter.run(shutdown_event)` implements the cycle logic defined in the Reporter Thread section
- The output JSON exactly matches the schema defined in the Output JSON Schema section
- `lag_seconds` is always an integer — never null, never a float
- `worst_partition` is null when lag is zero
- `status` values are lowercase strings
- The JSON file is written atomically: written to `{path}.tmp` first, then `os.replace()` to the final path
- If an error occurs during calculation for a specific group/topic, that entry is omitted from the output and the error is logged — the rest of the output is still written
- The thread checks `shutdown_event.is_set()` at the top of every cycle and exits cleanly

**Status:** TODO

---

# Task 9 — Implement `housekeeping.py`

**Goal / Why:**
Without periodic pruning the database grows unboundedly. Housekeeping enforces count-based row limits and reclaims disk space. It is deliberately simple — it knows nothing about group status or data tiers, because the sampler's write throttling design makes that complexity unnecessary.

**Expected Outcome / Acceptance Criteria:**
- `Housekeeping.__init__(config, db_conn)` stores all dependencies
- `Housekeeping.run(shutdown_event)` implements the cycle logic defined in the Housekeeping Thread section
- After each pruning call, the target partition has at most `max_entries_per_partition` rows in `partition_offsets`
- After each pruning call, the target group/partition has at most `max_commit_entries_per_partition` rows in `consumer_commits`
- Pruning always retains the most recent rows — oldest rows are deleted
- `run_incremental_vacuum` is called every cycle with `pages=100`
- The cycle logs a summary line: how many rows were pruned from each table and how long the cycle took
- The thread checks `shutdown_event.is_set()` at the top of every cycle and exits cleanly
- Any database error during housekeeping is logged but does not crash the thread

**Status:** TODO

---

# Task 10 — Implement `main.py`

**Goal / Why:**
The entry point owns the lifecycle of all other components. It is responsible for startup validation, thread management, signal handling, and clean shutdown. It is the only module that is allowed to start threads or handle OS signals.

**Expected Outcome / Acceptance Criteria:**
- Accepts `--config <path>` as a required CLI argument
- Calls `load_config()` first — exits immediately with a clear error message if config is invalid
- Calls `init_db()` and constructs all module instances before starting any threads
- Verifies Kafka connectivity at startup by calling `get_active_consumer_groups()` — if it returns an error, retries every 10 seconds up to a configurable timeout (default: 120 seconds), then exits with a clear error message
- All three worker threads (sampler, reporter, housekeeping) are started as daemon threads, each running inside a restart wrapper
- Restart wrapper: catches any unhandled exception from the thread's `run()` method, logs the full traceback, waits 30 seconds (checking shutdown_event during the wait), then calls `run()` again. Does not restart if `shutdown_event` is set.
- `SIGTERM` and `SIGINT` both set the `shutdown_event`
- Main thread logs a heartbeat at DEBUG level every 30 seconds containing the last-run timestamps of all three worker threads
- On shutdown: joins all threads with a 10-second timeout each, closes the DB connection, exits 0
- If a thread fails to join within the timeout, logs a warning and exits anyway

**Status:** TODO

---

# Task 11 — Write Unit Tests: `test_config.py`

**Goal / Why:**
Config loading is the first thing that runs and must fail clearly. Tests verify both the happy path and all defined failure modes.

**Expected Outcome / Acceptance Criteria:**
- Tests written using `pytest`
- Test: valid config file loads and all fields are accessible with correct types
- Test: missing required field raises `ConfigError` with the field name in the message
- Test: incorrect type for a field raises `ConfigError`
- Test: missing optional fields use their defined defaults
- Tests use `tmp_path` pytest fixture to create temporary config files — no hardcoded paths
- All tests pass with `pytest tests/test_config.py`

**Status:** TODO

---

# Task 12 — Write Unit Tests: `test_database.py`

**Goal / Why:**
Database functions must be verified against real SQLite behaviour. Tests use in-memory SQLite databases so they are fast and require no filesystem setup.

**Expected Outcome / Acceptance Criteria:**
- Tests written using `pytest`
- `conftest.py` provides a `db_conn` fixture that returns an in-memory SQLite connection initialised with `init_db(":memory:")`
- Test: all tables exist after `init_db()`
- Test: `PRAGMA journal_mode` returns `"wal"` after init
- Test: insert and retrieve `partition_offsets` rows — verify ordering
- Test: insert and retrieve `consumer_commits` rows
- Test: `upsert_group_status` inserts a new row correctly
- Test: `upsert_group_status` updates an existing row correctly (no duplicate rows)
- Test: `prune_partition_offsets` with more than `keep_n` rows — verify exactly `keep_n` remain and they are the most recent
- Test: `prune_partition_offsets` with fewer than `keep_n` rows — verify no rows are deleted
- Test: `get_interpolation_points` returns rows in descending timestamp order
- Test: exclusion checks return correct boolean for both config-excluded and table-excluded entries
- All tests pass with `pytest tests/test_database.py`

**Status:** TODO

---

# Task 13 — Write Unit Tests: `test_interpolation.py`

**Goal / Why:**
The interpolation engine is pure logic and must be tested exhaustively. Every edge case must be explicitly verified since incorrect lag calculations would silently produce wrong operational data.

**Expected Outcome / Acceptance Criteria:**
- Tests written using `pytest`
- Test: empty interpolation_points → returns `(0, "no_data")`
- Test: committed_offset equals newest offset in table → returns `(0, "current")`
- Test: committed_offset greater than newest offset → returns `(0, "current")`
- Test: exact match, single entry → returns correct lag
- Test: exact match, duplicate offsets at different timestamps → uses latest timestamp (larger lag)
- Test: interpolated case → worked example: `committed=250`, `lower=(240, 1597304361)`, `upper=(260, 1597304421)` → `lag = current_time - 1597304391`
- Test: extrapolated case (committed_offset below all table entries) → returns `(current_time - oldest_timestamp, "extrapolated")`
- Test: `interpolate_timestamp` division-by-zero (identical offsets) → returns later timestamp
- Test: `aggregate_partition_lags` returns max lag and correct worst partition
- Test: `aggregate_partition_lags` all zeros → returns `(0, None, "current")`
- Test: `format_lag_display` for 0 → `"< 1 minute"`
- Test: `format_lag_display` for 59 → `"< 1 minute"`
- Test: `format_lag_display` for 60 → `"1 minute"`
- Test: `format_lag_display` for 120 → `"2 minutes"`
- Test: `format_lag_display` for 3661 → `"1 hour 1 minute"`
- Test: `format_lag_display` for 90000 → `"1 day 1 hour"`
- All tests pass with `pytest tests/test_interpolation.py`

**Status:** TODO

---

# Task 14 — Write Unit Tests: `test_state_manager.py`

**Goal / Why:**
Thread safety is critical and must be verified. The state manager's contract — that it always returns copies, persists on write, and loads correctly from the database — must be explicitly tested.

**Expected Outcome / Acceptance Criteria:**
- Tests written using `pytest`
- Uses `db_conn` fixture from `conftest.py`
- Test: `StateManager` loads pre-existing group statuses from the database on construction
- Test: `get_group_status` for unknown group returns default ONLINE state dict
- Test: `set_group_status` updates in-memory state
- Test: `set_group_status` persists to database — verify by calling `database.get_group_status()` directly
- Test: `get_group_status` returns a copy — mutating the returned dict does not affect the internal state
- Test: `get_all_group_statuses` returns a copy — same contract
- Test: concurrent access — use `threading.Thread` to run multiple simultaneous reads and writes, verify no data corruption and no deadlock (run for at least 1 second with 10 threads)
- All tests pass with `pytest tests/test_state_manager.py`

**Status:** TODO

---

# Task 15 — Write Unit Tests: `test_sampler.py`

**Goal / Why:**
The sampler's write cadence and state machine logic must be verified without requiring a real Kafka server. All Kafka and database calls are mocked.

**Expected Outcome / Acceptance Criteria:**
- Tests written using `pytest` with `unittest.mock.patch` or `pytest-mock`
- Test: ONLINE group — write is issued when cadence interval has elapsed
- Test: ONLINE group — write is skipped when cadence interval has not elapsed
- Test: OFFLINE group — write is skipped at 60s interval, issued at 1800s interval
- Test: consumer_commits is always written regardless of group status or cadence
- Test: state machine ONLINE→OFFLINE transition — mock N consecutive static offset samples, verify transition occurs and is persisted
- Test: state machine OFFLINE→RECOVERING — mock advancing offsets after static period
- Test: state machine RECOVERING→ONLINE — mock lag below threshold with minimum duration elapsed
- Test: state machine RECOVERING→OFFLINE — mock offset going static again mid-recovery
- Test: Kafka call failure (mock returns empty dict) — sampler cycle completes without exception
- All tests pass with `pytest tests/test_sampler.py`

**Status:** TODO

---

# Task 16 — Write Unit Tests: `test_reporter.py`

**Goal / Why:**
The reporter's output format and calculation pipeline must be verified. The atomic write behaviour is particularly important to test.

**Expected Outcome / Acceptance Criteria:**
- Tests written using `pytest`
- Uses `db_conn` fixture from `conftest.py` and `tmp_path` for output file
- Test: insert known `partition_offsets` and `consumer_commits` rows, run reporter cycle, load the output JSON file, verify lag values match expected calculations
- Test: output JSON contains all required fields with correct types
- Test: `lag_seconds` is 0 (not null) for a current consumer
- Test: `worst_partition` is null when lag is 0
- Test: `status` is lowercase string
- Test: atomic write — output file is always a valid complete JSON file even if inspected mid-write (verify by checking the tmp file is cleaned up and the final file is valid)
- Test: calculation error for one group does not prevent output of other groups
- All tests pass with `pytest tests/test_reporter.py`

**Status:** TODO

---

# Task 17 — Write Unit Tests: `test_housekeeping.py`

**Goal / Why:**
Housekeeping must reliably enforce row limits and must never accidentally delete the wrong rows.

**Expected Outcome / Acceptance Criteria:**
- Tests written using `pytest`
- Uses `db_conn` fixture from `conftest.py`
- Test: insert 500 `partition_offsets` rows for a topic/partition, run housekeeping, verify exactly 300 remain
- Test: verify the 300 retained rows are the 300 most recent by timestamp
- Test: insert fewer than 300 rows, run housekeeping, verify all rows are retained (no over-deletion)
- Test: same tests for `consumer_commits` with `max_commit_entries_per_partition`
- Test: two different topic/partition combinations — verify pruning of one does not affect the other
- Test: `run_incremental_vacuum` completes without error on an in-memory database
- All tests pass with `pytest tests/test_housekeeping.py`

**Status:** TODO

---

# Task 18 — Full Unit Test Suite Verification

**Goal / Why:**
Verify the entire unit test suite passes cleanly as a whole before proceeding to integration testing. This is the gate between the unit-tested codebase and the live Kafka environment.

**Expected Outcome / Acceptance Criteria:**
- `pytest tests/ --ignore=tests/test_integration.py` runs with zero failures and zero errors
- No test emits warnings about deprecated usage
- Test coverage (if `pytest-cov` is available) shows >80% coverage across all non-integration modules
- All tests complete in under 30 seconds

**Status:** TODO

---

# Task 19 — Integration Test: Basic Connectivity and Sampling

**Goal / Why:**
Verify the system can connect to a real Kafka cluster, discover consumer groups and topics, and write correct data to the SQLite database. This is the first live test and validates the kafka_client and sampler against real Kafka behaviour.

**Prerequisites:** A running Kafka cluster with at least one active consumer group consuming at least one topic with multiple partitions. Connection details configured in a test `config.yaml`.

**Expected Outcome / Acceptance Criteria:**
- Daemon starts cleanly and logs successful Kafka connectivity
- After one sampler cycle, `partition_offsets` table contains rows for the expected topic/partition combinations
- After one sampler cycle, `consumer_commits` table contains rows for the active consumer groups
- `group_status` table contains entries for all monitored groups with status ONLINE
- No errors in logs during the cycle

**Status:** TODO

---

# Task 20 — Integration Test: JSON Output Validation

**Goal / Why:**
Verify the reporter produces correct, valid JSON output against real Kafka data.

**Prerequisites:** Task 19 completed. Daemon has been running for at least 2 full sampler cycles.

**Expected Outcome / Acceptance Criteria:**
- JSON output file exists at the configured path
- File is valid JSON (parse without error)
- All active consumer groups appear in the `consumers` array
- All excluded topics and groups are absent from the output
- `lag_seconds` values are plausible (not negative, not unreasonably large for groups known to be current)
- `status` is `"online"` for all known-active groups
- File is updated on each reporter cycle

**Status:** TODO

---

# Task 21 — Integration Test: Offline Detection and State Transitions

**Goal / Why:**
Verify the state machine transitions work correctly against a real Kafka cluster by stopping and restarting a consumer group.

**Prerequisites:** Task 20 completed. Ability to stop and start a specific consumer group.

**Expected Outcome / Acceptance Criteria:**
- Stop a consumer group. After `offline_detection_consecutive_samples` sampler cycles, the group's status transitions to OFFLINE and is logged
- Verify `partition_offsets` write frequency drops to coarse cadence after OFFLINE transition (check row timestamps in the database)
- JSON output shows `"status": "offline"` and `"data_resolution": "coarse"` for the stopped group
- Restart the consumer group. Verify status transitions to RECOVERING
- Verify status transitions to ONLINE once lag drops below `online_lag_threshold_seconds` and `recovering_minimum_duration_seconds` has elapsed
- JSON output shows `"status": "recovering"` during recovery and `"status": "online"` after full recovery
- Verify `partition_offsets` write frequency returns to fine cadence after ONLINE transition

**Status:** TODO

---

# Task 22 — Integration Test: Housekeeping Under Load

**Goal / Why:**
Verify housekeeping correctly enforces row limits on a real database that has been accumulating data.

**Prerequisites:** Task 21 completed. Daemon has been running long enough to accumulate meaningful data.

**Expected Outcome / Acceptance Criteria:**
- Manually insert rows to push a topic/partition above `max_entries_per_partition`
- Wait for a housekeeping cycle to run (or trigger manually in a test)
- Verify row count is back at or below the limit
- Verify the most recent rows were retained (check timestamps)
- Database file size is stable or decreasing over time (incremental vacuum is working)
- No errors or warnings in logs during housekeeping

**Status:** TODO
