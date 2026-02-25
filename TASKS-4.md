---
# MISSION
You are a Sparse Priming Representation (SPR) decompressor. An SPR is a particular kind of use of language for advanced NLP, NLU, and NLG tasks, particularly useful for the latest generation Large Language Models (LLMs). Each task list below is written in SPR format. You must fully unpack your task before starting it!

# THEORY
LLMs are a kind of deep neural network. They have been demonstrated to embed knowledge, abilities, and concepts, ranging from reasoning to planning, and even to theory of mind. These are called latent abilities and latent content, collectively referred to as latent space. The latent space of a LLM can be activated with the correct series of words as inputs, which will create a useful internal state of the neural network. This is not unlike how the right shorthand cues can prime a human mind to think in a certain way. Like human minds, LLMs are associative, meaning you only need to use the correct associations to "prime" another model to think in the same way.

# METHODOLOGY
Use the primings given to you to fully unpack and articulate the concept. Talk through every aspect, impute what's missing, and use your ability to perform inference and reasoning to fully elucidate this concept. Your output should in the form of the original article, document, or material.

---

## Context: Open tasks from prior rounds

The following tasks from TASKS-3.md were not completed and remain open. Read their original descriptions in TASKS-3.md before starting. Do not re-implement anything already confirmed fixed by REVIEW-ROUND-3.md or REVIEW-ROUND-4.md.

| Task | File | Description |
|------|------|-------------|
| TASK 32 (residual) | `sampler.py` | Ghost group `partition_offsets` stale — active-in-Kafka but zero-member groups not covered by the idle path |
| TASK 37 | `sampler.py` | `_get_partitions_for_topic` redundant DB query in `_evaluate_state_machine` |
| TASK 38 | `main.py` | `init_db` return value discarded |
| TASK 40 | `config.py` | No range validation on monitoring integer fields |
| SASL NOTE | `kafka_client.py` | SASL/TLS credentials not forwarded; no startup warning |

---

## New Tasks — Round 4

---

## Implementation Order

Work in this order to minimise risk of interacting changes:

1. **TASK 48** — trivial, isolated, no interactions
2. **TASK 49** — isolated to `config.py`, no runtime interactions
3. **TASK 45** — isolated to `main.py` startup path
4. **TASK 43** — `database.py` + `housekeeping.py`, verify with DB state assertions in tests
5. **TASK 44** — `sampler.py` state machine, one line + new test
6. **TASK 42** — `sampler.py`, `reporter.py`, `housekeeping.py` exception handlers
7. **TASK 46** — type annotation cascade across `config.py` + `database.py` + tests
8. **TASK 37** (carry-forward) — `sampler.py` refactor, interacts with TASK 32 residual
9. **TASK 32** (residual carry-forward) — `sampler.py`, after TASK 37 if TASK 37 is done
10. **TASK 47** — `kafka_client.py` + `config.py` SASL support
11. **TASK 50** — `database.py` defensive LIMIT, lowest priority
---

### 🔴 HIGH — TASK 42: Broken DB connection persists across cycles after per-cycle exception

**Files:** `sampler.py`

**Bug:** `self._db_conn` created once in `Sampler.__init__`. The `try/except Exception` in `run()` is a per-cycle handler — it is inside the `while not shutdown_event.is_set()` loop. `run_with_restart` in `main.py` almost never fires; the per-cycle handler is the real error boundary. If a `sqlite3.DatabaseError` fires mid-cycle (disk full, WAL lock timeout, page corruption), the exception is caught, logged as WARNING, and the loop continues — on the same broken connection. SQLite connections do not self-heal after errors. The following cycle uses the same bad connection; all DB operations silently fail or raise again. The monitor appears alive but writes nothing and reads stale data indefinitely.

**Expected outcome:** On a DB exception, the broken connection is closed and replaced with a fresh one before the next cycle runs. The repair is in the `except` block, not at the top of `run()` — that would only help if `run_with_restart` restarted the function, which requires `run()` itself to raise, which the per-cycle handler prevents.

**Implementation:**

```python
except Exception as e:
    logger.warning(f"Error during sampler cycle: {e}")
    if isinstance(e, sqlite3.DatabaseError):
        logger.warning("DB error detected — reconnecting before next cycle")
        try:
            self._db_conn.close()
        except Exception:
            pass
        self._db_conn = database.get_connection(self._db_path)
```

Add `import sqlite3` at the top of `sampler.py` if not already present. Apply the same pattern to `Reporter` and `Housekeeping` — they each hold a `self._db_conn` created in `__init__` and have a per-cycle exception handler in their own `run()` loops. The fix is identical in structure for all three classes.

**Notes:** Do NOT move the reconnect logic to the top of `run()`. That is the wrong call site. The `while` loop never exits normally; `run()` only exits if `shutdown_event` is set or an exception escapes the per-cycle handler, neither of which is the common failure scenario. The reconnect must be in the `except` block so it fires on the cycle that fails. Import `sqlite3` in `sampler.py` for the `isinstance` check — it is already imported in `database.py` but not in `sampler.py`. Write tests: mock DB connection to raise `sqlite3.OperationalError` on first use, assert fresh connection is obtained, assert subsequent cycle succeeds.

---

### 🟠 MEDIUM — TASK 43: Housekeeping prune functions each commit individually — 670 commits per cycle instead of 1

**Files:** `database.py`, `housekeeping.py`

**Bug:** `prune_partition_offsets` and `prune_consumer_commits` each call `conn.commit()` after their `DELETE` statement. Housekeeping calls these in a loop across all (topic, partition) and (group, topic, partition) combinations. With 134 partition-offsets partitions and 536 consumer-commits combos at the target deployment scale, each housekeeping cycle issues 670 individual commits. Each SQLite commit in WAL mode appends WAL frames and may trigger a checkpoint. 670 commits is 670x the necessary write amplification for what is logically a single batch operation.

**Expected outcome:** All prune DELETEs execute within a single transaction committed once at the end of `_run_cycle`. The prune functions become write-only (no commit); the caller owns the transaction boundary. This matches the pattern already established by the sampler's `commit_batch`.

**Implementation — two-part, both required:**

Part 1 — `database.py`: Remove `conn.commit()` from both prune functions. The functions accumulate DELETEs in the open transaction; they do not commit.

```python
def prune_partition_offsets(conn, topic, partition, keep_n):
    cursor = conn.execute("""DELETE FROM partition_offsets ...""", ...)
    # conn.commit() REMOVED — caller commits
    return cursor.rowcount

def prune_consumer_commits(conn, group_id, topic, partition, keep_n):
    cursor = conn.execute("""DELETE FROM consumer_commits ...""", ...)
    # conn.commit() REMOVED — caller commits
    return cursor.rowcount
```

Part 2 — `housekeeping.py`: Add a single `database.commit_batch(self._db_conn)` call at the end of `_run_cycle`, after all prune loops and after `run_incremental_vacuum`. Without this, all the DELETEs are rolled back when the connection closes — housekeeping will log correct row counts but prune nothing, silently.

```python
def _run_cycle(self):
    # ... existing prune loops (unchanged) ...
    # ... run_incremental_vacuum (unchanged) ...
    database.commit_batch(self._db_conn)   # ADD THIS — single commit for all prunes
    # ... existing log summary ...
```

**Notes:** `run_incremental_vacuum` executes `PRAGMA incremental_vacuum(N)` which operates independently of the transaction state — safe to call before or after commit. The key invariant is: prune functions accumulate DELETEs; `_run_cycle` commits. This is identical to how the sampler handles its writes. Tests: assert that after a housekeeping cycle, rows are actually deleted from the DB (not just that `rowcount` is returned correctly). The existing tests may pass even with missing commit if they check `rowcount` only — verify they check actual DB state post-prune.

---

### 🟠 MEDIUM — TASK 44: `consecutive_static` not reset when consumer catches up — premature OFFLINE transitions

**Files:** `sampler.py` → `_evaluate_state_machine`

**Bug:** State machine has three mutually exclusive conditions: `all_static_with_lag` (True), `any_advancing` (True), or neither (consumer caught up — no lag, static offsets). The `elif any_advancing` branch resets `consecutive_static = 0`. The neither-branch does nothing — counter retains its previous value. Scenario: consumer is 4 samples into a 5-sample OFFLINE threshold. Producer pauses, consumer catches up, lag goes to zero. Three quiet cycles pass — counter stays at 4. Producer resumes, one cycle of static + lag: counter hits 5 → OFFLINE. The transition fires on a single sample of lag, not five, because the prior accumulation was never cleared. The counter semantics are corrupted: it should track consecutive samples of "static with lag" uninterrupted, but cross-episode accumulation is possible.

**Expected outcome:** `consecutive_static` resets to 0 whenever the consumer is not in the `all_static_with_lag` condition. Both `any_advancing` and the caught-up (neither) path clear the counter. Conservative semantics: threshold samples of uninterrupted stasis + lag are required, not cumulative samples across separated episodes.

**Implementation — one-line change in `_evaluate_state_machine`:**

```python
if all_static_with_lag:
    new_consecutive_static = consecutive_static + 1
    if new_consecutive_static >= threshold:
        if current_status in ("ONLINE", "RECOVERING"):
            new_status = "OFFLINE"
elif any_advancing:
    new_consecutive_static = 0
    new_last_advancing_at = current_time
    # ... RECOVERING/ONLINE transition logic unchanged ...
else:
    # Caught up (no lag) or insufficient history — clear counter
    new_consecutive_static = 0
```

**Notes:** The `else` branch must only reset `new_consecutive_static`. It must NOT modify `new_last_advancing_at` — the consumer has not advanced (offsets are static), it has merely caught up. Conflating "no lag" with "advancing" would be wrong. Specifically: `any_advancing` requires `len(set(offsets)) != 1` (offsets changing); caught-up is `has_lag == False` with static offsets. The OFFLINE→RECOVERING transition in the `elif any_advancing` branch is untouched — recovery still requires actual offset movement, not just absence of lag. Write a test: pre-load `consecutive_static = threshold - 1`, run one cycle with no lag (caught-up), assert counter resets to 0, assert status remains unchanged. Then run one cycle with lag, assert counter is 1 (not threshold), assert no OFFLINE transition.

---

### 🟠 MEDIUM — TASK 45: Output directory not validated at startup — silent retry loop if path is wrong

**Files:** `main.py`

**Bug:** `output.json_path` is validated as a string in `load_config` but its parent directory is never checked for existence. If the parent directory does not exist, `reporter._write_output` raises `FileNotFoundError` on `open(tmp_path, "w")`. This propagates to `reporter.run`'s `except Exception` handler, which logs a traceback and sleeps 5 seconds before retrying. Since the directory won't create itself, this repeats indefinitely — a tight retry loop generating an exception traceback every 5 seconds for the lifetime of the process, masking the real cause (misconfiguration) under repeated noise.

**Expected outcome:** Startup fails fast with a clear error message if the output directory does not exist. Check is in `main()` after config load, before threads start. Process exits with code 1.

**Implementation — add to `main()` after config is loaded:**

```python
import os  # already imported in main.py

output_dir = os.path.dirname(os.path.abspath(cfg.output.json_path))
if not os.path.isdir(output_dir):
    logger.error(
        f"Output directory does not exist: {output_dir!r} "
        f"(from output.json_path: {cfg.output.json_path!r})"
    )
    return 1
```

Place this block after `cfg = config_module.load_config(...)` and before `database.init_db(...)`. Order matters: config must be loaded first; the check should happen before any threads are started or resources are allocated.

**Notes:** `os.path.abspath` handles relative paths in `json_path` correctly. `os.path.dirname` on a bare filename (no directory component, e.g. `"output.json"`) returns `""`, and `os.path.isdir("")` returns False on most platforms — this would incorrectly fail for a file in the current working directory. Guard against this: if `output_dir` is empty string, replace with `"."` and re-check. Alternatively: `output_dir = os.path.dirname(os.path.abspath(cfg.output.json_path)) or "."`. Write a test in `test_main.py` or `test_config.py` using a tmp path with a nonexistent parent, assert the process returns exit code 1 and logs an appropriate error.

---

### 🟡 LOW — TASK 46: Exclusion check uses O(n) list scan — convert to set; update type annotations

**Files:** `config.py`, `database.py`

**Bug:** `ExcludeConfig.topics` and `.groups` are `List[str]`. `is_topic_excluded` and `is_group_excluded` receive them as `config_exclusions: List[str]` and test membership with `if topic in config_exclusions` — Python `list.__contains__` is O(n). Called once per active group per topic per cycle. At typical scale (4 groups × 16 topics = 64 calls/cycle, 128/minute) this is negligible in absolute terms. The issue is type semantic incorrectness: exclusion lists are sets by definition (unordered, no duplicates meaningful). A `list` admits duplicates silently; a `set` documents intent and is correct by construction.

**Expected outcome:** `ExcludeConfig.topics` and `.groups` stored as `Set[str]`. `is_topic_excluded` and `is_group_excluded` parameter types updated to match. All construction sites updated. O(1) membership test as a side effect.

**Implementation — three locations, all required:**

1. `config.py` — `ExcludeConfig` dataclass:
```python
from typing import Set  # add to imports

@dataclass
class ExcludeConfig:
    topics: Set[str]   # was List[str]
    groups: Set[str]   # was List[str]
```

2. `config.py` — `load_config` construction:
```python
exclude = ExcludeConfig(topics=set(topics), groups=set(groups))
```
The `topics` and `groups` local variables remain lists (they're validated element-by-element as lists above), converted to sets at construction.

3. `database.py` — function signatures:
```python
from typing import Set  # add to imports

def is_topic_excluded(conn, topic: str, config_exclusions: Set[str]) -> bool: ...
def is_group_excluded(conn, group_id: str, config_exclusions: Set[str]) -> bool: ...
```

**Notes:** No changes needed in `sampler.py` call sites — `self._config.exclude.topics` and `.groups` are passed through unchanged; the type change is transparent at runtime. The `_validate_type` calls in `load_config` validate the YAML-parsed value (a list) before conversion — that validation is correct and should not be changed. Any test that constructs `ExcludeConfig(topics=[...], groups=[...])` will still work at runtime (Python accepts any iterable for a `set`-typed field in a dataclass) but should be updated to pass sets for type correctness. Check `src/tests/` for `ExcludeConfig(` and update accordingly.

---

### 🟡 LOW — TASK 47: SASL/TLS credentials not forwarded; no startup warning (carry-forward)

**Files:** `kafka_client.py`, `config.py`

**Bug (carry-forward from prior rounds):** `build_admin_client` passes only `bootstrap.servers` and `security.protocol` to the confluent-kafka `AdminClient`. If `security_protocol` is `SASL_PLAINTEXT`, `SASL_SSL`, or `SSL`, the client requires additional parameters (`sasl.mechanism`, `sasl.username`, `sasl.password`, `ssl.ca.location`, etc.) that are neither modelled in `KafkaConfig` nor passed. Connection fails at runtime with a low-signal authentication error after a 120-second retry loop. No startup warning exists.

**Minimum viable fix (warning only):** Add a startup warning to `build_admin_client` when `security_protocol` is not `PLAINTEXT`:

```python
def build_admin_client(config: Config) -> AdminClient:
    if config.kafka.security_protocol != "PLAINTEXT":
        logger.warning(
            f"security_protocol is '{config.kafka.security_protocol}' but SASL/TLS "
            f"parameters (sasl_mechanism, sasl_username, sasl_password, ssl_ca_location) "
            f"are not yet supported by this build. Connection will likely fail at "
            f"authentication. Set security_protocol: PLAINTEXT or extend KafkaConfig."
        )
    conf = {
        "bootstrap.servers": config.kafka.bootstrap_servers,
        "security.protocol": config.kafka.security_protocol,
    }
    return AdminClient(conf)
```

**Full fix (preferred):** Extend `KafkaConfig` with optional SASL/TLS fields and pass them conditionally:

```python
@dataclass
class KafkaConfig:
    bootstrap_servers: str
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: Optional[str] = None
    sasl_username: Optional[str] = None
    sasl_password: Optional[str] = None
    ssl_ca_location: Optional[str] = None
```

In `build_admin_client`, conditionally add keys:
```python
if config.kafka.sasl_mechanism:
    conf["sasl.mechanism"] = config.kafka.sasl_mechanism
if config.kafka.sasl_username:
    conf["sasl.username"] = config.kafka.sasl_username
if config.kafka.sasl_password:
    conf["sasl.password"] = config.kafka.sasl_password
if config.kafka.ssl_ca_location:
    conf["ssl.ca.location"] = config.kafka.ssl_ca_location
```

Add corresponding optional parsing in `load_config` (not required, defaults to None). Do not raise `ConfigError` if SASL fields are absent when `security_protocol` is non-PLAINTEXT — that is a runtime concern handled by the client. Log a warning if protocol is non-PLAINTEXT and SASL fields are None.

**Notes:** Do not log `sasl_password` value in any log output. Minimum viable fix is acceptable for immediate deployment; full fix is preferred if SASL is needed in the near term.

---

### 🟡 LOW — TASK 48: `init_db` return value discarded — connection leaks to GC (carry-forward)

**Files:** `main.py`

**Bug (carry-forward from TASKS-3.md TASK 38):** `database.init_db(cfg.database.path)` returns a `sqlite3.Connection` that is silently discarded. Connection is closed by CPython reference counting, but the pattern implies `init_db` is void. Any developer reading `main.py` may assume `init_db` has no meaningful return value and that pattern is not to be followed.

**Fix:** Close explicitly:

```python
init_conn = database.init_db(cfg.database.path)
init_conn.close()
logger.info(f"Database initialized at {cfg.database.path}")
```

Or restructure `init_db` to close internally and return `None`. If restructuring: verify no caller uses the returned connection (currently none do).

---

### 🟡 LOW — TASK 49: No range validation on monitoring config integer values (carry-forward)

**Files:** `config.py`

**Bug (carry-forward from TASKS-3.md TASK 40):** All `MonitoringConfig` integer fields accept any value including zero and negative. `sample_interval_seconds=0` causes `sleep_time` to always be negative → sampler busy-loops at full CPU, hammering Kafka and SQLite. `max_entries_per_partition=0` causes `prune_partition_offsets` to delete all rows, destroying interpolation history. `offline_detection_consecutive_samples=0` causes immediate OFFLINE for all new groups. No `ConfigError` is raised.

**Fix:** Add post-parse range validation in `load_config` for the following fields and constraints:

```python
# After all integer fields are parsed, before constructing MonitoringConfig:
if sample_interval_seconds <= 0:
    raise ConfigError("monitoring.sample_interval_seconds must be > 0")
if offline_sample_interval_seconds < sample_interval_seconds:
    raise ConfigError(
        "monitoring.offline_sample_interval_seconds must be >= sample_interval_seconds"
    )
if report_interval_seconds <= 0:
    raise ConfigError("monitoring.report_interval_seconds must be > 0")
if housekeeping_interval_seconds <= 0:
    raise ConfigError("monitoring.housekeeping_interval_seconds must be > 0")
if max_entries_per_partition < 2:
    raise ConfigError("monitoring.max_entries_per_partition must be >= 2")
if max_commit_entries_per_partition < 2:
    raise ConfigError("monitoring.max_commit_entries_per_partition must be >= 2")
if offline_detection_consecutive_samples < 1:
    raise ConfigError("monitoring.offline_detection_consecutive_samples must be >= 1")
if recovering_minimum_duration_seconds < 0:
    raise ConfigError("monitoring.recovering_minimum_duration_seconds must be >= 0")
if online_lag_threshold_seconds < 0:
    raise ConfigError("monitoring.online_lag_threshold_seconds must be >= 0")
if absent_group_retention_seconds <= 0:
    raise ConfigError("monitoring.absent_group_retention_seconds must be > 0")
```

**Notes:** `max_entries_per_partition >= 2` (not 1) because interpolation requires at least two distinct points to bracket a committed offset. A value of 1 would mean at most one row per partition — interpolation always extrapolates, lag is always inaccurate. Write tests for each constraint: one test per invalid value asserting `ConfigError` is raised with a meaningful message.

---

### 🟡 LOW — TASK 50: `get_interpolation_points` — add defensive LIMIT equal to `max_entries_per_partition`

**Files:** `database.py`

**Context:** Housekeeping bounds `partition_offsets` row count to `max_entries_per_partition` (default 300). The `get_interpolation_points` query has no LIMIT — in normal operation, housekeeping keeps the table bounded and this is not a production risk. The concern is a timing window (rows accumulate between housekeeping cycles) and a misconfiguration risk (if `max_entries_per_partition` is set very high, TASK 49 does not cap it). This is defensive hardening, not a fix for an active bug.

**Fix:** Add `LIMIT` to the query equal to the configured maximum. Since `database.py` functions do not receive config, the simplest approach is a caller-supplied `limit` parameter with a safe default:

```python
def get_interpolation_points(
    conn: sqlite3.Connection, topic: str, partition: int, limit: int = 500
) -> List[Tuple[int, int]]:
    cursor = conn.execute(
        """SELECT offset, sampled_at FROM partition_offsets
           WHERE topic = ? AND partition = ?
           ORDER BY sampled_at DESC
           LIMIT ?""",
        (topic, partition, limit),
    )
    return [(row[0], row[1]) for row in cursor.fetchall()]
```

Update callers in `sampler.py` and `reporter.py` to pass `self._config.monitoring.max_entries_per_partition` as the limit. Default of 500 provides safety for callers that do not pass a limit.

**Notes:** This is the lowest priority task in this round. Do not prioritise it above TASK 42–45. The interpolation algorithm only needs two bracketing rows but the current implementation fetches all rows and scans them — refactoring to a two-query fetch (one row above, one below the committed offset) would be a more correct fix but is a larger change and out of scope here.

