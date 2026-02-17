# AGENTS.md

## Commands
- **Test all:** `pytest tests/ --ignore=tests/test_integration.py`
- **Test single:** `pytest tests/test_interpolation.py::test_name -v`
- **Integration tests (requires live Kafka):** `pytest tests/test_integration.py`
- **Install deps:** `pip install -r requirements.txt` (confluent-kafka, pyyaml, pytest)

## Architecture
Development only has Python 3.12, but target Python 3.9+ daemon monitoring Kafka consumer group lag as **time-based** (not offset-based) using offset interpolation against historical snapshots stored in SQLite (WAL mode). Three worker threads (sampler, reporter, housekeeping) coordinate via `StateManager` (RLock-protected shared state). Output is an atomically-written JSON file.

**Modules:** `config.py` (YAML loading, `Config` dataclass), `database.py` (all SQL — no other module writes SQL), `kafka_client.py` (all `confluent-kafka` usage — no other module imports it), `interpolation.py` (pure functions, no I/O), `state_manager.py` (thread-safe shared state), `sampler.py`, `reporter.py`, `housekeeping.py`, `main.py` (entry point, signal handling, thread lifecycle).

**Database:** SQLite with tables `partition_offsets`, `consumer_commits`, `group_status`, `excluded_topics`, `excluded_groups`. See `docs/02-database-design.md`.

**State machine:** Per group/topic: ONLINE → OFFLINE → RECOVERING → ONLINE. Write cadence drops for non-ONLINE groups.

## Code Style
- **No external deps** beyond confluent-kafka, pyyaml, pytest. Check before adding any.
- Module boundaries are strict: SQL only in `database.py`, Kafka only in `kafka_client.py`, no I/O in `interpolation.py`.
- `kafka_client.py` functions catch all exceptions internally, log warnings, return empty/zero values — never raise.
- `StateManager` methods return copies of internal state, never references.
- Config is a dataclass; all modules receive `Config` — none read YAML directly.
- Tests use `pytest`; DB tests use in-memory SQLite via `conftest.py` fixture; use `tmp_path` for file tests.
- Design docs live in `docs/`; task list in `TASKS.md`; instructions to be followed in `WORKFLOW.md`.
