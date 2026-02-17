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

