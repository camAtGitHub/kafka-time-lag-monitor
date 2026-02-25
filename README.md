# Kafka Consumer Lag Monitor

A self-contained Python daemon that monitors Kafka consumer group lag across multiple topics and consumer groups. It answers the question: **"How old is the data currently being processed by each consumer group?"** — expressed as a time value in seconds, not a raw offset count.

## Why Time-Based Lag?

Raw offset lag (how many messages behind a consumer is) is a poor operational metric in isolation. A lag of 50,000 offsets is catastrophic on a high-throughput topic and completely irrelevant on a nearly-idle one. Time-based lag answers the question that actually matters operationally: **how stale is this consumer's view of the world right now?**

## Core Technique: Offset Interpolation

The system maintains a per-partition table of historical snapshots: at a given point in time, the partition's head was at a given offset. When the system needs to calculate the time lag for a consumer, it looks up the consumer's last committed offset in this table. If an exact match exists, it returns the timestamp for that entry. If the committed offset falls between two entries, it applies linear interpolation to estimate the timestamp. The lag is then `current_time - estimated_timestamp`.

## Consumer Group State Machine

Consumer groups are not always active. To handle groups that go offline (and may stay offline for days), the system implements a three-state machine per group/topic:

- **ONLINE** — offset is advancing and lag is below threshold. Fine-grained sampling (every 60 seconds).
- **OFFLINE** — offset has been static for N consecutive samples. Coarse sampling (every 30 minutes).
- **RECOVERING** — offset is advancing again after being offline, but lag is still above the online threshold. Coarse sampling maintained until lag drops sufficiently.

## Requirements

- Python 3.9+
- confluent-kafka
- pyyaml
- pytest (for testing)

## Installation

```bash
pip install -r requirements.txt
```

## Configuration

Create a `config.yaml` file with the following structure:

```yaml
kafka:
  bootstrap_servers: "broker1:9092,broker2:9092"
  security_protocol: PLAINTEXT   # or SASL_SSL, SASL_PLAINTEXT, SSL
  # Optional SASL/TLS configuration (required for SASL_SSL/SASL_PLAINTEXT)
  sasl_mechanism: PLAIN          # e.g., PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
  sasl_username: "your-username"
  sasl_password: "your-password"
  ssl_ca_location: "/path/to/ca-cert.pem"  # Required for SSL/SASL_SSL

monitoring:
  sample_interval_seconds: 60          # How often to sample ONLINE groups (must be > 0)
  offline_sample_interval_seconds: 1800  # How often to sample OFFLINE/RECOVERING groups (must be >= sample_interval_seconds)
  report_interval_seconds: 60         # How often to write JSON output (must be > 0)
  housekeeping_interval_seconds: 900  # How often to prune old data (must be > 0)
  max_entries_per_partition: 300      # Max partition_offsets rows per topic/partition (must be >= 2)
  max_commit_entries_per_partition: 100  # Max consumer_commits rows per group/topic/partition (must be >= 2)
  offline_detection_consecutive_samples: 5  # Samples before declaring OFFLINE (must be >= 1)
  recovering_minimum_duration_seconds: 900  # Min time in RECOVERING before ONLINE (must be >= 0)
  online_lag_threshold_seconds: 600     # Lag threshold to declare ONLINE (must be >= 0)
  absent_group_retention_seconds: 604800  # How long to retain absent groups (7 days, must be > 0)

database:
  path: "/var/lib/kafka-lag-monitor/state.db"

output:
  json_path: "/var/lib/kafka-lag-monitor/lag.json"

exclude:
  topics:
    - "__consumer_offsets"
    - "__transaction_state"
  groups:
    - "some-legacy-group"
```

### Configuration Options

| Section | Field | Description | Default | Validation |
|---------|-------|-------------|---------|------------|
| kafka | bootstrap_servers | Comma-separated list of Kafka brokers | (required) | - |
| kafka | security_protocol | Kafka security protocol | PLAINTEXT | - |
| kafka | sasl_mechanism | SASL mechanism (e.g., PLAIN, SCRAM-SHA-256) | None | Optional |
| kafka | sasl_username | SASL username | None | Optional |
| kafka | sasl_password | SASL password | None | Optional |
| kafka | ssl_ca_location | Path to CA certificate for SSL | None | Optional |
| monitoring | sample_interval_seconds | Sampling interval for ONLINE groups | 60 | Must be > 0 |
| monitoring | offline_sample_interval_seconds | Sampling interval for OFFLINE/RECOVERING groups | 1800 | Must be >= sample_interval_seconds |
| monitoring | report_interval_seconds | How often to write JSON output | 60 | Must be > 0 |
| monitoring | housekeeping_interval_seconds | How often to prune database | 900 | Must be > 0 |
| monitoring | max_entries_per_partition | Max rows in partition_offsets table | 300 | Must be >= 2 |
| monitoring | max_commit_entries_per_partition | Max rows in consumer_commits table | 100 | Must be >= 2 |
| monitoring | offline_detection_consecutive_samples | Samples before OFFLINE transition | 5 | Must be >= 1 |
| monitoring | recovering_minimum_duration_seconds | Min RECOVERING duration before ONLINE | 900 | Must be >= 0 |
| monitoring | online_lag_threshold_seconds | Lag threshold for ONLINE status | 600 | Must be >= 0 |
| monitoring | absent_group_retention_seconds | How long to retain absent groups | 604800 | Must be > 0 |
| database | path | Path to SQLite database file | (required) | - |
| output | json_path | Path for JSON output file | (required) | Directory must exist |
| exclude | topics | List of topics to exclude | [] | - |
| exclude | groups | List of consumer groups to exclude | [] | - |

**Note on SASL/TLS:** When using `security_protocol: SASL_SSL` or `SASL_PLAINTEXT`, you must provide `sasl_mechanism`, `sasl_username`, and `sasl_password`. For SSL-based protocols, `ssl_ca_location` is also required. The system will log a warning if SASL credentials are incomplete.

## Quick Start

1. **Create the required directories:**
   ```bash
   sudo mkdir -p /var/lib/kafka-lag-monitor
   sudo chown $USER /var/lib/kafka-lag-monitor
   ```

2. **Create a configuration file** (see Configuration section above)

3. **Run the daemon:**
   ```bash
   python -m main --config /path/to/config.yaml
   ```

The daemon also supports a `--debug` flag for verbose logging:
```bash
python -m main --config /path/to/config.yaml --debug
```

### Signal Handling

The daemon responds to `SIGTERM` and `SIGINT` signals for clean shutdown.

## Output Format

The JSON output file contains:

```json
{
  "generated_at": "2026-02-18T12:00:00Z",
  "consumers": [
    {
      "group_id": "my-consumer-group",
      "topic": "my-topic",
      "lag_seconds": 300,
      "lag_display": "5 minutes",
      "worst_partition": 3,
      "status": "online",
      "data_resolution": "fine",
      "partitions_monitored": 8,
      "calculated_at": "2026-02-18T12:00:00Z"
    }
  ]
}
```

- `lag_seconds`: Always an integer, never null. 0 means the consumer is current.
- `lag_display`: Human-readable lag (e.g., "< 1 minute", "5 minutes", "2 hours")
- `worst_partition`: Partition with highest lag, null when lag is 0
- `status`: One of "online", "offline", "recovering"
- `data_resolution`: "fine" for ONLINE groups, "coarse" for OFFLINE/RECOVERING

## Database

The system uses SQLite with WAL mode for concurrent access. Tables:

- `partition_offsets`: Historical snapshots of partition head offsets
- `consumer_commits`: Consumer group committed offsets over time
- `group_status`: Current state machine state per group/topic
- `excluded_topics`: Topics excluded from monitoring
- `excluded_groups`: Groups excluded from monitoring

## Architecture

- **sampler.py**: Collects Kafka state, writes to database, manages state machine transitions
- **reporter.py**: Reads database, calculates lags, writes JSON output
- **housekeeping.py**: Prunes old data, runs incremental vacuum
- **state_manager.py**: Thread-safe shared state coordination

## Testing

```bash
# Run all unit tests (excluding integration tests)
pytest src/tests/ --ignore=src/tests/test_integration.py

# Run a specific test file
pytest src/tests/test_interpolation.py -v

# Run integration tests (requires live Kafka)
pytest src/tests/test_integration.py
```
