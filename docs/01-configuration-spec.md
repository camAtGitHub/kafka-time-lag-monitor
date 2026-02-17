## Configuration Specification

### `config.yaml`

```yaml
kafka:
  bootstrap_servers: "broker1:9092,broker2:9092"
  security_protocol: PLAINTEXT   # or SASL_SSL etc.

monitoring:
  sample_interval_seconds: 60
  offline_sample_interval_seconds: 1800
  report_interval_seconds: 60
  housekeeping_interval_seconds: 900
  max_entries_per_partition: 300
  max_commit_entries_per_partition: 100
  offline_detection_consecutive_samples: 5
  recovering_minimum_duration_seconds: 900
  online_lag_threshold_seconds: 600

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

### `config.py` Responsibilities

- Load and parse `config.yaml`
- Validate all required fields are present and of correct type
- Expose a single `Config` dataclass consumed by all other modules
- Raise a `ConfigError` with a clear human-readable message on any validation failure
- No other module reads the config file directly — they all receive the `Config` object at instantiation

---
