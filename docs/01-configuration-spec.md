## Configuration Specification

### `config.yaml`

```yaml
kafka:
  bootstrap_servers: "broker1:9092,broker2:9092"
  security_protocol: PLAINTEXT   # or SASL_SSL, SASL_PLAINTEXT, SSL
  # Optional SASL/TLS configuration (required when using SASL protocols)
  sasl_mechanism: PLAIN          # e.g., PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
  sasl_username: "your-username"
  sasl_password: "your-password"
  ssl_ca_location: "/path/to/ca-cert.pem"  # Required for SSL/SASL_SSL

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
  absent_group_retention_seconds: 604800   # 7 days (optional, defaults to 604800)

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

### Configuration Validation

The system performs the following validations at startup:

**Kafka Section:**
- `bootstrap_servers`: Required, must be a string
- `security_protocol`: Optional, defaults to `PLAINTEXT`
- `sasl_mechanism`, `sasl_username`, `sasl_password`, `ssl_ca_location`: Optional
- Warning logged if `security_protocol` is `SASL_SSL` or `SASL_PLAINTEXT` but SASL credentials are incomplete

**Monitoring Section:**
- `sample_interval_seconds`: Must be > 0
- `offline_sample_interval_seconds`: Must be >= `sample_interval_seconds`
- `report_interval_seconds`: Must be > 0
- `housekeeping_interval_seconds`: Must be > 0
- `max_entries_per_partition`: Must be >= 2 (need at least 2 points for interpolation)
- `max_commit_entries_per_partition`: Must be >= 2
- `offline_detection_consecutive_samples`: Must be >= 1
- `recovering_minimum_duration_seconds`: Must be >= 0
- `online_lag_threshold_seconds`: Must be >= 0
- `absent_group_retention_seconds`: Must be > 0

**Output Section:**
- `json_path`: Required, and the parent directory must exist at startup

### `config.py` Responsibilities

- Load and parse `config.yaml`
- Validate all required fields are present and of correct type
- Validate range constraints on integer fields
- Expose a single `Config` dataclass consumed by all other modules
- Raise a `ConfigError` with a clear human-readable message on any validation failure
- No other module reads the config file directly — they all receive the `Config` object at instantiation

---
