## Kafka Client Abstraction

### `kafka_client.py` Responsibilities

All `confluent-kafka` usage is isolated here. No other module imports from `confluent-kafka`. Uses `AdminClient` only — the monitor never joins a consumer group and never affects committed offsets.

### SASL/TLS Support

The `build_admin_client()` function supports optional SASL and TLS configuration:

- **SASL mechanisms**: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, etc.
- **Security protocols**: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL
- Credentials are passed from the `Config` object if present
- A warning is logged if `security_protocol` requires SASL but credentials are incomplete

Example configuration for SASL_SSL:
```python
conf = {
    "bootstrap.servers": config.kafka.bootstrap_servers,
    "security.protocol": config.kafka.security_protocol,
    "sasl.mechanism": config.kafka.sasl_mechanism,
    "sasl.username": config.kafka.sasl_username,
    "sasl.password": config.kafka.sasl_password,
    "ssl.ca.location": config.kafka.ssl_ca_location,
}
```

### Required Functions

- `get_active_consumer_groups(admin_client) -> list[str]` — list of group_id strings. Uses `list_consumer_groups()`. Returns empty list on error.
- `get_committed_offsets(admin_client, group_id, topic_partitions) -> dict[(str, int), int]` — maps `(topic, partition)` to committed offset. Uses `list_consumer_group_offsets()`. Returns empty dict on error.
- `get_latest_produced_offsets(admin_client, topic_partitions) -> dict[(str, int), int]` — maps `(topic, partition)` to latest offset. Uses `list_offsets()` with `LATEST` spec. Returns empty dict on error.
- `get_topic_partition_count(admin_client, topic) -> int` — returns number of partitions for a topic. Returns 0 on error.
- `get_all_consumed_topic_partitions(admin_client, group_ids) -> set[tuple[str, int]]` — full set of unique `(topic, partition)` tuples actively consumed by the given groups.
- `build_admin_client(config: Config) -> AdminClient` — constructs and returns a configured AdminClient with optional SASL/TLS settings.

All functions must catch all exceptions internally, log a warning with context, and return an empty/zero value. The system must degrade gracefully when Kafka is briefly unreachable.

