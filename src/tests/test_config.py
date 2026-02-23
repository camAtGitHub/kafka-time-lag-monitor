"""Tests for config.py module."""

import pytest
from pathlib import Path
import config


def create_valid_config(tmp_path: Path) -> Path:
    """Helper to create a valid config file and return its path."""
    config_content = """
kafka:
  bootstrap_servers: "localhost:9092"
  security_protocol: "PLAINTEXT"

monitoring:
  sample_interval_seconds: 60
  offline_sample_interval_seconds: 1800
  report_interval_seconds: 30
  housekeeping_interval_seconds: 300
  max_entries_per_partition: 300
  max_commit_entries_per_partition: 200
  offline_detection_consecutive_samples: 3
  recovering_minimum_duration_seconds: 180
  online_lag_threshold_seconds: 60

database:
  path: "/var/lib/kafka-lag-monitor/lag.db"

output:
  json_path: "/var/lib/kafka-lag-monitor/lag.json"

exclude:
  topics: ["internal-topic"]
  groups: ["internal-group"]
"""
    config_file = tmp_path / "config.yaml"
    config_file.write_text(config_content)
    return config_file


def test_valid_config_loads_successfully(tmp_path):
    """Test that a valid config file loads and all fields are accessible."""
    config_file = create_valid_config(tmp_path)
    
    cfg = config.load_config(str(config_file))
    
    assert cfg.kafka.bootstrap_servers == "localhost:9092"
    assert cfg.kafka.security_protocol == "PLAINTEXT"
    assert cfg.monitoring.sample_interval_seconds == 60
    assert cfg.monitoring.offline_sample_interval_seconds == 1800
    assert cfg.monitoring.report_interval_seconds == 30
    assert cfg.monitoring.housekeeping_interval_seconds == 300
    assert cfg.monitoring.max_entries_per_partition == 300
    assert cfg.monitoring.max_commit_entries_per_partition == 200
    assert cfg.monitoring.offline_detection_consecutive_samples == 3
    assert cfg.monitoring.recovering_minimum_duration_seconds == 180
    assert cfg.monitoring.online_lag_threshold_seconds == 60
    assert cfg.monitoring.absent_group_retention_seconds == 604800  # default
    assert cfg.database.path == "/var/lib/kafka-lag-monitor/lag.db"
    assert cfg.output.json_path == "/var/lib/kafka-lag-monitor/lag.json"
    assert cfg.exclude.topics == ["internal-topic"]
    assert cfg.exclude.groups == ["internal-group"]


def test_missing_required_field_raises_config_error(tmp_path):
    """Test that missing required field raises ConfigError with field name."""
    config_content = """
kafka:
  bootstrap_servers: "localhost:9092"
  security_protocol: "PLAINTEXT"

monitoring:
  sample_interval_seconds: 60
  offline_sample_interval_seconds: 1800
  report_interval_seconds: 30
  housekeeping_interval_seconds: 300
  max_entries_per_partition: 300
  max_commit_entries_per_partition: 200
  offline_detection_consecutive_samples: 3
  recovering_minimum_duration_seconds: 180

database:
  path: "/var/lib/kafka-lag-monitor/lag.db"

output:
  json_path: "/var/lib/kafka-lag-monitor/lag.json"
"""
    config_file = tmp_path / "config.yaml"
    config_file.write_text(config_content)
    
    with pytest.raises(config.ConfigError) as exc_info:
        config.load_config(str(config_file))
    
    assert "online_lag_threshold_seconds" in str(exc_info.value)


def test_missing_bootstrap_servers_raises_config_error(tmp_path):
    """Test that missing bootstrap_servers raises ConfigError."""
    config_content = """
kafka:
  security_protocol: "PLAINTEXT"

monitoring:
  sample_interval_seconds: 60
  offline_sample_interval_seconds: 1800
  report_interval_seconds: 30
  housekeeping_interval_seconds: 300
  max_entries_per_partition: 300
  max_commit_entries_per_partition: 200
  offline_detection_consecutive_samples: 3
  recovering_minimum_duration_seconds: 180
  online_lag_threshold_seconds: 60

database:
  path: "/var/lib/kafka-lag-monitor/lag.db"

output:
  json_path: "/var/lib/kafka-lag-monitor/lag.json"
"""
    config_file = tmp_path / "config.yaml"
    config_file.write_text(config_content)
    
    with pytest.raises(config.ConfigError) as exc_info:
        config.load_config(str(config_file))
    
    assert "bootstrap_servers" in str(exc_info.value)


def test_invalid_type_raises_config_error(tmp_path):
    """Test that incorrect type for a field raises ConfigError."""
    config_content = """
kafka:
  bootstrap_servers: "localhost:9092"
  security_protocol: 123

monitoring:
  sample_interval_seconds: 60
  offline_sample_interval_seconds: 1800
  report_interval_seconds: 30
  housekeeping_interval_seconds: 300
  max_entries_per_partition: 300
  max_commit_entries_per_partition: 200
  offline_detection_consecutive_samples: 3
  recovering_minimum_duration_seconds: 180
  online_lag_threshold_seconds: 60

database:
  path: "/var/lib/kafka-lag-monitor/lag.db"

output:
  json_path: "/var/lib/kafka-lag-monitor/lag.json"
"""
    config_file = tmp_path / "config.yaml"
    config_file.write_text(config_content)
    
    with pytest.raises(config.ConfigError) as exc_info:
        config.load_config(str(config_file))
    
    assert "security_protocol" in str(exc_info.value)
    assert "string" in str(exc_info.value)


def test_invalid_integer_type_raises_config_error(tmp_path):
    """Test that invalid integer type raises ConfigError."""
    config_content = """
kafka:
  bootstrap_servers: "localhost:9092"

monitoring:
  sample_interval_seconds: "not-an-integer"
  offline_sample_interval_seconds: 1800
  report_interval_seconds: 30
  housekeeping_interval_seconds: 300
  max_entries_per_partition: 300
  max_commit_entries_per_partition: 200
  offline_detection_consecutive_samples: 3
  recovering_minimum_duration_seconds: 180
  online_lag_threshold_seconds: 60

database:
  path: "/var/lib/kafka-lag-monitor/lag.db"

output:
  json_path: "/var/lib/kafka-lag-monitor/lag.json"
"""
    config_file = tmp_path / "config.yaml"
    config_file.write_text(config_content)
    
    with pytest.raises(config.ConfigError) as exc_info:
        config.load_config(str(config_file))
    
    assert "sample_interval_seconds" in str(exc_info.value)
    assert "integer" in str(exc_info.value)


def test_optional_field_uses_default(tmp_path):
    """Test that missing optional fields use their defined defaults."""
    config_content = """
kafka:
  bootstrap_servers: "localhost:9092"

monitoring:
  sample_interval_seconds: 60
  offline_sample_interval_seconds: 1800
  report_interval_seconds: 30
  housekeeping_interval_seconds: 300
  max_entries_per_partition: 300
  max_commit_entries_per_partition: 200
  offline_detection_consecutive_samples: 3
  recovering_minimum_duration_seconds: 180
  online_lag_threshold_seconds: 60

database:
  path: "/var/lib/kafka-lag-monitor/lag.db"

output:
  json_path: "/var/lib/kafka-lag-monitor/lag.json"
"""
    config_file = tmp_path / "config.yaml"
    config_file.write_text(config_content)
    
    cfg = config.load_config(str(config_file))
    
    assert cfg.kafka.security_protocol == "PLAINTEXT"


def test_optional_exclude_section_defaults_to_empty(tmp_path):
    """Test that missing exclude section uses empty defaults."""
    config_content = """
kafka:
  bootstrap_servers: "localhost:9092"

monitoring:
  sample_interval_seconds: 60
  offline_sample_interval_seconds: 1800
  report_interval_seconds: 30
  housekeeping_interval_seconds: 300
  max_entries_per_partition: 300
  max_commit_entries_per_partition: 200
  offline_detection_consecutive_samples: 3
  recovering_minimum_duration_seconds: 180
  online_lag_threshold_seconds: 60

database:
  path: "/var/lib/kafka-lag-monitor/lag.db"

output:
  json_path: "/var/lib/kafka-lag-monitor/lag.json"
"""
    config_file = tmp_path / "config.yaml"
    config_file.write_text(config_content)
    
    cfg = config.load_config(str(config_file))
    
    assert cfg.exclude.topics == []
    assert cfg.exclude.groups == []


def test_empty_config_file_raises_config_error(tmp_path):
    """Test that empty config file raises ConfigError."""
    config_file = tmp_path / "config.yaml"
    config_file.write_text("")
    
    with pytest.raises(config.ConfigError) as exc_info:
        config.load_config(str(config_file))
    
    assert "empty" in str(exc_info.value).lower()


def test_nonexistent_file_raises_config_error(tmp_path):
    """Test that nonexistent config file raises ConfigError."""
    with pytest.raises(config.ConfigError) as exc_info:
        config.load_config(str(tmp_path / "nonexistent.yaml"))
    
    assert "not found" in str(exc_info.value).lower()


def test_invalid_yaml_raises_config_error(tmp_path):
    """Test that invalid YAML raises ConfigError."""
    config_file = tmp_path / "config.yaml"
    config_file.write_text("invalid: yaml: content:")

    with pytest.raises(config.ConfigError) as exc_info:
        config.load_config(str(config_file))

    assert "yaml" in str(exc_info.value).lower()


def test_absent_group_retention_seconds_uses_default_when_missing(tmp_path):
    """Test that absent_group_retention_seconds uses default value when missing."""
    config_content = """
kafka:
  bootstrap_servers: "localhost:9092"

monitoring:
  sample_interval_seconds: 60
  offline_sample_interval_seconds: 1800
  report_interval_seconds: 30
  housekeeping_interval_seconds: 300
  max_entries_per_partition: 300
  max_commit_entries_per_partition: 200
  offline_detection_consecutive_samples: 3
  recovering_minimum_duration_seconds: 180
  online_lag_threshold_seconds: 60

database:
  path: "/var/lib/kafka-lag-monitor/lag.db"

output:
  json_path: "/var/lib/kafka-lag-monitor/lag.json"
"""
    config_file = tmp_path / "config.yaml"
    config_file.write_text(config_content)

    cfg = config.load_config(str(config_file))

    assert cfg.monitoring.absent_group_retention_seconds == 604800  # 7 days default


def test_absent_group_retention_seconds_can_be_overridden(tmp_path):
    """Test that absent_group_retention_seconds can be set to a custom value."""
    config_content = """
kafka:
  bootstrap_servers: "localhost:9092"

monitoring:
  sample_interval_seconds: 60
  offline_sample_interval_seconds: 1800
  report_interval_seconds: 30
  housekeeping_interval_seconds: 300
  max_entries_per_partition: 300
  max_commit_entries_per_partition: 200
  offline_detection_consecutive_samples: 3
  recovering_minimum_duration_seconds: 180
  online_lag_threshold_seconds: 60
  absent_group_retention_seconds: 3600

database:
  path: "/var/lib/kafka-lag-monitor/lag.db"

output:
  json_path: "/var/lib/kafka-lag-monitor/lag.json"
"""
    config_file = tmp_path / "config.yaml"
    config_file.write_text(config_content)

    cfg = config.load_config(str(config_file))

    assert cfg.monitoring.absent_group_retention_seconds == 3600
