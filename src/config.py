"""Configuration loading and validation module.

This module handles all YAML configuration loading and provides a typed
Config dataclass consumed by all other modules.
"""

from dataclasses import dataclass
from typing import List, Optional, Any
import yaml


class ConfigError(Exception):
    """Raised when configuration is invalid or missing required fields."""
    pass


@dataclass
class KafkaConfig:
    """Kafka connection configuration."""
    bootstrap_servers: str
    security_protocol: str = "PLAINTEXT"


@dataclass
class MonitoringConfig:
    """Monitoring behavior configuration."""
    sample_interval_seconds: int
    offline_sample_interval_seconds: int
    report_interval_seconds: int
    housekeeping_interval_seconds: int
    max_entries_per_partition: int
    max_commit_entries_per_partition: int
    offline_detection_consecutive_samples: int
    recovering_minimum_duration_seconds: int
    online_lag_threshold_seconds: int


@dataclass
class DatabaseConfig:
    """Database configuration."""
    path: str


@dataclass
class OutputConfig:
    """Output configuration."""
    json_path: str


@dataclass
class ExcludeConfig:
    """Exclusion lists configuration."""
    topics: List[str]
    groups: List[str]


@dataclass
class Config:
    """Root configuration dataclass."""
    kafka: KafkaConfig
    monitoring: MonitoringConfig
    database: DatabaseConfig
    output: OutputConfig
    exclude: ExcludeConfig


def _get_nested(data: dict, path: str, required: bool = True, default: Any = None) -> Any:
    """Get a nested value from a dictionary using dot notation.
    
    Args:
        data: The dictionary to search
        path: Dot-separated path to the value (e.g., "kafka.bootstrap_servers")
        required: If True, raises ConfigError when value is missing
        default: Default value if not required and missing
        
    Returns:
        The value at the path, or default if not required and missing
        
    Raises:
        ConfigError: If required value is missing
    """
    keys = path.split(".")
    current = data
    
    for key in keys:
        if not isinstance(current, dict):
            if required:
                raise ConfigError(f"Configuration path '{path}' is not a valid nested structure")
            return default
        if key not in current:
            if required:
                raise ConfigError(f"Missing required configuration field: {path}")
            return default
        current = current[key]
    
    return current


def _validate_type(value: Any, expected_type: type, field_name: str) -> None:
    """Validate that a value is of the expected type.
    
    Args:
        value: The value to validate
        expected_type: The expected type
        field_name: Name of the field for error messages
        
    Raises:
        ConfigError: If value is not of the expected type
    """
    origin = getattr(expected_type, "__origin__", None)
    
    if origin is list or expected_type is list:
        if not isinstance(value, list):
            raise ConfigError(
                f"Field '{field_name}' must be a list, got {type(value).__name__}"
            )
    elif expected_type is int:
        if not isinstance(value, int) or isinstance(value, bool):
            raise ConfigError(
                f"Field '{field_name}' must be an integer, got {type(value).__name__}"
            )
    elif expected_type is str:
        if not isinstance(value, str):
            raise ConfigError(
                f"Field '{field_name}' must be a string, got {type(value).__name__}"
            )
    else:
        if not isinstance(value, expected_type):
            raise ConfigError(
                f"Field '{field_name}' must be of type {expected_type.__name__}, got {type(value).__name__}"
            )


def load_config(path: str) -> Config:
    """Load and validate configuration from a YAML file.
    
    Args:
        path: Path to the YAML configuration file
        
    Returns:
        Config: Validated configuration object
        
    Raises:
        ConfigError: If the file cannot be read or configuration is invalid
    """
    try:
        with open(path, 'r') as f:
            data = yaml.safe_load(f)
    except FileNotFoundError:
        raise ConfigError(f"Configuration file not found: {path}")
    except yaml.YAMLError as e:
        raise ConfigError(f"Invalid YAML in configuration file: {e}")
    
    if data is None:
        raise ConfigError("Configuration file is empty")
    
    if not isinstance(data, dict):
        raise ConfigError("Configuration file must contain a YAML dictionary")
    
    # Kafka configuration
    kafka_data = _get_nested(data, "kafka")
    bootstrap_servers = _get_nested(kafka_data, "bootstrap_servers")
    _validate_type(bootstrap_servers, str, "kafka.bootstrap_servers")
    
    security_protocol = _get_nested(
        kafka_data, "security_protocol", required=False, default="PLAINTEXT"
    )
    _validate_type(security_protocol, str, "kafka.security_protocol")
    
    kafka = KafkaConfig(
        bootstrap_servers=bootstrap_servers,
        security_protocol=security_protocol
    )
    
    # Monitoring configuration
    monitoring_data = _get_nested(data, "monitoring")
    
    sample_interval_seconds = _get_nested(monitoring_data, "sample_interval_seconds")
    _validate_type(sample_interval_seconds, int, "monitoring.sample_interval_seconds")
    
    offline_sample_interval_seconds = _get_nested(
        monitoring_data, "offline_sample_interval_seconds"
    )
    _validate_type(
        offline_sample_interval_seconds, int, "monitoring.offline_sample_interval_seconds"
    )
    
    report_interval_seconds = _get_nested(monitoring_data, "report_interval_seconds")
    _validate_type(report_interval_seconds, int, "monitoring.report_interval_seconds")
    
    housekeeping_interval_seconds = _get_nested(
        monitoring_data, "housekeeping_interval_seconds"
    )
    _validate_type(
        housekeeping_interval_seconds, int, "monitoring.housekeeping_interval_seconds"
    )
    
    max_entries_per_partition = _get_nested(monitoring_data, "max_entries_per_partition")
    _validate_type(
        max_entries_per_partition, int, "monitoring.max_entries_per_partition"
    )
    
    max_commit_entries_per_partition = _get_nested(
        monitoring_data, "max_commit_entries_per_partition"
    )
    _validate_type(
        max_commit_entries_per_partition, int, "monitoring.max_commit_entries_per_partition"
    )
    
    offline_detection_consecutive_samples = _get_nested(
        monitoring_data, "offline_detection_consecutive_samples"
    )
    _validate_type(
        offline_detection_consecutive_samples, int, "monitoring.offline_detection_consecutive_samples"
    )
    
    recovering_minimum_duration_seconds = _get_nested(
        monitoring_data, "recovering_minimum_duration_seconds"
    )
    _validate_type(
        recovering_minimum_duration_seconds, int, "monitoring.recovering_minimum_duration_seconds"
    )
    
    online_lag_threshold_seconds = _get_nested(
        monitoring_data, "online_lag_threshold_seconds"
    )
    _validate_type(
        online_lag_threshold_seconds, int, "monitoring.online_lag_threshold_seconds"
    )
    
    monitoring = MonitoringConfig(
        sample_interval_seconds=sample_interval_seconds,
        offline_sample_interval_seconds=offline_sample_interval_seconds,
        report_interval_seconds=report_interval_seconds,
        housekeeping_interval_seconds=housekeeping_interval_seconds,
        max_entries_per_partition=max_entries_per_partition,
        max_commit_entries_per_partition=max_commit_entries_per_partition,
        offline_detection_consecutive_samples=offline_detection_consecutive_samples,
        recovering_minimum_duration_seconds=recovering_minimum_duration_seconds,
        online_lag_threshold_seconds=online_lag_threshold_seconds
    )
    
    # Database configuration
    database_data = _get_nested(data, "database")
    database_path = _get_nested(database_data, "path")
    _validate_type(database_path, str, "database.path")
    
    database = DatabaseConfig(path=database_path)
    
    # Output configuration
    output_data = _get_nested(data, "output")
    json_path = _get_nested(output_data, "json_path")
    _validate_type(json_path, str, "output.json_path")
    
    output = OutputConfig(json_path=json_path)
    
    # Exclude configuration
    exclude_data = _get_nested(data, "exclude", required=False, default={})
    
    topics = _get_nested(exclude_data, "topics", required=False, default=[])
    _validate_type(topics, list, "exclude.topics")
    for i, topic in enumerate(topics):
        _validate_type(topic, str, f"exclude.topics[{i}]")
    
    groups = _get_nested(exclude_data, "groups", required=False, default=[])
    _validate_type(groups, list, "exclude.groups")
    for i, group in enumerate(groups):
        _validate_type(group, str, f"exclude.groups[{i}]")
    
    exclude = ExcludeConfig(topics=topics, groups=groups)
    
    return Config(
        kafka=kafka,
        monitoring=monitoring,
        database=database,
        output=output,
        exclude=exclude
    )
