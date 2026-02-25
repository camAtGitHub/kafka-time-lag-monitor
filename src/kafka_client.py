"""Kafka client abstraction module.

All confluent-kafka library usage is isolated here. No other module
imports from confluent-kafka.
"""

import logging
from typing import Any, Dict, List, Set, Tuple

from config import Config

logger = logging.getLogger(__name__)

# Try to import confluent-kafka, but allow module to be imported without it
# (for testing environments without Kafka dependencies)
try:
    from confluent_kafka import TopicPartition, ConsumerGroupTopicPartitions
    from confluent_kafka.admin import AdminClient, OffsetSpec

    _KAFKA_AVAILABLE = True
except ImportError:
    _KAFKA_AVAILABLE = False
    AdminClient = Any
    TopicPartition = Any
    ConsumerGroupTopicPartitions = Any
    OffsetSpec = Any


def build_admin_client(config: Config) -> AdminClient:
    """Construct and return a configured AdminClient.

    Args:
        config: Configuration object containing Kafka connection settings

    Returns:
        AdminClient: Configured Kafka admin client

    Raises:
        ImportError: If confluent-kafka is not installed
    """
    if not _KAFKA_AVAILABLE:
        raise ImportError("confluent-kafka is not installed")

    conf = {
        "bootstrap.servers": config.kafka.bootstrap_servers,
        "security.protocol": config.kafka.security_protocol,
    }

    # Add optional SASL/TLS configuration if provided
    if config.kafka.sasl_mechanism:
        conf["sasl.mechanism"] = config.kafka.sasl_mechanism
    if config.kafka.sasl_username:
        conf["sasl.username"] = config.kafka.sasl_username
    if config.kafka.sasl_password:
        conf["sasl.password"] = config.kafka.sasl_password
    if config.kafka.ssl_ca_location:
        conf["ssl.ca.location"] = config.kafka.ssl_ca_location

    # Warn if security protocol requires SASL but credentials are missing
    if config.kafka.security_protocol in ("SASL_PLAINTEXT", "SASL_SSL"):
        if not config.kafka.sasl_mechanism or not config.kafka.sasl_username:
            logger.warning(
                f"security_protocol={config.kafka.security_protocol} but SASL credentials "
                "are incomplete. Configure sasl_mechanism, sasl_username, sasl_password."
            )

    return AdminClient(conf)


def get_active_consumer_groups(admin_client: AdminClient) -> List[str]:
    """List all active consumer group IDs.

    Args:
        admin_client: Configured AdminClient instance

    Returns:
        List of group_id strings. Empty list on error.
    """
    try:
        future = admin_client.list_consumer_groups()
        result = future.result()

        group_ids = []
        for group in result.valid:
            group_ids.append(group.group_id)

        if result.errors:
            for error in result.errors:
                logger.warning(f"Error listing consumer groups: {error}")

        return group_ids
    except Exception as e:
        logger.warning(f"Failed to list consumer groups: {e}")
        return []


def get_committed_offsets(
    admin_client: AdminClient, group_id: str, topic_partitions: List[Tuple[str, int]]
) -> Dict[Tuple[str, int], int]:
    """Get committed offsets for a consumer group.

    Args:
        admin_client: Configured AdminClient instance
        group_id: Consumer group ID
        topic_partitions: List of (topic, partition) tuples

    Returns:
        Dict mapping (topic, partition) to committed offset.
        Empty dict on error.
    """
    try:
        tps = [
            TopicPartition(topic, partition) for topic, partition in topic_partitions
        ]

        groups = [ConsumerGroupTopicPartitions(group_id, tps)]
        future_map = admin_client.list_consumer_group_offsets(groups)

        offsets = {}
        for cg_id, future in future_map.items():
            try:
                result = future.result()
                for tp in result.topic_partitions:
                    if tp.error is None and tp.offset >= 0:
                        offsets[(tp.topic, tp.partition)] = tp.offset
                    elif tp.error:
                        logger.warning(
                            f"Error getting offset for {group_id}/{tp.topic}/{tp.partition}: {tp.error}"
                        )
            except Exception as e:
                logger.warning(f"Error getting offsets for {cg_id}: {e}")

        return offsets
    except Exception as e:
        logger.warning(f"Failed to get committed offsets for group {group_id}: {e}")
        return {}


def get_latest_produced_offsets(
    admin_client: AdminClient, topic_partitions: List[Tuple[str, int]]
) -> Dict[Tuple[str, int], int]:
    """Get the latest (high watermark) offsets for topic partitions.

    Args:
        admin_client: Configured AdminClient instance
        topic_partitions: List of (topic, partition) tuples

    Returns:
        Dict mapping (topic, partition) to latest offset.
        Empty dict on error.
    """
    try:
        request = {}
        for topic, partition in topic_partitions:
            request[TopicPartition(topic, partition)] = OffsetSpec.latest()

        results = admin_client.list_offsets(request)

        offsets = {}
        for tp_obj, future in results.items():
            try:
                result = future.result()
                # ListOffsetsResultInfo has 'offset' but no 'error' attribute
                offsets[(tp_obj.topic, tp_obj.partition)] = result.offset
            except Exception as e:
                logger.warning(
                    f"Exception getting latest offset for {tp_obj.topic}/{tp_obj.partition}: {e}"
                )

        return offsets
    except Exception as e:
        logger.warning(f"Failed to get latest produced offsets: {e}")
        return {}


def get_all_consumed_topic_partitions(
    admin_client: AdminClient, group_ids: List[str]
) -> Dict[str, Set[Tuple[str, int]]]:
    """Get all (topic, partition) tuples consumed by each group.

    Args:
        admin_client: Configured AdminClient instance
        group_ids: List of consumer group IDs

    Returns:
        Dict mapping group_id to Set of (topic, partition) tuples.
        Empty dict on error.
    """
    try:
        futures = admin_client.describe_consumer_groups(group_ids)

        result: Dict[str, Set[Tuple[str, int]]] = {}

        for group_id, future in futures.items():
            group_partitions: Set[Tuple[str, int]] = set()
            try:
                description = future.result()

                for member in description.members:
                    if member.assignment:
                        for topic_partition in member.assignment.topic_partitions:
                            group_partitions.add(
                                (topic_partition.topic, topic_partition.partition)
                            )
            except Exception as e:
                logger.warning(f"Error describe consumer group {group_id}: {e}")

            result[group_id] = group_partitions

        return result
    except Exception as e:
        logger.warning(f"Failed to get consumed topic partitions: {e}")
        return {}
