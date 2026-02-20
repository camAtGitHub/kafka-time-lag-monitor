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
        'bootstrap.servers': config.kafka.bootstrap_servers,
        'security.protocol': config.kafka.security_protocol,
    }
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
    admin_client: AdminClient,
    group_id: str,
    topic_partitions: List[Tuple[str, int]]
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
        tps = [TopicPartition(topic, partition) for topic, partition in topic_partitions]
        
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
    admin_client: AdminClient,
    topic_partitions: List[Tuple[str, int]]
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


def get_topic_partition_count(admin_client: AdminClient, topic: str) -> int:
    """Get the number of partitions for a topic.
    
    Args:
        admin_client: Configured AdminClient instance
        topic: Topic name
        
    Returns:
        Number of partitions. 0 on error.
    """
    try:
        future_map = admin_client.describe_topics([topic])
        future = future_map[topic]
        result = future.result()
        
        if result.error is not None:
            logger.warning(f"Error describing topic {topic}: {result.error}")
            return 0
        
        return len(result.partitions)
    except Exception as e:
        logger.warning(f"Failed to get partition count for topic {topic}: {e}")
        return 0


def get_all_consumed_topic_partitions(
    admin_client: AdminClient,
    group_ids: List[str]
) -> Set[Tuple[str, int]]:
    """Get all unique (topic, partition) tuples consumed by the given groups.
    
    Args:
        admin_client: Configured AdminClient instance
        group_ids: List of consumer group IDs
        
    Returns:
        Set of (topic, partition) tuples. Empty set on error.
    """
    try:
        # Describe all consumer groups to get their subscriptions
        futures = admin_client.describe_consumer_groups(group_ids)
        
        topic_partitions = set()
        
        for group_id, future in futures.items():
            try:
                description = future.result()
                
                # Extract topic partitions from member assignments
                for member in description.members:
                    if member.assignment:
                        for topic_partition in member.assignment.topic_partitions:
                            topic_partitions.add(
                                (topic_partition.topic, topic_partition.partition)
                            )
            except Exception as e:
                logger.warning(f"Error describing consumer group {group_id}: {e}")
        
        return topic_partitions
    except Exception as e:
        logger.warning(f"Failed to get consumed topic partitions: {e}")
        return set()
