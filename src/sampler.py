"""Sampler thread module for data collection.

Responsible for sampling Kafka state and writing to the database.
Drives all writes to partition_offsets and consumer_commits tables,
and manages the consumer group state machine transitions.
"""

import logging
import time
from typing import Any, Dict, List, Set, Tuple

import database
import interpolation
from config import Config

logger = logging.getLogger(__name__)


class Sampler:
    """Sampler thread for collecting Kafka offset data.

    Runs in a continuous loop, sampling partition offsets and consumer
    commits, while managing group status state machine transitions.
    """

    def __init__(
        self, config: Config, db_conn: Any, kafka_client: Any, state_manager: Any
    ) -> None:
        """Initialize the sampler.

        Args:
            config: Configuration object
            db_conn: SQLite database connection
            kafka_client: Kafka AdminClient instance
            state_manager: StateManager instance for group status
        """
        self._config = config
        self._db_conn = db_conn
        self._kafka_client = kafka_client
        self._state_manager = state_manager

    def run(self, shutdown_event: Any) -> None:
        """Run the sampler loop until shutdown.

        Args:
            shutdown_event: Threading event to signal shutdown
        """
        from kafka_client import (
            get_active_consumer_groups,
            get_all_consumed_topic_partitions,
            get_latest_produced_offsets,
            get_committed_offsets,
        )

        while not shutdown_event.is_set():
            cycle_start = time.time()

            try:
                # Step 1: Get active consumer groups, filter excluded
                all_groups = get_active_consumer_groups(self._kafka_client)
                active_groups = [
                    g
                    for g in all_groups
                    if not database.is_group_excluded(
                        self._db_conn, g, self._config.exclude.groups
                    )
                ]

                if not active_groups:
                    logger.debug("No active consumer groups to sample")
                else:
                    # Step 2: Get topic/partitions consumed by each group
                    group_topic_partitions = get_all_consumed_topic_partitions(
                        self._kafka_client, active_groups
                    )

                    # Filter excluded topics per group
                    for group_id in group_topic_partitions:
                        group_topic_partitions[group_id] = {
                            (t, p)
                            for t, p in group_topic_partitions[group_id]
                            if not database.is_topic_excluded(
                                self._db_conn, t, self._config.exclude.topics
                            )
                        }

                    # Step 3: Bulk fetch latest produced offsets for all partitions
                    all_topic_partitions = set()
                    for partitions in group_topic_partitions.values():
                        all_topic_partitions.update(partitions)

                    latest_offsets = {}
                    if all_topic_partitions:
                        latest_offsets = get_latest_produced_offsets(
                            self._kafka_client, list(all_topic_partitions)
                        )

                    # Step 4: Process each group
                    for group_id in active_groups:
                        self._process_group(
                            group_id,
                            group_topic_partitions,
                            latest_offsets,
                            cycle_start,
                        )

                # Update thread last run timestamp
                self._state_manager.update_thread_last_run("sampler", int(cycle_start))

            except Exception as e:
                logger.warning(f"Error during sampler cycle: {e}")

            # Step 5: Sleep for remainder of sample interval
            elapsed = time.time() - cycle_start
            sleep_time = self._config.monitoring.sample_interval_seconds - elapsed
            if sleep_time > 0:
                # Check shutdown event during sleep
                shutdown_event.wait(timeout=sleep_time)

    def _process_group(
        self,
        group_id: str,
        group_topic_partitions: Dict[str, Set[Tuple[str, int]]],
        latest_offsets: Dict[Tuple[str, int], int],
        cycle_start: float,
    ) -> None:
        """Process a single consumer group.

        Args:
            group_id: The consumer group ID
            group_topic_partitions: Dict mapping group_id to its topic/partitions
            latest_offsets: Latest produced offsets for all partitions
            cycle_start: Timestamp when the cycle started
        """
        from kafka_client import get_committed_offsets

        group_partitions = group_topic_partitions.get(group_id, set())

        if not group_partitions:
            logger.debug(f"No partitions assigned to group {group_id}")
            return

        # Fetch committed offsets for this group
        committed_offsets = get_committed_offsets(
            self._kafka_client, group_id, group_partitions
        )

        if not committed_offsets:
            logger.debug(f"No committed offsets found for group {group_id}")
            return

        # Group by topic for state machine evaluation
        topic_partitions_for_group: Dict[str, List[int]] = {}
        for (topic, partition), offset in committed_offsets.items():
            if topic not in topic_partitions_for_group:
                topic_partitions_for_group[topic] = []
            topic_partitions_for_group[topic].append(partition)

            # Step 5a: Always write to consumer_commits
            database.insert_consumer_commit(
                self._db_conn, group_id, topic, partition, offset, int(cycle_start)
            )

            # Step 5b-5d: Determine write cadence and write to partition_offsets
            self._write_partition_offset_if_needed(
                topic, partition, latest_offsets.get((topic, partition), 0), cycle_start
            )

        # Step 5e: Evaluate state machine for each topic
        for topic in topic_partitions_for_group:
            self._evaluate_state_machine(
                group_id, topic, committed_offsets, latest_offsets, cycle_start
            )

    def _write_partition_offset_if_needed(
        self, topic: str, partition: int, current_offset: int, cycle_start: float
    ) -> None:
        """Write to partition_offsets if cadence allows.

        Args:
            topic: Topic name
            partition: Partition number
            current_offset: Current partition offset
            cycle_start: Timestamp when the cycle started
        """
        # Get the last write time for this partition
        last_write = database.get_last_write_time(self._db_conn, topic, partition)
        current_time = int(cycle_start)

        # Determine write cadence based on group status
        # Since partition_offsets is shared, we use the coarse cadence
        # (30 minutes) as the threshold to avoid duplicates
        coarse_interval = self._config.monitoring.offline_sample_interval_seconds

        # Check if we should write
        should_write = False

        if last_write is None:
            # No previous write, always write
            should_write = True
        else:
            elapsed = current_time - last_write

            # Get the last stored offset
            interpolation_points = database.get_interpolation_points(
                self._db_conn, topic, partition
            )
            last_stored_offset = (
                interpolation_points[0][0] if interpolation_points else None
            )

            # Write if:
            # 1. Offset has changed, OR
            # 2. Coarse interval has elapsed (to maintain timestamp trail)
            if last_stored_offset is None or current_offset != last_stored_offset:
                should_write = True
            elif elapsed >= coarse_interval:
                should_write = True

        if should_write:
            database.insert_partition_offset(
                self._db_conn, topic, partition, current_offset, current_time
            )

    def _evaluate_state_machine(
        self,
        group_id: str,
        topic: str,
        committed_offsets: Dict[Tuple[str, int], int],
        latest_offsets: Dict[Tuple[str, int], int],
        cycle_start: float,
    ) -> None:
        """Evaluate and transition the state machine for a group/topic.

        Args:
            group_id: Consumer group ID
            topic: Topic name
            committed_offsets: Current committed offsets from Kafka
            latest_offsets: Latest produced offsets from Kafka
            cycle_start: Timestamp when the cycle started
        """
        status = self._state_manager.get_group_status(group_id, topic)
        current_status = status["status"]
        consecutive_static = status["consecutive_static"]
        last_advancing_at = status["last_advancing_at"]
        status_changed_at = status["status_changed_at"]

        partitions = self._get_partitions_for_topic(group_id, topic)

        if not partitions:
            return

        threshold = self._config.monitoring.offline_detection_consecutive_samples
        all_static_with_lag = True
        any_advancing = False

        for partition in partitions:
            recent_commits = database.get_recent_commits(
                self._db_conn, group_id, topic, partition, threshold
            )

            if len(recent_commits) < threshold:
                all_static_with_lag = False
                any_advancing = True
                break

            offsets = [commit[0] for commit in recent_commits]
            committed = offsets[0] if offsets else 0
            produced = latest_offsets.get((topic, partition), 0)
            has_lag = committed < produced

            if len(set(offsets)) == 1 and has_lag:
                pass
            else:
                all_static_with_lag = False
                if len(set(offsets)) != 1:
                    any_advancing = True
                break

        current_time = int(cycle_start)
        new_status = current_status
        new_consecutive_static = consecutive_static
        new_last_advancing_at = last_advancing_at

        if all_static_with_lag:
            new_consecutive_static = consecutive_static + 1

            if new_consecutive_static >= threshold:
                if current_status == "ONLINE":
                    new_status = "OFFLINE"
                elif current_status == "RECOVERING":
                    new_status = "OFFLINE"
        elif any_advancing:
            new_consecutive_static = 0
            new_last_advancing_at = current_time

            if current_status == "OFFLINE":
                new_status = "RECOVERING"
            elif current_status == "RECOVERING":
                lag_threshold = self._config.monitoring.online_lag_threshold_seconds
                min_duration = (
                    self._config.monitoring.recovering_minimum_duration_seconds
                )

                max_lag = self._calculate_max_lag(
                    group_id, topic, partitions, current_time
                )
                time_in_recovering = current_time - status_changed_at

                if max_lag < lag_threshold and time_in_recovering >= min_duration:
                    new_status = "ONLINE"

        # Persist status change if it occurred
        if new_status != current_status:
            logger.info(
                f"State transition for {group_id}/{topic}: "
                f"{current_status} -> {new_status}"
            )
            new_status_changed_at = current_time
        else:
            new_status_changed_at = status_changed_at

        self._state_manager.set_group_status(
            group_id,
            topic,
            new_status,
            new_status_changed_at,
            new_last_advancing_at,
            new_consecutive_static,
        )

    def _get_partitions_for_topic(self, group_id: str, topic: str) -> List[int]:
        """Get all partitions for a group/topic from recent commits.

        Args:
            group_id: Consumer group ID
            topic: Topic name

        Returns:
            List of partition numbers
        """
        # Query distinct partitions from consumer_commits
        cursor = self._db_conn.execute(
            """SELECT DISTINCT partition FROM consumer_commits 
               WHERE group_id = ? AND topic = ?""",
            (group_id, topic),
        )
        return [row[0] for row in cursor.fetchall()]

    def _calculate_max_lag(
        self, group_id: str, topic: str, partitions: List[int], current_time: int
    ) -> int:
        """Calculate the maximum lag across all partitions.

        Args:
            group_id: Consumer group ID
            topic: Topic name
            partitions: List of partition numbers
            current_time: Current timestamp

        Returns:
            Maximum lag in seconds
        """
        max_lag = 0

        for partition in partitions:
            # Get the most recent committed offset
            recent_commits = database.get_recent_commits(
                self._db_conn, group_id, topic, partition, 1
            )

            if not recent_commits:
                continue

            committed_offset = recent_commits[0][0]

            # Get interpolation points
            interpolation_points = database.get_interpolation_points(
                self._db_conn, topic, partition
            )

            # Calculate lag
            lag_seconds, _ = interpolation.calculate_lag_seconds(
                committed_offset, interpolation_points, current_time
            )

            max_lag = max(max_lag, lag_seconds)

        return max_lag
