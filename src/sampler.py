"""Sampler thread module for data collection.

Responsible for sampling Kafka state and writing to the database.
Drives all writes to partition_offsets and consumer_commits tables,
and manages the consumer group state machine transitions.
"""

import logging
import sqlite3
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
        self, config: Config, db_path: str, kafka_client: Any, state_manager: Any
    ) -> None:
        """Initialize the sampler.

        Args:
            config: Configuration object
            db_path: Path to the SQLite database file
            kafka_client: Kafka AdminClient instance
            state_manager: StateManager instance for group status
        """
        self._config = config
        self._db_path = db_path
        self._db_conn = database.get_connection(db_path)
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

                # Step 2: Get topic/partitions consumed by each group
                group_topic_partitions = {}
                if active_groups:
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

                # Augment with idle groups' historical partitions
                # Track idle partitions separately for write pass
                idle_topic_partitions = set()
                idle_groups_with_history = set()  # Track which groups are idle
                all_group_ids_with_history = database.get_all_groups_with_history(
                    self._db_conn
                )
                active_group_set = set(active_groups)
                for group_id in all_group_ids_with_history:
                    if group_id not in active_group_set:
                        idle_groups_with_history.add(group_id)
                        tracked_topics = database.get_group_tracked_topics(
                            self._db_conn, group_id
                        )
                        for topic in tracked_topics:
                            partitions = self._get_partitions_for_topic(
                                group_id, topic
                            )
                            for partition in partitions:
                                partition_tuple = (topic, partition)
                                all_topic_partitions.add(partition_tuple)
                                idle_topic_partitions.add(partition_tuple)

                latest_offsets = {}
                if all_topic_partitions:
                    latest_offsets = get_latest_produced_offsets(
                        self._kafka_client, list(all_topic_partitions)
                    )

                # Step 3b: Write partition offsets for idle group partitions
                # These won't be written by _process_group since they're not in active_groups
                for topic, partition in idle_topic_partitions:
                    if (topic, partition) in latest_offsets:
                        self._write_partition_offset_if_needed(
                            topic,
                            partition,
                            latest_offsets[(topic, partition)],
                            cycle_start,
                        )

                # Step 3c: Mark idle groups as OFFLINE (transition only — no repeat writes)
                for group_id in idle_groups_with_history:
                    self._handle_idle_group(group_id, cycle_start)

                # Step 3d: Retire ghost groups (absent from Kafka past retention period)
                self._retire_ghost_groups(idle_groups_with_history, cycle_start)

                # Step 4: Process each group
                for group_id in active_groups:
                    self._process_group(
                        group_id,
                        group_topic_partitions,
                        latest_offsets,
                        cycle_start,
                    )

                # Step 5: Persist group statuses to database
                # This folds all group status updates into the sampler's transaction
                self._state_manager.persist_group_statuses(self._db_conn)

                # Batch commit all writes at end of cycle
                database.commit_batch(self._db_conn)

                # Update thread last run timestamp
                self._state_manager.update_thread_last_run("sampler", int(cycle_start))

            except Exception as e:
                logger.warning(f"Error during sampler cycle: {e}")
                if isinstance(e, sqlite3.DatabaseError):
                    logger.warning("DB error detected — reconnecting before next cycle")
                    try:
                        self._db_conn.close()
                    except Exception:
                        pass
                    self._db_conn = database.get_connection(self._db_path)

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
            # Group has no active partitions — handle idle status transition
            self._handle_idle_group(group_id, cycle_start)

            # If group has DB history, write partition_offsets to keep them fresh
            # This handles "ghost groups" — active in Kafka but zero-member
            if database.has_group_history(self._db_conn, group_id):
                tracked_topics = database.get_group_tracked_topics(self._db_conn, group_id)
                for topic in tracked_topics:
                    partitions = self._get_partitions_for_topic(group_id, topic)
                    for partition in partitions:
                        if (topic, partition) in latest_offsets:
                            self._write_partition_offset_if_needed(
                                topic,
                                partition,
                                latest_offsets[(topic, partition)],
                                cycle_start,
                            )
            return

        # Fetch committed offsets for this group
        committed_offsets = get_committed_offsets(
            self._kafka_client, group_id, list(group_partitions)
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
            last_stored_offset = database.get_last_stored_offset(
                self._db_conn, topic, partition
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

        # Extract partitions from committed_offsets for this topic
        partitions = [
            partition
            for (t, partition) in committed_offsets.keys()
            if t == topic
        ]

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
        else:
            # Caught up (no lag) or insufficient history — clear counter
            new_consecutive_static = 0

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

    def _handle_idle_group(self, group_id: str, cycle_start: float) -> None:
        """Handle a group with no active partitions.

        Checks current state for each tracked topic. Only logs WARNING and
        writes OFFLINE status on the first cycle the group is detected as idle
        (i.e. a genuine state transition). Subsequent cycles where the group
        is already OFFLINE produce a DEBUG log only — no DB write.

        Args:
            group_id: The consumer group ID
            cycle_start: Timestamp when the cycle started
        """
        has_history = database.has_group_history(self._db_conn, group_id)

        if not has_history:
            logger.debug(f"No partitions assigned to group {group_id}")
            return

        tracked_topics = database.get_group_tracked_topics(self._db_conn, group_id)
        current_time = int(cycle_start)

        for topic in tracked_topics:
            existing_status = self._state_manager.get_group_status(group_id, topic)
            current_status = existing_status.get("status", "ONLINE")
            last_advancing_at = existing_status.get("last_advancing_at", 0)

            if current_status == "OFFLINE":
                # Already OFFLINE from a previous cycle — steady state, no action needed.
                # Retirement (if applicable) is handled separately by _retire_ghost_groups.
                logger.debug(
                    f"Group {group_id}/{topic} remains OFFLINE (no active partitions)"
                )
                continue

            # This is a genuine transition: group was ONLINE or RECOVERING and has
            # now lost all partitions. Log once and write the status change.
            logger.warning(
                f"Group {group_id} has no active partitions but has DB history "
                f"for topic '{topic}'. Transitioning {current_status} -> OFFLINE."
            )
            self._state_manager.set_group_status(
                group_id,
                topic,
                "OFFLINE",
                current_time,
                last_advancing_at,
                self._config.monitoring.offline_detection_consecutive_samples,
            )

    def _retire_ghost_groups(
        self, idle_groups_with_history: set, cycle_start: float
    ) -> None:
        """Retire groups that are absent from Kafka and have been OFFLINE long enough.

        Only called for groups in idle_groups_with_history — the set of groups
        that do not appear in get_active_consumer_groups() at all. Groups that
        ARE in Kafka (even with no active members / Empty state) are not retired
        here; their lifecycle is managed by the Kafka administrator.

        A group is retired when ALL of the following are true:
          - It is absent from Kafka entirely (in idle_groups_with_history)
          - Every tracked topic for the group has status OFFLINE
          - The earliest status_changed_at across all topics is older than
            absent_group_retention_seconds

        On retirement:
          - All consumer_commits rows for the group are deleted
          - All group_status rows for the group are deleted
          - In-memory state is cleared via state_manager.remove_group()
          - An INFO log is emitted with the group ID and how long it was OFFLINE

        partition_offsets is NOT cleaned up here — it is keyed by topic/partition
        only and shared across groups. Housekeeping prunes it by row count naturally.

        Args:
            idle_groups_with_history: Set of group IDs absent from Kafka but with
                                      historical data in the database
            cycle_start: Timestamp when the cycle started
        """
        retention = self._config.monitoring.absent_group_retention_seconds
        current_time = int(cycle_start)

        for group_id in idle_groups_with_history:
            tracked_topics = database.get_group_tracked_topics(
                self._db_conn, group_id
            )

            if not tracked_topics:
                continue

            # All topics must be OFFLINE, and we find the earliest OFFLINE timestamp
            should_retire = True
            earliest_offline_at = current_time

            for topic in tracked_topics:
                status = self._state_manager.get_group_status(group_id, topic)

                if status.get("status") != "OFFLINE":
                    # Group has a topic not yet OFFLINE — not ready for retirement.
                    # This can happen on the same cycle _handle_idle_group transitions
                    # it, since retirement runs after idle handling.
                    should_retire = False
                    break

                topic_offline_since = status.get("status_changed_at", current_time)
                earliest_offline_at = min(earliest_offline_at, topic_offline_since)

            if not should_retire:
                continue

            time_offline_seconds = current_time - earliest_offline_at

            if time_offline_seconds < retention:
                logger.debug(
                    f"Ghost group {group_id} has been OFFLINE for "
                    f"{time_offline_seconds // 3600:.1f}h — "
                    f"retention period is {retention // 3600:.1f}h, not retiring yet."
                )
                continue

            # Retention period exceeded — retire the group
            logger.info(
                f"Retiring ghost group '{group_id}': absent from Kafka, "
                f"OFFLINE for {time_offline_seconds // 3600:.1f}h "
                f"(retention threshold: {retention // 3600:.1f}h). "
                f"Removing from database. Topics were: {tracked_topics}"
            )

            database.delete_group_data(self._db_conn, group_id)
            self._state_manager.remove_group(group_id)
