"""Tests for sampler.py module."""

import pytest
import sqlite3
import time
from unittest.mock import MagicMock, patch

import database
from config import (
    Config,
    KafkaConfig,
    MonitoringConfig,
    DatabaseConfig,
    OutputConfig,
    ExcludeConfig,
)
import sampler
from sampler import Sampler


class MockStateManager:
    """Mock StateManager for testing."""

    def __init__(self):
        self._statuses = {}
        self._thread_last_run = {}
        self._last_json_output = None

    def get_group_status(self, group_id: str, topic: str):
        key = (group_id, topic)
        if key in self._statuses:
            return self._statuses[key]
        return {
            "status": "ONLINE",
            "status_changed_at": 0,
            "last_advancing_at": 0,
            "consecutive_static": 0,
        }

    def set_group_status(
        self,
        group_id: str,
        topic: str,
        status: str,
        status_changed_at: int,
        last_advancing_at: int,
        consecutive_static: int,
    ):
        key = (group_id, topic)
        self._statuses[key] = {
            "status": status,
            "status_changed_at": status_changed_at,
            "last_advancing_at": last_advancing_at,
            "consecutive_static": consecutive_static,
        }

    def get_all_group_statuses(self):
        return self._statuses.copy()

    def update_thread_last_run(self, thread_name: str, timestamp: int):
        self._thread_last_run[thread_name] = timestamp


class MockKafkaClient:
    """Mock Kafka client for testing."""

    def __init__(self, groups=None, committed_offsets=None, latest_offsets=None):
        self._groups = groups or []
        self._committed_offsets = committed_offsets or {}
        self._latest_offsets = latest_offsets or {}

    def get_active_consumer_groups(self):
        return self._groups

    def get_committed_offsets(self, group_id, topic_partitions):
        return self._committed_offsets

    def get_latest_produced_offsets(self, topic_partitions):
        return self._latest_offsets

    def get_all_consumed_topic_partitions(self, groups):
        return {g: list(self._latest_offsets.keys()) for g in groups}


def make_test_config():
    """Create a test configuration."""
    return Config(
        kafka=KafkaConfig(bootstrap_servers="localhost:9092"),
        monitoring=MonitoringConfig(
            sample_interval_seconds=60,
            offline_sample_interval_seconds=1800,
            report_interval_seconds=60,
            housekeeping_interval_seconds=300,
            max_entries_per_partition=300,
            max_commit_entries_per_partition=200,
            offline_detection_consecutive_samples=3,
            recovering_minimum_duration_seconds=300,
            online_lag_threshold_seconds=60,
        ),
        database=DatabaseConfig(path=":memory:"),
        output=OutputConfig(json_path="/tmp/test.json"),
        exclude=ExcludeConfig(topics=[], groups=[]),
    )


class TestSamplerOnlineGroup:
    """Tests for ONLINE group write behavior."""

    def test_online_group_write_issued_when_cadence_elapsed(self, db_path, db_conn):
        """ONLINE group — write is issued when cadence interval has elapsed"""
        config = make_test_config()
        state_manager = MockStateManager()
        state_manager.set_group_status("group1", "topic1", "ONLINE", 0, 0, 0)

        topic_partitions = {("topic1", 0): 100}
        kafka_client = MockKafkaClient(
            groups=["group1"],
            committed_offsets={("topic1", 0): 50},
            latest_offsets=topic_partitions,
        )

        s = Sampler(config, db_path, kafka_client, state_manager)

        shutdown_event = __import__("threading").Event()

        s._write_partition_offset_if_needed("topic1", 0, 100, time.time())

        rows = db_conn.execute(
            "SELECT topic, partition, offset FROM partition_offsets"
        ).fetchall()

        assert len(rows) == 1
        assert rows[0][0] == "topic1"
        assert rows[0][1] == 0
        assert rows[0][2] == 100

    def test_online_group_write_skipped_when_cadence_not_elapsed(
        self, db_path, db_conn
    ):
        """ONLINE group — write is skipped when cadence interval has not elapsed"""
        config = make_test_config()
        state_manager = MockStateManager()

        db_conn.execute(
            """INSERT INTO partition_offsets (topic, partition, offset, sampled_at)
               VALUES (?, ?, ?, ?)""",
            ("topic1", 0, 100, int(time.time())),
        )
        db_conn.commit()

        topic_partitions = {("topic1", 0): 100}
        kafka_client = MockKafkaClient(
            groups=["group1"],
            committed_offsets={("topic1", 0): 50},
            latest_offsets=topic_partitions,
        )

        s = Sampler(config, db_path, kafka_client, state_manager)

        s._write_partition_offset_if_needed("topic1", 0, 100, time.time())

        rows = db_conn.execute("SELECT COUNT(*) FROM partition_offsets").fetchone()[0]

        assert rows == 1


class TestSamplerOfflineGroup:
    """Tests for OFFLINE group write behavior."""

    def test_offline_group_write_coarse_cadence(self, db_path, db_conn):
        """OFFLINE group — write uses coarse cadence interval"""
        config = make_test_config()
        state_manager = MockStateManager()

        db_conn.execute(
            """INSERT INTO partition_offsets (topic, partition, offset, sampled_at)
               VALUES (?, ?, ?, ?)""",
            ("topic1", 0, 100, int(time.time()) - 1900),
        )
        db_conn.commit()

        topic_partitions = {("topic1", 0): 100}
        kafka_client = MockKafkaClient(
            groups=["group1"],
            committed_offsets={("topic1", 0): 50},
            latest_offsets=topic_partitions,
        )

        s = Sampler(config, db_path, kafka_client, state_manager)

        s._write_partition_offset_if_needed("topic1", 0, 100, time.time())

        rows = db_conn.execute("SELECT COUNT(*) FROM partition_offsets").fetchone()[0]

        assert rows == 2


class TestSamplerConsumerCommits:
    """Tests for consumer_commits write behavior."""

    @patch("kafka_client.get_committed_offsets")
    def test_consumer_commits_always_written(
        self, mock_get_committed, db_path, db_conn
    ):
        """consumer_commits is always written regardless of group status or cadence"""
        config = make_test_config()
        state_manager = MockStateManager()

        mock_get_committed.return_value = {("topic1", 0): 50}

        topic_partitions = {("topic1", 0): 100}

        # Use a mock kafka client
        mock_kafka = MagicMock()
        mock_kafka.get_active_consumer_groups.return_value = ["group1"]
        mock_kafka.get_all_consumed_topic_partitions.return_value = {
            "group1": set(topic_partitions.keys())
        }

        s = Sampler(config, db_path, mock_kafka, state_manager)

        current_time = int(time.time())
        s._process_group(
            "group1",
            {"group1": set(topic_partitions.keys())},
            topic_partitions,
            current_time,
        )

        rows = db_conn.execute(
            "SELECT group_id, topic, partition, committed_offset FROM consumer_commits"
        ).fetchall()

        assert len(rows) == 1
        assert rows[0][0] == "group1"
        assert rows[0][1] == "topic1"
        assert rows[0][2] == 0
        assert rows[0][3] == 50


class TestSamplerStateMachine:
    """Tests for state machine transitions."""

    def test_online_to_offline_transition(self, db_path, db_conn):
        """State machine ONLINE→OFFLINE transition - requires static offsets AND lag"""
        config = make_test_config()
        state_manager = MockStateManager()

        current_time = int(time.time())
        threshold = config.monitoring.offline_detection_consecutive_samples

        state_manager.set_group_status(
            "group1", "topic1", "ONLINE", current_time, 0, threshold - 1
        )

        for i in range(threshold):
            db_conn.execute(
                """INSERT INTO consumer_commits (group_id, topic, partition, committed_offset, recorded_at)
                   VALUES (?, ?, ?, ?, ?)""",
                ("group1", "topic1", 0, 100, current_time - (threshold - i - 1) * 60),
            )
        db_conn.commit()

        kafka_client = MockKafkaClient()

        s = Sampler(config, db_path, kafka_client, state_manager)

        committed_offsets = {("topic1", 0): 100}
        latest_offsets = {("topic1", 0): 200}

        s._evaluate_state_machine(
            "group1", "topic1", committed_offsets, latest_offsets, current_time
        )

        status = state_manager.get_group_status("group1", "topic1")

        assert status["status"] == "OFFLINE"
        assert status["consecutive_static"] >= threshold

    def test_offline_to_recovering_transition(self, db_path, db_conn):
        """State machine OFFLINE→RECOVERING"""
        config = make_test_config()
        state_manager = MockStateManager()

        current_time = int(time.time())
        threshold = config.monitoring.offline_detection_consecutive_samples

        state_manager.set_group_status(
            "group1", "topic1", "OFFLINE", current_time - 1000, 0, threshold
        )

        for i in range(threshold):
            db_conn.execute(
                """INSERT INTO consumer_commits (group_id, topic, partition, committed_offset, recorded_at)
                   VALUES (?, ?, ?, ?, ?)""",
                (
                    "group1",
                    "topic1",
                    0,
                    100 + i,
                    current_time - (threshold - i - 1) * 60,
                ),
            )
        db_conn.commit()

        kafka_client = MockKafkaClient()

        s = Sampler(config, db_path, kafka_client, state_manager)

        committed_offsets = {("topic1", 0): 102}
        latest_offsets = {("topic1", 0): 200}

        s._evaluate_state_machine(
            "group1", "topic1", committed_offsets, latest_offsets, current_time
        )

        status = state_manager.get_group_status("group1", "topic1")

        assert status["status"] == "RECOVERING"

    def test_recovering_to_online_transition(self, db_path, db_conn):
        """State machine RECOVERING→ONLINE"""
        config = make_test_config()
        state_manager = MockStateManager()

        current_time = int(time.time())
        threshold = config.monitoring.offline_detection_consecutive_samples
        min_duration = config.monitoring.recovering_minimum_duration_seconds
        lag_threshold = config.monitoring.online_lag_threshold_seconds

        status_changed_at = current_time - min_duration - 1
        state_manager.set_group_status(
            "group1", "topic1", "RECOVERING", status_changed_at, current_time - 60, 0
        )

        db_conn.execute(
            """INSERT INTO partition_offsets (topic, partition, offset, sampled_at)
               VALUES (?, ?, ?, ?)""",
            ("topic1", 0, 100, current_time - lag_threshold + 1),
        )
        db_conn.commit()

        db_conn.execute(
            """INSERT INTO consumer_commits (group_id, topic, partition, committed_offset, recorded_at)
               VALUES (?, ?, ?, ?, ?)""",
            ("group1", "topic1", 0, 99, current_time),
        )
        db_conn.execute(
            """INSERT INTO consumer_commits (group_id, topic, partition, committed_offset, recorded_at)
               VALUES (?, ?, ?, ?, ?)""",
            ("group1", "topic1", 0, 98, current_time - 10),
        )
        db_conn.execute(
            """INSERT INTO consumer_commits (group_id, topic, partition, committed_offset, recorded_at)
               VALUES (?, ?, ?, ?, ?)""",
            ("group1", "topic1", 0, 97, current_time - 20),
        )
        db_conn.commit()

        kafka_client = MockKafkaClient()

        s = Sampler(config, db_path, kafka_client, state_manager)

        committed_offsets = {("topic1", 0): 99}
        latest_offsets = {("topic1", 0): 100}

        s._evaluate_state_machine(
            "group1", "topic1", committed_offsets, latest_offsets, current_time
        )

        status = state_manager.get_group_status("group1", "topic1")

        assert status["status"] == "ONLINE"

    def test_recovering_to_offline_transition(self, db_path, db_conn):
        """State machine RECOVERING→OFFLINE - requires static offsets AND lag"""
        config = make_test_config()
        state_manager = MockStateManager()

        current_time = int(time.time())
        threshold = config.monitoring.offline_detection_consecutive_samples

        state_manager.set_group_status(
            "group1",
            "topic1",
            "RECOVERING",
            current_time - 1000,
            current_time - 100,
            threshold - 1,
        )

        for i in range(threshold):
            db_conn.execute(
                """INSERT INTO consumer_commits (group_id, topic, partition, committed_offset, recorded_at)
                   VALUES (?, ?, ?, ?, ?)""",
                ("group1", "topic1", 0, 100, current_time - (threshold - i - 1) * 60),
            )
        db_conn.commit()

        kafka_client = MockKafkaClient()

        s = Sampler(config, db_path, kafka_client, state_manager)

        committed_offsets = {("topic1", 0): 100}
        latest_offsets = {("topic1", 0): 200}

        s._evaluate_state_machine(
            "group1", "topic1", committed_offsets, latest_offsets, current_time
        )

        status = state_manager.get_group_status("group1", "topic1")

        assert status["status"] == "OFFLINE"

    def test_static_without_lag_stays_online(self, db_path, db_conn):
        """Static offsets without lag should NOT transition to OFFLINE (low-flow topic)"""
        config = make_test_config()
        state_manager = MockStateManager()

        current_time = int(time.time())
        threshold = config.monitoring.offline_detection_consecutive_samples

        state_manager.set_group_status(
            "group1", "topic1", "ONLINE", current_time, 0, threshold - 1
        )

        for i in range(threshold):
            db_conn.execute(
                """INSERT INTO consumer_commits (group_id, topic, partition, committed_offset, recorded_at)
                   VALUES (?, ?, ?, ?, ?)""",
                ("group1", "topic1", 0, 100, current_time - (threshold - i - 1) * 60),
            )
        db_conn.commit()

        kafka_client = MockKafkaClient()

        s = Sampler(config, db_path, kafka_client, state_manager)

        committed_offsets = {("topic1", 0): 100}
        latest_offsets = {("topic1", 0): 100}

        s._evaluate_state_machine(
            "group1", "topic1", committed_offsets, latest_offsets, current_time
        )

        status = state_manager.get_group_status("group1", "topic1")

        assert status["status"] == "ONLINE"


class TestSamplerErrorHandling:
    """Tests for error handling in sampler."""

    def test_database_write_error_handled_gracefully(self, db_path, db_conn, caplog):
        """Test that database write errors are caught and logged, not raised."""
        import logging
        import threading
        from unittest.mock import MagicMock, patch

        caplog.set_level(logging.DEBUG)

        config = make_test_config()
        state_manager = MockStateManager()

        mock_kafka = MagicMock()
        mock_kafka.get_active_consumer_groups.return_value = ["group1"]
        mock_kafka.get_all_consumed_topic_partitions.return_value = {
            "group1": {("topic1", 0)}
        }
        mock_kafka.get_latest_produced_offsets.return_value = {("topic1", 0): 100}
        mock_kafka.get_committed_offsets.return_value = {("topic1", 0): 50}

        with patch(
            "sampler.database.insert_consumer_commit",
            side_effect=sqlite3.OperationalError("database is locked"),
        ):
            s = Sampler(config, db_path, mock_kafka, state_manager)

            shutdown_event = threading.Event()
            shutdown_event.set()

            s.run(shutdown_event)


class TestSamplerRunLoop:
    """Tests for sampler run loop."""

    def test_run_respects_shutdown_event(self, db_path, db_conn):
        """Sampler run loop respects shutdown event"""
        config = make_test_config()
        state_manager = MockStateManager()

        mock_kafka = MagicMock()
        mock_kafka.get_active_consumer_groups.return_value = []

        s = Sampler(config, db_path, mock_kafka, state_manager)

        shutdown_event = __import__("threading").Event()
        shutdown_event.set()

        start = time.time()
        s.run(shutdown_event)
        elapsed = time.time() - start

        assert elapsed < 1

    def test_kafka_failure_does_not_crash(self, db_path, db_conn):
        """Kafka call failure (mock returns empty) — sampler cycle completes without exception"""
        config = make_test_config()
        state_manager = MockStateManager()

        mock_kafka = MagicMock()
        mock_kafka.get_active_consumer_groups.side_effect = Exception("Kafka error")

        s = Sampler(config, db_path, mock_kafka, state_manager)

        shutdown_event = __import__("threading").Event()
        shutdown_event.set()  # Set to avoid timeout

        s.run(shutdown_event)

        assert True

    def test_thread_last_run_updated(self, db_path, db_conn):
        """Sampler updates thread_last_run timestamp"""
        config = make_test_config()
        state_manager = MockStateManager()

        mock_kafka = MagicMock()
        mock_kafka.get_active_consumer_groups.return_value = []  # Empty groups - short cycle

        s = Sampler(config, db_path, mock_kafka, state_manager)

        # Use threading to set shutdown after a short delay
        import threading

        shutdown_event = threading.Event()

        def set_shutdown():
            time.sleep(0.1)
            shutdown_event.set()

        t = threading.Thread(target=set_shutdown)
        t.start()

        s.run(shutdown_event)

        t.join()

        last_run = state_manager._thread_last_run.get("sampler")
        assert last_run is not None
        assert last_run > 0


class TestSamplerGetPartitions:
    """Tests for _get_partitions_for_topic method."""

    def test_get_partitions_from_commits(self, db_path, db_conn):
        """_get_partitions_for_topic returns partitions from consumer_commits"""
        config = make_test_config()
        state_manager = MockStateManager()

        db_conn.execute(
            """INSERT INTO consumer_commits (group_id, topic, partition, committed_offset, recorded_at)
               VALUES (?, ?, ?, ?, ?)""",
            ("group1", "topic1", 0, 100, 1000),
        )
        db_conn.execute(
            """INSERT INTO consumer_commits (group_id, topic, partition, committed_offset, recorded_at)
               VALUES (?, ?, ?, ?, ?)""",
            ("group1", "topic1", 1, 200, 1000),
        )
        db_conn.execute(
            """INSERT INTO consumer_commits (group_id, topic, partition, committed_offset, recorded_at)
               VALUES (?, ?, ?, ?, ?)""",
            ("group1", "topic1", 2, 300, 1000),
        )
        db_conn.commit()

        mock_kafka = MagicMock()

        s = Sampler(config, db_path, mock_kafka, state_manager)

        partitions = s._get_partitions_for_topic("group1", "topic1")

        assert sorted(partitions) == [0, 1, 2]


class TestSamplerPerGroupPartitions:
    """Tests for per-group partition filtering."""

    @patch("kafka_client.get_committed_offsets")
    def test_process_group_uses_only_its_own_partitions(
        self, mock_get_committed_offsets, db_path, db_conn
    ):
        """Each group should only query its own partitions, not all partitions."""
        config = make_test_config()
        state_manager = MockStateManager()

        mock_kafka = MagicMock()

        group_topic_partitions = {
            "group1": {("topic1", 0), ("topic1", 1)},
            "group2": {("topic2", 0), ("topic2", 1)},
        }
        latest_offsets = {
            ("topic1", 0): 100,
            ("topic1", 1): 100,
            ("topic2", 0): 200,
            ("topic2", 1): 200,
        }
        mock_get_committed_offsets.return_value = {("topic1", 0): 50, ("topic1", 1): 60}

        s = Sampler(config, db_path, mock_kafka, state_manager)

        s._process_group(
            "group1",
            group_topic_partitions,
            latest_offsets,
            time.time(),
        )

        mock_get_committed_offsets.assert_called_once()
        call_args = mock_get_committed_offsets.call_args
        partitions_arg = call_args[0][2]

        assert set(partitions_arg) == {("topic1", 0), ("topic1", 1)}

    def test_group_with_no_partitions_logs_debug(self, db_path, db_conn, caplog):
        """Group with no assigned partitions should log debug and return early."""
        import logging

        caplog.set_level(logging.DEBUG)

        config = make_test_config()
        state_manager = MockStateManager()

        mock_kafka = MagicMock()

        s = Sampler(config, db_path, mock_kafka, state_manager)

        s._process_group(
            "empty_group",
            {"empty_group": set()},
            {},
            time.time(),
        )

        assert "No partitions assigned to group empty_group" in caplog.text


class TestSamplerIdleGroup:
    """Tests for idle/ghost group handling."""

    def test_idle_group_with_db_history_logs_warning(self, db_path_initialized, caplog):
        """Idle group with DB history should log WARNING."""
        import logging

        caplog.set_level(logging.WARNING)

        config = make_test_config()
        state_manager = MockStateManager()

        db_conn = database.init_db(db_path_initialized)
        db_conn.execute(
            """INSERT INTO consumer_commits (group_id, topic, partition, committed_offset, recorded_at)
               VALUES (?, ?, ?, ?, ?)""",
            ("idle_group", "topic1", 0, 100, 1000),
        )
        db_conn.commit()
        db_conn.close()

        mock_kafka = MagicMock()

        s = Sampler(config, db_path_initialized, mock_kafka, state_manager)

        current_time = time.time()
        s._handle_idle_group("idle_group", current_time)

        assert "idle_group has no active partitions but has DB history" in caplog.text

    def test_idle_group_with_db_history_marks_offline(
        self, db_path_initialized, caplog
    ):
        """Idle group with DB history should be marked as OFFLINE."""
        import logging

        caplog.set_level(logging.WARNING)

        config = make_test_config()
        state_manager = MockStateManager()

        db_conn = database.init_db(db_path_initialized)
        db_conn.execute(
            """INSERT INTO consumer_commits (group_id, topic, partition, committed_offset, recorded_at)
               VALUES (?, ?, ?, ?, ?)""",
            ("idle_group", "topic1", 0, 100, 1000),
        )
        db_conn.execute(
            """INSERT INTO consumer_commits (group_id, topic, partition, committed_offset, recorded_at)
               VALUES (?, ?, ?, ?, ?)""",
            ("idle_group", "topic2", 0, 200, 1000),
        )
        db_conn.commit()
        db_conn.close()

        mock_kafka = MagicMock()

        s = Sampler(config, db_path_initialized, mock_kafka, state_manager)

        current_time = time.time()
        s._handle_idle_group("idle_group", current_time)

        status1 = state_manager.get_group_status("idle_group", "topic1")
        status2 = state_manager.get_group_status("idle_group", "topic2")

        assert status1["status"] == "OFFLINE"
        assert status2["status"] == "OFFLINE"

    def test_idle_group_without_db_history_logs_debug(
        self, db_path_initialized, caplog
    ):
        """Idle group without DB history should log DEBUG."""
        import logging

        caplog.set_level(logging.DEBUG)

        config = make_test_config()
        state_manager = MockStateManager()

        mock_kafka = MagicMock()

        s = Sampler(config, db_path_initialized, mock_kafka, state_manager)

        s._handle_idle_group("new_group", time.time())

        assert "No partitions assigned to group new_group" in caplog.text
