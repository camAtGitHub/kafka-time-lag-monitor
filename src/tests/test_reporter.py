"""Tests for reporter.py module."""

import json
import os
import sqlite3
import threading
import time
from unittest.mock import Mock, MagicMock

import pytest

import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import database
import interpolation
from reporter import Reporter


@pytest.fixture
def db_conn():
    """Create an in-memory database for testing."""
    conn = database.init_db(":memory:")
    yield conn
    conn.close()


@pytest.fixture
def mock_config(tmp_path):
    """Create a mock config with temp output path."""
    config = Mock()
    config.monitoring.report_interval_seconds = 1
    config.output.json_path = str(tmp_path / "output.json")
    return config


@pytest.fixture
def mock_state_manager():
    """Create a mock state manager."""
    sm = Mock()
    sm.get_group_status.return_value = {
        "status": "ONLINE",
        "status_changed_at": 0,
        "last_advancing_at": 0,
        "consecutive_static": 0,
    }
    return sm


class TestReporterCycle:
    """Tests for the reporter cycle functionality."""

    def test_reporter_generates_correct_lag_values(
        self, db_conn, mock_config, mock_state_manager, tmp_path
    ):
        """Test that reporter calculates lag values correctly."""
        now = int(time.time())

        # Insert interpolation points for partition 0
        database.insert_partition_offset(db_conn, "test-topic", 0, 100, now - 100)
        database.insert_partition_offset(db_conn, "test-topic", 0, 200, now - 50)
        database.insert_partition_offset(db_conn, "test-topic", 0, 300, now)

        # Insert consumer commit at offset 150 (should interpolate to ~now-75)
        database.insert_consumer_commit(
            db_conn, "test-group", "test-topic", 0, 150, now
        )

        # Create reporter and run cycle
        reporter = Reporter(mock_config, db_conn, mock_state_manager)
        reporter._run_cycle()

        # Load and verify output
        with open(mock_config.output.json_path, "r") as f:
            output = json.load(f)

        assert "generated_at" in output
        assert "consumers" in output
        assert len(output["consumers"]) == 1

        consumer = output["consumers"][0]
        assert consumer["group_id"] == "test-group"
        assert consumer["topic"] == "test-topic"
        # Lag should be approximately 75 seconds (interpolated)
        assert 70 <= consumer["lag_seconds"] <= 80
        assert consumer["worst_partition"] == 0

    def test_output_has_required_fields_and_types(
        self, db_conn, mock_config, mock_state_manager
    ):
        """Test that output JSON has all required fields with correct types."""
        now = int(time.time())

        database.insert_partition_offset(db_conn, "topic1", 0, 100, now - 60)
        database.insert_consumer_commit(db_conn, "group1", "topic1", 0, 50, now)

        reporter = Reporter(mock_config, db_conn, mock_state_manager)
        reporter._run_cycle()

        with open(mock_config.output.json_path, "r") as f:
            output = json.load(f)

        # Check top-level fields
        assert isinstance(output["generated_at"], str)
        assert isinstance(output["consumers"], list)

        # Check consumer fields
        consumer = output["consumers"][0]
        assert isinstance(consumer["group_id"], str)
        assert isinstance(consumer["topic"], str)
        assert isinstance(consumer["lag_seconds"], int)
        assert isinstance(consumer["lag_display"], str)
        assert consumer["worst_partition"] is None or isinstance(
            consumer["worst_partition"], int
        )
        assert isinstance(consumer["status"], str)
        assert isinstance(consumer["data_resolution"], str)
        assert isinstance(consumer["partitions_monitored"], int)
        assert isinstance(consumer["calculated_at"], str)

    def test_lag_seconds_is_zero_for_current_consumer(
        self, db_conn, mock_config, mock_state_manager
    ):
        """Test that lag_seconds is 0 when consumer is at current offset."""
        now = int(time.time())

        database.insert_partition_offset(db_conn, "topic1", 0, 100, now)
        database.insert_consumer_commit(db_conn, "group1", "topic1", 0, 100, now)

        reporter = Reporter(mock_config, db_conn, mock_state_manager)
        reporter._run_cycle()

        with open(mock_config.output.json_path, "r") as f:
            output = json.load(f)

        assert output["consumers"][0]["lag_seconds"] == 0

    def test_worst_partition_is_null_when_lag_is_zero(
        self, db_conn, mock_config, mock_state_manager
    ):
        """Test that worst_partition is null when lag is 0."""
        now = int(time.time())

        database.insert_partition_offset(db_conn, "topic1", 0, 100, now)
        database.insert_consumer_commit(db_conn, "group1", "topic1", 0, 100, now)

        reporter = Reporter(mock_config, db_conn, mock_state_manager)
        reporter._run_cycle()

        with open(mock_config.output.json_path, "r") as f:
            output = json.load(f)

        assert output["consumers"][0]["worst_partition"] is None

    def test_status_is_lowercase_string(self, db_conn, mock_config, mock_state_manager):
        """Test that status values are lowercase strings."""
        now = int(time.time())

        database.insert_partition_offset(db_conn, "topic1", 0, 100, now)
        database.insert_consumer_commit(db_conn, "group1", "topic1", 0, 50, now)

        reporter = Reporter(mock_config, db_conn, mock_state_manager)
        reporter._run_cycle()

        with open(mock_config.output.json_path, "r") as f:
            output = json.load(f)

        status = output["consumers"][0]["status"]
        assert isinstance(status, str)
        assert status == status.lower()
        assert status in ["online", "offline", "recovering"]


class TestReporterAtomicWrite:
    """Tests for atomic write behavior."""

    def test_atomic_write_uses_temp_file(
        self, db_conn, mock_config, mock_state_manager
    ):
        """Test that reporter uses temp file for atomic writes."""
        now = int(time.time())

        database.insert_partition_offset(db_conn, "topic1", 0, 100, now)
        database.insert_consumer_commit(db_conn, "group1", "topic1", 0, 50, now)

        reporter = Reporter(mock_config, db_conn, mock_state_manager)
        reporter._run_cycle()

        # Temp file should not exist after successful write
        tmp_path = f"{mock_config.output.json_path}.tmp"
        assert not os.path.exists(tmp_path)

        # Final file should exist and be valid JSON
        assert os.path.exists(mock_config.output.json_path)
        with open(mock_config.output.json_path, "r") as f:
            output = json.load(f)
        assert "consumers" in output

    def test_output_is_always_valid_json(
        self, db_conn, mock_config, mock_state_manager
    ):
        """Test that output file is always valid JSON."""
        now = int(time.time())

        database.insert_partition_offset(db_conn, "topic1", 0, 100, now)
        database.insert_consumer_commit(db_conn, "group1", "topic1", 0, 50, now)

        reporter = Reporter(mock_config, db_conn, mock_state_manager)
        reporter._run_cycle()

        # Verify file can be parsed
        with open(mock_config.output.json_path, "r") as f:
            content = f.read()

        # Should be valid JSON
        output = json.loads(content)
        assert isinstance(output, dict)


class TestReporterErrorHandling:
    """Tests for reporter error handling."""

    def test_json_write_error_does_not_crash_reporter(
        self, db_conn, mock_config, mock_state_manager, tmp_path
    ):
        """Test that JSON write errors don't crash the reporter thread."""
        import logging
        import threading
        import time
        import logging.handlers

        now = int(time.time())
        database.insert_partition_offset(db_conn, "topic1", 0, 100, now)
        database.insert_consumer_commit(db_conn, "group1", "topic1", 0, 50, now)

        mock_config.output.json_path = str(tmp_path / "nonexistent" / "output.json")

        reporter = Reporter(mock_config, db_conn, mock_state_manager)

        shutdown_event = threading.Event()
        shutdown_event.set()

        reporter.run(shutdown_event)

    def test_json_write_permission_error_does_not_crash_reporter(
        self, db_conn, mock_config, mock_state_manager, tmp_path
    ):
        """Test that JSON permission errors don't crash the reporter thread."""
        import stat
        import time

        now = int(time.time())
        database.insert_partition_offset(db_conn, "topic1", 0, 100, now)
        database.insert_consumer_commit(db_conn, "group1", "topic1", 0, 50, now)

        json_path = tmp_path / "output.json"
        json_path.touch()
        json_path.chmod(stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)

        mock_config.output.json_path = str(json_path)

        reporter = Reporter(mock_config, db_conn, mock_state_manager)

        shutdown_event = threading.Event()
        shutdown_event.set()

        reporter.run(shutdown_event)

    def test_calculation_error_does_not_prevent_other_groups(
        self, db_conn, mock_config, mock_state_manager
    ):
        """Test that error for one group doesn't block others."""
        now = int(time.time())

        # Insert data for group1
        database.insert_partition_offset(db_conn, "topic1", 0, 100, now)
        database.insert_consumer_commit(db_conn, "group1", "topic1", 0, 50, now)

        # Insert data for group2
        database.insert_partition_offset(db_conn, "topic2", 0, 100, now)
        database.insert_consumer_commit(db_conn, "group2", "topic2", 0, 50, now)

        # Make state_manager raise error for group1
        call_count = [0]

        def side_effect(group_id, topic):
            call_count[0] += 1
            if group_id == "group1":
                raise RuntimeError("Test error")
            return {
                "status": "ONLINE",
                "status_changed_at": 0,
                "last_advancing_at": 0,
                "consecutive_static": 0,
            }

        mock_state_manager.get_group_status.side_effect = side_effect

        reporter = Reporter(mock_config, db_conn, mock_state_manager)
        reporter._run_cycle()

        # Should still have output with group2
        with open(mock_config.output.json_path, "r") as f:
            output = json.load(f)

        assert len(output["consumers"]) == 1
        assert output["consumers"][0]["group_id"] == "group2"


class TestReporterDataResolution:
    """Tests for data_resolution field."""

    def test_fine_resolution_for_online_status(
        self, db_conn, mock_config, mock_state_manager
    ):
        """Test that ONLINE status produces fine resolution."""
        now = int(time.time())

        database.insert_partition_offset(db_conn, "topic1", 0, 100, now)
        database.insert_consumer_commit(db_conn, "group1", "topic1", 0, 50, now)

        mock_state_manager.get_group_status.return_value = {
            "status": "ONLINE",
            "status_changed_at": 0,
            "last_advancing_at": 0,
            "consecutive_static": 0,
        }

        reporter = Reporter(mock_config, db_conn, mock_state_manager)
        reporter._run_cycle()

        with open(mock_config.output.json_path, "r") as f:
            output = json.load(f)

        assert output["consumers"][0]["data_resolution"] == "fine"

    def test_coarse_resolution_for_offline_status(
        self, db_conn, mock_config, mock_state_manager
    ):
        """Test that OFFLINE status produces coarse resolution."""
        now = int(time.time())

        database.insert_partition_offset(db_conn, "topic1", 0, 100, now)
        database.insert_consumer_commit(db_conn, "group1", "topic1", 0, 50, now)

        mock_state_manager.get_group_status.return_value = {
            "status": "OFFLINE",
            "status_changed_at": now,
            "last_advancing_at": now - 3600,
            "consecutive_static": 5,
        }

        reporter = Reporter(mock_config, db_conn, mock_state_manager)
        reporter._run_cycle()

        with open(mock_config.output.json_path, "r") as f:
            output = json.load(f)

        assert output["consumers"][0]["data_resolution"] == "coarse"

    def test_coarse_resolution_for_recovering_status(
        self, db_conn, mock_config, mock_state_manager
    ):
        """Test that RECOVERING status produces coarse resolution."""
        now = int(time.time())

        database.insert_partition_offset(db_conn, "topic1", 0, 100, now)
        database.insert_consumer_commit(db_conn, "group1", "topic1", 0, 50, now)

        mock_state_manager.get_group_status.return_value = {
            "status": "RECOVERING",
            "status_changed_at": now,
            "last_advancing_at": now - 300,
            "consecutive_static": 0,
        }

        reporter = Reporter(mock_config, db_conn, mock_state_manager)
        reporter._run_cycle()

        with open(mock_config.output.json_path, "r") as f:
            output = json.load(f)

        assert output["consumers"][0]["data_resolution"] == "coarse"


class TestReporterMultiplePartitions:
    """Tests for multi-partition scenarios."""

    def test_multiple_partitions_reported_correctly(
        self, db_conn, mock_config, mock_state_manager
    ):
        """Test that multiple partitions are aggregated correctly."""
        now = int(time.time())

        # Insert data for 3 partitions with different lags
        # Partition 0: committed at 100, latest produced at now (0 lag)
        # Partition 1: committed at 90, latest produced at now-20
        # Partition 2: committed at 80, latest produced at now-40
        for partition in range(3):
            # Each partition has a different "latest" produced offset
            latest_offset = 100 - (partition * 10)
            database.insert_partition_offset(
                db_conn, "topic1", partition, latest_offset, now - (partition * 10)
            )
            # Consumer is at the same offset as production - no lag
            database.insert_consumer_commit(
                db_conn, "group1", "topic1", partition, latest_offset, now
            )

        reporter = Reporter(mock_config, db_conn, mock_state_manager)
        reporter._run_cycle()

        with open(mock_config.output.json_path, "r") as f:
            output = json.load(f)

        consumer = output["consumers"][0]
        assert consumer["partitions_monitored"] == 3
        # All lags are 0 since consumer matches latest produced
        assert consumer["worst_partition"] is None


class TestReporterRunLoop:
    """Tests for the reporter run loop."""

    def test_run_respects_shutdown_event(
        self, db_conn, mock_config, mock_state_manager
    ):
        """Test that run() exits when shutdown_event is set."""
        shutdown_event = threading.Event()
        shutdown_event.set()  # Set immediately

        reporter = Reporter(mock_config, db_conn, mock_state_manager)
        reporter.run(shutdown_event)

        # Should exit cleanly without writing any output
        assert not os.path.exists(mock_config.output.json_path)

    def test_run_updates_thread_last_run(
        self, db_conn, mock_config, mock_state_manager
    ):
        """Test that run() updates thread_last_run timestamp."""
        shutdown_event = threading.Event()

        now = int(time.time())
        database.insert_partition_offset(db_conn, "topic1", 0, 100, now)
        database.insert_consumer_commit(db_conn, "group1", "topic1", 0, 50, now)

        # Set shutdown after first cycle
        def set_shutdown_after_delay():
            time.sleep(0.5)
            shutdown_event.set()

        threading.Thread(target=set_shutdown_after_delay, daemon=True).start()

        reporter = Reporter(mock_config, db_conn, mock_state_manager)
        reporter.run(shutdown_event)

        # Verify thread_last_run was updated
        mock_state_manager.update_thread_last_run.assert_called_with(
            "reporter", pytest.approx(int(time.time()), abs=5)
        )
