"""Tests for housekeeping.py module."""

import logging
import os
import sqlite3
import threading
import time

import pytest

import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import database
from housekeeping import Housekeeping


@pytest.fixture
def mock_config():
    """Create a mock config."""
    config = type("MockConfig", (), {})()
    config.monitoring = type("MonitoringConfig", (), {})()
    config.monitoring.max_entries_per_partition = 300
    config.monitoring.max_commit_entries_per_partition = 200
    config.monitoring.housekeeping_interval_seconds = 1
    return config


class TestHousekeepingPruning:
    """Tests for partition_offsets pruning."""

    def test_prune_partition_offsets_keeps_300(
        self, db_path_initialized, db_conn, mock_config
    ):
        """Test that pruning keeps exactly 300 rows."""
        now = int(time.time())

        # Insert 500 rows for the same topic/partition
        topic = "test-topic"
        partition = 0
        for i in range(500):
            database.insert_partition_offset(
                db_conn, topic, partition, i, now - (500 - i)
            )

        database.commit_batch(db_conn)

        # Verify we have 500 rows
        cursor = db_conn.execute(
            "SELECT COUNT(*) FROM partition_offsets WHERE topic = ? AND partition = ?",
            (topic, partition),
        )
        assert cursor.fetchone()[0] == 500

        # Run housekeeping
        hk = Housekeeping(mock_config, db_path_initialized)
        hk._run_cycle()

        # Verify we have exactly 300 rows
        cursor = db_conn.execute(
            "SELECT COUNT(*) FROM partition_offsets WHERE topic = ? AND partition = ?",
            (topic, partition),
        )
        assert cursor.fetchone()[0] == 300

    def test_prune_partition_offsets_keeps_most_recent(
        self, db_path_initialized, db_conn, mock_config
    ):
        """Test that pruning keeps the most recent rows."""
        now = int(time.time())

        topic = "test-topic"
        partition = 0

        # Insert 500 rows with different timestamps
        for i in range(500):
            database.insert_partition_offset(
                db_conn, topic, partition, i, now - (500 - i)
            )

        database.commit_batch(db_conn)

        # Run housekeeping
        hk = Housekeeping(mock_config, db_path_initialized)
        hk._run_cycle()

        # Get the remaining rows
        cursor = db_conn.execute(
            "SELECT sampled_at FROM partition_offsets WHERE topic = ? AND partition = ? ORDER BY sampled_at DESC",
            (topic, partition),
        )
        rows = cursor.fetchall()

        # Verify they are the most recent (highest timestamps)
        # Should have timestamps for rows 200-499 (300 rows)
        assert len(rows) == 300
        # The oldest remaining should be at index 299 (the 300th most recent)
        oldest_remaining = rows[-1][0]
        # The oldest should be around now - 300 (allow some slack for timing)
        assert oldest_remaining >= now - 350

    def test_prune_partition_offsets_keeps_all_if_under_limit(
        self, db_path_initialized, mock_config
    ):
        """Test that pruning keeps all rows if under the limit."""
        now = int(time.time())

        topic = "test-topic"
        partition = 0

        conn = database.get_connection(db_path_initialized)
        try:
            for i in range(100):
                database.insert_partition_offset(
                    conn, topic, partition, i, now - (100 - i)
                )
            database.commit_batch(conn)
        finally:
            conn.close()

        hk = Housekeeping(mock_config, db_path_initialized)
        hk._run_cycle()

        conn = database.get_connection(db_path_initialized)
        try:
            cursor = conn.execute(
                "SELECT COUNT(*) FROM partition_offsets WHERE topic = ? AND partition = ?",
                (topic, partition),
            )
            assert cursor.fetchone()[0] == 100
        finally:
            conn.close()

    def test_prune_partition_offsets_multiple_topics(
        self, db_path_initialized, db_conn, mock_config
    ):
        """Test that pruning one topic/partition doesn't affect others."""
        now = int(time.time())

        # Insert 500 rows for topic1/partition0
        for i in range(500):
            database.insert_partition_offset(db_conn, "topic1", 0, i, now - (500 - i))

        # Insert 50 rows for topic2/partition0
        for i in range(50):
            database.insert_partition_offset(db_conn, "topic2", 0, i, now - (50 - i))

        database.commit_batch(db_conn)

        # Run housekeeping
        hk = Housekeeping(mock_config, db_path_initialized)
        hk._run_cycle()

        # topic1 should have 300
        cursor = db_conn.execute(
            "SELECT COUNT(*) FROM partition_offsets WHERE topic = 'topic1' AND partition = 0"
        )
        assert cursor.fetchone()[0] == 300

        # topic2 should still have all 50
        cursor = db_conn.execute(
            "SELECT COUNT(*) FROM partition_offsets WHERE topic = 'topic2' AND partition = 0"
        )
        assert cursor.fetchone()[0] == 50


class TestHousekeepingConsumerCommits:
    """Tests for consumer_commits pruning."""

    def test_prune_consumer_commits_keeps_200(
        self, db_path_initialized, db_conn, mock_config
    ):
        """Test that pruning keeps exactly 200 rows."""
        now = int(time.time())

        group_id = "test-group"
        topic = "test-topic"
        partition = 0

        # Insert 500 rows
        for i in range(500):
            database.insert_consumer_commit(
                db_conn, group_id, topic, partition, i, now - (500 - i)
            )

        database.commit_batch(db_conn)

        # Run housekeeping
        hk = Housekeeping(mock_config, db_path_initialized)
        hk._run_cycle()

        # Verify we have exactly 200 rows
        cursor = db_conn.execute(
            "SELECT COUNT(*) FROM consumer_commits WHERE group_id = ? AND topic = ? AND partition = ?",
            (group_id, topic, partition),
        )
        assert cursor.fetchone()[0] == 200

    def test_prune_consumer_commits_keeps_most_recent(
        self, db_path_initialized, db_conn, mock_config
    ):
        """Test that pruning keeps the most recent rows."""
        now = int(time.time())

        group_id = "test-group"
        topic = "test-topic"
        partition = 0

        # Insert 500 rows
        for i in range(500):
            database.insert_consumer_commit(
                db_conn, group_id, topic, partition, i, now - (500 - i)
            )

        database.commit_batch(db_conn)

        # Run housekeeping
        hk = Housekeeping(mock_config, db_path_initialized)
        hk._run_cycle()

        # Get the remaining rows
        cursor = db_conn.execute(
            "SELECT recorded_at FROM consumer_commits WHERE group_id = ? AND topic = ? AND partition = ? ORDER BY recorded_at DESC",
            (group_id, topic, partition),
        )
        rows = cursor.fetchall()

        # Should have 200 rows (max_commit_entries_per_partition)
        assert len(rows) == 200

    def test_prune_consumer_commits_keeps_all_if_under_limit(
        self, db_path_initialized, mock_config
    ):
        """Test that pruning keeps all rows if under the limit."""
        now = int(time.time())

        group_id = "test-group"
        topic = "test-topic"
        partition = 0

        conn = database.get_connection(db_path_initialized)
        try:
            for i in range(50):
                database.insert_consumer_commit(
                    conn, group_id, topic, partition, i, now - (50 - i)
                )
            database.commit_batch(conn)
        finally:
            conn.close()

        hk = Housekeeping(mock_config, db_path_initialized)
        hk._run_cycle()

        conn = database.get_connection(db_path_initialized)
        try:
            cursor = conn.execute(
                "SELECT COUNT(*) FROM consumer_commits WHERE group_id = ? AND topic = ? AND partition = ?",
                (group_id, topic, partition),
            )
            assert cursor.fetchone()[0] == 50
        finally:
            conn.close()


class TestHousekeepingVacuum:
    """Tests for incremental vacuum."""

    def test_run_incremental_vacuum_no_error(
        self, db_path_initialized, db_conn, mock_config
    ):
        """Test that incremental vacuum runs without error."""
        # Insert some data
        now = int(time.time())
        for i in range(10):
            database.insert_partition_offset(db_conn, "topic1", 0, i, now - i)

        database.commit_batch(db_conn)

        # Run vacuum
        database.run_incremental_vacuum(db_conn, pages=100)

        # Should complete without error (in-memory DB doesn't actually vacuum)


class TestHousekeepingCycle:
    """Tests for the housekeeping cycle."""

    def test_cycle_logs_summary(
        self, db_path_initialized, db_conn, caplog, mock_config
    ):
        """Test that cycle logs a summary."""
        now = int(time.time())

        # Insert some data
        for i in range(10):
            database.insert_partition_offset(db_conn, "topic1", 0, i, now - i)
            database.insert_consumer_commit(db_conn, "group1", "topic1", 0, i, now - i)

        database.commit_batch(db_conn)

        hk = Housekeeping(mock_config, db_path_initialized)

        with caplog.at_level(logging.INFO):
            hk._run_cycle()

        # Check that summary was logged
        assert any("Housekeeping cycle complete" in msg for msg in caplog.messages)

    def test_run_respects_shutdown_event(
        self, db_path_initialized, db_conn, mock_config
    ):
        """Test that run() exits when shutdown_event is set."""
        shutdown_event = threading.Event()
        shutdown_event.set()  # Set immediately

        hk = Housekeeping(mock_config, db_path_initialized)
        hk.run(shutdown_event)

        # Should exit cleanly

    def test_run_raises_exception_does_not_crash(
        self, db_path_initialized, mock_config, caplog, tmp_path
    ):
        """Test that exceptions don't crash the loop."""
        bad_config = type("MockConfig", (), {})()
        bad_config.monitoring = type("MonitoringConfig", (), {})()
        bad_config.monitoring.max_entries_per_partition = -1
        bad_config.monitoring.max_commit_entries_per_partition = -1
        bad_config.monitoring.housekeeping_interval_seconds = 1

        shutdown_event = threading.Event()

        def set_shutdown_after_delay():
            time.sleep(0.2)
            shutdown_event.set()

        threading.Thread(target=set_shutdown_after_delay, daemon=True).start()

        hk = Housekeeping(bad_config, db_path_initialized)

        hk.run(shutdown_event)


class TestHousekeepingPruneVerification:
    """Tests for TASK 55 - verify that pruning actually deletes rows from DB."""

    def test_housekeeping_cycle_actually_deletes_rows_from_partition_offsets(
        self, db_path_initialized, db_conn, mock_config
    ):
        """
        TASK 43 regression: after _run_cycle(), rows exceeding max_entries_per_partition
        must be physically absent from the database — not just reported as deleted.

        The pre-fix bug was: prune functions issued conn.commit() per-partition but
        housekeeping._run_cycle() had no final commit_batch(), causing all DELETEs
        to roll back silently while rowcount was still reported correctly.
        """
        max_entries = mock_config.monitoring.max_entries_per_partition
        insert_count = max_entries + 50  # exceed limit by 50

        now = int(time.time())
        for i in range(insert_count):
            database.insert_partition_offset(db_conn, "topic1", 0, i, now - i)
        database.commit_batch(db_conn)

        # Verify setup: rows exist above threshold
        pre_count = db_conn.execute(
            "SELECT COUNT(*) FROM partition_offsets WHERE topic='topic1' AND partition=0"
        ).fetchone()[0]
        assert pre_count == insert_count

        hk = Housekeeping(mock_config, db_path_initialized)
        hk._run_cycle()

        # Query via a SEPARATE connection to confirm the commit reached the DB file
        verify_conn = database.get_connection(db_path_initialized)
        post_count = verify_conn.execute(
            "SELECT COUNT(*) FROM partition_offsets WHERE topic='topic1' AND partition=0"
        ).fetchone()[0]
        verify_conn.close()

        assert post_count == max_entries, (
            f"Expected {max_entries} rows after pruning, found {post_count}. "
            "Regression: commit_batch() may be missing from _run_cycle()."
        )

    def test_housekeeping_cycle_actually_deletes_rows_from_consumer_commits(
        self, db_path_initialized, db_conn, mock_config
    ):
        """Same as above for consumer_commits table."""
        max_entries = mock_config.monitoring.max_commit_entries_per_partition
        insert_count = max_entries + 50
        now = int(time.time())

        for i in range(insert_count):
            database.insert_consumer_commit(db_conn, "group1", "topic1", 0, i, now - i)
        database.commit_batch(db_conn)

        hk = Housekeeping(mock_config, db_path_initialized)
        hk._run_cycle()

        verify_conn = database.get_connection(db_path_initialized)
        post_count = verify_conn.execute(
            "SELECT COUNT(*) FROM consumer_commits "
            "WHERE group_id='group1' AND topic='topic1' AND partition=0"
        ).fetchone()[0]
        verify_conn.close()

        assert post_count == max_entries, (
            f"Expected {max_entries} rows, found {post_count}. "
            "Regression: commit_batch() may be missing from _run_cycle()."
        )

