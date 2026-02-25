"""Tests for database.py module."""

import pytest
import sqlite3
import time
from database import (
    init_db,
    insert_partition_offset,
    insert_consumer_commit,
    commit_batch,
    get_interpolation_points,
    get_recent_commits,
    get_last_write_time,
    get_last_stored_offset,
    get_group_status,
    upsert_group_status,
    load_all_group_statuses,
    is_topic_excluded,
    is_group_excluded,
    prune_partition_offsets,
    prune_consumer_commits,
    get_all_partition_keys,
    get_all_commit_keys,
    run_incremental_vacuum,
    has_group_history,
    get_group_tracked_topics,
)


class TestInitDb:
    """Tests for database initialization."""

    def test_all_tables_exist_after_init(self, db_conn):
        """Verify all required tables are created."""
        cursor = db_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        tables = [row[0] for row in cursor.fetchall()]
        expected = [
            "consumer_commits",
            "excluded_groups",
            "excluded_topics",
            "group_status",
            "partition_offsets",
        ]
        assert tables == expected

    def test_pragma_journal_mode_wal(self, db_conn):
        """Verify WAL mode is enabled (not applicable to in-memory DBs)."""
        cursor = db_conn.execute("PRAGMA journal_mode")
        result = cursor.fetchone()[0]
        # In-memory databases don't support WAL mode, they return "memory"
        assert result in ("wal", "memory")

    def test_pragma_synchronous_normal(self, db_conn):
        """Verify synchronous mode is NORMAL."""
        cursor = db_conn.execute("PRAGMA synchronous")
        result = cursor.fetchone()[0]
        assert result == 1  # NORMAL = 1

    def test_pragma_auto_vacuum_incremental(self, db_conn):
        """Verify auto_vacuum is INCREMENTAL."""
        cursor = db_conn.execute("PRAGMA auto_vacuum")
        result = cursor.fetchone()[0]
        assert result == 2  # INCREMENTAL = 2


class TestPartitionOffsets:
    """Tests for partition_offsets table operations."""

    def test_insert_and_retrieve_partition_offset(self, db_conn):
        """Test inserting and retrieving a partition offset."""
        insert_partition_offset(db_conn, "test-topic", 0, 1000, 1234567890)
        points = get_interpolation_points(db_conn, "test-topic", 0)
        assert len(points) == 1
        assert points[0] == (1000, 1234567890)

    def test_interpolation_points_ordered_descending(self, db_conn):
        """Verify points are returned in descending timestamp order."""
        insert_partition_offset(db_conn, "test-topic", 0, 1000, 1234567890)
        insert_partition_offset(db_conn, "test-topic", 0, 1100, 1234567900)
        insert_partition_offset(db_conn, "test-topic", 0, 1200, 1234567910)
        points = get_interpolation_points(db_conn, "test-topic", 0)
        assert len(points) == 3
        # Should be ordered by sampled_at DESC
        assert points[0] == (1200, 1234567910)
        assert points[1] == (1100, 1234567900)
        assert points[2] == (1000, 1234567890)

    def test_partition_isolation(self, db_conn):
        """Verify different partitions are isolated."""
        insert_partition_offset(db_conn, "test-topic", 0, 1000, 1234567890)
        insert_partition_offset(db_conn, "test-topic", 1, 2000, 1234567890)
        points_0 = get_interpolation_points(db_conn, "test-topic", 0)
        points_1 = get_interpolation_points(db_conn, "test-topic", 1)
        assert len(points_0) == 1
        assert len(points_1) == 1
        assert points_0[0][0] == 1000
        assert points_1[0][0] == 2000

    def test_topic_isolation(self, db_conn):
        """Verify different topics are isolated."""
        insert_partition_offset(db_conn, "topic-a", 0, 1000, 1234567890)
        insert_partition_offset(db_conn, "topic-b", 0, 2000, 1234567890)
        points_a = get_interpolation_points(db_conn, "topic-a", 0)
        points_b = get_interpolation_points(db_conn, "topic-b", 0)
        assert len(points_a) == 1
        assert len(points_b) == 1
        assert points_a[0][0] == 1000
        assert points_b[0][0] == 2000

    def test_get_interpolation_points_respects_limit(self, db_conn):
        """TASK 52: get_interpolation_points must return at most `limit` rows."""
        now = int(time.time())
        for i in range(20):
            insert_partition_offset(db_conn, "topic1", 0, i * 10, now - i)
        commit_batch(db_conn)

        result = get_interpolation_points(db_conn, "topic1", 0, limit=5)
        assert len(result) == 5, f"Expected 5 rows with limit=5, got {len(result)}"

    def test_get_interpolation_points_default_limit_is_safe(self, db_conn):
        """
        Default limit (500) must be applied when no limit is passed explicitly.
        Insert 600 rows; expect at most 500 returned.
        """
        now = int(time.time())
        for i in range(600):
            insert_partition_offset(db_conn, "topic1", 0, i, now - i)
        commit_batch(db_conn)

        result = get_interpolation_points(db_conn, "topic1", 0)
        assert len(result) <= 500, (
            f"Default limit not enforced: got {len(result)} rows, expected <= 500"
        )

    def test_get_interpolation_points_returns_most_recent_rows(self, db_conn):
        """
        When limit truncates results, the most recent rows (highest sampled_at)
        must be returned, ordered DESC by sampled_at.
        """
        now = int(time.time())
        for i in range(10):
            insert_partition_offset(db_conn, "topic1", 0, i * 100, now - i)
        commit_batch(db_conn)

        result = get_interpolation_points(db_conn, "topic1", 0, limit=3)
        assert len(result) == 3
        assert result[0][1] == now       # most recent first
        assert result[1][1] == now - 1
        assert result[2][1] == now - 2

    def test_get_last_write_time_returns_most_recent(self, db_conn):
        """Verify get_last_write_time returns the most recent timestamp."""
        insert_partition_offset(db_conn, "test-topic", 0, 1000, 1234567890)
        insert_partition_offset(db_conn, "test-topic", 0, 1100, 1234567900)
        last_time = get_last_write_time(db_conn, "test-topic", 0)
        assert last_time == 1234567900

    def test_get_last_write_time_returns_none_for_empty(self, db_conn):
        """Verify get_last_write_time returns None when no rows exist."""
        last_time = get_last_write_time(db_conn, "test-topic", 0)
        assert last_time is None

    def test_get_last_stored_offset_returns_most_recent(self, db_conn):
        """Verify get_last_stored_offset returns the most recent offset."""
        insert_partition_offset(db_conn, "test-topic", 0, 1000, 1234567890)
        insert_partition_offset(db_conn, "test-topic", 0, 1100, 1234567900)
        last_offset = get_last_stored_offset(db_conn, "test-topic", 0)
        assert last_offset == 1100

    def test_get_last_stored_offset_returns_none_for_empty(self, db_conn):
        """Verify get_last_stored_offset returns None when no rows exist."""
        last_offset = get_last_stored_offset(db_conn, "test-topic", 0)
        assert last_offset is None


class TestConsumerCommits:
    """Tests for consumer_commits table operations."""

    def test_insert_and_retrieve_consumer_commit(self, db_conn):
        """Test inserting and retrieving a consumer commit."""
        insert_consumer_commit(db_conn, "group-1", "test-topic", 0, 500, 1234567890)
        commits = get_recent_commits(db_conn, "group-1", "test-topic", 0, 10)
        assert len(commits) == 1
        assert commits[0] == (500, 1234567890)

    def test_recent_commits_ordered_descending(self, db_conn):
        """Verify commits are returned in descending timestamp order."""
        insert_consumer_commit(db_conn, "group-1", "test-topic", 0, 500, 1234567890)
        insert_consumer_commit(db_conn, "group-1", "test-topic", 0, 600, 1234567900)
        insert_consumer_commit(db_conn, "group-1", "test-topic", 0, 700, 1234567910)
        commits = get_recent_commits(db_conn, "group-1", "test-topic", 0, 10)
        assert len(commits) == 3
        assert commits[0] == (700, 1234567910)
        assert commits[1] == (600, 1234567900)
        assert commits[2] == (500, 1234567890)

    def test_recent_commits_respects_limit(self, db_conn):
        """Verify the limit parameter is respected."""
        for i in range(10):
            insert_consumer_commit(
                db_conn, "group-1", "test-topic", 0, i, 1234567890 + i
            )
        commits = get_recent_commits(db_conn, "group-1", "test-topic", 0, 5)
        assert len(commits) == 5

    def test_commits_isolated_by_group(self, db_conn):
        """Verify different groups are isolated."""
        insert_consumer_commit(db_conn, "group-1", "test-topic", 0, 500, 1234567890)
        insert_consumer_commit(db_conn, "group-2", "test-topic", 0, 600, 1234567890)
        commits_1 = get_recent_commits(db_conn, "group-1", "test-topic", 0, 10)
        commits_2 = get_recent_commits(db_conn, "group-2", "test-topic", 0, 10)
        assert len(commits_1) == 1
        assert len(commits_2) == 1
        assert commits_1[0][0] == 500
        assert commits_2[0][0] == 600


class TestGroupStatus:
    """Tests for group_status table operations."""

    def test_upsert_group_status_insert(self, db_conn):
        """Test inserting a new group status."""
        upsert_group_status(
            db_conn, "group-1", "test-topic", "ONLINE", 1234567890, 1234567890, 0
        )
        status = get_group_status(db_conn, "group-1", "test-topic")
        assert status is not None
        assert status["status"] == "ONLINE"
        assert status["status_changed_at"] == 1234567890
        assert status["last_advancing_at"] == 1234567890
        assert status["consecutive_static"] == 0

    def test_upsert_group_status_update(self, db_conn):
        """Test updating an existing group status."""
        upsert_group_status(
            db_conn, "group-1", "test-topic", "ONLINE", 1234567890, 1234567890, 0
        )
        upsert_group_status(
            db_conn, "group-1", "test-topic", "OFFLINE", 1234567900, 1234567890, 3
        )
        status = get_group_status(db_conn, "group-1", "test-topic")
        assert status["status"] == "OFFLINE"
        assert status["status_changed_at"] == 1234567900
        assert status["consecutive_static"] == 3
        # Verify only one row exists (no duplicates)
        cursor = db_conn.execute(
            "SELECT COUNT(*) FROM group_status WHERE group_id = ? AND topic = ?",
            ("group-1", "test-topic"),
        )
        assert cursor.fetchone()[0] == 1

    def test_get_group_status_returns_none_for_missing(self, db_conn):
        """Verify get_group_status returns None for non-existent group."""
        status = get_group_status(db_conn, "non-existent", "test-topic")
        assert status is None

    def test_load_all_group_statuses(self, db_conn):
        """Test loading all group statuses."""
        upsert_group_status(
            db_conn, "group-1", "topic-a", "ONLINE", 1234567890, 1234567890, 0
        )
        upsert_group_status(
            db_conn, "group-1", "topic-b", "OFFLINE", 1234567900, 1234567900, 5
        )
        upsert_group_status(
            db_conn, "group-2", "topic-a", "RECOVERING", 1234567910, 1234567910, 2
        )
        all_statuses = load_all_group_statuses(db_conn)
        assert len(all_statuses) == 3
        assert all_statuses[("group-1", "topic-a")]["status"] == "ONLINE"
        assert all_statuses[("group-1", "topic-b")]["status"] == "OFFLINE"
        assert all_statuses[("group-2", "topic-a")]["status"] == "RECOVERING"


class TestExclusions:
    """Tests for exclusion checking."""

    def test_is_topic_excluded_config_only(self, db_conn):
        """Test topic exclusion from config only."""
        result = is_topic_excluded(db_conn, "excluded-topic", ["excluded-topic"])
        assert result is True

    def test_is_topic_excluded_config_not_excluded(self, db_conn):
        """Test topic not excluded in config."""
        result = is_topic_excluded(db_conn, "normal-topic", ["excluded-topic"])
        assert result is False

    def test_is_topic_excluded_database(self, db_conn):
        """Test topic exclusion from database."""
        db_conn.execute(
            "INSERT INTO excluded_topics (topic) VALUES (?)", ("db-excluded",)
        )
        db_conn.commit()
        result = is_topic_excluded(db_conn, "db-excluded", [])
        assert result is True

    def test_is_topic_excluded_both(self, db_conn):
        """Test topic excluded in both config and database."""
        db_conn.execute(
            "INSERT INTO excluded_topics (topic) VALUES (?)", ("db-excluded",)
        )
        db_conn.commit()
        result = is_topic_excluded(db_conn, "db-excluded", ["config-excluded"])
        assert result is True

    def test_is_group_excluded_config_only(self, db_conn):
        """Test group exclusion from config only."""
        result = is_group_excluded(db_conn, "excluded-group", ["excluded-group"])
        assert result is True

    def test_is_group_excluded_database(self, db_conn):
        """Test group exclusion from database."""
        db_conn.execute(
            "INSERT INTO excluded_groups (group_id) VALUES (?)", ("db-excluded",)
        )
        db_conn.commit()
        result = is_group_excluded(db_conn, "db-excluded", [])
        assert result is True


class TestPruning:
    """Tests for data pruning operations."""

    def test_prune_partition_offsets_deletes_oldest(self, db_conn):
        """Verify pruning keeps most recent rows."""
        for i in range(10):
            insert_partition_offset(db_conn, "test-topic", 0, i * 100, 1234567890 + i)
        deleted = prune_partition_offsets(db_conn, "test-topic", 0, 5)
        assert deleted == 5
        points = get_interpolation_points(db_conn, "test-topic", 0)
        assert len(points) == 5
        # Should have the 5 most recent (timestamps 5-9)
        timestamps = [p[1] for p in points]
        assert timestamps == [
            1234567899,
            1234567898,
            1234567897,
            1234567896,
            1234567895,
        ]

    def test_prune_partition_offsets_no_deletion_when_under_limit(self, db_conn):
        """Verify no deletion when row count is under limit."""
        for i in range(5):
            insert_partition_offset(db_conn, "test-topic", 0, i * 100, 1234567890 + i)
        deleted = prune_partition_offsets(db_conn, "test-topic", 0, 10)
        assert deleted == 0
        points = get_interpolation_points(db_conn, "test-topic", 0)
        assert len(points) == 5

    def test_prune_consumer_commits_deletes_oldest(self, db_conn):
        """Verify pruning keeps most recent commits."""
        for i in range(10):
            insert_consumer_commit(
                db_conn, "group-1", "test-topic", 0, i * 100, 1234567890 + i
            )
        deleted = prune_consumer_commits(db_conn, "group-1", "test-topic", 0, 5)
        assert deleted == 5
        commits = get_recent_commits(db_conn, "group-1", "test-topic", 0, 10)
        assert len(commits) == 5

    def test_prune_partition_isolated_by_topic_partition(self, db_conn):
        """Verify pruning one topic/partition doesn't affect others."""
        for i in range(10):
            insert_partition_offset(db_conn, "topic-a", 0, i * 100, 1234567890 + i)
            insert_partition_offset(db_conn, "topic-b", 0, i * 100, 1234567890 + i)
        prune_partition_offsets(db_conn, "topic-a", 0, 5)
        points_a = get_interpolation_points(db_conn, "topic-a", 0)
        points_b = get_interpolation_points(db_conn, "topic-b", 0)
        assert len(points_a) == 5
        assert len(points_b) == 10


class TestGetAllKeys:
    """Tests for retrieving all partition/commit keys."""

    def test_get_all_partition_keys(self, db_conn):
        """Test retrieving all distinct topic/partition combinations."""
        insert_partition_offset(db_conn, "topic-a", 0, 1000, 1234567890)
        insert_partition_offset(db_conn, "topic-a", 1, 1000, 1234567890)
        insert_partition_offset(db_conn, "topic-b", 0, 1000, 1234567890)
        keys = get_all_partition_keys(db_conn)
        assert len(keys) == 3
        assert ("topic-a", 0) in keys
        assert ("topic-a", 1) in keys
        assert ("topic-b", 0) in keys

    def test_get_all_commit_keys(self, db_conn):
        """Test retrieving all distinct group/topic/partition combinations."""
        insert_consumer_commit(db_conn, "group-1", "topic-a", 0, 100, 1234567890)
        insert_consumer_commit(db_conn, "group-1", "topic-a", 1, 100, 1234567890)
        insert_consumer_commit(db_conn, "group-2", "topic-a", 0, 100, 1234567890)
        keys = get_all_commit_keys(db_conn)
        assert len(keys) == 3
        assert ("group-1", "topic-a", 0) in keys
        assert ("group-1", "topic-a", 1) in keys
        assert ("group-2", "topic-a", 0) in keys


class TestIncrementalVacuum:
    """Tests for incremental vacuum."""

    def test_run_incremental_vacuum_completes(self, db_conn):
        """Verify incremental vacuum runs without error."""
        run_incremental_vacuum(db_conn, pages=10)

    def test_run_incremental_vacuum_invalid_pages_raises(self, db_conn):
        """Verify validation rejects invalid pages values."""
        with pytest.raises(ValueError, match="pages must be a positive integer"):
            run_incremental_vacuum(db_conn, pages=0)

        with pytest.raises(ValueError, match="pages must be a positive integer"):
            run_incremental_vacuum(db_conn, pages=-1)

        with pytest.raises(ValueError, match="pages must be a positive integer"):
            run_incremental_vacuum(db_conn, pages="100")  # type: ignore


class TestGroupHistory:
    """Tests for group history functions."""

    def test_has_group_history_true(self, db_conn):
        """Test has_group_history returns True when group has commits."""
        insert_consumer_commit(db_conn, "group-1", "topic1", 0, 100, 1234567890)
        result = has_group_history(db_conn, "group-1")
        assert result is True

    def test_has_group_history_false(self, db_conn):
        """Test has_group_history returns False when group has no history."""
        result = has_group_history(db_conn, "nonexistent-group")
        assert result is False

    def test_get_group_tracked_topics(self, db_conn):
        """Test get_group_tracked_topics returns all topics for a group."""
        insert_consumer_commit(db_conn, "group-1", "topic1", 0, 100, 1234567890)
        insert_consumer_commit(db_conn, "group-1", "topic2", 0, 200, 1234567890)
        insert_consumer_commit(db_conn, "group-2", "topic1", 0, 300, 1234567890)
        topics = get_group_tracked_topics(db_conn, "group-1")
        assert set(topics) == {"topic1", "topic2"}

    def test_get_group_tracked_topics_empty(self, db_conn):
        """Test get_group_tracked_topics returns empty list for unknown group."""
        topics = get_group_tracked_topics(db_conn, "nonexistent-group")
        assert topics == []

    def test_get_all_groups_with_history(self, db_conn):
        """Test get_all_groups_with_history returns all groups with history."""
        from database import (
            insert_consumer_commit,
            get_all_groups_with_history,
        )

        insert_consumer_commit(db_conn, "group-1", "topic1", 0, 100, 1234567890)
        insert_consumer_commit(db_conn, "group-2", "topic1", 0, 200, 1234567890)
        insert_consumer_commit(db_conn, "group-2", "topic2", 0, 300, 1234567890)

        groups = get_all_groups_with_history(db_conn)
        assert set(groups) == {"group-1", "group-2"}

    def test_get_all_groups_with_history_empty(self, db_conn):
        """Test get_all_groups_with_history returns empty list when no history."""
        from database import get_all_groups_with_history

        groups = get_all_groups_with_history(db_conn)
        assert groups == []


class TestDeleteGroupData:
    """Tests for delete_group_data function."""

    def test_delete_group_data_removes_consumer_commits(self, db_conn):
        """Test delete_group_data removes all consumer_commits for a group."""
        from database import delete_group_data, commit_batch

        insert_consumer_commit(db_conn, "group-1", "topic1", 0, 100, 1234567890)
        insert_consumer_commit(db_conn, "group-1", "topic2", 0, 200, 1234567890)
        insert_consumer_commit(db_conn, "group-2", "topic1", 0, 300, 1234567890)

        delete_group_data(db_conn, "group-1")
        commit_batch(db_conn)

        cursor = db_conn.execute(
            "SELECT COUNT(*) FROM consumer_commits WHERE group_id = ?",
            ("group-1",)
        )
        assert cursor.fetchone()[0] == 0

        # group-2 should still exist
        cursor = db_conn.execute(
            "SELECT COUNT(*) FROM consumer_commits WHERE group_id = ?",
            ("group-2",)
        )
        assert cursor.fetchone()[0] == 1

    def test_delete_group_data_removes_group_status(self, db_conn):
        """Test delete_group_data removes all group_status for a group."""
        from database import delete_group_data, upsert_group_status, commit_batch

        upsert_group_status(db_conn, "group-1", "topic1", "ONLINE", 1234567890, 1234567890, 0)
        upsert_group_status(db_conn, "group-1", "topic2", "OFFLINE", 1234567890, 1234567890, 5)
        upsert_group_status(db_conn, "group-2", "topic1", "ONLINE", 1234567890, 1234567890, 0)

        delete_group_data(db_conn, "group-1")
        commit_batch(db_conn)

        cursor = db_conn.execute(
            "SELECT COUNT(*) FROM group_status WHERE group_id = ?",
            ("group-1",)
        )
        assert cursor.fetchone()[0] == 0

        # group-2 should still exist
        cursor = db_conn.execute(
            "SELECT COUNT(*) FROM group_status WHERE group_id = ?",
            ("group-2",)
        )
        assert cursor.fetchone()[0] == 1

    def test_delete_group_data_does_not_affect_partition_offsets(self, db_conn):
        """Test delete_group_data does not touch partition_offsets table."""
        from database import delete_group_data, commit_batch

        # Add consumer commits for context
        insert_consumer_commit(db_conn, "group-1", "topic1", 0, 100, 1234567890)

        # Add partition offsets (shared across groups)
        insert_partition_offset(db_conn, "topic1", 0, 1000, 1234567890)

        delete_group_data(db_conn, "group-1")
        commit_batch(db_conn)

        # partition_offsets should remain untouched
        cursor = db_conn.execute(
            "SELECT COUNT(*) FROM partition_offsets WHERE topic = ?",
            ("topic1",)
        )
        assert cursor.fetchone()[0] == 1
