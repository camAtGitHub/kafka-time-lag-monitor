"""Database module for SQLite operations.

All SQL operations are isolated here. No other module writes SQL.
"""

import sqlite3
from typing import Optional, List, Dict, Any, Tuple, Set


def init_db(path: str) -> sqlite3.Connection:
    """Initialize database with tables and PRAGMAs.

    Args:
        path: Path to the SQLite database file (use ':memory:' for in-memory)

    Returns:
        sqlite3.Connection: Database connection with row factory set
    """
    conn = _create_connection(path)

    # Create tables
    conn.execute("""
        CREATE TABLE IF NOT EXISTS partition_offsets (
            topic TEXT NOT NULL,
            partition INTEGER NOT NULL,
            offset INTEGER NOT NULL,
            sampled_at INTEGER NOT NULL,
            PRIMARY KEY (topic, partition, sampled_at)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS consumer_commits (
            group_id TEXT NOT NULL,
            topic TEXT NOT NULL,
            partition INTEGER NOT NULL,
            committed_offset INTEGER NOT NULL,
            recorded_at INTEGER NOT NULL,
            PRIMARY KEY (group_id, topic, partition, recorded_at)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS group_status (
            group_id TEXT NOT NULL,
            topic TEXT NOT NULL,
            status TEXT NOT NULL,
            status_changed_at INTEGER NOT NULL,
            last_advancing_at INTEGER NOT NULL,
            consecutive_static INTEGER NOT NULL,
            PRIMARY KEY (group_id, topic)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS excluded_topics (
            topic TEXT PRIMARY KEY
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS excluded_groups (
            group_id TEXT PRIMARY KEY
        )
    """)

    conn.commit()
    return conn


def _create_connection(path: str) -> sqlite3.Connection:
    """Create a new database connection with proper settings.

    Args:
        path: Path to the SQLite database file

    Returns:
        sqlite3.Connection: Database connection with row factory and PRAGMAs set
    """
    conn = sqlite3.connect(path, check_same_thread=False, timeout=30.0)
    conn.row_factory = sqlite3.Row

    # Set required PRAGMAs
    conn.execute("PRAGMA auto_vacuum=INCREMENTAL")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.commit()

    return conn


def get_connection(path: str) -> sqlite3.Connection:
    """Get a new database connection for a worker thread.

    Each thread should call this to get its own connection to avoid
    thread-safety issues with SQLite connections.

    Args:
        path: Path to the SQLite database file

    Returns:
        sqlite3.Connection: New database connection with row factory and PRAGMAs set
    """
    return _create_connection(path)


def insert_partition_offset(
    conn: sqlite3.Connection, topic: str, partition: int, offset: int, sampled_at: int
) -> None:
    """Insert a partition offset snapshot.

    Args:
        conn: Database connection
        topic: Topic name
        partition: Partition number
        offset: The offset value
        sampled_at: Unix timestamp
    """
    conn.execute(
        "INSERT INTO partition_offsets (topic, partition, offset, sampled_at) VALUES (?, ?, ?, ?)",
        (topic, partition, offset, sampled_at),
    )


def insert_consumer_commit(
    conn: sqlite3.Connection,
    group_id: str,
    topic: str,
    partition: int,
    committed_offset: int,
    recorded_at: int,
) -> None:
    """Insert a consumer commit record.

    Args:
        conn: Database connection
        group_id: Consumer group ID
        topic: Topic name
        partition: Partition number
        committed_offset: The committed offset value
        recorded_at: Unix timestamp
    """
    conn.execute(
        """INSERT INTO consumer_commits 
           (group_id, topic, partition, committed_offset, recorded_at) 
           VALUES (?, ?, ?, ?, ?)""",
        (group_id, topic, partition, committed_offset, recorded_at),
    )


def commit_batch(conn: sqlite3.Connection) -> None:
    """Commit all pending writes in a single transaction.

    Call this at the end of a sampler cycle to batch all writes
    into a single transaction, reducing fsync overhead.

    Args:
        conn: Database connection
    """
    conn.commit()


def get_interpolation_points(
    conn: sqlite3.Connection, topic: str, partition: int
) -> List[Tuple[int, int]]:
    """Get all interpolation points for a topic/partition ordered by sampled_at DESC.

    Args:
        conn: Database connection
        topic: Topic name
        partition: Partition number

    Returns:
        List of (offset, sampled_at) tuples ordered by sampled_at DESC
    """
    cursor = conn.execute(
        """SELECT offset, sampled_at FROM partition_offsets 
           WHERE topic = ? AND partition = ? 
           ORDER BY sampled_at DESC""",
        (topic, partition),
    )
    return [(row[0], row[1]) for row in cursor.fetchall()]


def get_recent_commits(
    conn: sqlite3.Connection, group_id: str, topic: str, partition: int, limit: int
) -> List[Tuple[int, int]]:
    """Get recent consumer commits for a group/topic/partition.

    Args:
        conn: Database connection
        group_id: Consumer group ID
        topic: Topic name
        partition: Partition number
        limit: Maximum number of rows to return

    Returns:
        List of (committed_offset, recorded_at) tuples
    """
    cursor = conn.execute(
        """SELECT committed_offset, recorded_at FROM consumer_commits 
           WHERE group_id = ? AND topic = ? AND partition = ? 
           ORDER BY recorded_at DESC 
           LIMIT ?""",
        (group_id, topic, partition, limit),
    )
    return [(row[0], row[1]) for row in cursor.fetchall()]


def get_last_write_time(
    conn: sqlite3.Connection, topic: str, partition: int
) -> Optional[int]:
    """Get the most recent sampled_at timestamp for a topic/partition.

    Args:
        conn: Database connection
        topic: Topic name
        partition: Partition number

    Returns:
        Unix timestamp or None if no rows exist
    """
    cursor = conn.execute(
        """SELECT MAX(sampled_at) FROM partition_offsets 
           WHERE topic = ? AND partition = ?""",
        (topic, partition),
    )
    result = cursor.fetchone()[0]
    return result if result is not None else None


def get_last_stored_offset(
    conn: sqlite3.Connection, topic: str, partition: int
) -> Optional[int]:
    """Get the most recent offset for a topic/partition.

    Args:
        conn: Database connection
        topic: Topic name
        partition: Partition number

    Returns:
        Most recent offset or None if no rows exist
    """
    cursor = conn.execute(
        """SELECT offset FROM partition_offsets 
           WHERE topic = ? AND partition = ? 
           ORDER BY sampled_at DESC LIMIT 1""",
        (topic, partition),
    )
    result = cursor.fetchone()
    return result[0] if result is not None else None


def get_group_status(
    conn: sqlite3.Connection, group_id: str, topic: str
) -> Optional[Dict[str, Any]]:
    """Get the status for a group/topic combination.

    Args:
        conn: Database connection
        group_id: Consumer group ID
        topic: Topic name

    Returns:
        Status dict or None if not found
    """
    cursor = conn.execute(
        """SELECT status, status_changed_at, last_advancing_at, consecutive_static 
           FROM group_status WHERE group_id = ? AND topic = ?""",
        (group_id, topic),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    return {
        "status": row[0],
        "status_changed_at": row[1],
        "last_advancing_at": row[2],
        "consecutive_static": row[3],
    }


def upsert_group_status(
    conn: sqlite3.Connection,
    group_id: str,
    topic: str,
    status: str,
    status_changed_at: int,
    last_advancing_at: int,
    consecutive_static: int,
) -> None:
    """Insert or update group status using INSERT OR REPLACE semantics.

    Args:
        conn: Database connection
        group_id: Consumer group ID
        topic: Topic name
        status: Status string (ONLINE, OFFLINE, or RECOVERING)
        status_changed_at: Unix timestamp of status change
        last_advancing_at: Unix timestamp when offset last advanced
        consecutive_static: Counter for consecutive static samples
    """
    conn.execute(
        """INSERT OR REPLACE INTO group_status
           (group_id, topic, status, status_changed_at, last_advancing_at, consecutive_static)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (
            group_id,
            topic,
            status,
            status_changed_at,
            last_advancing_at,
            consecutive_static,
        ),
    )


def load_all_group_statuses(
    conn: sqlite3.Connection,
) -> Dict[Tuple[str, str], Dict[str, Any]]:
    """Load all group statuses from the database.

    Args:
        conn: Database connection

    Returns:
        Dict mapping (group_id, topic) to status dict
    """
    cursor = conn.execute(
        """SELECT group_id, topic, status, status_changed_at, last_advancing_at, consecutive_static 
           FROM group_status"""
    )
    result = {}
    for row in cursor.fetchall():
        key = (row[0], row[1])
        result[key] = {
            "status": row[2],
            "status_changed_at": row[3],
            "last_advancing_at": row[4],
            "consecutive_static": row[5],
        }
    return result


def is_topic_excluded(
    conn: sqlite3.Connection, topic: str, config_exclusions: Set[str]
) -> bool:
    """Check if a topic is excluded (in config or database).

    Args:
        conn: Database connection
        topic: Topic name
        config_exclusions: Set of excluded topics from config

    Returns:
        True if topic is excluded
    """
    if topic in config_exclusions:
        return True
    cursor = conn.execute("SELECT 1 FROM excluded_topics WHERE topic = ?", (topic,))
    return cursor.fetchone() is not None


def is_group_excluded(
    conn: sqlite3.Connection, group_id: str, config_exclusions: Set[str]
) -> bool:
    """Check if a group is excluded (in config or database).

    Args:
        conn: Database connection
        group_id: Consumer group ID
        config_exclusions: Set of excluded groups from config

    Returns:
        True if group is excluded
    """
    if group_id in config_exclusions:
        return True
    cursor = conn.execute(
        "SELECT 1 FROM excluded_groups WHERE group_id = ?", (group_id,)
    )
    return cursor.fetchone() is not None


def prune_partition_offsets(
    conn: sqlite3.Connection, topic: str, partition: int, keep_n: int
) -> int:
    """Delete all but the most recent keep_n rows for a topic/partition.

    Args:
        conn: Database connection
        topic: Topic name
        partition: Partition number
        keep_n: Number of most recent rows to keep

    Returns:
        Number of rows deleted
    """
    cursor = conn.execute(
        """DELETE FROM partition_offsets
           WHERE topic = ? AND partition = ?
           AND sampled_at NOT IN (
               SELECT sampled_at FROM partition_offsets
               WHERE topic = ? AND partition = ?
               ORDER BY sampled_at DESC
               LIMIT ?
           )""",
        (topic, partition, topic, partition, keep_n),
    )
    return cursor.rowcount


def prune_consumer_commits(
    conn: sqlite3.Connection, group_id: str, topic: str, partition: int, keep_n: int
) -> int:
    """Delete all but the most recent keep_n rows for a group/topic/partition.

    Args:
        conn: Database connection
        group_id: Consumer group ID
        topic: Topic name
        partition: Partition number
        keep_n: Number of most recent rows to keep

    Returns:
        Number of rows deleted
    """
    cursor = conn.execute(
        """DELETE FROM consumer_commits
           WHERE group_id = ? AND topic = ? AND partition = ?
           AND recorded_at NOT IN (
               SELECT recorded_at FROM consumer_commits
               WHERE group_id = ? AND topic = ? AND partition = ?
               ORDER BY recorded_at DESC
               LIMIT ?
           )""",
        (group_id, topic, partition, group_id, topic, partition, keep_n),
    )
    return cursor.rowcount


def get_all_partition_keys(conn: sqlite3.Connection) -> List[Tuple[str, int]]:
    """Get all distinct topic/partition combinations.

    Args:
        conn: Database connection

    Returns:
        List of (topic, partition) tuples
    """
    cursor = conn.execute("SELECT DISTINCT topic, partition FROM partition_offsets")
    return [(row[0], row[1]) for row in cursor.fetchall()]


def get_all_commit_keys(conn: sqlite3.Connection) -> List[Tuple[str, str, int]]:
    """Get all distinct group/topic/partition combinations.

    Args:
        conn: Database connection

    Returns:
        List of (group_id, topic, partition) tuples
    """
    cursor = conn.execute(
        "SELECT DISTINCT group_id, topic, partition FROM consumer_commits"
    )
    return [(row[0], row[1], row[2]) for row in cursor.fetchall()]


def run_incremental_vacuum(conn: sqlite3.Connection, pages: int = 100) -> None:
    """Run incremental vacuum to reclaim disk space.

    Args:
        conn: Database connection
        pages: Number of pages to vacuum

    Raises:
        ValueError: If pages is not a positive integer
    """
    if not isinstance(pages, int) or pages <= 0:
        raise ValueError(f"pages must be a positive integer, got {pages!r}")
    conn.execute(f"PRAGMA incremental_vacuum({pages})")


def get_group_tracked_topics(conn: sqlite3.Connection, group_id: str) -> List[str]:
    """Get all topics that have been tracked for a group.

    Args:
        conn: Database connection
        group_id: Consumer group ID

    Returns:
        List of topic names that have history for this group
    """
    cursor = conn.execute(
        "SELECT DISTINCT topic FROM consumer_commits WHERE group_id = ?",
        (group_id,),
    )
    return [row[0] for row in cursor.fetchall()]


def has_group_history(conn: sqlite3.Connection, group_id: str) -> bool:
    """Check if a group has any history in the database.

    Args:
        conn: Database connection
        group_id: Consumer group ID

    Returns:
        True if the group has any history (commits or status)
    """
    cursor = conn.execute(
        "SELECT 1 FROM consumer_commits WHERE group_id = ? LIMIT 1",
        (group_id,),
    )
    return cursor.fetchone() is not None


def get_all_groups_with_history(conn: sqlite3.Connection) -> List[str]:
    """Get all group IDs that have history in the database.

    Args:
        conn: Database connection

    Returns:
        List of group IDs with history
    """
    cursor = conn.execute("SELECT DISTINCT group_id FROM consumer_commits")
    return [row[0] for row in cursor.fetchall()]


def delete_group_data(conn: sqlite3.Connection, group_id: str) -> None:
    """Delete all database records for a consumer group.

    Removes rows from consumer_commits and group_status for the given group.
    Does NOT touch partition_offsets — that table is keyed by topic/partition
    only and is shared across groups; housekeeping prunes it by row count.

    Args:
        conn: Database connection
        group_id: Consumer group ID to remove
    """
    conn.execute(
        "DELETE FROM consumer_commits WHERE group_id = ?",
        (group_id,),
    )
    conn.execute(
        "DELETE FROM group_status WHERE group_id = ?",
        (group_id,),
    )
    # Note: caller is responsible for committing the transaction
