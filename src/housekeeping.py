"""Housekeeping thread module for database maintenance.

Handles periodic pruning and vacuum operations to manage database size.
"""

import logging
import sqlite3
import time
from typing import Any

import database


logger = logging.getLogger(__name__)


class Housekeeping:
    """Housekeeping thread that manages database size and performs vacuum.

    Runs periodically and enforces count-based row limits. Knows nothing
    about group status or data tiers.
    """

    def __init__(self, config: Any, db_path: str) -> None:
        """Initialize the housekeeping thread.

        Args:
            config: Configuration object with monitoring settings
            db_path: Path to the SQLite database file
        """
        self._config = config
        self._db_path = db_path
        self._db_conn = database.get_connection(db_path)

    def run(self, shutdown_event: Any) -> None:
        """Run the housekeeping loop.

        Continuously performs database maintenance at the configured interval
        until shutdown_event is set.

        Args:
            shutdown_event: Threading event to signal shutdown
        """
        while not shutdown_event.is_set():
            try:
                cycle_start = time.time()
                self._run_cycle()

                # Sleep for remainder of housekeeping interval
                elapsed = time.time() - cycle_start
                sleep_time = (
                    self._config.monitoring.housekeeping_interval_seconds - elapsed
                )
                if sleep_time > 0:
                    # Check shutdown_event during sleep
                    shutdown_event.wait(timeout=sleep_time)

            except Exception as e:
                logger.exception(f"Error in housekeeping cycle: {e}")
                if isinstance(e, sqlite3.DatabaseError):
                    logger.warning("DB error detected — reconnecting before next cycle")
                    try:
                        self._db_conn.close()
                    except Exception:
                        pass
                    self._db_conn = database.get_connection(self._db_path)
                # Sleep briefly before retrying
                if shutdown_event.wait(timeout=5):
                    break

    def _run_cycle(self) -> None:
        """Execute a single housekeeping cycle."""
        cycle_start = time.time()

        # Prune partition_offsets
        partition_keys = database.get_all_partition_keys(self._db_conn)
        total_partition_rows_deleted = 0

        for topic, partition in partition_keys:
            deleted = database.prune_partition_offsets(
                self._db_conn,
                topic,
                partition,
                self._config.monitoring.max_entries_per_partition,
            )
            total_partition_rows_deleted += deleted

        # Prune consumer_commits
        commit_keys = database.get_all_commit_keys(self._db_conn)
        total_commit_rows_deleted = 0

        for group_id, topic, partition in commit_keys:
            deleted = database.prune_consumer_commits(
                self._db_conn,
                group_id,
                topic,
                partition,
                self._config.monitoring.max_commit_entries_per_partition,
            )
            total_commit_rows_deleted += deleted

        # Run incremental vacuum
        database.run_incremental_vacuum(self._db_conn, pages=100)

        # Commit all prune operations in a single transaction
        database.commit_batch(self._db_conn)

        # Log summary
        elapsed = time.time() - cycle_start
        logger.info(
            f"Housekeeping cycle complete: "
            f"partition_offsets: {total_partition_rows_deleted} rows pruned, "
            f"consumer_commits: {total_commit_rows_deleted} rows pruned, "
            f"elapsed: {elapsed:.2f}s"
        )
