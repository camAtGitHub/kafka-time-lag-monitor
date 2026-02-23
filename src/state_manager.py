"""State manager module for thread-safe shared state.

Provides controlled access to in-memory state across threads with proper
synchronization.

Startup Behavior Note:
    Groups/topics not in the database (first time seen) start with
    consecutive_static=0 and status=ONLINE. They will report as ONLINE
    for threshold cycles (e.g., 5 x 30s = 2.5 minutes) before potentially
    being detected as OFFLINE. This is a known startup blind spot for
    truly new groups. Groups with DB history are loaded from the database
    at startup and don't have this delay.
"""

import copy
import sqlite3
import threading
from typing import Any, Dict, Optional, Tuple

import database


class StateManager:
    """Thread-safe manager for shared in-memory state.

    All access to shared state is protected by an RLock. The state manager
    is the single controlled access point for inter-thread shared data.
    """

    def __init__(self, db_path: str, config: Any) -> None:
        """Initialize the state manager and load persisted state from database.

        Args:
            db_path: Path to the SQLite database file
            config: Configuration object
        """
        self._db_path = db_path
        self._config = config
        self._lock = threading.RLock()

        # Initialize in-memory state structure
        self._state: Dict[str, Any] = {
            "group_statuses": {},
            "last_json_output": {},
            "thread_last_run": {"sampler": 0, "reporter": 0, "housekeeping": 0},
        }

        # Load persisted group statuses from database
        self._load_group_statuses()

    def _load_group_statuses(self) -> None:
        """Load all group statuses from database into memory."""
        # Open connection inline for startup only
        conn = database.get_connection(self._db_path)
        try:
            statuses = database.load_all_group_statuses(conn)
            with self._lock:
                self._state["group_statuses"] = statuses
        finally:
            conn.close()

    def get_group_status(self, group_id: str, topic: str) -> Dict[str, Any]:
        """Get the status for a group/topic combination.

        Returns a copy of the status dict. If no status exists, returns a
        default ONLINE state dict with consecutive_static=0. This means
        newly discovered groups will take threshold cycles to detect as
        OFFLINE (see module docstring for startup behavior details).

        Args:
            group_id: Consumer group ID
            topic: Topic name

        Returns:
            Status dict with status, status_changed_at, last_advancing_at,
            and consecutive_static fields
        """
        key = (group_id, topic)
        with self._lock:
            if key in self._state["group_statuses"]:
                return copy.deepcopy(self._state["group_statuses"][key])
            # Return default ONLINE state for unknown groups
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
    ) -> None:
        """Set the status for a group/topic combination.

        Updates in-memory state only. Persistence is handled by
        persist_group_statuses() which is called by the sampler at the
        end of its cycle as part of the batch transaction.

        Args:
            group_id: Consumer group ID
            topic: Topic name
            status: Status string (ONLINE, OFFLINE, or RECOVERING)
            status_changed_at: Unix timestamp of status change
            last_advancing_at: Unix timestamp when offset last advanced
            consecutive_static: Counter for consecutive static samples
        """
        key = (group_id, topic)
        status_dict = {
            "status": status,
            "status_changed_at": status_changed_at,
            "last_advancing_at": last_advancing_at,
            "consecutive_static": consecutive_static,
        }

        with self._lock:
            self._state["group_statuses"][key] = status_dict

    def persist_group_statuses(self, conn: sqlite3.Connection) -> None:
        """Persist all group statuses to database using the provided connection.

        Called by the sampler at the end of its cycle to fold all group status
        updates into the sampler's batch transaction. No commit is issued here;
        that's handled by commit_batch().

        Args:
            conn: Database connection (sampler's connection)
        """
        # Snapshot current group statuses under lock
        with self._lock:
            statuses_snapshot = copy.deepcopy(self._state["group_statuses"])

        # Write each entry to database using sampler's connection
        for (group_id, topic), status_dict in statuses_snapshot.items():
            database.upsert_group_status(
                conn,
                group_id,
                topic,
                status_dict["status"],
                status_dict["status_changed_at"],
                status_dict["last_advancing_at"],
                status_dict["consecutive_static"],
            )

    def get_all_group_statuses(self) -> Dict[Tuple[str, str], Dict[str, Any]]:
        """Get a snapshot copy of all group statuses.

        Returns:
            Deep copy of the full status dict keyed by (group_id, topic)
        """
        with self._lock:
            return copy.deepcopy(self._state["group_statuses"])

    def update_thread_last_run(self, thread_name: str, timestamp: int) -> None:
        """Update the last run timestamp for a thread.

        Args:
            thread_name: Name of the thread (sampler, reporter, housekeeping)
            timestamp: Unix timestamp
        """
        with self._lock:
            self._state["thread_last_run"][thread_name] = timestamp

    def get_thread_last_run(self, thread_name: str) -> int:
        """Get the last run timestamp for a thread.

        Args:
            thread_name: Name of the thread (sampler, reporter, housekeeping)

        Returns:
            Unix timestamp, or 0 if thread has never run
        """
        with self._lock:
            return self._state["thread_last_run"].get(thread_name, 0)

    def set_last_json_output(self, output_dict: Dict[str, Any]) -> None:
        """Set the most recent JSON output.

        Args:
            output_dict: The output dictionary to store
        """
        with self._lock:
            self._state["last_json_output"] = copy.deepcopy(output_dict)

    def get_last_json_output(self) -> Dict[str, Any]:
        """Get the most recent JSON output.

        Returns:
            Copy of the last JSON output dict, or empty dict if none set
        """
        with self._lock:
            return copy.deepcopy(self._state["last_json_output"])

    def remove_group(self, group_id: str) -> None:
        """Remove all in-memory state for a consumer group.

        Called when a ghost group is retired from the database. Clears all
        (group_id, topic) keys from the in-memory status dict so the group
        is no longer iterated over or written by persist_group_statuses.

        Args:
            group_id: Consumer group ID to remove
        """
        with self._lock:
            keys_to_remove = [
                key for key in self._state["group_statuses"] if key[0] == group_id
            ]
            for key in keys_to_remove:
                del self._state["group_statuses"][key]
            if keys_to_remove:
                import logging
                logger = logging.getLogger(__name__)
                logger.debug(
                    f"Removed in-memory state for retired group {group_id} "
                    f"({len(keys_to_remove)} topic(s))"
                )
