"""State manager module for thread-safe shared state.

Provides controlled access to in-memory state across threads with proper
synchronization.
"""

import copy
import threading
import time
from typing import Any, Dict, Optional, Tuple

import database


class StateManager:
    """Thread-safe manager for shared in-memory state.
    
    All access to shared state is protected by an RLock. The state manager
    is the single controlled access point for inter-thread shared data.
    """
    
    def __init__(self, db_conn: Any, config: Any) -> None:
        """Initialize the state manager and load persisted state from database.
        
        Args:
            db_conn: SQLite database connection
            config: Configuration object
        """
        self._db_conn = db_conn
        self._config = config
        self._lock = threading.RLock()
        
        # Initialize in-memory state structure
        self._state: Dict[str, Any] = {
            "group_statuses": {},
            "last_json_output": {},
            "thread_last_run": {
                "sampler": 0,
                "reporter": 0,
                "housekeeping": 0
            }
        }
        
        # Load persisted group statuses from database
        self._load_group_statuses()
    
    def _load_group_statuses(self) -> None:
        """Load all group statuses from database into memory."""
        statuses = database.load_all_group_statuses(self._db_conn)
        with self._lock:
            self._state["group_statuses"] = statuses
    
    def get_group_status(self, group_id: str, topic: str) -> Dict[str, Any]:
        """Get the status for a group/topic combination.
        
        Returns a copy of the status dict. If no status exists, returns a
        default ONLINE state dict.
        
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
            # Use current time to avoid huge time delta calculations
            current_time = int(time.time())
            return {
                "status": "ONLINE",
                "status_changed_at": current_time,
                "last_advancing_at": current_time,
                "consecutive_static": 0
            }
    
    def set_group_status(
        self,
        group_id: str,
        topic: str,
        status: str,
        status_changed_at: int,
        last_advancing_at: int,
        consecutive_static: int
    ) -> None:
        """Set the status for a group/topic combination.
        
        Updates both in-memory state and persists to database atomically
        within the same lock acquisition.
        
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
            "consecutive_static": consecutive_static
        }
        
        with self._lock:
            # Update in-memory state
            self._state["group_statuses"][key] = status_dict
            # Persist to database
            database.upsert_group_status(
                self._db_conn,
                group_id,
                topic,
                status,
                status_changed_at,
                last_advancing_at,
                consecutive_static
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
