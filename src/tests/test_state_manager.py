"""Tests for state_manager.py module."""

import copy
import pytest
import sqlite3
import threading
import time
from typing import Any, Dict, Tuple

import database
import state_manager
from state_manager import StateManager


class MockConfig:
    """Mock configuration object for testing."""
    pass


class TestStateManager:
    """Test suite for StateManager class."""
    
    def test_init_loads_preexisting_group_statuses(self, db_conn):
        """Test that StateManager loads pre-existing group statuses from database on construction."""
        # Insert some group statuses into the database first
        database.upsert_group_status(
            db_conn, "group1", "topic1", "ONLINE", 1000, 900, 0
        )
        database.upsert_group_status(
            db_conn, "group2", "topic2", "OFFLINE", 2000, 1500, 5
        )
        database.upsert_group_status(
            db_conn, "group1", "topic2", "RECOVERING", 3000, 2500, 2
        )
        
        config = MockConfig()
        sm = StateManager(db_conn, config)
        
        # Verify all statuses were loaded
        all_statuses = sm.get_all_group_statuses()
        assert len(all_statuses) == 3
        
        assert ("group1", "topic1") in all_statuses
        assert all_statuses[("group1", "topic1")]["status"] == "ONLINE"
        
        assert ("group2", "topic2") in all_statuses
        assert all_statuses[("group2", "topic2")]["status"] == "OFFLINE"
        
        assert ("group1", "topic2") in all_statuses
        assert all_statuses[("group1", "topic2")]["status"] == "RECOVERING"
    
    def test_get_group_status_unknown_returns_default_online(self, db_conn):
        """Test that get_group_status returns default ONLINE state for unknown groups."""
        config = MockConfig()
        sm = StateManager(db_conn, config)
        
        status = sm.get_group_status("unknown_group", "unknown_topic")
        
        assert status["status"] == "ONLINE"
        assert status["status_changed_at"] == 0
        assert status["last_advancing_at"] == 0
        assert status["consecutive_static"] == 0
    
    def test_set_group_status_updates_in_memory(self, db_conn):
        """Test that set_group_status updates in-memory state."""
        config = MockConfig()
        sm = StateManager(db_conn, config)
        
        sm.set_group_status("group1", "topic1", "OFFLINE", 1000, 900, 3)
        
        status = sm.get_group_status("group1", "topic1")
        assert status["status"] == "OFFLINE"
        assert status["status_changed_at"] == 1000
        assert status["last_advancing_at"] == 900
        assert status["consecutive_static"] == 3
    
    def test_set_group_status_persists_to_database(self, db_conn):
        """Test that set_group_status persists to database."""
        config = MockConfig()
        sm = StateManager(db_conn, config)
        
        sm.set_group_status("group1", "topic1", "RECOVERING", 2000, 1800, 1)
        
        # Verify by querying database directly
        db_status = database.get_group_status(db_conn, "group1", "topic1")
        assert db_status is not None
        assert db_status["status"] == "RECOVERING"
        assert db_status["status_changed_at"] == 2000
        assert db_status["last_advancing_at"] == 1800
        assert db_status["consecutive_static"] == 1
    
    def test_get_group_status_returns_copy(self, db_conn):
        """Test that get_group_status returns a copy, not a reference."""
        config = MockConfig()
        sm = StateManager(db_conn, config)
        
        sm.set_group_status("group1", "topic1", "ONLINE", 1000, 900, 0)
        
        # Get the status
        status1 = sm.get_group_status("group1", "topic1")
        
        # Modify the returned dict
        status1["status"] = "OFFLINE"
        status1["consecutive_static"] = 999
        
        # Get the status again
        status2 = sm.get_group_status("group1", "topic1")
        
        # Original should be unchanged
        assert status2["status"] == "ONLINE"
        assert status2["consecutive_static"] == 0
    
    def test_get_all_group_statuses_returns_copy(self, db_conn):
        """Test that get_all_group_statuses returns a copy, not a reference."""
        config = MockConfig()
        sm = StateManager(db_conn, config)
        
        sm.set_group_status("group1", "topic1", "ONLINE", 1000, 900, 0)
        
        # Get all statuses
        statuses1 = sm.get_all_group_statuses()
        
        # Modify the returned dict
        statuses1[("group1", "topic1")]["status"] = "OFFLINE"
        del statuses1[("group1", "topic1")]
        
        # Get all statuses again
        statuses2 = sm.get_all_group_statuses()
        
        # Original should be unchanged
        assert ("group1", "topic1") in statuses2
        assert statuses2[("group1", "topic1")]["status"] == "ONLINE"
    
    def test_update_and_get_thread_last_run(self, db_conn):
        """Test update_thread_last_run and get_thread_last_run."""
        config = MockConfig()
        sm = StateManager(db_conn, config)
        
        # Initially should be 0
        assert sm.get_thread_last_run("sampler") == 0
        assert sm.get_thread_last_run("reporter") == 0
        assert sm.get_thread_last_run("housekeeping") == 0
        
        # Update timestamps
        sm.update_thread_last_run("sampler", 1000)
        sm.update_thread_last_run("reporter", 2000)
        sm.update_thread_last_run("housekeeping", 3000)
        
        # Verify updates
        assert sm.get_thread_last_run("sampler") == 1000
        assert sm.get_thread_last_run("reporter") == 2000
        assert sm.get_thread_last_run("housekeeping") == 3000
        
        # Unknown thread returns 0
        assert sm.get_thread_last_run("unknown") == 0
    
    def test_set_and_get_last_json_output(self, db_conn):
        """Test set_last_json_output and get_last_json_output."""
        config = MockConfig()
        sm = StateManager(db_conn, config)
        
        # Initially should be empty
        assert sm.get_last_json_output() == {}
        
        # Set output
        output = {
            "timestamp": 1000,
            "consumers": [
                {"group_id": "group1", "lag_seconds": 60}
            ]
        }
        sm.set_last_json_output(output)
        
        # Verify
        retrieved = sm.get_last_json_output()
        assert retrieved["timestamp"] == 1000
        assert len(retrieved["consumers"]) == 1
        
        # Verify it's a copy
        retrieved["timestamp"] = 9999
        retrieved2 = sm.get_last_json_output()
        assert retrieved2["timestamp"] == 1000
    
    def test_concurrent_access_no_data_corruption(self, db_conn):
        """Test concurrent access from multiple threads."""
        config = MockConfig()
        sm = StateManager(db_conn, config)
        
        errors = []
        stop_event = threading.Event()
        
        def writer_thread(thread_id: int) -> None:
            """Thread that writes group statuses."""
            try:
                for i in range(100):
                    sm.set_group_status(
                        f"group{thread_id}",
                        f"topic{thread_id}",
                        "ONLINE",
                        int(time.time()),
                        int(time.time()) - 100,
                        i
                    )
                    time.sleep(0.001)  # Small delay to allow interleaving
            except Exception as e:
                errors.append(f"Writer {thread_id}: {e}")
        
        def reader_thread(thread_id: int) -> None:
            """Thread that reads group statuses."""
            try:
                for _ in range(100):
                    # Read various groups
                    for j in range(3):
                        _ = sm.get_group_status(f"group{j}", f"topic{j}")
                        _ = sm.get_all_group_statuses()
                        _ = sm.get_thread_last_run("sampler")
                    time.sleep(0.001)
            except Exception as e:
                errors.append(f"Reader {thread_id}: {e}")
        
        # Start threads
        threads = []
        
        # 5 writer threads
        for i in range(5):
            t = threading.Thread(target=writer_thread, args=(i,))
            threads.append(t)
        
        # 5 reader threads
        for i in range(5):
            t = threading.Thread(target=reader_thread, args=(i,))
            threads.append(t)
        
        # Run for at least 1 second
        start_time = time.time()
        for t in threads:
            t.start()
        
        # Wait for at least 1 second
        time.sleep(1.1)
        stop_event.set()
        
        # Join all threads
        for t in threads:
            t.join(timeout=5.0)
        
        elapsed = time.time() - start_time
        
        # Verify no errors occurred
        assert len(errors) == 0, f"Concurrent access errors: {errors}"
        
        # Verify we ran for at least 1 second
        assert elapsed >= 1.0
        
        # Verify data integrity - all groups should have valid states
        all_statuses = sm.get_all_group_statuses()
        assert len(all_statuses) >= 5  # At least one per writer thread
        
        for key, status in all_statuses.items():
            assert "status" in status
            assert "status_changed_at" in status
            assert "last_advancing_at" in status
            assert "consecutive_static" in status
            assert status["status"] in ["ONLINE", "OFFLINE", "RECOVERING"]
    
    def test_status_update_overwrites_previous(self, db_conn):
        """Test that status updates overwrite previous values."""
        config = MockConfig()
        sm = StateManager(db_conn, config)
        
        # Set initial status
        sm.set_group_status("group1", "topic1", "ONLINE", 1000, 900, 0)
        
        # Update to OFFLINE
        sm.set_group_status("group1", "topic1", "OFFLINE", 2000, 900, 5)
        
        status = sm.get_group_status("group1", "topic1")
        assert status["status"] == "OFFLINE"
        assert status["status_changed_at"] == 2000
        assert status["consecutive_static"] == 5
        
        # Verify in database as well
        db_status = database.get_group_status(db_conn, "group1", "topic1")
        assert db_status["status"] == "OFFLINE"
        
        # There should only be one row in the database (no duplicates)
        cursor = db_conn.execute(
            "SELECT COUNT(*) FROM group_status WHERE group_id = ? AND topic = ?",
            ("group1", "topic1")
        )
        count = cursor.fetchone()[0]
        assert count == 1
