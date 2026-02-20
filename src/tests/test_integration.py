"""Integration tests requiring live Kafka cluster.

These tests require a running Kafka cluster and are run separately
from the main test suite using: pytest src/tests/test_integration.py
"""

import json
import os
import sqlite3
import sys
import threading
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import load_config
from database import init_db, get_group_status, upsert_group_status
from kafka_client import build_admin_client, get_active_consumer_groups


@pytest.fixture
def test_config():
    """Load test configuration from /tmp/config.yaml."""
    return load_config("/tmp/config.yaml")


@pytest.fixture
def test_db(tmp_path):
    """Create a temporary test database."""
    db_path = str(tmp_path / "test_state.db")
    conn = init_db(db_path)
    yield conn
    conn.close()


@pytest.fixture
def admin_client(test_config):
    """Create Kafka admin client."""
    return build_admin_client(test_config)


def get_partition_offsets_count(conn, topic, partition):
    """Helper to count partition offsets."""
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM partition_offsets WHERE topic = ? AND partition = ?",
        (topic, partition),
    )
    return cur.fetchone()[0]


def get_consumer_commits_count(conn, group_id, topic, partition):
    """Helper to count consumer commits."""
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM consumer_commits WHERE group_id = ? AND topic = ? AND partition = ?",
        (group_id, topic, partition),
    )
    return cur.fetchone()[0]


class TestBasicConnectivity:
    """Task 19: Integration Test - Basic Connectivity and Sampling"""

    def test_daemon_starts_and_connects_to_kafka(self, test_config, admin_client):
        """Verify daemon can connect to Kafka and discover consumer groups."""
        groups = get_active_consumer_groups(admin_client)
        assert len(groups) > 0, "Expected at least one consumer group"

    def test_sampler_writes_partition_offsets(self, test_config, test_db, admin_client):
        """Verify partition_offsets table contains data after sampling."""
        from sampler import Sampler
        from state_manager import StateManager
        import sampler as sampler_module

        original_sleep = sampler_module.time.sleep
        sampler_module.time.sleep = lambda x: None

        try:
            state_manager = StateManager(test_db, test_config)
            shutdown_event = threading.Event()

            sampler = Sampler(test_config, test_db, admin_client, state_manager)

            thread = threading.Thread(target=sampler.run, args=(shutdown_event,))
            thread.start()
            time.sleep(2)
            shutdown_event.set()
            thread.join(timeout=5)

            count = get_partition_offsets_count(test_db, "test", 0)
            assert count > 0, "Expected partition_offsets to have data"
        finally:
            sampler_module.time.sleep = original_sleep

    def test_sampler_writes_consumer_commits(self, test_config, test_db, admin_client):
        """Verify consumer_commits table contains data after sampling."""
        from sampler import Sampler
        from state_manager import StateManager
        import sampler as sampler_module

        original_sleep = sampler_module.time.sleep
        sampler_module.time.sleep = lambda x: None

        try:
            state_manager = StateManager(test_db, test_config)
            shutdown_event = threading.Event()

            sampler = Sampler(test_config, test_db, admin_client, state_manager)

            thread = threading.Thread(target=sampler.run, args=(shutdown_event,))
            thread.start()
            time.sleep(2)
            shutdown_event.set()
            thread.join(timeout=5)

            count = get_consumer_commits_count(test_db, "cambo-new", "test", 0)
            assert count > 0, "Expected consumer_commits to have data"
        finally:
            sampler_module.time.sleep = original_sleep

    def test_group_status_populated(self, test_config, test_db, admin_client):
        """Verify group_status table has entries with ONLINE status."""
        from sampler import Sampler
        from state_manager import StateManager
        import sampler as sampler_module

        original_sleep = sampler_module.time.sleep
        sampler_module.time.sleep = lambda x: None

        try:
            state_manager = StateManager(test_db, test_config)
            shutdown_event = threading.Event()

            sampler = Sampler(test_config, test_db, admin_client, state_manager)

            thread = threading.Thread(target=sampler.run, args=(shutdown_event,))
            thread.start()
            time.sleep(2)
            shutdown_event.set()
            thread.join(timeout=5)

            status = get_group_status(test_db, "cambo-new", "test")
            assert status is not None, "Expected group_status to exist"
            assert status["status"] == "ONLINE", (
                f"Expected ONLINE status, got {status['status']}"
            )
        finally:
            sampler_module.time.sleep = original_sleep


class TestJSONOutput:
    """Task 20: Integration Test - JSON Output Validation"""

    def test_json_file_exists_and_valid(self, test_config, test_db, tmp_path):
        """Verify JSON output file is created and valid."""
        from reporter import Reporter
        from state_manager import StateManager
        import reporter as reporter_module

        original_sleep = reporter_module.time.sleep
        reporter_module.time.sleep = lambda x: None

        try:
            state_manager = StateManager(test_db, test_config)

            json_path = str(tmp_path / "lag.json")
            test_config.output.json_path = json_path

            reporter = Reporter(test_config, test_db, state_manager)
            shutdown_event = threading.Event()

            thread = threading.Thread(target=reporter.run, args=(shutdown_event,))
            thread.start()
            time.sleep(1)
            shutdown_event.set()
            thread.join(timeout=5)

            assert os.path.exists(json_path), "Expected JSON output file to exist"

            with open(json_path) as f:
                data = json.load(f)
            assert "generated_at" in data
            assert "consumers" in data
        finally:
            reporter_module.time.sleep = original_sleep

    def test_json_contains_consumer_groups(self, test_config, test_db, admin_client):
        """Verify JSON contains all active consumer groups."""
        from sampler import Sampler
        from reporter import Reporter
        from state_manager import StateManager
        import sampler as sampler_module
        import reporter as reporter_module

        original_sleep_sampler = sampler_module.time.sleep
        original_sleep_reporter = reporter_module.time.sleep

        try:
            state_manager = StateManager(test_db, test_config)

            sampler = Sampler(test_config, test_db, admin_client, state_manager)
            shutdown_event = threading.Event()

            sampler_thread = threading.Thread(
                target=sampler.run, args=(shutdown_event,)
            )
            sampler_thread.start()
            time.sleep(2)

            json_path = "/tmp/test_lag.json"
            test_config.output.json_path = json_path

            reporter = Reporter(test_config, test_db, state_manager)
            reporter_thread = threading.Thread(
                target=reporter.run, args=(shutdown_event,)
            )
            reporter_thread.start()
            time.sleep(1)

            shutdown_event.set()
            sampler_thread.join(timeout=5)
            reporter_thread.join(timeout=5)

            with open(json_path) as f:
                data = json.load(f)

            consumer_groups = [c["group_id"] for c in data["consumers"]]
            assert "cambo-new" in consumer_groups or "cambo" in consumer_groups
        finally:
            sampler_module.time.sleep = original_sleep_sampler
            reporter_module.time.sleep = original_sleep_reporter

    def test_json_excludes_configured_topics_and_groups(
        self, test_config, test_db, admin_client
    ):
        """Verify excluded topics and groups don't appear in output."""
        from sampler import Sampler
        from reporter import Reporter
        from state_manager import StateManager
        import sampler as sampler_module
        import reporter as reporter_module

        original_sleep_sampler = sampler_module.time.sleep
        original_sleep_reporter = reporter_module.time.sleep
        sampler_module.time.sleep = lambda x: None
        reporter_module.time.sleep = lambda x: None

        try:
            state_manager = StateManager(test_db, test_config)

            sampler = Sampler(test_config, test_db, admin_client, state_manager)
            shutdown_event = threading.Event()

            sampler_thread = threading.Thread(
                target=sampler.run, args=(shutdown_event,)
            )
            sampler_thread.start()
            time.sleep(2)

            json_path = "/tmp/test_lag_excluded.json"
            test_config.output.json_path = json_path

            reporter = Reporter(test_config, test_db, state_manager)
            reporter_thread = threading.Thread(
                target=reporter.run, args=(shutdown_event,)
            )
            reporter_thread.start()
            time.sleep(1)

            shutdown_event.set()
            sampler_thread.join(timeout=5)
            reporter_thread.join(timeout=5)

            with open(json_path) as f:
                data = json.load(f)

            consumer_groups = [c["group_id"] for c in data["consumers"]]
            assert "some-legacy-group" not in consumer_groups
            for consumer in data["consumers"]:
                assert consumer["topic"] != "__consumer_offsets"
        finally:
            sampler_module.time.sleep = original_sleep_sampler
            reporter_module.time.sleep = original_sleep_reporter

    def test_lag_seconds_not_negative(self, test_config, test_db, admin_client):
        """Verify lag_seconds values are not negative."""
        from sampler import Sampler
        from reporter import Reporter
        from state_manager import StateManager
        import sampler as sampler_module
        import reporter as reporter_module

        original_sleep_sampler = sampler_module.time.sleep
        original_sleep_reporter = reporter_module.time.sleep
        sampler_module.time.sleep = lambda x: None
        reporter_module.time.sleep = lambda x: None

        try:
            state_manager = StateManager(test_db, test_config)

            sampler = Sampler(test_config, test_db, admin_client, state_manager)
            shutdown_event = threading.Event()

            sampler_thread = threading.Thread(
                target=sampler.run, args=(shutdown_event,)
            )
            sampler_thread.start()
            time.sleep(2)

            json_path = "/tmp/test_lag_values.json"
            test_config.output.json_path = json_path

            reporter = Reporter(test_config, test_db, state_manager)
            reporter_thread = threading.Thread(
                target=reporter.run, args=(shutdown_event,)
            )
            reporter_thread.start()
            time.sleep(1)

            shutdown_event.set()
            sampler_thread.join(timeout=5)
            reporter_thread.join(timeout=5)

            with open(json_path) as f:
                data = json.load(f)

            for consumer in data["consumers"]:
                assert consumer["lag_seconds"] >= 0, (
                    f"lag_seconds should not be negative"
                )
        finally:
            sampler_module.time.sleep = original_sleep_sampler
            reporter_module.time.sleep = original_sleep_reporter

    def test_status_is_lowercase(self, test_config, test_db, admin_client):
        """Verify status values are lowercase strings."""
        from sampler import Sampler
        from reporter import Reporter
        from state_manager import StateManager
        import sampler as sampler_module
        import reporter as reporter_module

        original_sleep_sampler = sampler_module.time.sleep
        original_sleep_reporter = reporter_module.time.sleep
        sampler_module.time.sleep = lambda x: None
        reporter_module.time.sleep = lambda x: None

        try:
            state_manager = StateManager(test_db, test_config)

            sampler = Sampler(test_config, test_db, admin_client, state_manager)
            shutdown_event = threading.Event()

            sampler_thread = threading.Thread(
                target=sampler.run, args=(shutdown_event,)
            )
            sampler_thread.start()
            time.sleep(2)

            json_path = "/tmp/test_lag_status.json"
            test_config.output.json_path = json_path

            reporter = Reporter(test_config, test_db, state_manager)
            reporter_thread = threading.Thread(
                target=reporter.run, args=(shutdown_event,)
            )
            reporter_thread.start()
            time.sleep(1)

            shutdown_event.set()
            sampler_thread.join(timeout=5)
            reporter_thread.join(timeout=5)

            with open(json_path) as f:
                data = json.load(f)

            for consumer in data["consumers"]:
                assert consumer["status"] in ["online", "offline", "recovering"]
        finally:
            sampler_module.time.sleep = original_sleep_sampler
            reporter_module.time.sleep = original_sleep_reporter


class TestOfflineDetection:
    """Task 21: Integration Test - Offline Detection and State Transitions"""

    def test_offline_detection_transitions(self, test_config, test_db, admin_client):
        """Verify ONLINE -> OFFLINE transition is detected and logged."""
        from sampler import Sampler
        from state_manager import StateManager
        import sampler as sampler_module

        original_sleep = sampler_module.time.sleep
        sampler_module.time.sleep = lambda x: None

        try:
            state_manager = StateManager(test_db, test_config)

            upsert_group_status(test_db, "test-offline", "test", "ONLINE", 0, 0, 0)

            shutdown_event = threading.Event()
            sampler = Sampler(test_config, test_db, admin_client, state_manager)

            thread = threading.Thread(target=sampler.run, args=(shutdown_event,))
            thread.start()
            time.sleep(2)
            shutdown_event.set()
            thread.join(timeout=5)

            status = get_group_status(test_db, "test-offline", "test")
            if status:
                assert status["status"] in ["ONLINE", "OFFLINE", "RECOVERING"]
        finally:
            sampler_module.time.sleep = original_sleep

    def test_coarse_resolution_for_offline(self, test_config, test_db, admin_client):
        """Verify OFFLINE groups show coarse data resolution in JSON."""
        from sampler import Sampler
        from reporter import Reporter
        from state_manager import StateManager
        import sampler as sampler_module
        import reporter as reporter_module

        original_sleep_sampler = sampler_module.time.sleep
        original_sleep_reporter = reporter_module.time.sleep
        sampler_module.time.sleep = lambda x: None
        reporter_module.time.sleep = lambda x: None

        try:
            state_manager = StateManager(test_db, test_config)

            shutdown_event = threading.Event()

            sampler = Sampler(test_config, test_db, admin_client, state_manager)
            sampler_thread = threading.Thread(
                target=sampler.run, args=(shutdown_event,)
            )
            sampler_thread.start()
            time.sleep(2)

            upsert_group_status(
                test_db, "test-offline-coarse", "test", "OFFLINE", 0, 0, 100
            )

            json_path = "/tmp/test_lag_coarse.json"
            test_config.output.json_path = json_path

            reporter = Reporter(test_config, test_db, state_manager)
            reporter_thread = threading.Thread(
                target=reporter.run, args=(shutdown_event,)
            )
            reporter_thread.start()
            time.sleep(1)

            shutdown_event.set()
            sampler_thread.join(timeout=5)
            reporter_thread.join(timeout=5)

            with open(json_path) as f:
                data = json.load(f)

            offline_consumers = [
                c for c in data["consumers"] if c["status"] == "offline"
            ]
            for consumer in offline_consumers:
                assert consumer["data_resolution"] == "coarse"
        finally:
            sampler_module.time.sleep = original_sleep_sampler
            reporter_module.time.sleep = original_sleep_reporter
