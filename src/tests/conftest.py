"""Pytest configuration and shared fixtures."""

import pytest
import sqlite3
import sys
import os
import tempfile

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import database


@pytest.fixture
def db_path(tmp_path):
    """Provide a path to a temporary SQLite database file."""
    return str(tmp_path / "test.db")


@pytest.fixture
def db_path_initialized(db_path):
    """Provide a path to a temporary SQLite database file with schema initialized."""
    database.init_db(db_path)
    return db_path


@pytest.fixture
def db_conn(db_path):
    """Provide a SQLite database connection initialized with schema.

    Creates a file-based database at db_path so that multiple connections
    (e.g., from worker threads) can access the same database.
    """
    conn = database.init_db(db_path)
    yield conn
    conn.close()
