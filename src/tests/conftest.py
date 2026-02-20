"""Pytest configuration and shared fixtures."""

import pytest
import sqlite3
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import database


@pytest.fixture
def db_conn():
    """Provide an in-memory SQLite database initialized with schema."""
    conn = database.init_db(":memory:")
    yield conn
    conn.close()
