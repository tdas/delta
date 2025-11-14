#!/usr/bin/env python3
"""
Integration tests for SparkApp client.

These tests require a running SparkApp server.
Run with: python -m pytest test/test_sparkapp_client.py -v
"""

import unittest
import time
import sys
import os

# Add parent directory to path to import sparkapp_client
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sparkapp_client import SparkAppClient, execute_sql


class TestSparkAppClient(unittest.TestCase):
    """Test cases for SparkApp client."""

    @classmethod
    def setUpClass(cls):
        """Set up test fixtures that are used by all tests."""
        cls.client = SparkAppClient(host="localhost", port=8080)

        # Check if server is running
        if not cls.client.is_healthy():
            raise RuntimeError(
                "SparkApp server is not running on localhost:8080. "
                "Please start the server with: bin/start.sh"
            )

        print("\n✓ Connected to SparkApp server")

    def test_01_health_check(self):
        """Test health check endpoint."""
        health = self.client.health_check()
        self.assertIsInstance(health, dict)
        self.assertEqual(health.get("status"), "ok")
        self.assertIn("message", health)

    def test_02_server_info(self):
        """Test server info endpoint."""
        info = self.client.server_info()
        self.assertIsInstance(info, dict)
        self.assertIn("sparkVersion", info)
        self.assertIn("port", info)
        self.assertIn("endpoints", info)

    def test_03_is_healthy(self):
        """Test is_healthy method."""
        self.assertTrue(self.client.is_healthy())

    def test_04_simple_select(self):
        """Test simple SELECT query."""
        success, result, error = self.client.execute_sql(
            "SELECT 1 as id, 'test' as name"
        )
        self.assertTrue(success, f"Query failed: {error}")
        self.assertIsNotNone(result)
        self.assertIn("id", result)
        self.assertIn("name", result)
        self.assertIsNone(error)

    def test_05_create_table(self):
        """Test CREATE TABLE command."""
        # Drop table if exists
        self.client.execute_sql("DROP TABLE IF EXISTS test_users")

        # Create table
        success, result, error = self.client.execute_sql(
            "CREATE TABLE test_users (id INT, name STRING, age INT)"
        )
        self.assertTrue(success, f"CREATE TABLE failed: {error}")
        self.assertIsNotNone(result)

    def test_06_insert_data(self):
        """Test INSERT command."""
        success, result, error = self.client.execute_sql(
            "INSERT INTO test_users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)"
        )
        self.assertTrue(success, f"INSERT failed: {error}")
        self.assertIsNotNone(result)

    def test_07_select_all(self):
        """Test SELECT * query."""
        success, result, error = self.client.execute_sql(
            "SELECT * FROM test_users ORDER BY id"
        )
        self.assertTrue(success, f"SELECT failed: {error}")
        self.assertIsNotNone(result)
        self.assertIn("Alice", result)
        self.assertIn("Bob", result)
        self.assertIn("Charlie", result)

    def test_08_select_with_where(self):
        """Test SELECT with WHERE clause."""
        success, result, error = self.client.execute_sql(
            "SELECT name, age FROM test_users WHERE age > 25 ORDER BY age"
        )
        self.assertTrue(success, f"SELECT with WHERE failed: {error}")
        self.assertIsNotNone(result)
        self.assertIn("Alice", result)
        self.assertIn("Charlie", result)
        # Bob should not be in results (age = 25, not > 25)
        self.assertNotIn("Bob", result)

    def test_09_aggregation(self):
        """Test aggregation query."""
        success, result, error = self.client.execute_sql(
            "SELECT COUNT(*) as count, AVG(age) as avg_age, MAX(age) as max_age FROM test_users"
        )
        self.assertTrue(success, f"Aggregation query failed: {error}")
        self.assertIsNotNone(result)
        self.assertIn("count", result)
        self.assertIn("avg_age", result)
        self.assertIn("max_age", result)

    def test_10_invalid_sql(self):
        """Test error handling with invalid SQL."""
        success, result, error = self.client.execute_sql(
            "SELECT * FROM nonexistent_table"
        )
        self.assertFalse(success)
        self.assertIsNone(result)
        self.assertIsNotNone(error)
        # Check for "table" in error message (case insensitive)
        self.assertTrue(
            "table" in error.lower() or "view" in error.lower(),
            f"Expected 'table' or 'view' in error message, got: {error}"
        )

    def test_11_empty_sql(self):
        """Test error handling with empty SQL."""
        success, result, error = self.client.execute_sql("")
        self.assertFalse(success)
        self.assertIsNone(result)
        self.assertIsNotNone(error)

    def test_12_convenience_function(self):
        """Test the convenience execute_sql function."""
        success, result, error = execute_sql(
            "SELECT 'test' as value",
            host="localhost",
            port=8080
        )
        self.assertTrue(success, f"Convenience function failed: {error}")
        self.assertIsNotNone(result)

    @classmethod
    def tearDownClass(cls):
        """Clean up after all tests."""
        # Drop test table
        cls.client.execute_sql("DROP TABLE IF EXISTS test_users")
        print("\n✓ Cleanup completed")


class TestSparkAppClientOffline(unittest.TestCase):
    """Test cases for client behavior when server is offline."""

    def test_unhealthy_server(self):
        """Test is_healthy with non-existent server."""
        client = SparkAppClient(host="localhost", port=9999)
        self.assertFalse(client.is_healthy())


if __name__ == "__main__":
    # Run tests with verbose output
    unittest.main(verbosity=2)
