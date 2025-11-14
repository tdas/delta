#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Integration tests for SparkShell class.

These tests will automatically start a SparkApp server using SparkShell,
run tests against it, and clean up automatically.

Run with: python -m pytest tests/test_spark_shell.py -v
Or: python tests/test_spark_shell.py
"""

import unittest
import sys
import os

# Add parent directory to path to import spark_shell
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from spark_shell import SparkShell


class TestSparkShell(unittest.TestCase):
    """Test cases for SparkShell class."""

    @classmethod
    def setUpClass(cls):
        """Set up test fixtures - start SparkShell server."""
        print("\n" + "="*70)
        print("Setting up SparkShell for testing...")
        print("="*70)
        
        # Get the parent directory (sparkshell directory)
        test_dir = os.path.dirname(os.path.abspath(__file__))
        sparkshell_dir = os.path.dirname(test_dir)
        
        # Create SparkShell instance
        # Use a unique port to avoid conflicts
        cls.shell = SparkShell(
            source=sparkshell_dir,
            port=8090,
            auto_build=True,
            auto_start=True,
            cleanup_on_exit=True
        )
        
        # Start it manually (not using context manager since we want it for all tests)
        cls.shell.setup()
        print("✓ Setup complete")
        
        cls.shell.build()
        print("✓ Build complete")
        
        cls.shell.start()
        print("✓ Server started on port 8090")
        print("="*70 + "\n")

    @classmethod
    def tearDownClass(cls):
        """Clean up after all tests - shutdown SparkShell server."""
        print("\n" + "="*70)
        print("Cleaning up SparkShell...")
        print("="*70)
        
        # Drop test table if it exists
        try:
            cls.shell.execute_sql("DROP TABLE IF EXISTS test_users")
            print("✓ Test table dropped")
        except:
            pass
        
        cls.shell.shutdown()
        print("✓ Server shutdown")
        
        cls.shell.cleanup()
        print("✓ Cleanup complete")
        print("="*70 + "\n")

    def test_01_get_server_info(self):
        """Test get_server_info method."""
        info = self.shell.get_server_info()
        self.assertIsInstance(info, dict)
        self.assertIn("sparkVersion", info)
        self.assertIn("port", info)
        self.assertEqual(str(info["port"]), "8090")
        self.assertIn("endpoints", info)
        print(f"✓ Server info: Spark {info['sparkVersion']} on port {info['port']}")

    def test_02_table_operations(self):
        """Test CREATE, INSERT, SELECT, and WHERE operations."""
        # Drop table if exists
        try:
            self.shell.execute_sql("DROP TABLE IF EXISTS test_users")
        except:
            pass

        # Create table
        result = self.shell.execute_sql(
            "CREATE TABLE test_users (id INT, name STRING, age INT) USING parquet"
        )
        self.assertIsNotNone(result)
        
        # Insert data
        result = self.shell.execute_sql(
            "INSERT INTO test_users VALUES (1, 'Alice', 30), (2, 'Bob', 25)"
        )
        self.assertIsNotNone(result)
        
        # Query data
        result = self.shell.execute_sql("SELECT * FROM test_users WHERE age > 25")
        self.assertIsNotNone(result)
        self.assertIn("Alice", result)
        self.assertNotIn("Bob", result)  # age = 25, not > 25
        print("✓ Table operations (CREATE/INSERT/SELECT/WHERE) successful")

    def test_03_aggregation(self):
        """Test aggregation query."""
        result = self.shell.execute_sql(
            "SELECT COUNT(*) as count, AVG(age) as avg_age FROM test_users"
        )
        self.assertIsNotNone(result)
        self.assertIsInstance(result, str)
        self.assertIn("count", result)
        self.assertIn("avg_age", result)
        print("✓ Aggregation query successful")

    def test_04_error_handling(self):
        """Test error handling with invalid and empty SQL."""
        # Invalid SQL
        with self.assertRaises(RuntimeError) as context:
            self.shell.execute_sql("SELECT * FROM nonexistent_table")
        error_msg = str(context.exception)
        self.assertTrue(
            "table" in error_msg.lower() or "view" in error_msg.lower() or "not found" in error_msg.lower(),
            f"Expected error message, got: {error_msg}"
        )
        
        # Empty SQL
        with self.assertRaises(RuntimeError):
            self.shell.execute_sql("")
        
        print("✓ Error handling works correctly")


class TestSparkShellContextManager(unittest.TestCase):
    """Test SparkShell context manager functionality."""

    def test_context_manager(self):
        """Test SparkShell with context manager (with statement)."""
        test_dir = os.path.dirname(os.path.abspath(__file__))
        sparkshell_dir = os.path.dirname(test_dir)
        
        with SparkShell(source=sparkshell_dir, port=8091, cleanup_on_exit=True) as shell:
            # Test SQL execution
            result = shell.execute_sql("SELECT 1 as test_value")
            self.assertIsNotNone(result)
            self.assertIn("test_value", result)
        
        # After exiting context, server should be shutdown
        self.assertFalse(shell.is_ready)
        print("✓ Context manager works correctly")


class TestSparkShellManualControl(unittest.TestCase):
    """Test SparkShell manual control (step-by-step)."""

    def test_manual_lifecycle(self):
        """Test manual control of SparkShell lifecycle."""
        test_dir = os.path.dirname(os.path.abspath(__file__))
        sparkshell_dir = os.path.dirname(test_dir)
        
        shell = SparkShell(
            source=sparkshell_dir,
            port=8092,
            auto_build=False,
            auto_start=False,
            cleanup_on_exit=False
        )
        
        try:
            # Test setup and build
            shell.setup()
            self.assertIsNotNone(shell.work_dir)
            shell.build()
            self.assertIsNotNone(shell.jar_path)
            print("✓ Manual setup/build successful")
            
            # Test start and SQL execution
            shell.start()
            self.assertTrue(shell.is_ready)
            result = shell.execute_sql("SELECT 1 as val")
            self.assertIn("val", result)
            print("✓ Manual start/SQL successful")
            
        finally:
            shell.shutdown()
            shell.cleanup()
            print("✓ Manual cleanup successful")


if __name__ == "__main__":
    # Run tests with verbose output
    print("\n" + "="*70)
    print("SparkShell Integration Tests")
    print("="*70)
    print("This will build and start SparkShell, run tests, and cleanup.")
    print("First run may take 3-6 minutes due to SBT build.")
    print("="*70 + "\n")
    
    unittest.main(verbosity=2)

