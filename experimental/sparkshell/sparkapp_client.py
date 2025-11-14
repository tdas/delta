#!/usr/bin/env python3
"""
SparkApp REST API Client

A simple Python client for interacting with the SparkApp REST server.
"""

import json
import requests
from typing import Dict, Optional, Tuple


class SparkAppClient:
    """Client for SparkApp REST API."""

    def __init__(self, host: str = "localhost", port: int = 8080):
        """
        Initialize the SparkApp client.

        Args:
            host: Server hostname (default: localhost)
            port: Server port (default: 8080)
        """
        self.base_url = f"http://{host}:{port}"

    def execute_sql(self, sql: str) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        Execute a SQL command on the SparkApp server.

        Args:
            sql: The SQL command to execute

        Returns:
            A tuple of (success, result, error)
            - success: Boolean indicating if the command succeeded
            - result: The query result as a string (None if failed)
            - error: The error message (None if succeeded)

        Raises:
            requests.exceptions.RequestException: If the HTTP request fails
        """
        url = f"{self.base_url}/sql"
        payload = {"sql": sql}
        headers = {"Content-Type": "application/json"}

        response = requests.post(url, json=payload, headers=headers)
        data = response.json()

        return (
            data.get("success", False),
            data.get("result"),
            data.get("error")
        )

    def health_check(self) -> Dict:
        """
        Check if the server is running.

        Returns:
            Dictionary with server health status

        Raises:
            requests.exceptions.RequestException: If the HTTP request fails
        """
        url = f"{self.base_url}/health"
        response = requests.get(url)
        return response.json()

    def server_info(self) -> Dict:
        """
        Get server information including Spark version and available endpoints.

        Returns:
            Dictionary with server information

        Raises:
            requests.exceptions.RequestException: If the HTTP request fails
        """
        url = f"{self.base_url}/info"
        response = requests.get(url)
        return response.json()

    def is_healthy(self) -> bool:
        """
        Check if the server is healthy.

        Returns:
            True if server is running and healthy, False otherwise
        """
        try:
            health = self.health_check()
            return health.get("status") == "ok"
        except:
            return False


def execute_sql(sql: str, host: str = "localhost", port: int = 8080) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Convenience function to execute SQL without creating a client instance.

    Args:
        sql: The SQL command to execute
        host: Server hostname (default: localhost)
        port: Server port (default: 8080)

    Returns:
        A tuple of (success, result, error)

    Example:
        >>> success, result, error = execute_sql("SELECT 1 as id, 'Alice' as name")
        >>> if success:
        >>>     print(result)
    """
    client = SparkAppClient(host, port)
    return client.execute_sql(sql)


# Example usage
if __name__ == "__main__":
    # Create a client
    client = SparkAppClient()

    # Check server health
    print("Checking server health...")
    try:
        health = client.health_check()
        print(f"Health: {health}")
    except Exception as e:
        print(f"Failed to connect: {e}")
        exit(1)

    # Get server info
    print("\nGetting server info...")
    info = client.server_info()
    print(f"Spark Version: {info.get('sparkVersion')}")
    print(f"Port: {info.get('port')}")

    # Execute SQL commands
    print("\n" + "="*50)
    print("Example 1: Create a table")
    print("="*50)
    success, result, error = client.execute_sql(
        "CREATE TABLE IF NOT EXISTS users (id INT, name STRING, age INT)"
    )
    if success:
        print(f"✓ {result}")
    else:
        print(f"✗ Error: {error}")

    print("\n" + "="*50)
    print("Example 2: Insert data")
    print("="*50)
    success, result, error = client.execute_sql(
        "INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)"
    )
    if success:
        print(f"✓ {result}")
    else:
        print(f"✗ Error: {error}")

    print("\n" + "="*50)
    print("Example 3: Query all data")
    print("="*50)
    success, result, error = client.execute_sql("SELECT * FROM users")
    if success:
        print("✓ Query result:")
        print(result)
    else:
        print(f"✗ Error: {error}")

    print("\n" + "="*50)
    print("Example 4: Query with WHERE clause")
    print("="*50)
    success, result, error = client.execute_sql(
        "SELECT name, age FROM users WHERE age > 25"
    )
    if success:
        print("✓ Query result:")
        print(result)
    else:
        print(f"✗ Error: {error}")

    print("\n" + "="*50)
    print("Example 5: Aggregation query")
    print("="*50)
    success, result, error = client.execute_sql(
        "SELECT COUNT(*) as total_users, AVG(age) as avg_age FROM users"
    )
    if success:
        print("✓ Query result:")
        print(result)
    else:
        print(f"✗ Error: {error}")

    print("\n" + "="*50)
    print("Example 6: Error handling")
    print("="*50)
    success, result, error = client.execute_sql("SELECT * FROM nonexistent_table")
    if success:
        print(f"✓ {result}")
    else:
        print(f"✗ Error: {error}")
