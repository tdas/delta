#!/usr/bin/env python3
"""
Simple example of using the SparkApp Python client.
"""

from sparkapp_client import SparkAppClient

def main():
    # Create client
    client = SparkAppClient(host="localhost", port=8080)

    # Check if server is running
    if not client.is_healthy():
        print("ERROR: SparkApp server is not running!")
        print("Please start the server with: ./start.sh")
        return

    print("✓ Connected to SparkApp server\n")

    # Example 1: Simple SELECT
    print("1. Simple SELECT query:")
    success, result, error = client.execute_sql("SELECT 1 as number, 'Hello' as message")
    if success:
        print(result)
    else:
        print(f"Error: {error}")

    print("\n" + "-"*50 + "\n")

    # Example 2: Create and use a table
    print("2. Create a table:")
    success, result, error = client.execute_sql(
        "CREATE TABLE IF NOT EXISTS products (id INT, name STRING, price DOUBLE)"
    )
    print(result if success else f"Error: {error}")

    print("\n3. Insert data:")
    success, result, error = client.execute_sql(
        "INSERT INTO products VALUES (1, 'Laptop', 999.99), (2, 'Mouse', 29.99), (3, 'Keyboard', 79.99)"
    )
    print(result if success else f"Error: {error}")

    print("\n4. Query the data:")
    success, result, error = client.execute_sql("SELECT * FROM products ORDER BY price DESC")
    if success:
        print(result)
    else:
        print(f"Error: {error}")

    print("\n" + "-"*50 + "\n")

    # Example 3: Aggregation
    print("5. Aggregation query:")
    success, result, error = client.execute_sql(
        "SELECT COUNT(*) as count, SUM(price) as total, AVG(price) as avg_price FROM products"
    )
    if success:
        print(result)
    else:
        print(f"Error: {error}")

if __name__ == "__main__":
    main()
