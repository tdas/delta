#!/usr/bin/env python3
"""
Simple example of using the SparkShell class.

This example shows how to use SparkShell to automatically start a SparkApp
server and execute SQL commands, with automatic cleanup.
"""

from spark_shell import SparkShell


def main():
    print("=" * 70)
    print("SparkShell Example - Automatic Server Management")
    print("=" * 70)
    print("\nThis example will:")
    print("  1. Start a SparkApp server automatically")
    print("  2. Execute SQL commands")
    print("  3. Clean up automatically when done")
    print("\nFirst run may take 3-6 minutes to build...\n")
    print("=" * 70 + "\n")

    # Use SparkShell with context manager (recommended)
    # It automatically handles setup, build, start, and cleanup
    
    # Optional: Pass custom Spark configurations
    # spark_configs = {
    #     "spark.executor.memory": "2g",
    #     "spark.sql.shuffle.partitions": "10"
    # }
    # with SparkShell(source=".", port=8080, spark_configs=spark_configs) as shell:
    
    with SparkShell(source=".", port=8080) as shell:
        print("✓ SparkApp server started!\n")

        # Example 1: Simple SELECT
        print("1. Simple SELECT query:")
        result = shell.execute_sql("SELECT 1 as number, 'Hello' as message")
        print(result)
        print("\n" + "-"*50 + "\n")

        # Example 2: Create and use a table
        print("2. Create a table:")
        result = shell.execute_sql(
            "CREATE TABLE IF NOT EXISTS products (id INT, name STRING, price DOUBLE) USING parquet"
        )
        print(result)

        print("\n3. Insert data:")
        result = shell.execute_sql(
            "INSERT INTO products VALUES (1, 'Laptop', 999.99), (2, 'Mouse', 29.99), (3, 'Keyboard', 79.99)"
        )
        print(result)

        print("\n4. Query the data:")
        result = shell.execute_sql("SELECT * FROM products ORDER BY price DESC")
        print(result)
        print("\n" + "-"*50 + "\n")

        # Example 3: Aggregation
        print("5. Aggregation query:")
        result = shell.execute_sql(
            "SELECT COUNT(*) as count, SUM(price) as total, AVG(price) as avg_price FROM products"
        )
        print(result)

        print("\n" + "-"*50 + "\n")

        # Example 4: Error handling
        print("6. Error handling example:")
        try:
            result = shell.execute_sql("SELECT * FROM nonexistent_table")
            print(result)
        except RuntimeError as e:
            print(f"✓ Caught expected error: {e}")

        print("\n" + "-"*50 + "\n")

        # Cleanup
        print("7. Cleanup:")
        result = shell.execute_sql("DROP TABLE IF EXISTS products")
        print(result)

    # Server is automatically stopped and cleaned up when exiting the 'with' block
    print("\n" + "=" * 70)
    print("✓ Example completed! Server automatically shut down and cleaned up.")
    print("=" * 70)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
