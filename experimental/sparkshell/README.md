# SparkApp - Spark SQL REST Server

A simple REST API server that executes Spark SQL commands and returns results as JSON.

## Features

- REST API with JSON request/response format
- Execute SQL commands via simple HTTP POST requests
- Returns formatted query results or command execution status
- Supports both queries (SELECT) and commands (CREATE, INSERT, etc.)
- Easy to use with curl, Postman, or any HTTP client

## Building the Application

This project is completely self-contained with its own SBT installation.

```bash
cd experimental/sparkshell
build/sbt compile
```

## Running the Server

### Interactive Mode

Run the server in the foreground (you'll see all logs):

```bash
cd experimental/sparkshell
build/sbt run
```

Default port is 8080 if not specified.

To specify a custom port:
```bash
build/sbt "run 3000"
```

### Background Mode (Daemon)

The project includes convenient scripts to manage the server as a background process:

**Start the server:**
```bash
bin/start.sh [port]
```
Example:
```bash
bin/start.sh          # Start on default port 8080
bin/start.sh 3000     # Start on custom port 3000
```

**Stop the server:**
```bash
bin/stop.sh
```

**Restart the server:**
```bash
bin/restart.sh [port]
```

**Check server status:**
```bash
bin/status.sh
```

**View logs:**
```bash
tail -f sparkapp.log
```

The background scripts will:
- Save the process PID to `sparkapp.pid`
- Write all logs to `sparkapp.log`
- Handle graceful shutdown with fallback to force kill if needed
- Detect and clean up stale PID files

## API Endpoints

### Health Check
```bash
curl http://localhost:8080/health
```

Response:
```json
{
  "status": "ok",
  "message": "SparkApp server is running"
}
```

### Server Info
```bash
curl http://localhost:8080/info
```

Response:
```json
{
  "sparkVersion": "3.5.0",
  "port": "8080",
  "endpoints": {
    "health": "GET /health",
    "execute": "POST /sql",
    "info": "GET /info"
  }
}
```

### Execute SQL
```bash
curl -X POST http://localhost:8080/sql \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT 1 as id, '\''Alice'\'' as name"}'
```

Response:
```json
{
  "success": true,
  "result": "id | name\n-----------\n1 | Alice\n\nTotal rows: 1",
  "error": null
}
```

## Usage Examples with curl

### Create a Table
```bash
curl -X POST http://localhost:8080/sql \
  -H "Content-Type: application/json" \
  -d '{"sql": "CREATE TABLE users (id INT, name STRING, age INT)"}'
```

### Insert Data
```bash
curl -X POST http://localhost:8080/sql \
  -H "Content-Type: application/json" \
  -d '{"sql": "INSERT INTO users VALUES (1, '\''Alice'\'', 30), (2, '\''Bob'\'', 25)"}'
```

### Query Data
```bash
curl -X POST http://localhost:8080/sql \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM users"}'
```

### Query with WHERE Clause
```bash
curl -X POST http://localhost:8080/sql \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT name, age FROM users WHERE age > 25"}'
```

### Error Response Example
If there's an error, you'll get:
```json
{
  "success": false,
  "result": null,
  "error": "Table or view not found: nonexistent_table"
}
```

## Python Client

A Python client module is included for easy integration with Python applications.

### Installation

Install the required dependencies:
```bash
pip install -r requirements.txt
# or
pip install requests
```

### Usage

#### Using the Client Class

```python
from sparkapp_client import SparkAppClient

# Create a client
client = SparkAppClient(host="localhost", port=8080)

# Check server health
health = client.health_check()
print(health)  # {"status": "ok", "message": "SparkApp server is running"}

# Get server info
info = client.server_info()
print(f"Spark Version: {info['sparkVersion']}")

# Execute SQL
success, result, error = client.execute_sql("SELECT 1 as id, 'Alice' as name")
if success:
    print(result)
else:
    print(f"Error: {error}")
```

#### Using the Convenience Function

```python
from sparkapp_client import execute_sql

# Quick one-liner for SQL execution
success, result, error = execute_sql("SELECT * FROM users")
if success:
    print(result)
```

#### Running the Example Scripts

The client includes comprehensive examples:

```bash
# Start the server first
bin/start.sh

# Run the full example
python sparkapp_client.py

# Or run the simpler example
python example.py
```

These will demonstrate:
- Health checking
- Creating tables
- Inserting data
- Querying data
- Aggregations
- Error handling

### Client API Reference

**SparkAppClient(host="localhost", port=8080)**
- `execute_sql(sql: str) -> (bool, str, str)`: Execute SQL and return (success, result, error)
- `health_check() -> dict`: Get server health status
- `server_info() -> dict`: Get server information including Spark version
- `is_healthy() -> bool`: Check if server is healthy (returns True/False)

**execute_sql(sql, host="localhost", port=8080)**
- Convenience function for quick SQL execution without creating a client instance

## Testing

The project includes comprehensive test suites for both Scala and Python code.

### Running All Tests

Use the unified test runner:

```bash
./run-tests
```

This script will:
- Run all Scala (SBT) tests
- Run all Python integration tests
- Exit with error code if any tests fail
- Show a summary of results

### Running Individual Test Suites

**Scala Tests (SBT):**
```bash
# Run all Scala tests
build/sbt test

# Run specific test suite
build/sbt "testOnly com.sparkapp.SparkSqlExecutorSpec"
build/sbt "testOnly com.sparkapp.JsonSerializationSpec"
```

**Python Tests:**
```bash
# Install test dependencies first
pip install -r requirements.txt

# Start the server
bin/start.sh

# Run Python tests
python -m pytest tests/test_sparkapp_client.py -v

# Or run directly
python tests/test_sparkapp_client.py
```

### Test Coverage

**Scala Tests** (14 tests):
- SparkSqlExecutor tests: SQL execution, error handling, aggregations
- JSON serialization tests: Request/response serialization

**Python Tests** (13+ tests):
- Health check and server info endpoints
- Simple SELECT queries
- CREATE TABLE and INSERT operations
- SELECT with WHERE clauses
- Aggregation queries (COUNT, AVG, MAX)
- Error handling (invalid SQL, empty SQL)
- Offline server detection

All tests use temporary tables that are cleaned up after completion.

## Project Structure

```
experimental/sparkshell/
├── bin/                      # Management scripts
│   ├── start.sh             # Start server in background
│   ├── stop.sh              # Stop server
│   ├── restart.sh           # Restart server
│   └── status.sh            # Check server status
├── build/                    # Self-contained SBT installation
├── src/main/
│   └── scala/com/sparkapp/
│       ├── RestApi.scala             # REST API implementation
│       ├── SparkAppServer.scala      # Server entry point
│       └── SparkSqlExecutor.scala    # SQL execution logic
├── src/test/scala/com/sparkapp/  # Scala tests
│   ├── SparkSqlExecutorSpec.scala
│   └── JsonSerializationSpec.scala
├── tests/                    # Python tests
│   ├── test_sparkapp_client.py
│   └── __init__.py
├── sparkapp_client.py        # Python client library
├── example.py                # Simple usage example
├── run-tests                 # Unified test runner
├── build.sbt                 # Build configuration
└── README.md
```

## Architecture

1. **RestApi**: REST API implementation using Spark Java framework
2. **SparkSqlExecutor**: Executes SQL commands using Spark and formats results
3. **SparkAppServer**: Main entry point that initializes Spark and starts the REST server

## Notes

- The server runs Spark in local mode (`local[*]`)
- Log level is set to WARN to reduce noise
- The server will gracefully shutdown Spark when terminated
- Default port is 8080 (configurable)
- Uses Spark Java framework (not Apache Spark) for HTTP routing
- Supports CORS for cross-origin requests
