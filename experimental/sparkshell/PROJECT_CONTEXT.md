# SparkApp Project Context

## Overview

**SparkApp** is a self-contained REST API server that executes Apache Spark SQL commands and returns results as JSON. It's designed to provide a simple HTTP interface to Spark SQL, making it easy to execute queries from any HTTP client or programming language.

## Project Location

```
/Users/tdas/Projects/delta2/experimental/sparkshell/
```

This is an experimental project within the Delta Lake repository, isolated in the `experimental/` directory with no dependencies on the parent project.

## Technology Stack

- **Language**: Scala 2.13.15
- **Build Tool**: SBT 1.9.7 (self-contained in `build/` directory)
- **Spark**: Apache Spark 4.0.0 (upgraded from 3.5.0)
- **HTTP Framework**: Spark Java 2.9.4 (for REST endpoints)
- **JSON**: Google Gson 2.10.1
- **Testing**: ScalaTest 3.2.17, Python pytest
- **Java**: Java 22.0.1 (requires special JVM flags for Spark 4.0)

## Architecture

### Core Components

1. **SparkAppServer** (`src/main/scala/com/sparkapp/SparkAppServer.scala`)
   - Entry point of the application
   - Initializes Spark Session in local mode
   - Eagerly initializes Spark internals to avoid lazy loading issues
   - Manages server lifecycle

2. **RestApi** (`src/main/scala/com/sparkapp/RestApi.scala`)
   - Implements REST endpoints using Spark Java framework
   - Handles HTTP requests and responses
   - Endpoints:
     - `GET /health` - Health check
     - `GET /info` - Server information (Spark version, port, endpoints)
     - `POST /sql` - Execute SQL commands

3. **SparkSqlExecutor** (`src/main/scala/com/sparkapp/SparkSqlExecutor.scala`)
   - Executes SQL commands using Spark Session
   - Formats query results as human-readable strings
   - Handles both queries (SELECT) and commands (CREATE, INSERT, DROP, etc.)
   - Error handling with Try/Success/Failure pattern

### Data Flow

```
HTTP Request → RestApi → SparkSqlExecutor → Spark Session → Result
                ↓
         JSON Response
```

### JSON Request/Response Format

**Request (POST /sql):**
```json
{
  "sql": "SELECT * FROM users"
}
```

**Success Response:**
```json
{
  "success": true,
  "result": "id | name\n--------\n1 | Alice\n\nTotal rows: 1",
  "error": null
}
```

**Error Response:**
```json
{
  "success": false,
  "result": null,
  "error": "Table or view not found: users"
}
```

## Building and Running

### Build Process

The project uses assembly JAR approach (not `sbt run`) to avoid SBT classloader issues with Spark 4.0:

```bash
build/sbt compile   # Compile source code
build/sbt test      # Run Scala tests
build/sbt assembly  # Build fat JAR
```

**Output**: `target/scala-2.13/sparkapp.jar` (self-contained executable)

### Running the Server

**Background mode (recommended):**
```bash
bin/start.sh [port]  # Default port: 8080
```

**Foreground mode:**
```bash
java -jar target/scala-2.13/sparkapp.jar [port]
```

**Management scripts:**
- `bin/start.sh` - Start server (uses assembly JAR)
- `bin/stop.sh` - Stop server gracefully
- `bin/restart.sh` - Restart server
- `bin/status.sh` - Check server status
- `bin/start-sbt.sh` - Legacy SBT runner (has issues with Spark 4.0)

### Runtime Files

When running:
- `sparkapp.pid` - Process ID file
- `sparkapp.log` - Server logs
- `spark-warehouse/` - Spark data warehouse
- `metastore_db/` - Derby metastore

## Key Design Decisions

### 1. Assembly JAR Approach

**Problem**: Spark 4.0 has classloader issues when running via `sbt run` in background mode. The JARs are stored in SBT's temporary background job directory which gets cleaned up, causing `NoSuchFileException` for Hadoop JARs.

**Solution**: Build an assembly JAR that bundles all dependencies, avoiding SBT's classloader issues.

### 2. Eager Spark Initialization

In `SparkAppServer.scala`, we execute a dummy query (`SELECT 1`) during startup to eagerly initialize Spark internals. This ensures all Hadoop and Spark classes are loaded before the server starts accepting requests.

### 3. Case Classes for JSON

Originally used Scala's `Map` for JSON serialization, which caused Gson to expose internal field names like `scala$collection$immutable$Map$Map3$$key1`.

**Solution**: Use case classes (`ServerInfo`, `EndpointsInfo`, `SqlRequestJson`, `SqlResponseJson`) for proper JSON serialization.

### 4. JVM Options for Java 22

Spark 4.0 requires special JVM flags to access internal Java modules:

```scala
javaOptions ++= Seq(
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  // ... more flags
)
```

## Project Structure

```
experimental/sparkshell/
├── bin/                              # Management scripts
│   ├── start.sh                      # Start server (uses JAR)
│   ├── stop.sh                       # Stop server
│   ├── restart.sh                    # Restart server
│   ├── status.sh                     # Check status
│   └── start-sbt.sh                  # Legacy SBT runner
│
├── build/                            # Self-contained SBT
│   ├── sbt                           # SBT launcher
│   ├── sbt-launch-1.5.5.jar         # SBT JAR
│   └── sbt-config/                   # SBT config
│
├── src/
│   ├── main/scala/com/sparkapp/
│   │   ├── RestApi.scala             # REST endpoints
│   │   ├── SparkAppServer.scala      # Main server
│   │   └── SparkSqlExecutor.scala    # SQL execution
│   │
│   └── test/scala/com/sparkapp/
│       ├── SparkSqlExecutorSpec.scala    # SQL tests (9)
│       └── JsonSerializationSpec.scala   # JSON tests (5)
│
├── tests/                            # Python tests
│   ├── test_spark_shell.py           # Integration tests (6)
│   └── __init__.py
│
├── spark_shell.py                    # SparkShell class (automatic mgmt)
├── example.py                        # Usage example
├── run-tests                         # Unified test runner
├── build.sbt                         # Build configuration
├── requirements.txt                  # Python dependencies
├── README.md                         # User documentation
└── PROJECT_CONTEXT.md                # This file
```

## Testing

### Test Organization

- **Scala Tests** (14 total): Unit tests for core functionality
  - `SparkSqlExecutorSpec`: SQL execution, error handling (9 tests)
  - `JsonSerializationSpec`: JSON serialization (5 tests)

- **Python Tests** (13 total): Integration tests with running server
  - Health checks, SQL queries, error handling
  - Requires running server on localhost:8080

### Running Tests

```bash
# All tests (Scala + Python)
./run-tests

# Scala only
build/sbt test

# Python only (automatically starts/stops server)
python -m pytest tests/test_spark_shell.py -v
```

**Exit codes**: Returns 0 on success, 1 on any failure (CI/CD compatible)

## Python Client

### SparkShell Class (Recommended - Automatic Server Management)

```python
from spark_shell import SparkShell

# Automatically starts server, executes SQL, and cleans up
with SparkShell(source=".", port=8080) as shell:
    # Execute SQL (returns just the result string)
    result = shell.execute_sql("SELECT 1 as id")
    print(result)
    
    # Get server info
    info = shell.get_server_info()  # {"sparkVersion": "4.0.0", ...}
```

### REST API (For running servers)

```python
import requests

# Execute SQL
response = requests.post(
    "http://localhost:8080/sql",
    json={"sql": "SELECT * FROM users"}
)
data = response.json()
if data["success"]:
    print(data["result"])
```

## Common Operations

### Create Table
```bash
curl -X POST http://localhost:8080/sql \
  -H "Content-Type: application/json" \
  -d '{"sql": "CREATE TABLE users (id INT, name STRING)"}'
```

### Insert Data
```bash
curl -X POST http://localhost:8080/sql \
  -H "Content-Type: application/json" \
  -d '{"sql": "INSERT INTO users VALUES (1, \"Alice\")"}'
```

### Query Data
```bash
curl -X POST http://localhost:8080/sql \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM users"}'
```

## Configuration

### Port Configuration

Default port: 8080

Change via:
- Command line: `bin/start.sh 3000`
- Direct JAR: `java -jar target/scala-2.13/sparkapp.jar 3000`

### Spark Configuration

In `SparkAppServer.scala`:
```scala
val spark = SparkSession.builder()
  .appName("SparkApp SQL REST Server")
  .master("local[*]")  // Local mode with all cores
  .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
  .getOrCreate()
```

### Log Level

Set to WARN by default:
```scala
spark.sparkContext.setLogLevel("WARN")
```

## Troubleshooting

### Common Issues

1. **Port already in use**
   - Error: "Failed to bind to /0.0.0.0:8080"
   - Solution: `lsof -ti :8080 | xargs kill -9`

2. **SBT classloader issues with Spark 4.0**
   - Error: "NoSuchFileException: hadoop-client-api-3.3.4.jar"
   - Solution: Use `bin/start.sh` (assembly JAR) instead of `bin/start-sbt.sh`

3. **Java module access errors**
   - Error: "IllegalAccessError: module java.base does not export sun.nio.ch"
   - Solution: JVM flags already configured in `build.sbt`

4. **Python tests fail**
   - Ensure server is running: `bin/status.sh`
   - Check port: Default is 8080
   - Install dependencies: `pip install -r requirements.txt`

## Development Workflow

### Making Changes

1. Edit source files in `src/main/scala/com/sparkapp/`
2. Compile: `build/sbt compile`
3. Run tests: `build/sbt test`
4. Build JAR: `build/sbt assembly`
5. Restart server: `bin/restart.sh`

### Adding Dependencies

Edit `build.sbt`:
```scala
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "4.0.0",
  "your.library" % "artifact" % "version"
)
```

### Adding Endpoints

Edit `RestApi.scala`:
```scala
Spark.post("/your-endpoint", (req: Request, res: Response) => {
  res.`type`("application/json")
  // Your logic here
  gson.toJson(responseObject)
})
```

## Version History

### Current: Spark 4.0.0 / Scala 2.13.15
- Upgraded from Spark 3.5.0 / Scala 2.12.18
- Changed from `sbt run` to assembly JAR approach
- Added eager Spark initialization
- Fixed JSON serialization with case classes
- All tests passing (27 total)

### Key Migrations
1. **Spark 3.5 → 4.0**: Required Scala 2.13, assembly JAR, JVM flags
2. **gRPC → REST**: Simplified from gRPC to REST API for easier usage
3. **test/ → tests/**: Unified test directory structure

## Future Considerations

- Add authentication/authorization
- Support for async query execution
- Query result pagination
- Configurable Spark settings via REST API
- Support for uploading data files
- WebSocket support for streaming queries
- Metrics and monitoring endpoints

## Dependencies

### Runtime
- Apache Spark 4.0.0
- Spark Java 2.9.4 (HTTP framework)
- Google Gson 2.10.1

### Testing
- ScalaTest 3.2.17
- pytest (Python)
- requests (Python)

### Build
- SBT 1.9.7
- sbt-assembly 2.1.5

## License and Usage

This is an experimental project within the Delta Lake repository. Modifications should be contained within the `experimental/sparkshell/` directory and should not affect the parent project.

## Quick Reference

```bash
# Build
build/sbt assembly

# Start server
bin/start.sh

# Check status
bin/status.sh

# View logs
tail -f sparkapp.log

# Run tests
./run-tests

# Stop server
bin/stop.sh

# Quick test
curl http://localhost:8080/health
```

## Contact Points in Code

- **Main entry**: `src/main/scala/com/sparkapp/SparkAppServer.scala:8`
- **REST endpoints**: `src/main/scala/com/sparkapp/RestApi.scala:23-85`
- **SQL execution**: `src/main/scala/com/sparkapp/SparkSqlExecutor.scala:7`
- **Build config**: `build.sbt:1`
- **Python client**: `spark_shell.py:30` (SparkShell class)
