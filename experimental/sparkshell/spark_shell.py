#!/usr/bin/env python3
"""
SparkShell - Standalone Python class to download, build, start, and interact with SparkApp server.

Usage:
    # From GitHub
    with SparkShell(source="https://github.com/user/repo/path/to/sparkshell") as shell:
        result = shell.execute_sql("SELECT 1 as id")
        print(result)
    
    # From local directory
    with SparkShell(source="/path/to/local/sparkshell", port=8080) as shell:
        result = shell.execute_sql("CREATE TABLE test (id INT)")
        print(result)
"""

import os
import sys
import time
import shutil
import tempfile
import subprocess
import json
import requests
from pathlib import Path
from typing import Optional, Union, Tuple


class SparkShell:
    """
    A standalone class to manage SparkApp server lifecycle and SQL execution.
    
    Features:
    - Download from GitHub or copy from local directory
    - Build the assembly JAR automatically
    - Start/stop the server
    - Execute SQL commands and get results
    - Context manager support for automatic cleanup
    """
    
    def __init__(
        self,
        source: str,
        port: int = 8080,
        temp_dir: Optional[str] = None,
        auto_build: bool = True,
        auto_start: bool = True,
        cleanup_on_exit: bool = True,
        startup_timeout: int = 60,
        build_timeout: int = 300,
        spark_configs: Optional[dict] = None,
        uc_uri: Optional[str] = None,
        uc_token: Optional[str] = None,
        uc_catalog: Optional[str] = None,
        uc_schema: Optional[str] = None
    ):
        """
        Initialize SparkShell.
        
        Args:
            source: GitHub URL or local directory path containing SparkApp code
            port: Port for the server (default: 8080)
            temp_dir: Custom temp directory (default: system temp)
            auto_build: Automatically build assembly JAR (default: True)
            auto_start: Automatically start server (default: True)
            cleanup_on_exit: Clean up temp files on exit (default: True)
            startup_timeout: Server startup timeout in seconds (default: 60)
            build_timeout: Build timeout in seconds (default: 300)
            spark_configs: Dict of Spark configuration options (default: None)
                          Example: {"spark.executor.memory": "2g", "spark.sql.shuffle.partitions": "10"}
            uc_uri: Unity Catalog server URI (default: None)
            uc_token: Unity Catalog authentication token (default: None)
            uc_catalog: Unity Catalog catalog name (default: None, uses "unity" if not specified)
            uc_schema: Unity Catalog schema name (default: None)
        """
        self.source = source
        self.port = port
        self.temp_dir = temp_dir
        self.auto_build = auto_build
        self.auto_start = auto_start
        self.cleanup_on_exit = cleanup_on_exit
        self.startup_timeout = startup_timeout
        self.build_timeout = build_timeout
        self.spark_configs = spark_configs or {}
        self.uc_uri = uc_uri
        self.uc_token = uc_token
        self.uc_catalog = uc_catalog or "unity"  # Default to "unity" if not specified
        self.uc_schema = uc_schema
        
        # Configure Unity Catalog if URI and token are provided
        if self.uc_uri and self.uc_token:
            # Register the catalog type
            self.spark_configs[f"spark.sql.catalog.{self.uc_catalog}"] = "io.unitycatalog.spark.UCSingleCatalog"
            self.spark_configs[f"spark.sql.catalog.{self.uc_catalog}.uri"] = self.uc_uri
            self.spark_configs[f"spark.sql.catalog.{self.uc_catalog}.token"] = self.uc_token
            self.spark_configs["spark.sql.defaultCatalog"] = self.uc_catalog
            # self.spark_configs[f"spark.sql.catalog.{self.uc_catalog}.warehouse"] = f"/tmp/{self.uc_catalog}-warehouse"
        
        # Runtime state
        self.work_dir: Optional[Path] = None
        self.process: Optional[subprocess.Popen] = None
        self.jar_path: Optional[Path] = None
        self.is_ready = False
        
        # API base URL
        self.base_url = f"http://localhost:{self.port}"
    
    def __enter__(self):
        """Context manager entry - setup and start server."""
        self.setup()
        if self.auto_build:
            self.build()
        if self.auto_start:
            self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - cleanup."""
        self.shutdown()
        if self.cleanup_on_exit:
            self.cleanup()
        return False
    
    def setup(self):
        """Download or copy SparkApp code to temp directory."""
        print(f"[SparkShell] Setting up from source: {self.source}")
        
        # Create temp directory
        if self.temp_dir:
            self.work_dir = Path(self.temp_dir)
            self.work_dir.mkdir(parents=True, exist_ok=True)
        else:
            self.work_dir = Path(tempfile.mkdtemp(prefix="sparkshell_"))
        
        print(f"[SparkShell] Working directory: {self.work_dir}")
        
        # Determine if source is GitHub URL or local path
        if self.source.startswith("http://") or self.source.startswith("https://"):
            self._download_from_github()
        else:
            self._copy_from_local()
        
        # Verify required files exist
        required_files = ["build.sbt", "build/sbt"]
        for file in required_files:
            if not (self.work_dir / file).exists():
                raise FileNotFoundError(
                    f"Required file not found: {file}. "
                    f"Ensure source contains a valid SparkApp project."
                )
        
        print("[SparkShell] Setup complete")
    
    def _download_from_github(self):
        """Download SparkApp code from GitHub."""
        print("[SparkShell] Downloading from GitHub...")
        
        # Parse GitHub URL to get repo and path
        # Support formats:
        # - https://github.com/user/repo/tree/branch/path/to/dir
        # - https://github.com/user/repo (clone entire repo)
        
        if "/tree/" in self.source:
            # Sparse checkout for specific directory
            parts = self.source.split("/tree/")
            repo_url = parts[0]
            branch_and_path = parts[1].split("/", 1)
            branch = branch_and_path[0]
            subdir = branch_and_path[1] if len(branch_and_path) > 1 else ""
            
            # Clone with sparse checkout
            try:
                # Initialize git repo
                subprocess.run(
                    ["git", "init"],
                    cwd=self.work_dir,
                    check=True,
                    capture_output=True
                )
                
                # Add remote
                subprocess.run(
                    ["git", "remote", "add", "origin", repo_url],
                    cwd=self.work_dir,
                    check=True,
                    capture_output=True
                )
                
                # Enable sparse checkout
                subprocess.run(
                    ["git", "config", "core.sparseCheckout", "true"],
                    cwd=self.work_dir,
                    check=True,
                    capture_output=True
                )
                
                # Specify path to checkout
                sparse_checkout_file = self.work_dir / ".git" / "info" / "sparse-checkout"
                sparse_checkout_file.write_text(f"{subdir}\n")
                
                # Pull the specific branch
                subprocess.run(
                    ["git", "pull", "origin", branch, "--depth=1"],
                    cwd=self.work_dir,
                    check=True,
                    capture_output=True
                )
                
                # Move files from subdir to root if needed
                if subdir:
                    subdir_path = self.work_dir / subdir
                    if subdir_path.exists():
                        for item in subdir_path.iterdir():
                            shutil.move(str(item), str(self.work_dir / item.name))
                        # Remove empty subdirectories
                        shutil.rmtree(subdir_path.parent if subdir_path.parent != self.work_dir else subdir_path)
                
                print("[SparkShell] Download complete")
            except subprocess.CalledProcessError as e:
                raise RuntimeError(f"Failed to clone from GitHub: {e.stderr.decode() if e.stderr else str(e)}")
        else:
            # Full repo clone
            try:
                subprocess.run(
                    ["git", "clone", "--depth=1", self.source, str(self.work_dir)],
                    check=True,
                    capture_output=True
                )
                print("[SparkShell] Clone complete")
            except subprocess.CalledProcessError as e:
                raise RuntimeError(f"Failed to clone from GitHub: {e.stderr.decode() if e.stderr else str(e)}")
    
    def _copy_from_local(self):
        """Copy SparkApp code from local directory."""
        print("[SparkShell] Copying from local directory...")
        
        source_path = Path(self.source).expanduser().resolve()
        if not source_path.exists():
            raise FileNotFoundError(f"Source directory not found: {source_path}")
        
        # Copy all files
        for item in source_path.iterdir():
            if item.name in [".git", "target", "project/target", "sparkapp.log", "sparkapp.pid"]:
                continue  # Skip unnecessary files
            
            dest = self.work_dir / item.name
            if item.is_dir():
                shutil.copytree(item, dest, ignore=shutil.ignore_patterns("target", ".git"))
            else:
                shutil.copy2(item, dest)
        
        print("[SparkShell] Copy complete")
    
    def build(self):
        """Build the assembly JAR using SBT."""
        print("[SparkShell] Building assembly JAR...")
        print("[SparkShell] This may take several minutes on first run...")
        
        sbt_script = self.work_dir / "build" / "sbt"
        if not sbt_script.exists():
            raise FileNotFoundError(f"SBT script not found: {sbt_script}")
        
        # Make sbt executable
        os.chmod(sbt_script, 0o755)
        
        try:
            # Run sbt assembly
            result = subprocess.run(
                [str(sbt_script), "assembly"],
                cwd=self.work_dir,
                timeout=self.build_timeout,
                capture_output=True,
                text=True
            )
            
            if result.returncode != 0:
                print(f"[SparkShell] Build failed with exit code {result.returncode}")
                print(f"[SparkShell] STDOUT: {result.stdout}")
                print(f"[SparkShell] STDERR: {result.stderr}")
                raise RuntimeError(f"Build failed: {result.stderr}")
            
            # Find the JAR file
            jar_path = self.work_dir / "target" / "scala-2.13" / "sparkshell.jar"
            if not jar_path.exists():
                raise FileNotFoundError(f"Assembly JAR not found at: {jar_path}")
            
            self.jar_path = jar_path
            print(f"[SparkShell] Build complete: {self.jar_path}")
            
        except subprocess.TimeoutExpired:
            raise RuntimeError(f"Build timeout after {self.build_timeout} seconds")
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Build failed: {e.stderr if e.stderr else str(e)}")
    
    def start(self):
        """Start the SparkApp server."""
        if not self.jar_path or not self.jar_path.exists():
            raise RuntimeError("Assembly JAR not found. Run build() first.")
        
        print(f"[SparkShell] Starting server on port {self.port}...")
        
        # Check if port is already in use
        if self._is_port_in_use():
            raise RuntimeError(f"Port {self.port} is already in use")
        
        # Start the server process
        log_file = self.work_dir / "sparkshell.log"
        
        # Build command with port and optional Spark configs
        cmd = ["java", "-jar", str(self.jar_path), str(self.port)]
        
        # Add Spark configurations as key=value arguments
        if self.spark_configs:
            for key, value in self.spark_configs.items():
                cmd.append(f"{key}={value}")
                print(f"[SparkShell] Setting Spark config: {key}={value}")
        
        with open(log_file, "w") as log:
            self.process = subprocess.Popen(
                cmd,
                cwd=self.work_dir,
                stdout=log,
                stderr=subprocess.STDOUT,
                preexec_fn=os.setsid if sys.platform != "win32" else None
            )
        
        # Wait for server to be ready
        print("[SparkShell] Waiting for server to start...")
        start_time = time.time()
        while time.time() - start_time < self.startup_timeout:
            if self._check_health():
                self.is_ready = True
                print(f"[SparkShell] Server ready at {self.base_url}")
                
                # Set Unity Catalog schema if configured (catalog is already set via defaultCatalog config)
                if self.uc_uri and self.uc_token:
                    print(f"[SparkShell] Unity Catalog enabled: {self.uc_catalog}")
                    
                    if self.uc_schema:
                        try:
                            print(f"[SparkShell] Setting default schema: {self.uc_schema}")
                            self.execute_sql(f"USE {self.uc_schema}")
                            print(f"[SparkShell] Tables can be referenced as: {self.uc_catalog}.{self.uc_schema}.table_name or table_name")
                        except RuntimeError as e:
                            print(f"[SparkShell] Warning: Failed to set schema: {e}")
                            print(f"[SparkShell] Tables can be referenced as: {self.uc_catalog}.{self.uc_schema}.table_name")
                    else:
                        print(f"[SparkShell] Tables must be referenced as: {self.uc_catalog}.schema.table_name")
                
                return
            
            # Check if process died
            if self.process.poll() is not None:
                with open(log_file) as f:
                    log_contents = f.read()
                raise RuntimeError(f"Server process died. Log:\n{log_contents}")
            
            time.sleep(1)
        
        raise RuntimeError(f"Server failed to start within {self.startup_timeout} seconds")
    
    def _is_port_in_use(self) -> bool:
        """Check if the port is already in use."""
        try:
            response = requests.get(f"{self.base_url}/health", timeout=2)
            return response.status_code == 200
        except requests.exceptions.RequestException:
            return False
    
    def _check_health(self) -> bool:
        """Check if server is healthy."""
        try:
            response = requests.get(f"{self.base_url}/health", timeout=2)
            return response.status_code == 200
        except requests.exceptions.RequestException:
            return False
    
    def execute_sql(self, sql: str) -> str:
        """
        Execute SQL command and return only the result output.
        
        Args:
            sql: SQL command to execute
            
        Returns:
            str: Query result as formatted string
            
        Raises:
            RuntimeError: If server is not ready or SQL execution fails
        """
        if not self.is_ready:
            raise RuntimeError("Server is not ready. Call start() first.")
        
        try:
            response = requests.post(
                f"{self.base_url}/sql",
                headers={"Content-Type": "application/json"},
                json={"sql": sql},
                timeout=300  # 5 minutes timeout for long queries
            )
            
            if response.status_code != 200:
                raise RuntimeError(f"HTTP error {response.status_code}: {response.text}")
            
            data = response.json()
            
            if not data.get("success", False):
                error_msg = data.get("error", "Unknown error")
                raise RuntimeError(f"SQL execution failed: {error_msg}")
            
            return data.get("result", "")
            
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Failed to execute SQL: {str(e)}")
    
    def get_server_info(self) -> dict:
        """Get server information."""
        if not self.is_ready:
            raise RuntimeError("Server is not ready. Call start() first.")
        
        try:
            response = requests.get(f"{self.base_url}/info", timeout=5)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Failed to get server info: {str(e)}")
    
    def shutdown(self):
        """Shutdown the server gracefully."""
        if self.process is None:
            return
        
        print("[SparkShell] Shutting down server...")
        
        try:
            # Try graceful shutdown first
            self.process.terminate()
            
            # Wait up to 10 seconds for graceful shutdown
            try:
                self.process.wait(timeout=10)
                print("[SparkShell] Server shutdown complete")
            except subprocess.TimeoutExpired:
                print("[SparkShell] Forcing server shutdown...")
                self.process.kill()
                self.process.wait()
                print("[SparkShell] Server killed")
        except Exception as e:
            print(f"[SparkShell] Error during shutdown: {e}")
        finally:
            self.process = None
            self.is_ready = False
    
    def cleanup(self):
        """Clean up temporary files."""
        if self.work_dir and self.work_dir.exists():
            print(f"[SparkShell] Cleaning up: {self.work_dir}")
            try:
                shutil.rmtree(self.work_dir)
                print("[SparkShell] Cleanup complete")
            except Exception as e:
                print(f"[SparkShell] Error during cleanup: {e}")
    
    def __del__(self):
        """Destructor - ensure cleanup."""
        if hasattr(self, 'process') and self.process:
            self.shutdown()


def main():
    """Example usage."""
    import argparse
    
    parser = argparse.ArgumentParser(description="SparkShell - Manage SparkApp server")
    parser.add_argument("source", help="GitHub URL or local directory path")
    parser.add_argument("--port", type=int, default=8080, help="Server port (default: 8080)")
    parser.add_argument("--no-cleanup", action="store_true", help="Don't cleanup temp files")
    parser.add_argument("--sql", help="SQL command to execute")
    
    args = parser.parse_args()
    
    # Example usage
    try:
        with SparkShell(
            source=args.source,
            port=args.port,
            cleanup_on_exit=not args.no_cleanup
        ) as shell:
            # Get server info
            info = shell.get_server_info()
            print(f"\n{'='*60}")
            print(f"Server Info: Spark {info.get('sparkVersion', 'unknown')}")
            print(f"Port: {info.get('port', 'unknown')}")
            print(f"{'='*60}\n")
            
            # Execute SQL if provided
            if args.sql:
                print(f"Executing: {args.sql}")
                result = shell.execute_sql(args.sql)
                print(f"\nResult:\n{result}\n")
            else:
                # Run example queries
                print("Running example queries...\n")
                
                # Create table
                result = shell.execute_sql(
                    "CREATE TABLE test_users (id INT, name STRING, age INT) USING parquet"
                )
                print(f"1. Create table:\n{result}\n")
                
                # Insert data
                result = shell.execute_sql(
                    "INSERT INTO test_users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)"
                )
                print(f"2. Insert data:\n{result}\n")
                
                # Query data
                result = shell.execute_sql("SELECT * FROM test_users ORDER BY age")
                print(f"3. Query data:\n{result}\n")
                
                # Aggregation
                result = shell.execute_sql("SELECT AVG(age) as avg_age FROM test_users")
                print(f"4. Aggregation:\n{result}\n")
                
                # Drop table
                result = shell.execute_sql("DROP TABLE test_users")
                print(f"5. Drop table:\n{result}\n")
            
            print("✓ All operations completed successfully!")
    
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

