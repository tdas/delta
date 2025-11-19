#!/bin/bash
# Script to update PR descriptions with concise summaries
# Usage: ./update_pr_descriptions.sh <GITHUB_TOKEN>

set -e

if [ -z "$1" ]; then
    echo "Usage: $0 <GITHUB_TOKEN>"
    echo "Get your token from: https://github.com/settings/tokens"
    exit 1
fi

TOKEN="$1"
REPO="delta-io/delta"

# Function to update PR
update_pr() {
    local pr_number="$1"
    local description="$2"
    
    echo "Updating PR #$pr_number..."
    
    curl -X PATCH \
        -H "Authorization: Bearer $TOKEN" \
        -H "Accept: application/vnd.github+json" \
        -H "X-GitHub-Api-Version: 2022-11-28" \
        "https://api.github.com/repos/$REPO/pulls/$pr_number" \
        -d "{\"body\":$(echo "$description" | jq -Rs .)}" \
        -s -o /dev/null -w "HTTP %{http_code}\n"
}

# PR 1
update_pr 5523 "Establishes foundational Unity Catalog integration for Delta Lake with embedded UC server lifecycle management and comprehensive test framework.

Adds new \`sparkUnityCatalog\` SBT module with Unity Catalog 0.3.0 dependencies, \`UnityCatalogSupport\` trait for managing UC server lifecycle in tests, and \`UnityCatalogSupportSuite\` with 4 integration tests validating UC-Delta connectivity and table operations. Uses shaded UC server JAR to avoid dependency conflicts. Compatible with Spark 4.0 and Delta Lake. Includes repository setup (.gitignore updates)."

# PR 2
update_pr 5524 "Adds comprehensive Data Manipulation Language (DML) test suite with pluggable SQLExecutor framework for Unity Catalog managed Delta tables.

Introduces \`UCDeltaTableIntegrationSuiteBase\` with pluggable SQLExecutor framework and helper methods (\`withNewTable\`, \`sql\`, \`check\`), and \`UCDeltaTableDMLSuite\` with 13 tests covering INSERT (append, overwrite, replace, multiple patterns), UPDATE, DELETE, and MERGE operations including schema evolution. Framework is validated through immediate real-world usage with no untested code."

# PR 3
update_pr 5525 "Comprehensive Data Definition Language (DDL) test suite for Unity Catalog managed Delta tables covering schema operations, metadata management, and table lifecycle.

Adds \`UCDeltaTableDDLSuite\` with 9 tests covering CREATE/DROP tables with various data types (BIGINT, STRING, DECIMAL, BOOLEAN, TIMESTAMP), CREATE TABLE AS SELECT (CTAS), IF NOT EXISTS patterns, DESCRIBE and DESCRIBE EXTENDED for table introspection, and Unity Catalog table registration verification."

# PR 4
update_pr 5526 "Comprehensive test suite for Delta utility and maintenance operations on Unity Catalog managed tables including optimization, history tracking, and catalog metadata queries.

Adds \`UCDeltaTableUtilitySuite\` with 10 tests covering OPTIMIZE and ZORDER BY operations, DESCRIBE HISTORY with flexible UC-specific validation, SHOW CATALOGS/SCHEMAS/COLUMNS for catalog metadata, and concurrent-safe operation validation."

# PR 5
update_pr 5527 "Unity Catalog-specific read capabilities including time travel, access patterns, schema evolution, and advanced query support for managed Delta tables.

Adds \`UCDeltaTableReadSuite\` with 7 tests covering version-based and timestamp-based time travel with graceful UC limitation handling, catalog-qualified vs spark_catalog access patterns, read consistency across multiple operations, concurrent read safety, and schema evolution support."

echo "✅ All PR descriptions updated!"

