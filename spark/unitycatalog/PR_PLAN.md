# Unity Catalog Integration PR Plan

## Overview
Breaking Unity Catalog integration changes into 5 self-contained, well-tested PRs of ~500 lines each.

**Branch Naming**: Follows global convention at `~/.config/dev-conventions/BRANCH_NAMING_CONVENTION.md`

## Working Branch Reference

### Current Development Branch
**Main Branch:** https://github.com/tdas/delta/tree/spark-uc  
**All Changes:** https://github.com/delta-io/delta/compare/master...tdas:spark-uc

### Key Files Created/Modified

#### Core Infrastructure (PR 1)
- **build.sbt** - https://github.com/tdas/delta/blob/spark-uc/build.sbt (lines 776-814: sparkUnityCatalog module)
- **UnityCatalogSupport.scala** - https://github.com/tdas/delta/blob/spark-uc/spark/unitycatalog/src/test/scala/com/sparkuctest/UnityCatalogSupport.scala
- **UnityCatalogSupportSuite.scala** - https://github.com/tdas/delta/blob/spark-uc/spark/unitycatalog/src/test/scala/com/sparkuctest/UnityCatalogSupportSuite.scala

#### Test Framework & DML (PR 2) 
- **UCDeltaTableIntegrationSuiteBase.scala** - https://github.com/tdas/delta/blob/spark-uc/spark/unitycatalog/src/test/scala/com/sparkuctest/UCDeltaTableIntegrationSuiteBase.scala
- **UCDeltaTableDMLSuite.scala** - https://github.com/tdas/delta/blob/spark-uc/spark/unitycatalog/src/test/scala/com/sparkuctest/UCDeltaTableDMLSuite.scala

#### Specialized Test Suites (PR 3-5)
- **UCDeltaTableDDLSuite.scala** - https://github.com/tdas/delta/blob/spark-uc/spark/unitycatalog/src/test/scala/com/sparkuctest/UCDeltaTableDDLSuite.scala  
- **UCDeltaTableUtilitySuite.scala** - https://github.com/tdas/delta/blob/spark-uc/spark/unitycatalog/src/test/scala/com/sparkuctest/UCDeltaTableUtilitySuite.scala
- **UCDeltaTableReadSuite.scala** - https://github.com/tdas/delta/blob/spark-uc/spark/unitycatalog/src/test/scala/com/sparkuctest/UCDeltaTableReadSuite.scala

#### Documentation & Repository Setup (Merged into PR 1)
- **.gitignore** - https://github.com/tdas/delta/blob/spark-uc/.gitignore (lines 139-141: UC artifacts)
- **PR_PLAN.md** - https://github.com/tdas/delta/blob/spark-uc/spark/unitycatalog/PR_PLAN.md
- **MANAGE_GIT_STACK.md** - https://github.com/tdas/delta/blob/spark-uc/spark/unitycatalog/MANAGE_GIT_STACK.md

### Individual PR Branches
- **PR 1 Foundation + Setup:** https://github.com/tdas/delta/tree/pr1-uc-foundation (includes PR6 cleanup)
- **PR 2 DML Framework:** https://github.com/tdas/delta/tree/pr2-uc-dml-framework  
- **PR 3 DDL Operations:** https://github.com/tdas/delta/tree/pr3-uc-ddl
- **PR 4 Utility Operations:** https://github.com/tdas/delta/tree/pr4-uc-utility
- **PR 5 Read Operations:** https://github.com/tdas/delta/tree/pr5-uc-read

## Test Coverage Summary
- **Total Lines of Code:** ~1,898 lines across 8 files
- **Total Tests:** 43 comprehensive tests  
- **Test Categories:** Foundation (4), DML (13), DDL (9), Utility (10), Read (7)
- **All Tests Passing:** ✅ Verified with Spark 4.0 and Unity Catalog 0.3.0

## PR Strategy Summary

### **PR 1: Unity Catalog Foundation + Repository Setup** (~620 lines)
**Files:** build.sbt, UnityCatalogSupport.scala, UnityCatalogSupportSuite.scala, .gitignore, documentation  
**Purpose:** Establish complete foundational Unity Catalog integration with proper repository hygiene  
**Tests:** 4 comprehensive tests validating UC server startup, Spark configuration, table operations  
**Includes:** Repository setup (.gitignore), documentation (PR_PLAN.md, MANAGE_GIT_STACK.md), standardized validation methods

### **PR 2: SQLExecutor Framework + DML Operations** (~549 lines)
**Files:** UCDeltaTableIntegrationSuiteBase.scala, UCDeltaTableDMLSuite.scala  
**Purpose:** Add flexible test framework AND validate all Data Manipulation Language operations  
**Tests:** 13 DML tests including INSERT (append/overwrite/replace), UPDATE, DELETE, MERGE operations  
**Key Benefit:** No untested framework code - SQLExecutor tested immediately upon introduction

### **PR 3: DDL Operations** (~274 lines)
**Files:** UCDeltaTableDDLSuite.scala  
**Purpose:** Validate Data Definition Language operations in Unity Catalog  
**Tests:** 9 tests covering CREATE/DROP tables, DESCRIBE operations, schema variations

### **PR 4: Utility Operations** (~251 lines)
**Files:** UCDeltaTableUtilitySuite.scala  
**Purpose:** Validate Delta utility and maintenance operations  
**Tests:** 10 tests covering OPTIMIZE/ZORDER, DESCRIBE HISTORY, catalog metadata queries

### **PR 5: Advanced Read Operations** (~260 lines)
**Files:** UCDeltaTableReadSuite.scala  
**Purpose:** Validate Unity Catalog-specific read capabilities  
**Tests:** 7 tests covering version/timestamp time travel, catalog access patterns, schema evolution

## Key Benefits

1. **Self-Contained**: Each PR introduces a complete, functional set of features
2. **Well-Tested**: Every PR includes comprehensive test coverage for its functionality  
3. **Size-Controlled**: All PRs under 500 lines except foundational PR 1 (546 lines)
4. **No Orphaned Code**: All framework code is tested immediately upon introduction
5. **Incremental**: Can be reviewed and merged independently
6. **Logical Progression**: Foundation → Framework+Usage → Specialized Categories → Cleanup
7. **Rollback-Safe**: Each PR can be reverted independently if needed

## Testing Strategy Per PR

- **PR 1**: Infrastructure, basic UC connectivity, and repository setup
- **PR 2**: SQLExecutor framework validation through comprehensive DML operations (including INSERT patterns)
- **PR 3-5**: Specific categories of Delta operations (DDL, Utility, Read)

## Current Status

All changes have been developed and tested locally on branch `spark-uc`. Ready to be split into the above PR sequence for review and merging.

## GitHub PR Creation

All 5 PR branches have been pushed to `tdas/delta` fork. PRs have been created using git-stack.

### PR 1: Unity Catalog Foundation + Repository Setup
**PR #5523:** https://github.com/delta-io/delta/pull/5523  
**Compare:** https://github.com/delta-io/delta/compare/master...tdas:spark-uc-pr1-foundation  
**Title:** `Unity Catalog integration foundation and repository setup for Delta Lake`  
**Description:**
```
Establishes foundational Unity Catalog integration for Delta Lake with embedded UC server lifecycle management and comprehensive test framework.

Adds new `sparkUnityCatalog` SBT module with Unity Catalog 0.3.0 dependencies, `UnityCatalogSupport` trait for managing UC server lifecycle in tests, and `UnityCatalogSupportSuite` with 4 integration tests validating UC-Delta connectivity and table operations. Uses shaded UC server JAR to avoid dependency conflicts. Compatible with Spark 4.0 and Delta Lake. Includes repository setup (.gitignore updates).
```

### PR 2: DML Framework
**PR #5524:** https://github.com/delta-io/delta/pull/5524 (Draft)  
**Compare:** https://github.com/delta-io/delta/compare/master...tdas:spark-uc-pr2-dml-framework  
**Title:** `Unity Catalog DML operations with pluggable SQL execution framework`  
**Description:**
```
Adds comprehensive Data Manipulation Language (DML) test suite with pluggable SQLExecutor framework for Unity Catalog managed Delta tables.

Introduces `UCDeltaTableIntegrationSuiteBase` with pluggable SQLExecutor framework and helper methods (`withNewTable`, `sql`, `check`), and `UCDeltaTableDMLSuite` with 13 tests covering INSERT (append, overwrite, replace, multiple patterns), UPDATE, DELETE, and MERGE operations including schema evolution. Framework is validated through immediate real-world usage with no untested code.
```

### PR 3: DDL Operations
**PR #5525:** https://github.com/delta-io/delta/pull/5525 (Draft)  
**Compare:** https://github.com/delta-io/delta/compare/master...tdas:spark-uc-pr3-ddl  
**Title:** `Unity Catalog DDL operations comprehensive test coverage`  
**Description:**
```
Comprehensive Data Definition Language (DDL) test suite for Unity Catalog managed Delta tables covering schema operations, metadata management, and table lifecycle.

Adds `UCDeltaTableDDLSuite` with 9 tests covering CREATE/DROP tables with various data types (BIGINT, STRING, DECIMAL, BOOLEAN, TIMESTAMP), CREATE TABLE AS SELECT (CTAS), IF NOT EXISTS patterns, DESCRIBE and DESCRIBE EXTENDED for table introspection, and Unity Catalog table registration verification.
```

### PR 4: Utility Operations
**PR #5526:** https://github.com/delta-io/delta/pull/5526 (Draft)  
**Compare:** https://github.com/delta-io/delta/compare/master...tdas:spark-uc-pr4-utility  
**Title:** `Unity Catalog utility operations and maintenance commands`  
**Description:**
```
Comprehensive test suite for Delta utility and maintenance operations on Unity Catalog managed tables including optimization, history tracking, and catalog metadata queries.

Adds `UCDeltaTableUtilitySuite` with 10 tests covering OPTIMIZE and ZORDER BY operations, DESCRIBE HISTORY with flexible UC-specific validation, SHOW CATALOGS/SCHEMAS/COLUMNS for catalog metadata, and concurrent-safe operation validation.
```

### PR 5: Advanced Read Operations
**PR #5527:** https://github.com/delta-io/delta/pull/5527 (Draft)  
**Compare:** https://github.com/delta-io/delta/compare/master...tdas:spark-uc-pr5-read  
**Title:** `Unity Catalog advanced read operations and time travel support`  
**Description:**
```
Unity Catalog-specific read capabilities including time travel, access patterns, schema evolution, and advanced query support for managed Delta tables.

Adds `UCDeltaTableReadSuite` with 7 tests covering version-based and timestamp-based time travel with graceful UC limitation handling, catalog-qualified vs spark_catalog access patterns, read consistency across multiple operations, concurrent read safety, and schema evolution support.
```

### Quick PR Creation Steps
1. **Click each URL above** - takes you directly to PR creation page
2. **Copy-paste title and description** for each PR
3. **Verify base branch** is set to `delta-io:master` 
4. **Add labels** like `enhancement`, `unity-catalog`, `testing`
5. **Create the PR**

### PR Dependencies
- PR 2 builds on PR 1 (foundation + repository setup)
- PR 3 builds on PR 2 (DML framework) 
- PR 4 builds on PR 3 (DDL operations)
- PR 5 builds on PR 4 (utility operations)

All PRs can be created simultaneously since they target `master` branch. After PR merges, subsequent PRs may need rebasing as described in `MANAGE_GIT_STACK.md`.

## Commit History Reference

Original commits that will be restructured:
- `ebc9f84eca` - spark-uc module (foundation)
- `08708fb06f` - Move to com.sparkuctest package  
- `a08625488c` - Add SQLExecutor framework
- `367f3fe157` - Simplify integration suite
- `e7779c57b0` - Add comprehensive test suites
- `4ad682ff1b` - Standardize validation methods
- `258525ed8c` - Add .gitignore entries