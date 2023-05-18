/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.delta.hooks

import scala.util.control.NonFatal

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.connector.catalog.CatalogManager.SESSION_CATALOG_NAME
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.{DeltaConfigs, OptimisticTransactionImpl, Snapshot}
import org.apache.spark.sql.delta.actions.{Action, Metadata}

// scalastyle:off println

/**
 * Base trait for post commit hooks that want to update the catalog with the
 * latest table schema and properties.
 */
case class UpdateCatalog(table: CatalogTable) extends PostCommitHook with DeltaLogging {

  override val name: String = "Update Catalog"

  override def run(
    spark: SparkSession,
    txn: OptimisticTransactionImpl,
    committedVersion: Long,
    postCommitSnapshot: Snapshot,
    actions: Seq[Action]): Unit = {
    // There's a potential race condition here, where a newer commit has already triggered
    // this to run. That's fine.
    println("Running the post commit hook")
    execute(spark, postCommitSnapshot)
  }

  /** Update the entry in the Catalog to reflect the latest schema and table properties. */
  protected def execute(
      spark: SparkSession,
      snapshot: Snapshot): Unit = {
      recordDeltaOperation(snapshot.deltaLog, "delta.catalog.update") {
      val properties = snapshot.getProperties.toMap
      // If the metastore entry is at an older version and not the timestamp of that version, e.g.
      // a table can be rm -rf'd and get the same version number with a different timestamp
      try {
        if (schemaHasChanged(snapshot, spark)) {
          updateSchema(spark, snapshot)
        } else if (propertiesHaveChanged(properties, snapshot.metadata, spark)) {
          updateProperties(spark, snapshot)
        }
      } catch {
        case NonFatal(e) =>
          logWarning(s"Failed to update the catalog for ${table.identifier} with the latest " +
            s"table information.", e)
      }
    }
  }

  /**
   * Checks if the table schema has changed in the Snapshot with respect to what's stored in
   * the catalog.
   */
  def schemaHasChanged(snapshot: Snapshot, spark: SparkSession): Boolean = {
    // We need to check whether the schema in the catalog matches the current schema.
    val schemaChanged = snapshot.schema != table.schema
    println("schema has changed")
    schemaChanged && spark.sessionState.catalog.tableExists(table.identifier)
  }

  /**
   * Checks if the table properties have changed in the Snapshot with respect to what's stored in
   * the catalog. We check to see if our table properties are a subset of what is in the MetaStore
   * to avoid flip-flopping the information between older and newer versions of DBR. The assumption
   * here is that newer DBRs will only add newer table properties and not remove them.
   */
  def propertiesHaveChanged(
    properties: Map[String, String],
    metadata: Metadata,
    spark: SparkSession): Boolean = {
    val propertiesChanged = !properties.forall { case (k, v) =>
      table.properties.get(k) == Some(v)
    }
    // The table may have been dropped as we're just about to update the information. There is
    // unfortunately no great way to avoid a race condition, but we do one last check here as
    // updates may have been queued for some time.
    propertiesChanged && spark.sessionState.catalog.tableExists(table.identifier)
  }

  /**
   * Update the schema in the catalog based on the provided snapshot.
   */
  def updateSchema(spark: SparkSession, snapshot: Snapshot): Unit = {
    println("updating schema")
    UpdateCatalog.replaceTable(spark, snapshot, table)
  }

  /**
   * Update the properties in the catalog based on the provided snapshot.
   */
  protected def updateProperties(spark: SparkSession, snapshot: Snapshot): Unit = {
    spark.sessionState.catalog.alterTable(
      table.copy(properties = snapshot.getProperties.toMap))
  }
}


object UpdateCatalog {
  /** Replace the table definition in the MetaStore. */
  private def replaceTable(spark: SparkSession, snapshot: Snapshot, table: CatalogTable): Unit = {
    val catalog = spark.sessionState.catalog
    val db = table.database
    val tblName = table.identifier.table
    val schema = snapshot.schema

    // We call the lower level API so that we can actually drop columns. We also assume that
    // all columns are data columns so that we don't have to deal with partition columns
    // having to be at the end of the schema, which Hive follows.
    val catalogName = table.identifier.catalog.getOrElse(
      spark.sessionState.catalogManager.currentCatalog.name())
    println("catalog name: " + catalogName)
    if (catalogName == SESSION_CATALOG_NAME &&
      catalog.externalCatalog.tableExists(db, tblName)) {
      println("updating schema to " + schema)
      catalog.externalCatalog.alterTableDataSchema(db, tblName, schema)
    }

    // We have to update the properties anyway with the latest version/timestamp information
    catalog.alterTable(table.copy(properties = snapshot.getProperties.toMap))
  }
}
