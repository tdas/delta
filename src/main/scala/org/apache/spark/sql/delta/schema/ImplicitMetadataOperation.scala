/*
 * Copyright 2019 Databricks, Inc.
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

package org.apache.spark.sql.delta.schema

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.util.PartitionUtils

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types.StructType

/**
 * A trait that writers into Delta can extend to update the schema and/or partitioning of the table.
 */
trait ImplicitMetadataOperation extends DeltaLogging {

  protected val canMergeSchema: Boolean
  protected val canOverwriteSchema: Boolean

  private def normalizePartitionColumns(
      spark: SparkSession,
      partitionCols: Seq[String],
      schema: StructType): Seq[String] = {
    partitionCols.map { columnName =>
      val colMatches = schema.filter(s => SchemaUtils.DELTA_COL_RESOLVER(s.name, columnName))
      if (colMatches.length > 1) {
        throw DeltaErrors.ambiguousPartitionColumnException(columnName, colMatches)
      } else if (colMatches.isEmpty) {
        throw DeltaErrors.partitionColumnNotFoundException(columnName, schema.toAttributes)
      }
      colMatches.head.name
    }
  }

  protected final def updateMetadata(
      txn: OptimisticTransaction,
      data: Dataset[_],
      partitionColumns: Seq[String],
      configuration: Map[String, String],
      isOverwriteMode: Boolean): Unit = {
    val dataSchema = data.schema.asNullable
    val mergedSchema = if (isOverwriteMode && canOverwriteSchema) {
      dataSchema
    } else {
      SchemaUtils.mergeSchemas(txn.metadata.schema, dataSchema)
    }
    val normalizedPartitionCols =
      normalizePartitionColumns(data.sparkSession, partitionColumns, dataSchema)
    // Merged schema will contain additional columns at the end
    def isNewSchema: Boolean = txn.metadata.schema != mergedSchema
    // We need to make sure that the partitioning order and naming is consistent
    // if provided. Otherwise we follow existing partitioning
    def isNewPartitioning: Boolean = normalizedPartitionCols.nonEmpty &&
      txn.metadata.partitionColumns != normalizedPartitionCols
    PartitionUtils.validatePartitionColumn(
      mergedSchema,
      normalizedPartitionCols,
      // Delta is case insensitive regarding internal column naming
      caseSensitive = false)

    if (txn.readVersion == -1) {
      if (dataSchema.isEmpty) {
        throw DeltaErrors.emptyDataException
      }
      recordDeltaEvent(txn.deltaLog, "delta.ddl.initializeSchema")
      // If this is the first write, configure the metadata of the table.
      txn.updateMetadata(
        Metadata(
          schemaString = dataSchema.json,
          partitionColumns = normalizedPartitionCols,
          configuration = configuration))
    } else if (isOverwriteMode && canOverwriteSchema && (isNewSchema || isNewPartitioning)) {
      // Can define new partitioning in overwrite mode
      val newMetadata = txn.metadata.copy(
        schemaString = dataSchema.json,
        partitionColumns = normalizedPartitionCols
      )
      recordDeltaEvent(txn.deltaLog, "delta.ddl.overwriteSchema")
      txn.updateMetadata(newMetadata)
    } else if (isNewSchema && canMergeSchema && !isNewPartitioning) {
      logInfo(s"New merged schema: ${mergedSchema.treeString}")
      recordDeltaEvent(txn.deltaLog, "delta.ddl.mergeSchema")
      txn.updateMetadata(txn.metadata.copy(schemaString = mergedSchema.json))
    } else if (isNewSchema || isNewPartitioning) {
      recordDeltaEvent(txn.deltaLog, "delta.schemaValidation.failure")
      val errorBuilder = new MetadataMismatchErrorBuilder
      if (isNewSchema) {
        errorBuilder.addSchemaMismatch(txn.metadata.schema, dataSchema)
      }
      if (isNewPartitioning) {
        errorBuilder.addPartitioningMismatch(txn.metadata.partitionColumns, normalizedPartitionCols)
      }
      if (isOverwriteMode) {
        errorBuilder.addOverwriteBit()
      }
      errorBuilder.finalizeAndThrow()
    }
  }
}
