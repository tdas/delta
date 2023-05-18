package org.apache.spark.sql.delta

import org.apache.spark.sql.{QueryTest, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}
import org.apache.spark.sql.types.StructType


class DeltaUpdateCatalogSuite
  extends QueryTest
    with SharedSparkSession
    with SQLTestUtils
    with DeltaSQLCommandTest {

  protected val tbl = "delta_table"

  import testImplicits._

  protected def deltaLog: DeltaLog = DeltaLog.forTable(spark, TableIdentifier(tbl))
  protected def snapshot: Snapshot = deltaLog.unsafeVolatileSnapshot
  protected def snapshotAt(v: Long): Snapshot = deltaLog.getSnapshotAt(v)

  protected def getBaseProperties(snapshot: Snapshot): Map[String, String] = {
    Map(
      DeltaConfigs.MIN_READER_VERSION.key -> snapshot.protocol.minReaderVersion.toString,
      DeltaConfigs.MIN_WRITER_VERSION.key -> snapshot.protocol.minWriterVersion.toString) ++
      snapshot.protocol.readerAndWriterFeatureNames.map { name =>
        s"${TableFeatureProtocolUtils.FEATURE_PROP_PREFIX}$name" ->
          TableFeatureProtocolUtils.FEATURE_PROP_SUPPORTED
      }
  }

  /** Verifies that the table metadata in the catalog are up-to-date. */
  protected def verifyTableMetadata(
    expectedSchema: StructType,
    expectedProperties: Map[String, String] = getBaseProperties(snapshot),
    table: String = tbl,
    partitioningCols: Seq[String] = Nil): Unit = {
    DeltaLog.clearCache()
    // All the information should be available in the MetaStore and should not require any
    // DeltaLog computation
    val cat = spark.sessionState.catalog.externalCatalog.getTable("default", table)
    assert(cat.schema === expectedSchema, s"Schema didn't match for table: $table")
    assert(cat.partitionColumnNames === partitioningCols)
    assert(cat.properties === expectedProperties,
      s"Properties didn't match for table: $table")
  }


  test("mergeSchema") {
    withTable(tbl) {
      val df = spark.range(10).withColumn("part", 'id / 2)
      df.writeTo(tbl).using("delta").create()

      verifyTableMetadata(expectedSchema = df.schema.asNullable)

      val df2 = spark.range(10).withColumn("part", 'id / 2).withColumn("id2", 'id)
      df2.writeTo(tbl)
        .option("mergeSchema", "true")
        .append()

      verifyTableMetadata(expectedSchema = df2.schema.asNullable)
    }
  }
}
