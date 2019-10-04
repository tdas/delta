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

package org.apache.spark.sql.delta

// scalastyle:off import.ordering.noEmptyLine
import java.io.{File, FileNotFoundException, IOException}
import java.util.concurrent.{Callable, TimeUnit}
import java.util.concurrent.locks.ReentrantLock

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
import scala.util.control.NonFatal

import com.databricks.spark.util.TagDefinitions._
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.commands.WriteIntoDelta
import org.apache.spark.sql.delta.files.{TahoeBatchFileIndex, TahoeLogFileIndex}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.storage.LogStoreProvider
import com.google.common.cache.{CacheBuilder, RemovalListener, RemovalNotification}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.{Resolver, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, Expression, In, InSet, Literal}
import org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation}
import org.apache.spark.util.{Clock, SystemClock, ThreadUtils}

/**
 * Used to query the current state of the log as well as modify it by adding
 * new atomic collections of actions.
 *
 * Internally, this class implements an optimistic concurrency control
 * algorithm to handle multiple readers or writers. Any single read
 * is guaranteed to see a consistent snapshot of the table.
 */
class DeltaLog private(
    val logPath: Path,
    val dataPath: Path,
    val clock: Clock)
  extends DeltaLogReader[Dataset[SingleAction], Dataset[AddFile], Snapshot]
  with Checkpoints
  with MetadataCleanup
  with LogStoreProvider
  with VerifyChecksum {

  import org.apache.spark.sql.delta.util.FileNames._


  private lazy implicit val _clock = clock
  override protected[delta] def currentTimeMillis(): Long = clock.getTimeMillis()

  @volatile private[delta] var asyncUpdateTask: Future[Unit] = _
  /** The timestamp when the last successful update action is finished. */
  @volatile private var lastUpdateTimestamp = -1L

  protected def spark = SparkSession.active

  /** Used to read and write physical log files and checkpoints. */
  val store = createLogStore(spark)
  /** Direct access to the underlying storage system. */
  protected[delta] val fs = logPath.getFileSystem(spark.sessionState.newHadoopConf)

  /** Delta History Manager containing version and commit history. */
  lazy val history = new DeltaHistoryManager(
    this, spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_HISTORY_PAR_SEARCH_THRESHOLD))

  /* --------------- *
   |  Configuration  |
   * --------------- */

  /** Returns the checkpoint interval for this log. Not transactional. */
  def checkpointInterval: Int = DeltaConfigs.CHECKPOINT_INTERVAL.fromMetaData(metadata)

  /**
   * The max lineage length of a Snapshot before Delta forces to build a Snapshot from scratch.
   * Delta will build a Snapshot on top of the previous one if it doesn't see a checkpoint.
   * However, there is a race condition that when two writers are writing at the same time,
   * a writer may fail to pick up checkpoints written by another one, and the lineage will grow
   * and finally cause StackOverflowError. Hence we have to force to build a Snapshot from scratch
   * when the lineage length is too large to avoid hitting StackOverflowError.
   */
  override def maxSnapshotLineageLength: Int =
    spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_MAX_SNAPSHOT_LINEAGE_LENGTH)

  /**
   * Checks whether this table only accepts appends. If so it will throw an error in operations that
   * can remove data such as DELETE/UPDATE/MERGE.
   */
  def assertRemovable(): Unit = {
    if (DeltaConfigs.IS_APPEND_ONLY.fromMetaData(metadata)) {
      throw DeltaErrors.modifyAppendOnlyTableException
    }
  }

  currentSnapshot

  /* ------------------ *
   |  State Management  |
   * ------------------ */

  override protected def createSnapshot(
      path: Path,
      version: Long,
      previousSnapshot: Option[Dataset[SingleAction]],
      files: Seq[Path],
      minFileRetentionTimestamp: Long,
      deltaLogReader: DeltaLogReader[_, _, _],
      timestamp: Long,
      lineageLength: Int = 1): Snapshot = {
    new Snapshot(
      path, version, previousSnapshot, files, minFileRetentionTimestamp,
      this, timestamp, lineageLength)
  }

  /** Checks if the snapshot of the table has surpassed our allowed staleness. */
  private def isSnapshotStale: Boolean = {
    val stalenessLimit = spark.sessionState.conf.getConf(
      DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT)
    stalenessLimit == 0L || lastUpdateTimestamp < 0 ||
      clock.getTimeMillis() - lastUpdateTimestamp >= stalenessLimit
  }

  /**
   * Update ActionLog by applying the new delta files if any.
   *
   * @param stalenessAcceptable Whether we can accept working with a stale version of the table. If
   *                            the table has surpassed our staleness tolerance, we will update to
   *                            the latest state of the table synchronously. If staleness is
   *                            acceptable, and the table hasn't passed the staleness tolerance, we
   *                            will kick off a job in the background to update the table state,
   *                            and can return a stale snapshot in the meantime.
   */
  override def update(stalenessAcceptable: Boolean = false): Snapshot = {
    val doAsync = stalenessAcceptable && !isSnapshotStale
    if (!doAsync) {
      lockInterruptibly {
        updateInternal(isAsync = false)
      }
    } else {
      if (asyncUpdateTask == null || asyncUpdateTask.isCompleted) {
        val jobGroup = spark.sparkContext.getLocalProperty(SparkContext.SPARK_JOB_GROUP_ID)
        asyncUpdateTask = Future[Unit] {
          spark.sparkContext.setLocalProperty("spark.scheduler.pool", "deltaStateUpdatePool")
          spark.sparkContext.setJobGroup(
            jobGroup,
            s"Updating state of Delta table at ${currentSnapshot.path}",
            interruptOnCancel = true)
          tryUpdate(isAsync = true)
        }(DeltaLog.deltaLogAsyncUpdateThreadPool)
      }
      currentSnapshot
    }
  }

  /* ------------------ *
   |  Delta Management  |
   * ------------------ */

  /**
   * Returns a new [[OptimisticTransaction]] that can be used to read the current state of the
   * log and then commit updates. The reads and updates will be checked for logical conflicts
   * with any concurrent writes to the log.
   *
   * Note that all reads in a transaction must go through the returned transaction object, and not
   * directly to the [[DeltaLog]] otherwise they will not be checked for conflicts.
   */
  def startTransaction(): OptimisticTransaction = {
    update()
    new OptimisticTransaction(this)
  }

  /**
   * Execute a piece of code within a new [[OptimisticTransaction]]. Reads/write sets will
   * be recorded for this table, and all other tables will be read
   * at a snapshot that is pinned on the first access.
   *
   * @note This uses thread-local variable to make the active transaction visible. So do not use
   *       multi-threaded code in the provided thunk.
   */
  def withNewTransaction[T](thunk: OptimisticTransaction => T): T = {
    try {
      update()
      val txn = new OptimisticTransaction(this)
      OptimisticTransaction.setActive(txn)
      thunk(txn)
    } finally {
      OptimisticTransaction.clearActive()
    }
  }

  /* --------------------- *
   |  Protocol handling    |
   * --------------------- */

  /**
   * Upgrade the table's protocol version, by default to the maximum recognized reader and writer
   * versions in this DBR release.
   */
  def upgradeProtocol(newVersion: Protocol = Protocol()): Unit = {
    val currentVersion = snapshot.protocol
    if (newVersion.minReaderVersion < currentVersion.minReaderVersion ||
        newVersion.minWriterVersion < currentVersion.minWriterVersion) {
      throw new ProtocolDowngradeException(currentVersion, newVersion)
    } else if (newVersion.minReaderVersion == currentVersion.minReaderVersion &&
               newVersion.minWriterVersion == currentVersion.minWriterVersion) {
      logConsole(s"Table $dataPath is already at protocol version $newVersion.")
      return
    }

    val txn = startTransaction()
    try {
      SchemaUtils.checkColumnNameDuplication(txn.metadata.schema, "in the table schema")
    } catch {
      case e: AnalysisException =>
        throw new AnalysisException(
          e.getMessage + "\nPlease remove duplicate columns before you update your table.")
    }
    txn.commit(Seq(newVersion), DeltaOperations.UpgradeProtocol(newVersion))
    logConsole(s"Upgraded table at $dataPath to $newVersion.")
  }

  override protected def oldProtocolMessage(protocol: Protocol): String =
    s"WARNING: The Delta Lake table at $dataPath has version " +
      s"${protocol.simpleString}, but the latest version is " +
      s"${Protocol().simpleString}. To take advantage of the latest features and bug fixes, " +
      "we recommend that you upgrade the table.\n" +
      "First update all clusters that use this table to the latest version of Databricks " +
      "Runtime, and then run the following command in a notebook:\n" +
      "'%scala com.databricks.delta.Delta.upgradeTable(\"" + s"$dataPath" + "\")'\n\n" +
      "For more information about Delta Lake table versions, see " +
      s"${DeltaErrors.baseDocsPath(spark)}/delta/versioning.html"

  /* ---------------------------------------- *
   |  Log Directory Management and Retention  |
   * ---------------------------------------- */

  def isValid(): Boolean = {
    val expectedExistingFile = deltaFile(logPath, currentSnapshot.version)
    try {
      store.listFrom(expectedExistingFile)
        .take(1)
        .exists(_.getPath.getName == expectedExistingFile.getName)
    } catch {
      case _: FileNotFoundException =>
        // Parent of expectedExistingFile doesn't exist
        false
    }
  }

  def isSameLogAs(otherLog: DeltaLog): Boolean = this.tableId == otherLog.tableId

  /** Creates the log directory if it does not exist. */
  def ensureLogDirectoryExist(): Unit = {
    if (!fs.exists(logPath)) {
      if (!fs.mkdirs(logPath)) {
        throw new IOException(s"Cannot create $logPath")
      }
    }
  }

  /* ------------  *
   |  Integration  |
   * ------------  */

  /**
   * Returns a [[org.apache.spark.sql.DataFrame]] containing the new files within the specified
   * version range.
   */
  def createDataFrame(
      snapshot: Snapshot,
      addFiles: Seq[AddFile],
      isStreaming: Boolean = false,
      actionTypeOpt: Option[String] = None): DataFrame = {
    val actionType = actionTypeOpt.getOrElse(if (isStreaming) "streaming" else "batch")
    val fileIndex = new TahoeBatchFileIndex(spark, actionType, addFiles, this, dataPath, snapshot)

    val relation = HadoopFsRelation(
      fileIndex,
      partitionSchema = snapshot.metadata.partitionSchema,
      dataSchema = snapshot.metadata.schema,
      bucketSpec = None,
      snapshot.fileFormat,
      snapshot.metadata.format.options)(spark)

    Dataset.ofRows(spark, LogicalRelation(relation, isStreaming = isStreaming))
  }

  /**
   * Returns a [[BaseRelation]] that contains all of the data present
   * in the table. This relation will be continually updated
   * as files are added or removed from the table. However, new [[BaseRelation]]
   * must be requested in order to see changes to the schema.
   */
  def createRelation(
      partitionFilters: Seq[Expression] = Nil,
      timeTravel: Option[DeltaTimeTravelSpec] = None): BaseRelation = {

    val versionToUse = timeTravel.map { tt =>
      val (version, accessType) = DeltaTableUtils.resolveTimeTravelVersion(
        spark.sessionState.conf, this, tt)
      val source = tt.creationSource.getOrElse("unknown")
      recordDeltaEvent(this, s"delta.timeTravel.$source", data = Map(
        "tableVersion" -> snapshot.version,
        "queriedVersion" -> version,
        "accessType" -> accessType
      ))
      version
    }

    /** Used to link the files present in the table into the query planner. */
    val fileIndex = TahoeLogFileIndex(spark, this, dataPath, partitionFilters, versionToUse)
    val snapshotToUse = versionToUse.map(getSnapshotAt(_)).getOrElse(snapshot)

    new HadoopFsRelation(
      fileIndex,
      partitionSchema = snapshotToUse.metadata.partitionSchema,
      dataSchema = snapshotToUse.metadata.schema,
      bucketSpec = None,
      snapshotToUse.fileFormat,
      snapshotToUse.metadata.format.options)(spark) with InsertableRelation {
      def insert(data: DataFrame, overwrite: Boolean): Unit = {
        val mode = if (overwrite) SaveMode.Overwrite else SaveMode.Append
        WriteIntoDelta(
          deltaLog = DeltaLog.this,
          mode = mode,
          new DeltaOptions(Map.empty[String, String], spark.sessionState.conf),
          partitionColumns = Seq.empty,
          configuration = Map.empty,
          data = data).run(spark)
      }
    }
  }
}

object DeltaLog extends DeltaLogging {

  protected lazy val deltaLogAsyncUpdateThreadPool = {
    val tpe = ThreadUtils.newDaemonCachedThreadPool("delta-state-update", 8)
    ExecutionContext.fromExecutorService(tpe)
  }

  /**
   * We create only a single [[DeltaLog]] for any given path to avoid wasted work
   * in reconstructing the log.
   */
  private val deltaLogCache = {
    val builder = CacheBuilder.newBuilder()
      .expireAfterAccess(60, TimeUnit.MINUTES)
      .removalListener(new RemovalListener[Path, DeltaLog] {
        override def onRemoval(removalNotification: RemovalNotification[Path, DeltaLog]) = {
          val log = removalNotification.getValue
          try log.snapshot.uncache() catch {
            case _: java.lang.NullPointerException =>
              // Various layers will throw null pointer if the RDD is already gone.
          }
        }
      })
    sys.props.get("delta.log.cacheSize")
      .flatMap(v => Try(v.toLong).toOption)
      .foreach(builder.maximumSize)
    builder.build[Path, DeltaLog]()
  }

  /** Helper for creating a log when it stored at the root of the data. */
  def forTable(spark: SparkSession, dataPath: String): DeltaLog = {
    apply(spark, new Path(dataPath, "_delta_log"), new SystemClock)
  }

  /** Helper for creating a log when it stored at the root of the data. */
  def forTable(spark: SparkSession, dataPath: File): DeltaLog = {
    apply(spark, new Path(dataPath.getAbsolutePath, "_delta_log"), new SystemClock)
  }

  /** Helper for creating a log when it stored at the root of the data. */
  def forTable(spark: SparkSession, dataPath: Path): DeltaLog = {
    apply(spark, new Path(dataPath, "_delta_log"), new SystemClock)
  }

  /** Helper for creating a log when it stored at the root of the data. */
  def forTable(spark: SparkSession, dataPath: String, clock: Clock): DeltaLog = {
    apply(spark, new Path(dataPath, "_delta_log"), clock)
  }

  /** Helper for creating a log when it stored at the root of the data. */
  def forTable(spark: SparkSession, dataPath: File, clock: Clock): DeltaLog = {
    apply(spark, new Path(dataPath.getAbsolutePath, "_delta_log"), clock)
  }

  /** Helper for creating a log when it stored at the root of the data. */
  def forTable(spark: SparkSession, dataPath: Path, clock: Clock): DeltaLog = {
    apply(spark, new Path(dataPath, "_delta_log"), clock)
  }
  // TODO: Don't assume the data path here.
  def apply(spark: SparkSession, rawPath: Path, clock: Clock = new SystemClock): DeltaLog = {
    val hadoopConf = spark.sessionState.newHadoopConf()
    val fs = rawPath.getFileSystem(hadoopConf)
    val path = fs.makeQualified(rawPath)
    // The following cases will still create a new ActionLog even if there is a cached
    // ActionLog using a different format path:
    // - Different `scheme`
    // - Different `authority` (e.g., different user tokens in the path)
    // - Different mount point.
    val cached = try {
      deltaLogCache.get(path, new Callable[DeltaLog] {
        override def call(): DeltaLog = recordDeltaOperation(
            null, "delta.log.create", Map(TAG_TAHOE_PATH -> path.getParent.toString)) {
          AnalysisHelper.allowInvokingTransformsInAnalyzer {
            new DeltaLog(path, path.getParent, clock)
          }
        }
      })
    } catch {
      case e: com.google.common.util.concurrent.UncheckedExecutionException =>
        throw e.getCause
    }

    // Invalidate the cache if the reference is no longer valid as a result of the
    // log being deleted.
    if (cached.snapshot.version == -1 || cached.isValid()) {
      cached
    } else {
      deltaLogCache.invalidate(path)
      apply(spark, path)
    }
  }

  /** Invalidate the cached DeltaLog object for the given `dataPath`. */
  def invalidateCache(spark: SparkSession, dataPath: Path): Unit = {
    try {
      val rawPath = new Path(dataPath, "_delta_log")
      val fs = rawPath.getFileSystem(spark.sessionState.newHadoopConf())
      val path = fs.makeQualified(rawPath)
      deltaLogCache.invalidate(path)
    } catch {
      case NonFatal(e) => logWarning(e.getMessage, e)
    }
  }

  def clearCache(): Unit = {
    deltaLogCache.invalidateAll()
  }

  /**
   * Filters the given [[Dataset]] by the given `partitionFilters`, returning those that match.
   * @param files The active files in the DeltaLog state, which contains the partition value
   *              information
   * @param partitionFilters Filters on the partition columns
   * @param partitionColumnPrefixes The path to the `partitionValues` column, if it's nested
   */
  def filterFileList(
      partitionColumns: Seq[String],
      files: DataFrame,
      partitionFilters: Seq[Expression],
      partitionColumnPrefixes: Seq[String] = Nil): DataFrame = {
    val rewrittenFilters = rewritePartitionFilters(
      partitionColumns,
      files.sparkSession.sessionState.conf.resolver,
      partitionFilters,
      partitionColumnPrefixes)
    val columnFilter = new Column(rewrittenFilters.reduceLeftOption(And).getOrElse(Literal(true)))
    files.filter(columnFilter)
  }

  /**
   * Rewrite the given `partitionFilters` to be used for filtering partition values.
   * We need to explicitly resolve the partitioning columns here because the partition columns
   * are stored as keys of a Map type instead of attributes in the AddFile schema (below) and thus
   * cannot be resolved automatically.
   *
   * @param partitionFilters Filters on the partition columns
   * @param partitionColumnPrefixes The path to the `partitionValues` column, if it's nested
   */
  def rewritePartitionFilters(
      partitionColumns: Seq[String],
      resolver: Resolver,
      partitionFilters: Seq[Expression],
      partitionColumnPrefixes: Seq[String] = Nil): Seq[Expression] = {
    partitionFilters.map(_.transform {
      case a: Attribute =>
        val colName = partitionColumns.find(resolver(_, a.name)).getOrElse(a.name)
        UnresolvedAttribute(partitionColumnPrefixes ++ Seq("partitionValues", colName))
    }.transform {
      // TODO(SC-10573): This is a temporary fix.
      // What we really need to do is ensure that the partition filters are evaluated against
      // the actual partition values. Right now they're evaluated against a String-casted version
      // of the partition value in AddFile.
      // As a warmfixable change, we're just transforming the only operator we've seen cause
      // problems.
      case InSet(a, set) => In(a, set.toSeq.map(Literal(_)))
    })
  }
}
