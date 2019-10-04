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
import java.io.{BufferedReader, FileNotFoundException, InputStreamReader}
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.locks.ReentrantLock

import scala.util.control.NonFatal

import com.databricks.spark.util.{DatabricksLogging, TagDefinition}
import com.databricks.spark.util.MetricDefinitions._
import com.databricks.spark.util.TagDefinitions._
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.util.{DeltaProgressReporter, JsonUtils}
import org.apache.spark.sql.delta.util.FileNames._
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

/**
 * Used to query the current state of the log as well as modify it by adding
 * new atomic collections of actions.
 *
 * Internally, this class implements an optimistic concurrency control
 * algorithm to handle multiple readers or writers. Any single read
 * is guaranteed to see a consistent snapshot of the table.
 */
trait DeltaLogReader[
    ActionsRepr,
    AddFilesRepr,
    SnapshotType <: SnapshotLike[ActionsRepr, AddFilesRepr]]
  extends CheckpointReader
  with DeltaProgressReporter
  with DatabricksLogging {

  protected def createSnapshot(
      path: Path,
      version: Long,
      previousSnapshot: Option[ActionsRepr],
      files: Seq[Path],
      minFileRetentionTimestamp: Long,
      deltaLogReader: DeltaLogReader[_, _, _],
      timestamp: Long,
      lineageLength: Int = 1): SnapshotType

  def logPath: Path
  def dataPath: Path

  protected[delta] def currentTimeMillis(): Long = System.currentTimeMillis()

  /** The timestamp when the last successful update action is finished. */
  @volatile private var lastUpdateTimestamp = -1L

  /** Use ReentrantLock to allow us to call `lockInterruptibly` */
  protected[delta] val deltaLogLock = new ReentrantLock()

  /* --------------- *
   |  Configuration  |
   * --------------- */

  /**
   * The max lineage length of a Snapshot before Delta forces to build a Snapshot from scratch.
   * Delta will build a Snapshot on top of the previous one if it doesn't see a checkpoint.
   * However, there is a race condition that when two writers are writing at the same time,
   * a writer may fail to pick up checkpoints written by another one, and the lineage will grow
   * and finally cause StackOverflowError. Hence we have to force to build a Snapshot from scratch
   * when the lineage length is too large to avoid hitting StackOverflowError.
   */
  def maxSnapshotLineageLength: Int = 50

  /** How long to keep around logically deleted files before physically deleting them. */
  private[delta] def tombstoneRetentionMillis: Long =
    DeltaConfigs.TOMBSTONE_RETENTION.fromMetaData(metadata).milliseconds()

  // TODO: There is a race here where files could get dropped when increasing the
  // retention interval...
  protected def metadata = if (_currentSnapshot == null) Metadata() else _currentSnapshot.metadata

  /**
   * Tombstones before this timestamp will be dropped from the state and the files can be
   * garbage collected.
   */
  def minFileRetentionTimestamp: Long = currentTimeMillis() - tombstoneRetentionMillis

  /** The unique identifier for this table. */
  def tableId: String = metadata.id

  /* ------------------ *
   |  State Management  |
   * ------------------ */

  @volatile protected var _currentSnapshot: SnapshotType = _

  private def initializeSnapshot(): SnapshotType = lastCheckpoint.map { c =>
    val checkpointFiles = c.parts
      .map(p => checkpointFileWithParts(logPath, c.version, p))
      .getOrElse(Seq(checkpointFileSingular(logPath, c.version)))
    val deltas = listFrom(deltaFile(logPath, c.version + 1))
      .filter(f => isDeltaFile(f.getPath))
      .toArray
    val deltaVersions = deltas.map(f => deltaVersion(f.getPath))
    verifyDeltaVersions(deltaVersions)
    val newVersion = deltaVersions.lastOption.getOrElse(c.version)
    val deltaFiles = ((c.version + 1) to newVersion).map(deltaFile(logPath, _))
    logInfo(s"Loading version $newVersion starting from checkpoint ${c.version}")
    try {
      val snapshot = createSnapshot(
        logPath,
        newVersion,
        None,
        checkpointFiles ++ deltaFiles,
        minFileRetentionTimestamp,
        this,
        // we don't want to make an additional RPC here to get commit timestamps when "deltas" is
        // empty. The next "update" call will take care of that if there are delta files.
        deltas.lastOption.map(_.getModificationTime).getOrElse(-1L))
      lastUpdateTimestamp = currentTimeMillis()
      snapshot
    } catch {
      case e: Exception if Option(e.getMessage).exists(_.contains("Path does not exist")) =>
        throw e
    }
  }.getOrElse {
    createSnapshot(logPath, -1, None, Nil, minFileRetentionTimestamp, this, -1L)
  }

  protected def currentSnapshot: SnapshotType = {
    if (_currentSnapshot == null) {
      _currentSnapshot = initializeSnapshot()
      if (_currentSnapshot.version == -1) {
        // No checkpoint exists. Call "update" to load delta files.
        update()
      }
    }
    _currentSnapshot
  }

  /**
   * Verify the versions are contiguous.
   */
  private def verifyDeltaVersions(versions: Array[Long]): Unit = {
    // Turn this to a vector so that we can compare it with a range.
    val deltaVersions = versions.toVector
    if (deltaVersions.nonEmpty &&
      (deltaVersions.head to deltaVersions.last) != deltaVersions) {
      throw new IllegalStateException(s"versions ($deltaVersions) are not contiguous")
    }
  }

  /** Returns the current snapshot. Note this does not automatically `update()`. */
  def snapshot: SnapshotType = currentSnapshot

  /**
   * Run `body` inside `deltaLogLock` lock using `lockInterruptibly` so that the thread can be
   * interrupted when waiting for the lock.
   */
  def lockInterruptibly[T](body: => T): T = {
    deltaLogLock.lockInterruptibly()
    try {
      body
    } finally {
      deltaLogLock.unlock()
    }
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
  def update(stalenessAcceptable: Boolean = false): SnapshotType = {
    lockInterruptibly {
      updateInternal(isAsync = false)
    }
  }

  /**
   * Try to update ActionLog. If another thread is updating ActionLog, then this method returns
   * at once and return the current snapshot. The return snapshot may be stale.
   */
  def tryUpdate(isAsync: Boolean = false): SnapshotType = {
    if (deltaLogLock.tryLock()) {
      try {
        updateInternal(isAsync)
      } finally {
        deltaLogLock.unlock()
      }
    } else {
      currentSnapshot
    }
  }

  /**
   * Queries the store for new delta files and applies them to the current state.
   * Note: the caller should hold `deltaLogLock` before calling this method.
   */
  protected def updateInternal(isAsync: Boolean): SnapshotType = {
    withStatusCode("DELTA", "Updating the Delta table's state") {
      try {
        val newFiles =
          // List from the current version since we want to get the checkpoint file for the current
          // version
          listFrom(checkpointPrefix(logPath, math.max(currentSnapshot.version, 0L)))
          // Pick up checkpoint files not older than the current version and delta files newer than
          // the current version
          .filter { file =>
            isCheckpointFile(file.getPath) ||
              (isDeltaFile(file.getPath) && deltaVersion(file.getPath) > currentSnapshot.version)
        }.toArray

        val (checkpoints, deltas) = newFiles.partition(f => isCheckpointFile(f.getPath))
        if (deltas.isEmpty) {
          lastUpdateTimestamp = currentTimeMillis()
          return currentSnapshot
        }

        val deltaVersions = deltas.map(f => deltaVersion(f.getPath))
        verifyDeltaVersions(deltaVersions)
        val lastChkpoint = lastCheckpoint.map(CheckpointInstance.apply)
            .getOrElse(CheckpointInstance.MaxValue)
        val checkpointFiles = checkpoints.map(f => CheckpointInstance(f.getPath))
        val newCheckpoint = getLatestCompleteCheckpointFromList(checkpointFiles, lastChkpoint)
        val newSnapshot = if (newCheckpoint.isDefined) {
          // If there is a new checkpoint, start new lineage there.
          val newCheckpointVersion = newCheckpoint.get.version
          assert(
            newCheckpointVersion >= currentSnapshot.version,
            s"Attempting to load a checkpoint($newCheckpointVersion) " +
                s"older than current version (${currentSnapshot.version})")
          val newCheckpointFiles = newCheckpoint.get.getCorrespondingFiles(logPath)

          val newVersion = deltaVersions.last
          val deltaFiles =
            ((newCheckpointVersion + 1) to newVersion).map(deltaFile(logPath, _))

          logInfo(s"Loading version $newVersion starting from checkpoint $newCheckpointVersion")

          createSnapshot(
            logPath,
            newVersion,
            None,
            newCheckpointFiles ++ deltaFiles,
            minFileRetentionTimestamp,
            this,
            deltas.last.getModificationTime)
        } else {
          // If there is no new checkpoint, just apply the deltas to the existing state.
          assert(currentSnapshot.version + 1 == deltaVersions.head,
            s"versions in [${currentSnapshot.version + 1}, ${deltaVersions.head}) are missing")
          if (currentSnapshot.lineageLength >= maxSnapshotLineageLength) {
            // Load Snapshot from scratch to avoid StackOverflowError
            getSnapshotAt(deltaVersions.last, Some(deltas.last.getModificationTime))
          } else {
            createSnapshot(
              logPath,
              deltaVersions.last,
              Some(currentSnapshot.state),
              deltas.map(_.getPath),
              minFileRetentionTimestamp,
              this,
              deltas.last.getModificationTime,
              lineageLength = currentSnapshot.lineageLength + 1)
          }
        }
        _currentSnapshot = newSnapshot
      } catch {
        case f: FileNotFoundException =>
          val message = s"No delta log found for the Delta table at $logPath"
          logInfo(message)
          // When the state is empty, this is expected. The log will be lazily created when needed.
          // When the state is not empty, it's a real issue and we can't continue to execution.
          if (currentSnapshot.version != -1) {
            val e = new FileNotFoundException(message)
            e.setStackTrace(f.getStackTrace())
            throw e
          }
      }
      lastUpdateTimestamp = currentTimeMillis()
      currentSnapshot
    }
  }


  /* ------------------ *
   |  Delta Management  |
   * ------------------ */

  /**
   * Get all actions starting from "startVersion" (inclusive). If `startVersion` doesn't exist,
   * return an empty Iterator.
   */
  def getChanges(startVersion: Long): Iterator[(Long, Seq[Action])] = {
    val deltas = listFrom(deltaFile(logPath, startVersion))
      .filter(f => isDeltaFile(f.getPath))
    deltas.map { status =>
      val p = status.getPath
      val version = deltaVersion(p)

      (version, read(p).map(Action.fromJson))
    }
  }

  /* --------------------- *
   |  Protocol validation  |
   * --------------------- */

  protected def oldProtocolMessage(protocol: Protocol): String =
    s"WARNING: The Delta table at $dataPath has version " +
      s"${protocol.simpleString}, but the latest version is " +
      s"${Protocol().simpleString}. To take advantage of the latest features and bug fixes, " +
      "we recommend that you upgrade the table.\n" +
      "First update all clusters that use this table to the latest version of Databricks " +
      "Runtime, and then run the following command in a notebook:\n" +
      "'%scala com.databricks.delta.Delta.upgradeTable(\"" + s"$dataPath" + "\")'\n\n" +
      "For more information about Delta table versions, see the docs."

  /**
   * If the given `protocol` is older than that of the client.
   */
  protected def isProtocolOld(protocol: Protocol): Boolean = protocol != null &&
    (Action.readerVersion > protocol.minReaderVersion ||
      Action.writerVersion > protocol.minWriterVersion)

  /**
   * Asserts that the client is up to date with the protocol and
   * allowed to read the table that is using the given `protocol`.
   */
  def protocolRead(protocol: Protocol): Unit = {
    if (protocol != null &&
      Action.readerVersion < protocol.minReaderVersion) {
      recordDeltaEvent(
        this.dataPath,
        this.snapshot.metadata.id,
        "delta.protocol.failure.read",
        data = Map(
          "clientVersion" -> Action.readerVersion,
          "minReaderVersion" -> protocol.minReaderVersion))
      throw new InvalidProtocolVersionException
    }

    if (isProtocolOld(protocol)) {
      recordDeltaEvent(
        this.dataPath, this.snapshot.metadata.id, "delta.protocol.warning")
      logConsole(oldProtocolMessage(protocol))
    }
  }

  /**
   * Asserts that the client is up to date with the protocol and
   * allowed to write to the table that is using the given `protocol`.
   */
  def protocolWrite(protocol: Protocol, logUpgradeMessage: Boolean = true): Unit = {
    if (protocol != null && Action.writerVersion < protocol.minWriterVersion) {
      recordDeltaEvent(
        this.dataPath,
        this.snapshot.metadata.id,
        opType = "delta.protocol.failure.write",
        data = Map(
          "clientVersion" -> Action.writerVersion,
          "minWriterVersion" -> protocol.minWriterVersion))
      throw new InvalidProtocolVersionException
    }

    if (logUpgradeMessage && isProtocolOld(protocol)) {
      recordDeltaEvent(
        this.dataPath, this.snapshot.metadata.id, "delta.protocol.warning")
      logConsole(oldProtocolMessage(protocol))
    }
  }

  /* ------------------- *
   |  History Management |
   * ------------------- */

  /** Get the snapshot at `version`. */
  def getSnapshotAt(
      version: Long,
      commitTimestamp: Option[Long] = None,
      lastCheckpointHint: Option[CheckpointInstance] = None): SnapshotType = {
    val current = snapshot
    if (current.version == version) {
      return current
    }

    // Do not use the hint if the version we're asking for is smaller than the last checkpoint hint
    val lastCheckpoint = lastCheckpointHint.collect { case ci if ci.version <= version => ci }
      .orElse(findLastCompleteCheckpoint(CheckpointInstance(version, None)))
    val lastCheckpointFiles = lastCheckpoint.map { c =>
      c.getCorrespondingFiles(logPath)
    }.toSeq.flatten
    val checkpointVersion = lastCheckpoint.map(_.version)
    if (checkpointVersion.isEmpty) {
      val versionZeroFile = deltaFile(logPath, 0L)
      val versionZeroFileExists = listFrom(versionZeroFile)
        .take(1)
        .exists(_.getPath.getName == versionZeroFile.getName)
      if (!versionZeroFileExists) {
        throw logFileNotFoundException(versionZeroFile, 0L, metadata)
      }
    }
    val deltaData =
      ((checkpointVersion.getOrElse(-1L) + 1) to version).map(deltaFile(logPath, _))
    createSnapshot(
      logPath,
      version,
      None,
      lastCheckpointFiles ++ deltaData,
      minFileRetentionTimestamp,
      this,

      commitTimestamp.getOrElse(-1L))
  }

  private def logFileNotFoundException(
      path: Path,
      version: Long,
      metadata: Metadata): Throwable = {
    val logRetention = DeltaConfigs.LOG_RETENTION.fromMetaData(metadata)
    val checkpointRetention = DeltaConfigs.CHECKPOINT_RETENTION_DURATION.fromMetaData(metadata)
    new FileNotFoundException(s"$path: Unable to reconstruct state at version $version as the " +
      s"transaction log has been truncated due to manual deletion or the log retention policy " +
      s"(${DeltaConfigs.LOG_RETENTION.key}=$logRetention) and checkpoint retention policy " +
      s"(${DeltaConfigs.CHECKPOINT_RETENTION_DURATION.key}=$checkpointRetention)")
  }

  private def recordDeltaEvent(
      deltaDataPath: Path,
      deltaMetadataId: String,
      opType: String,
      tags: Map[TagDefinition, String] = Map.empty,
      data: AnyRef = null): Unit = {
    try {
      val json = if (data != null) JsonUtils.toJson(data) else ""
      val tableTags = Seq(
        TAG_TAHOE_PATH -> deltaDataPath.toString,
        TAG_TAHOE_ID -> deltaMetadataId
      ).filter(_._2 != null).toMap
      recordEvent(
        EVENT_TAHOE,
        Map(TAG_OP_TYPE -> opType) ++ tableTags ++ tags,
        blob = json)
    } catch {
      case NonFatal(e) =>
        recordEvent(
          EVENT_LOGGING_FAILURE,
          blob = JsonUtils.toJson(
            Map(
              "exception" -> e.getMessage,
              "opType" -> opType,
              "method" -> "recordDeltaEvent"))
        )
    }
  }
}

