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

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.SnapshotLike._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path


/**
 * An immutable snapshot of the state of the log at some delta version. Internally
 * this class manages the replay of actions stored in checkpoint or delta files,
 * given an optional starting snapshot.
 *
 * After resolving any new actions, it caches the result and collects the
 * following basic information to the driver:
 *  - Protocol Version
 *  - Metadata
 *  - Transaction state
 *
 * @param timestamp The timestamp of the latest commit in milliseconds. Can also be set to -1 if the
 *                  timestamp of the commit is unknown or the table has not been initialized, i.e.
 *                  `version = -1`.
 */
class SnapshotLocal(
    val path: Path,
    val version: Long,
    previousSnapshot: Option[Iterator[SingleAction]],
    files: Seq[Path],
    val minFileRetentionTimestamp: Long,
    val deltaLogReader: DeltaLogReader[_, _, _],
    val timestamp: Long,
    val hadoopConf: Configuration,
    val lineageLength: Int = 1)
  extends SnapshotLike[Iterator[SingleAction], Iterator[AddFile]] {

  // Reconstruct the state by applying deltas in order to the checkpoint.
  // We partition by path as it is likely the bulk of the data is add/remove.
  // Non-path based actions will be collocated to a single partition.
  private val stateReconstruction = {
    // val numPartitions = spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_SNAPSHOT_PARTITIONS)

    val checkpointData = previousSnapshot.getOrElse(emptyActions).toIterator
    // val allActions = checkpointData ++ deltaData
    val time = minFileRetentionTimestamp
    val logPath = path.toUri // for serializability


    val deltaData = files.sortBy(_.getName).toIterator.flatMap(f => load(Seq(f)))
    val allActions = (checkpointData ++ deltaData).flatMap {
      _.unwrap match {
        case add: AddFile => Some(add.copy(path = canonicalizePath(add.path, hadoopConf)).wrap)
        case rm: RemoveFile => Some(rm.copy(path = canonicalizePath(rm.path, hadoopConf)).wrap)
        case other if other == null => None
        case other => Some(other.wrap)
      }
    }
    val state = new InMemoryLogReplay(time)
    state.append(0, allActions.map(_.unwrap))
    state.checkpoint.map(_.wrap)
  }

  private val cachedState = stateReconstruction.toSeq

  /** The current set of actions in this [[Snapshot]]. */
  def state: Iterator[SingleAction] = cachedState.toIterator

  // Force materialization of the cache and collect the basics to the driver for fast access.
  // Here we need to bypass the ACL checks for SELECT anonymous function permissions.
  override val State(
  protocol, metadata, setTransactions, sizeInBytes, numOfFiles, numOfMetadata,
  numOfProtocol, numOfRemoves, numOfSetTransactions) = {

    import scala.collection.mutable._
    var lastProtocol: Protocol = Protocol()
    var lastMetadata: Metadata = Metadata()
    val setTransactions: HashSet[SetTransaction] = new HashSet()
    var sizeInBytes: Long = 0
    var numOfFiles: Long = 0
    var numOfMetadata: Long = 0
    var numOfProtocol: Long = 0
    var numOfRemoves: Long = 0
    var numOfSetTransactions: Long = 0

    state.foreach { x =>
      x.unwrap match {
        case m: Metadata => lastMetadata = m; numOfMetadata += 1
        case p: Protocol => lastProtocol = p; numOfProtocol += 1
        case a: AddFile => numOfFiles += 1; sizeInBytes = a.size
        case _: RemoveFile => numOfRemoves += 1
        case s: SetTransaction => setTransactions += s; numOfSetTransactions += 1
        case _ =>
      }
    }
    State(
      lastProtocol, lastMetadata, setTransactions.toSeq, sizeInBytes, numOfFiles,
      numOfMetadata, numOfProtocol, numOfRemoves, numOfSetTransactions)
  }

  deltaLogReader.protocolRead(protocol)

  /** A map to look up transaction version by appId. */
  lazy val transactions = setTransactions.map(t => t.appId -> t.version).toMap

  /** All of the files present in this [[Snapshot]]. */
  def allFiles: Iterator[AddFile] = {
    state.filter(_.add != null).map(_.add)
  }

  /**
   * Load the transaction logs from paths. The files here may have different file formats and the
   * file format can be extracted from the file extensions.
   *
   * Here we are reading the transaction log, and we need to bypass the ACL checks
   * for SELECT any file permissions.
   */
  private def load(paths: Seq[Path]): Seq[SingleAction] = {
    /*
    // TODO: Read json and parquet files

    val spark = SparkSession.getActiveSession.get
    val pathAndFormats = paths.map(_.toString).map(path => path -> path.split("\\.").last)
    pathAndFormats.groupBy(_._2).map { case (format, paths) =>
      spark.read.format(format).schema(logSchema)
        .load(paths.map(_._1): _*).as[SingleAction]
    }.reduceOption(_.union(_)).map(_.collect().toSeq).getOrElse(emptyActions)

    */
    Seq.empty
  }

  private def readParquetCheckpoint(path: Path): Unit = {
    
  }

  private def emptyActions = Seq[SingleAction]()
}
