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

import java.util.UUID

import scala.util.control.NonFatal

import org.apache.spark.sql.delta.actions.{Action, Metadata, SingleAction}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.storage.LogStore
import org.apache.spark.sql.delta.util.DeltaFileOperations
import org.apache.spark.sql.delta.util.FileNames._
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.hadoop.fs.{FileContext, FileStatus, Path}
import org.apache.hadoop.mapred.{JobConf, TaskAttemptContextImpl, TaskAttemptID}
import org.apache.hadoop.mapreduce.{Job, TaskType}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.util.SerializableConfiguration

trait Checkpoints extends CheckpointReader {
  self: DeltaLog =>

  def logPath: Path
  def dataPath: Path
  def snapshot: Snapshot
  def store: LogStore
  protected def metadata: Metadata

  /** Used to clean up stale log files. */
  protected def doLogCleanup(): Unit

  /** Creates a checkpoint at the current log version. */
  def checkpoint(): Unit = recordDeltaOperation(this, "delta.checkpoint") {
    val checkpointMetaData = checkpoint(snapshot)
    val json = JsonUtils.toJson(checkpointMetaData)
    store.write(LAST_CHECKPOINT, Iterator(json), overwrite = true)

    doLogCleanup()
  }

  protected def checkpoint(snapshotToCheckpoint: Snapshot): CheckpointMetaData = {
    Checkpoints.writeCheckpoint(spark, this, snapshotToCheckpoint)
  }

  override def listFrom(path: Path): Iterator[FileStatus] = store.listFrom(path)

  override def read(path: Path): Seq[String] = store.read(path)
}

object Checkpoints {
  /**
   * Writes out the contents of a [[Snapshot]] into a checkpoint file that
   * can be used to short-circuit future replays of the log.
   *
   * Returns the checkpoint metadata to be committed to a file. We will use the value
   * in this file as the source of truth of the last valid checkpoint.
   */
  private[delta] def writeCheckpoint(
      spark: SparkSession,
      deltaLog: DeltaLog,
      snapshot: Snapshot): CheckpointMetaData = {
    import SingleAction._

    val (factory, serConf) = {
      val format = new ParquetFileFormat()
      val job = Job.getInstance()
      (format.prepareWrite(spark, job, Map.empty, Action.logSchema),
        new SerializableConfiguration(job.getConfiguration))
    }

    // The writing of checkpoints doesn't go through log store, so we need to check with the
    // log store and decide whether to use rename.
    val useRename = deltaLog.store.isPartialWriteVisible(deltaLog.logPath)

    val checkpointSize = spark.sparkContext.longAccumulator("checkpointSize")
    val numOfFiles = spark.sparkContext.longAccumulator("numOfFiles")
    // Use the string in the closure as Path is not Serializable.
    val path = checkpointFileSingular(snapshot.path, snapshot.version).toString
    val writtenPath = snapshot.state
      .repartition(1)
      .map { action =>
        if (action.add != null) {
          numOfFiles.add(1)
        }
        action
      }
      .queryExecution // This is a hack to get spark to write directly to a file.
      .executedPlan
      .execute()
      .mapPartitions { iter =>
        val writtenPath =
          if (useRename) {
            val p = new Path(path)
            // Two instances of the same task may run at the same time in some cases (e.g.,
            // speculation, stage retry), so generate the temp path here to avoid two tasks
            // using the same path.
            val tempPath = new Path(p.getParent, s".${p.getName}.${UUID.randomUUID}.tmp")
            DeltaFileOperations.registerTempFileDeletionTaskFailureListener(serConf.value, tempPath)
            tempPath.toString
          } else {
            path
          }
        try {
          val writer = factory.newInstance(
            writtenPath,
            Action.logSchema,
            new TaskAttemptContextImpl(
              new JobConf(serConf.value),
              new TaskAttemptID("", 0, TaskType.REDUCE, 0, 0)))

          iter.foreach { row =>
            checkpointSize.add(1)
            writer.write(row)
          }
          writer.close()
        } catch {
          case e: org.apache.hadoop.fs.FileAlreadyExistsException if !useRename =>
            val p = new Path(writtenPath)
            if (p.getFileSystem(serConf.value).exists(p)) {
              // The file has been written by a zombie task. We can just use this checkpoint file
              // rather than failing a Delta commit.
            } else {
              throw e
            }
        }
        Iterator(writtenPath)
      }.collect().head

    if (useRename) {
      val src = new Path(writtenPath)
      val dest = new Path(path)
      val fs = dest.getFileSystem(spark.sessionState.newHadoopConf)
      var renameDone = false
      try {
        if (fs.rename(src, dest)) {
          renameDone = true
        } else {
          // There should be only one writer writing the checkpoint file, so there must be
          // something wrong here.
          throw new IllegalStateException(s"Cannot rename $src to $dest")
        }
      } finally {
        if (!renameDone) {
          fs.delete(src, false)
        }
      }
    }

    if (numOfFiles.value != snapshot.numOfFiles) {
      throw new IllegalStateException(
        "State of the checkpoint doesn't match that of the snapshot.")
    }
    CheckpointMetaData(snapshot.version, checkpointSize.value, None)
  }
}
