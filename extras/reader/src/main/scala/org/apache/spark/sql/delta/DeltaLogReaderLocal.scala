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

import java.io.{BufferedReader, FileNotFoundException, InputStreamReader}
import java.nio.charset.StandardCharsets.UTF_8

import org.apache.spark.sql.delta.actions.{AddFile, SingleAction}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}


class DeltaLogReaderLocal(
  val logPath: Path,
  val dataPath: Path,
  val hadoopConf: Configuration)
  extends DeltaLogReader[Iterator[SingleAction], Iterator[AddFile], SnapshotLocal] {

  override protected def createSnapshot(
    path: Path,
    version: Long,
    previousSnapshot: Option[Iterator[SingleAction]],
    files: Seq[Path],
    minFileRetentionTimestamp: Long,
    deltaLogReader: DeltaLogReader[_, _, _],
    timestamp: Long,
    lineageLength: Int = 1): SnapshotLocal = {
    new SnapshotLocal(
      path, version, previousSnapshot, files, minFileRetentionTimestamp,
      this, timestamp, hadoopConf, lineageLength)
  }

  def listFrom(path: Path): Iterator[FileStatus] = {
    val fs = path.getFileSystem(hadoopConf)
    if (!fs.exists(path.getParent)) {
      throw new FileNotFoundException(s"No such file or directory: ${path.getParent}")
    }
    val files = fs.listStatus(path.getParent)
    files.filter(_.getPath.getName >= path.getName).sortBy(_.getPath.getName).iterator
  }

  def read(path: Path): Seq[String] = {
    import scala.collection.JavaConverters._

    val fs = path.getFileSystem(hadoopConf)
    val stream = fs.open(path)
    try {
      val reader = new BufferedReader(new InputStreamReader(stream, UTF_8))
      IOUtils.readLines(reader).asScala.map(_.trim)
    } finally {
      stream.close()
    }
  }

  currentSnapshot
}
