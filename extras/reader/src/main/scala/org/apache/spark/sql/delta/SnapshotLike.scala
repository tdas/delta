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
import java.net.URI

import org.apache.spark.sql.delta.SnapshotLike._
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.actions.Action.logSchema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

trait SnapshotLike[+ActionsRepr, +AddFilesRepr] extends AnyRef {

  def version: Long

  def lineageLength: Int

  /** The current set of actions in this [[Snapshot]]. */
  def state: ActionsRepr

  def allFiles: AddFilesRepr

  val State(protocol, metadata, setTransactions, sizeInBytes, numOfFiles, numOfMetadata,
      numOfProtocol, numOfRemoves, numOfSetTransactions) = {
    State(null, null, null, 0, 0, 0, 0, 0, 0)
  }
}

object SnapshotLike {

  /** Canonicalize the paths for Actions */
  def canonicalizePath(path: String, hadoopConf: Configuration): String = {
    val hadoopPath = new Path(new URI(path))
    if (hadoopPath.isAbsoluteAndSchemeAuthorityNull) {
      val fs = FileSystem.get(hadoopConf)
      fs.makeQualified(hadoopPath).toUri.toString
    } else {
      // return untouched if it is a relative path or is already fully qualified
      hadoopPath.toUri.toString
    }
  }

  case class State(
    protocol: Protocol,
    metadata: Metadata,
    setTransactions: Seq[SetTransaction],
    sizeInBytes: Long,
    numOfFiles: Long,
    numOfMetadata: Long,
    numOfProtocol: Long,
    numOfRemoves: Long,
    numOfSetTransactions: Long)
}

