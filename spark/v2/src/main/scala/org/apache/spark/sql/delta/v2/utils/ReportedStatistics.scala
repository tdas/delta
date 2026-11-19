/*
 * Copyright (2026) The Delta Lake Project Authors.
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
package org.apache.spark.sql.delta.v2.utils

import java.util.{Map => JMap, OptionalLong}

import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.read.Statistics
import org.apache.spark.sql.connector.read.colstats.ColumnStatistics

/**
 * Connector [[Statistics]] that report a fixed set of precomputed values.
 *
 * Equality is reference identity, as for any [[Statistics]] that does not override it: two
 * instances holding equal values are not equal. Use a dedicated implementation where callers
 * need value equality.
 */
class ReportedStatistics(
    reportedSizeInBytes: OptionalLong,
    reportedNumRows: OptionalLong,
    reportedColumnStats: JMap[NamedReference, ColumnStatistics]
) extends Statistics {

  override def sizeInBytes(): OptionalLong = reportedSizeInBytes

  override def numRows(): OptionalLong = reportedNumRows

  override def columnStats(): JMap[NamedReference, ColumnStatistics] = reportedColumnStats
}
