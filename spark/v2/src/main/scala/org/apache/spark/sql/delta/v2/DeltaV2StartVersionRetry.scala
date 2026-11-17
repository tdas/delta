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
package org.apache.spark.sql.delta.v2

import io.delta.kernel.exceptions.{
  CommitRangeNotFoundException,
  StartVersionNotFoundException
}

import org.apache.spark.SparkException

private[v2] object DeltaV2StartVersionRetry extends DeltaV2Logging {
  private val RetryLogKeys = org.apache.spark.sql.delta.logging.DeltaLogKeys

  /**
   * Attempt 1 uses the requested start version. Attempt 2 uses the earliest available version.
   * Attempt 3 guards against another log-retention prune before the second attempt starts.
   */
  private val MaxAttempts = 3

  /**
   * Fetches with up to three attempts, retrying a missing start version when data loss is allowed.
   * Returns None when the fetch throws [[CommitRangeNotFoundException]], indicating an empty range.
   */
  def fetchWithStartVersionRetry[T](
      startVersion: Long,
      tablePath: String,
      failOnDataLoss: Boolean)(
      fetch: Long => T): Option[T] = {
    var earliestVersionToFetch = startVersion
    var attempt = 1
    while (attempt <= MaxAttempts) {
      try {
        return Some(fetch(earliestVersionToFetch))
      } catch {
        case e: StartVersionNotFoundException =>
          if (failOnDataLoss || !e.getEarliestAvailableVersion.isPresent ||
              attempt >= MaxAttempts) {
            throw e
          }
          earliestVersionToFetch = e.getEarliestAvailableVersion.get()
          logWarning(log"Start version commit " +
            log"${MDC(RetryLogKeys.START_VERSION, startVersion)} no longer exists for table " +
            log"${MDC(RetryLogKeys.PATH, tablePath)} " +
            log"(attempt ${MDC(RetryLogKeys.ATTEMPT, attempt)}/" +
            log"${MDC(RetryLogKeys.NUM_ATTEMPT, MaxAttempts)}). Skipping to earliest available " +
            log"version ${MDC(RetryLogKeys.VERSION, earliestVersionToFetch)} because " +
            log"failOnDataLoss is false.")
        case _: CommitRangeNotFoundException =>
          return None
      }
      attempt += 1
    }
    throw SparkException.internalError("Unreachable start-version retry state")
  }
}
