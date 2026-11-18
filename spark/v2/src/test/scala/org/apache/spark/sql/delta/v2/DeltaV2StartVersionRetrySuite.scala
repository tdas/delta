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

import java.util.Optional

import scala.collection.mutable.ArrayBuffer

import org.apache.logging.log4j.Level
import io.delta.kernel.exceptions.{
  CommitRangeNotFoundException,
  StartVersionNotFoundException
}

import org.apache.spark.SparkFunSuite

class DeltaV2StartVersionRetrySuite extends SparkFunSuite {
  private val tablePath = "/table"

  private def startVersionNotFound(
      startVersion: Long,
      earliestAvailableVersion: Option[Long]): StartVersionNotFoundException = {
    new StartVersionNotFoundException(
      tablePath,
      startVersion,
      earliestAvailableVersion
        .map(version => Optional.of(Long.box(version)))
        .getOrElse(Optional.empty[java.lang.Long]()))
  }

  test("returns the first successful fetch without retrying") {
    val fetchedVersions = ArrayBuffer.empty[Long]

    val result = DeltaV2StartVersionRetry.fetchWithStartVersionRetry(
      startVersion = 5L,
      tablePath,
      failOnDataLoss = false) { version =>
        fetchedVersions += version
        "result"
      }

    assert(result.contains("result"))
    assert(fetchedVersions === Seq(5L))
  }

  test("rethrows a missing start version when data loss is disallowed") {
    val error = startVersionNotFound(5L, Some(7L))

    val thrown = intercept[StartVersionNotFoundException] {
      DeltaV2StartVersionRetry.fetchWithStartVersionRetry(
        5L, tablePath, failOnDataLoss = true)(_ => throw error)
    }

    assert(thrown eq error)
  }

  test("retries from the earliest available version and logs a warning") {
    val fetchedVersions = ArrayBuffer.empty[Long]
    val logs = new LogAppender("missing start version retry")
    var result: Option[String] = None

    withLogAppender(logs, level = Some(Level.WARN)) {
      result = DeltaV2StartVersionRetry.fetchWithStartVersionRetry(
        5L, tablePath, failOnDataLoss = false) { version =>
        fetchedVersions += version
        if (fetchedVersions.size == 1) throw startVersionNotFound(version, Some(7L))
        "result"
      }
    }

    assert(result.contains("result"))
    assert(fetchedVersions === Seq(5L, 7L))
    assert(logs.loggingEvents.exists(_.getMessage.getFormattedMessage.contains(
      "Skipping to earliest available version 7")))
  }

  test("rethrows the final missing start version after the maximum attempts") {
    val errors = Seq(
      startVersionNotFound(5L, Some(7L)),
      startVersionNotFound(7L, Some(8L)),
      startVersionNotFound(8L, Some(9L)))
    var attempts = 0

    val thrown = intercept[StartVersionNotFoundException] {
      DeltaV2StartVersionRetry.fetchWithStartVersionRetry(
        5L, tablePath, failOnDataLoss = false) { _ =>
        val error = errors(attempts)
        attempts += 1
        throw error
      }
    }

    assert(attempts === 3)
    assert(thrown eq errors.last)
  }

  test("rethrows a missing start version without an earliest available version") {
    val error = startVersionNotFound(5L, None)

    val thrown = intercept[StartVersionNotFoundException] {
      DeltaV2StartVersionRetry.fetchWithStartVersionRetry(
        5L, tablePath, failOnDataLoss = false)(_ => throw error)
    }

    assert(thrown eq error)
  }

  test("returns None for an empty commit range") {
    val error = new CommitRangeNotFoundException(tablePath, 5L, Optional.of(Long.box(6L)))

    val result = DeltaV2StartVersionRetry.fetchWithStartVersionRetry(
      5L, tablePath, failOnDataLoss = false)(_ => throw error)

    assert(result.isEmpty)
  }

  test("propagates unrelated exceptions without retrying") {
    val error = new IllegalStateException("fetch failed")
    var attempts = 0

    val thrown = intercept[IllegalStateException] {
      DeltaV2StartVersionRetry.fetchWithStartVersionRetry(
        5L, tablePath, failOnDataLoss = false) { _ =>
        attempts += 1
        throw error
      }
    }

    assert(attempts === 1)
    assert(thrown eq error)
  }
}
