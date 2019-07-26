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

package io.delta.execution

import scala.collection.Map

import org.apache.spark.sql.delta.PreprocessTableUpdate
import org.apache.spark.sql.delta.{DeltaErrors, DeltaFullTable, DeltaLog}
import org.apache.spark.sql.delta.commands.{DeleteCommand, VacuumCommand}
import org.apache.spark.sql.delta.util.AnalysisHelper
import io.delta.DeltaTable

import org.apache.spark.sql.{functions, Column, DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Expression, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical._

/**
 * Interface to provide the actual implementations of DeltaTable operations.
 */
trait DeltaTableOperations extends AnalysisHelper { self: DeltaTable =>

  protected def executeDelete(condition: Option[Expression]): Unit = {
    val delete = Delete(self.toDF.queryExecution.analyzed, condition)

    // current DELETE does not support subquery,
    // and the reason why perform checking here is that
    // we want to have more meaningful exception messages,
    // instead of having some random msg generated by executePlan().
    subqueryNotSupportedCheck(condition, "DELETE")

    val qe = sparkSession.sessionState.executePlan(delete)
    val resolvedDelete = qe.analyzed.asInstanceOf[Delete]
    val deleteCommand = DeleteCommand(resolvedDelete)
    deleteCommand.run(sparkSession)
  }

  protected def toStrColumnMap(map: Map[String, String]): Map[String, Column] = {
    map.toSeq.map { case (k, v) => k -> functions.expr(v) }.toMap
  }

  protected def makeUpdateTable(
      target: DeltaTable,
      onCondition: Option[Column],
      setColumns: Seq[(String, Column)]): UpdateTable = {
    val updateColumns = setColumns.map { x => UnresolvedAttribute.quotedString(x._1) }
    val updateExpressions = setColumns.map{ x => x._2.expr }
    val condition = onCondition.map {_.expr}
    UpdateTable(
      target.toDF.queryExecution.analyzed, updateColumns, updateExpressions, condition)
  }

  protected def executeUpdate(
      set: Map[String, Column],
      condition: Option[Column]): Unit = {
    val setColumns = set.map{ case (col, expr) => (col, expr) }.toSeq

    // Current UPDATE does not support subquery,
    // and the reason why perform checking here is that
    // we want to have more meaningful exception messages,
    // instead of having some random msg generated by executePlan().
    subqueryNotSupportedCheck(condition.map {_.expr}, "UPDATE")

    val update = makeUpdateTable(self, condition, setColumns)
    val resolvedUpdate =
      UpdateTable.resolveReferences(update, tryResolveReferences(sparkSession)(_, update))
    val updateCommand = PreprocessTableUpdate(sparkSession.sessionState.conf)(resolvedUpdate)
    updateCommand.run(sparkSession)
  }

  private def subqueryNotSupportedCheck(condition: Option[Expression], op: String): Unit = {
    condition match {
      case Some(cond) if SubqueryExpression.hasSubquery(cond) =>
        throw DeltaErrors.subqueryNotSupportedException(op, cond)
      case _ =>
    }
  }

  protected def executeVacuum(
      deltaLog: DeltaLog,
      retentionHours: Option[Double]): DataFrame = {
    val sparkSession = self.toDF.sparkSession
    VacuumCommand.gc(sparkSession, deltaLog, false, retentionHours)
    sparkSession.emptyDataFrame
  }

  protected lazy val deltaLog = (EliminateSubqueryAliases(self.toDF.queryExecution.analyzed) match {
    case DeltaFullTable(tahoeFileIndex) =>
      tahoeFileIndex
  }).deltaLog

  protected lazy val sparkSession: SparkSession = self.toDF.sparkSession
}
