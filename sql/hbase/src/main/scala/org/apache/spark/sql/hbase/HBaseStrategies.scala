/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hbase

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.{PhysicalOperation, QueryPlanner}
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoTable, LogicalPlan}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.hbase.execution._

private[hbase] trait HBaseStrategies extends QueryPlanner[SparkPlan] {
  self: SQLContext#SparkPlanner =>

  val hbaseSQLContext: HBaseSQLContext

  /**
   * Retrieves data using a HBaseTableScan.  Partition pruning predicates are also detected and
   * applied.
   */
  object HBaseTableScans extends Strategy {
    // YZ: to be revisited!
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case PhysicalOperation(projectList, inPredicates, relation: HBaseRelation) =>

        // Filter out all predicates that only deal with partition keys
        // val partitionsKeys = AttributeSet(relation.partitionKeys)
        // val (rowKeyPredicates, otherPredicates) = inPredicates.partition {
        //  _.references.subsetOf(partitionsKeys)
        //}

        // TODO: Ensure the outputs from the relation match the expected columns of the query

        /*
        val predAttributes = AttributeSet(inPredicates.flatMap(_.references))
        val projectSet = AttributeSet(projectList.flatMap(_.references))
        val attributes = projectSet ++ predAttributes

        val rowPrefixPredicates = relation.getRowPrefixPredicates(rowKeyPredicates)

        val rowKeyPreds: Seq[Expression] = if (!rowPrefixPredicates.isEmpty) {
          Seq(rowPrefixPredicates.reduceLeft(And))
        } else {
          Nil
        }
        */

        // TODO: add pushdowns
        val filterPred = inPredicates.reduceLeftOption(And)
        val scanBuilder: (Seq[Attribute] => SparkPlan) = HBaseSQLTableScan(
          relation,
          _,
          None, // row key predicate
          None, // value predicate
          filterPred, // partition predicate
          None // coprocSubPlan
        )(hbaseSQLContext)

        pruneFilterProject(
          projectList,
          inPredicates,
          identity[Seq[Expression]], // removeRowKeyPredicates,
          scanBuilder) :: Nil

      case _ =>
        Nil
    }
  }

  object HBaseOperations extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case logical.CreateHBaseTablePlan(
      tableName, nameSpace, hbaseTableName, colsSeq, keyCols, nonKeyCols) =>
        Seq(execution.CreateHBaseTableCommand(
          tableName, nameSpace, hbaseTableName, colsSeq, keyCols, nonKeyCols)
          (hbaseSQLContext))
      case logical.BulkLoadPlan(path, table: HBaseRelation, isLocal, delimiter) =>
        execution.BulkLoadIntoTable(path, table, isLocal, delimiter)(hbaseSQLContext) :: Nil
      case InsertIntoTable(table: HBaseRelation, partition, child, _) =>
        new InsertIntoHBaseTable(table, planLater(child))(hbaseSQLContext) :: Nil
      case logical.AlterDropColPlan(tableName: HBaseRelation, colName) =>
        Seq(AlterDropColCommand(tableName, colName)
          (hbaseSQLContext))
      case logical.AlterAddColPlan(tableName: HBaseRelation, colName, colType, colFamily, colQualifier) =>
        Seq(AlterAddColCommand(tableName, colName, colType, colFamily, colQualifier)
          (hbaseSQLContext))
      case logical.DropTablePlan(tableName: HBaseRelation) =>
        Seq(DropHbaseTableCommand(tableName)
          (hbaseSQLContext))
      case _ => Nil
    }
  }

}
