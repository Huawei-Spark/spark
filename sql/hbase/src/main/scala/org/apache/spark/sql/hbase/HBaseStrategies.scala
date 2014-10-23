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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.{PhysicalOperation, QueryPlanner}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hbase.execution.HBaseSQLTableScan

/**
 * HBaseStrategies
 * Created by sboesch on 8/22/14.
 */
private[hbase] trait HBaseStrategies extends QueryPlanner[SparkPlan] {
  self: SQLContext#SparkPlanner =>

  val hbaseContext: HBaseSQLContext


  /**
   * Retrieves data using a HBaseTableScan.  Partition pruning predicates are also detected and
   * applied.
   */
  object HBaseTableScans extends Strategy {
    // YZ: to be revisited!
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case PhysicalOperation(projectList, inPredicates, relation: HBaseRelation) =>

        // Filter out all predicates that only deal with partition keys
        val partitionsKeys = AttributeSet(relation.partitionKeys)
        val (rowKeyPredicates, otherPredicates) = inPredicates.partition {
          _.references.subsetOf(partitionsKeys)
        }

        // TODO: Ensure the outputs from the relation match the expected columns of the query

        val predAttributes = AttributeSet(inPredicates.flatMap(_.references))
        val projectSet = AttributeSet(projectList.flatMap(_.references))

        val attributes = projectSet ++ predAttributes

        val rowPrefixPredicates = relation.getRowPrefixPredicates(rowKeyPredicates)

        def projectionToHBaseColumn(expr: NamedExpression,
                                    hbaseRelation: HBaseRelation): ColumnName = {
          hbaseRelation.catalogTable.allColumns.findBySqlName(expr.name).map(_.toColumnName).get
        }

        val rowKeyPreds: Seq[Expression] = if (!rowPrefixPredicates.isEmpty) {
          Seq(rowPrefixPredicates.reduceLeft(And))
        } else {
          Nil
        }

        val scanBuilder: (Seq[Attribute] => SparkPlan) = HBaseSQLTableScan(
          _,   // TODO: this first parameter is not used but can not compile without it
          attributes.map {
            _.toAttribute
          }.toSeq,
          relation,
          projectList,
          otherPredicates,
          rowKeyPreds,
          rowKeyPreds,
          None // coprocSubPlan
        )(hbaseContext)

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
      case logical.BulkLoadIntoTable(table: HBaseRelation, path) =>
        execution.BulkLoadIntoTable(table: HBaseRelation, path)

      case logical.CreateHBaseTablePlan(tableName, nameSpace, hbaseTableName, keyCols, nonKeyCols) =>
        // todo:issues here, should transfer to physical plan
        Seq(CreateHBaseTableCommand(tableName, nameSpace, hbaseTableName, keyCols, nonKeyCols)
          (hbaseContext))

      case logical.InsertIntoTable(table: HBaseRelation, partition, child) =>
        execution.InsertIntoHBaseTable(table, planLater(child) )(hbaseContext) :: Nil
      case logical.DropTablePlan(tableName) => Seq(DropHbaseTableCommand(tableName)(hbaseContext))
      case _ => Nil
    }
  }
}
