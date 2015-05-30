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

package org.apache.spark.sql.hbase.execution

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.hbase._

private[hbase] case class AddCoprocessor(sqlContext: SQLContext) extends Rule[SparkPlan] {
  private lazy val catalog = sqlContext.asInstanceOf[HBaseSQLContext].catalog

  private def coprocessorIsAvailable(relation: HBaseRelation): Boolean = {
    catalog.deploySuccessfully.get && catalog.hasCoprocessor(relation.hbaseTableName)
  }

  private def generateNewSubplan(origPlan: SparkPlan, oldScan: HBaseSQLTableScan): SparkPlan = {
    var subplan: SparkPlan = origPlan

    // If any current directory of regionserver is not accessible,
    // we could not use codegen, or else it will lead to crashing the HBase regionserver!!!
    // For details, please read the comment in CheckDirEndPointImpl.
    val codegenEnabled = catalog.pwdIsAccessible && oldScan.codegenEnabled

    val newSubplan = subplan.transformUp {
      case subplanScan: HBaseSQLTableScan =>
        assert(subplanScan == oldScan, "Unexpected mismatch of scan")
        val rdd = new HBaseCoprocessorSQLReaderRDD(
          null, codegenEnabled, oldScan.output, null, sqlContext)
        HBaseSQLTableScan(oldScan.relation, oldScan.output, rdd)
    }

    val output = subplan.output

    // reduce project (network transfer) if projection list has duplicate
    if (output.distinct.size < output.size) {
      subplan = Project(output.distinct, subplan)
    }

    val oldRDD: HBaseSQLReaderRDD = oldScan.result.asInstanceOf[HBaseSQLReaderRDD]

    val newRDD = new HBasePostCoprocessorSQLReaderRDD(
      oldRDD.relation, codegenEnabled, oldRDD.output,
      oldRDD.filterPred, newSubplan, sqlContext)

    val newScan = new HBaseSQLTableScan(oldRDD.relation, subplan.output, newRDD)

    // add project spark plan if projection list has duplicate
    if (output.distinct.size < output.size) Project(output, newScan)
    else newScan
  }

  def apply(plan: SparkPlan): SparkPlan = {
    var createSubplan: Boolean = false
    var oldScan: HBaseSQLTableScan = null
    var path: List[SparkPlan] = List[SparkPlan]()
    plan match {
      // If the plan is tableScan directly, we don't need to use coprocessor
      case HBaseSQLTableScan(_,_,_) => plan
      case _ => {
        val result = plan.transformUp {
          // TODO: check if Join always follows Exchange
          // If subplan is needed then we need coprocessor plans for both children
          case binaryNode: BinaryNode =>
            createSubplan = false
            binaryNode
          case scan: HBaseSQLTableScan if coprocessorIsAvailable(scan.relation) =>
            createSubplan = true
            oldScan = scan
            scan

          // Since the following two plans using shuffledRDD,
          // we could not pass them to the coprocessor.
          // Thus, their child are used as the subplan for coprocessor processing.
          case exchange: Exchange if createSubplan=>
            createSubplan = false
            val newPlan = generateNewSubplan(exchange.child, oldScan)
            exchange.withNewChildren(Seq(newPlan))
          case limit: Limit if createSubplan=>
            createSubplan = false
            val newPlan = generateNewSubplan(limit.child, oldScan)
            limit.withNewChildren(Seq(newPlan))
        }
        // Use coprocessor even without shufffling
        if (createSubplan) generateNewSubplan(result, oldScan)
        else result
      }
    }
  }
}
