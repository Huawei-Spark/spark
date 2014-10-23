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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution._
import org.apache.spark.sql.hbase.HBaseCatalog._

/**
 * An instance of the Spark SQL execution engine that integrates with data stored in Hive.
 * Configuration for Hive is read from hive-site.xml on the classpath.
 */
class HBaseSQLContext(@transient val sc: SparkContext)
  extends SQLContext(sc) with Serializable {
  self =>
  // Todo: add analyzer (add a rule from LoadDataIntoTable to BulkLoadIntoTable)
  override protected[sql] lazy val catalog: HBaseCatalog = new HBaseCatalog(this)

  @transient val hBasePlanner = new SparkPlanner with HBaseStrategies {

    SparkPlan.currentContext.set(self)

    override val strategies: Seq[Strategy] = Seq(
      CommandStrategy(self),
      TakeOrdered,
      InMemoryScans,
      HBaseTableScans,
      HashAggregation,
      LeftSemiJoin,
      HashJoin,
      BasicOperators,
      CartesianProduct,
      BroadcastNestedLoopJoin,
      HBaseOperations
    )
  }

  @transient
  override protected[sql] val planner = hBasePlanner

  // TODO: YZ: removed and use the one in SQLConf
  override private[spark] val dialect: String = "hbaseql"

  override protected[sql] def executePlan(plan: LogicalPlan): this.QueryExecution =
    new this.QueryExecution {
      val logical = plan
    }

  /** Extends QueryExecution with HBase specific features. */
  protected[sql] abstract class QueryExecution extends super.QueryExecution {
  }

  @transient
  override protected[sql] val parser = new HBaseSQLParser

  override def parseSql(sql: String): LogicalPlan = parser(sql)

  override def sql(sqlText: String): SchemaRDD = {
    if (dialect == "sql") {
      sys.error(s"SQL dialect in HBase context")
    } else if (dialect == "hbaseql") {
      new SchemaRDD(this, parser(sqlText))
    } else {
      sys.error(s"Unsupported SQL dialect: $dialect.  Try 'sql' or 'hbaseql'")
    }
  }
}
