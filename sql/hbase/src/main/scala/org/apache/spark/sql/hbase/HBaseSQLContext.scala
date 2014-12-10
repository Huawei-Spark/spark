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

import org.apache.hadoop.conf.Configuration

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.execution._

/**
 * An instance of the Spark SQL execution engine that integrates with data stored in Hive.
 * Configuration for Hive is read from hive-site.xml on the classpath.
 */
class HBaseSQLContext(@transient val sc: SparkContext,
                      val optConfiguration: Option[Configuration] = None)
  extends SQLContext(sc) {
  self =>

  override protected[sql] lazy val catalog: HBaseCatalog = new HBaseCatalog(this)

  // TODO: can we use SparkSQLParser directly instead of HBaseSparkSQLParser?
  @transient
  override protected[sql] val sqlParser = {
    val fallback = new HBaseSQLParser
    new HBaseSparkSQLParser(fallback(_))
  }

  override def sql(sqlText: String): SchemaRDD = {
    if (dialect == "sql") {
      new SchemaRDD(this, sqlParser(sqlText))
    } else {
      sys.error(s"Unsupported SQL dialect: $dialect.  Try 'sql' or 'hbaseql'")
    }
  }

  protected[sql] class HBasePlanner extends SparkPlanner with HBaseStrategies {
    val hbaseSQLContext = self
    SparkPlan.currentContext.set(self)

    // TODO: suggest to append our strategies to parent's strategies using ++, currently there
    // is compilation problem using super.strategies
    override val strategies: Seq[Strategy] = Seq(
      CommandStrategy(self),
      HBaseCommandStrategy(self),
      TakeOrdered,
      HashAggregation,
      LeftSemiJoin,
      HashJoin,
      InMemoryScans,
      HBaseTableScans,
      DataSink,
      BasicOperators,
      CartesianProduct,
      BroadcastNestedLoopJoin
    )
  }

  @transient
  override protected[sql] val planner = new HBasePlanner

}
