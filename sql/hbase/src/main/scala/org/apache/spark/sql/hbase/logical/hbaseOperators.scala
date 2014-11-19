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
package org.apache.spark.sql.hbase.logical

import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode, Command}

case class CreateHBaseTablePlan(tableName: String,
                                nameSpace: String,
                                hbaseTable: String,
                                colsSeq: Seq[String],
                                keyCols: Seq[(String, String)],
                                nonKeyCols: Seq[(String, String, String, String)]) extends Command

case class DropTablePlan(tableName: LogicalPlan) extends Command

case class AlterDropColPlan(tableName: LogicalPlan, colName: String) extends Command

case class AlterAddColPlan(tableName: LogicalPlan,
                           colName: String,
                           colType: String,
                           colFamily: String,
                           colQualifier: String) extends Command

/**
 * Logical plan for Bulkload
 * @param path input data file path
 * @param child target relation
 * @param isLocal using HDFS or local file
 * @param delimiter character in terminated by
 */
case class BulkLoadPlan(path: String, child: LogicalPlan,
                             isLocal: Boolean, delimiter: Option[String])
  extends UnaryNode {

  override def output = Nil

  override def toString = s"LogicalPlan: LoadDataIntoTable(LOAD $path INTO $child)"
}
