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

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.hbase.logical.LoadDataIntoTable
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.apache.spark.{SparkContext, Logging}
import org.apache.spark.sql.catalyst.types.IntegerType
import org.apache.spark.sql.hbase.execution.BulkLoadIntoTable
import org.apache.hadoop.hbase.util.Bytes

class InsertValueSuite extends FunSuite with BeforeAndAfterAll with Logging{

  val sc = new SparkContext("local", "test")
  val hbc = new HBaseSQLContext(sc)

  ignore("insert values into table test") {

    val sql1 =
      s"""CREATE TABLE testinsert(col1 STRING, col2 STRING, col3 STRING)
          MAPPED BY (wf, KEYS=[col1], COLS=[col2=cf1.a, col3=cf1.b])"""
        .stripMargin

    val executeSql1 = hbc.executeSql(sql1)
    executeSql1.toRdd.collect().foreach(println)

    // insert data into table
    val sql2 = "INSERT INTO testinsert VALUES (value1, value2, value3)"

    val executeSql2 = hbc.executeSql(sql2)
    executeSql2.toRdd.collect().foreach(println)
    hbc.sql("select * from testinsert").collect().foreach(println)
  }

}
