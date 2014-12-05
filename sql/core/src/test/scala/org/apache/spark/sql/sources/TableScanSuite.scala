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

package org.apache.spark.sql.sources

import org.apache.spark.sql._

class DefaultSource extends SimpleScanSource

class SimpleScanSource extends RelationProvider {
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: Option[StructType]): BaseRelation = {
    SimpleScan(parameters("from").toInt, parameters("to").toInt, schema)(sqlContext)
  }
}

case class SimpleScan(
    from: Int,
    to: Int,
    _schema: Option[StructType])(@transient val sqlContext: SQLContext)
  extends TableScan {

  override def schema = _schema.getOrElse(
    StructType(StructField("i", IntegerType, nullable = false) :: Nil)
  )
  override def buildScan() = sqlContext.sparkContext.parallelize(from to to).map(Row(_))
}

class TableScanSuite extends DataSourceTest {
  import caseInsensisitiveContext._

  before {
    sql(
      """
        |CREATE TEMPORARY TABLE oneToTen
        |USING org.apache.spark.sql.sources.SimpleScanSource
        |OPTIONS (
        |  from '1',
        |  to '10'
        |)
      """.stripMargin)

    sql(
      """
        |CREATE TEMPORARY TABLE oneToTen_with_schema(i int)
        |USING org.apache.spark.sql.sources.SimpleScanSource
        |OPTIONS (
        |  from '1',
        |  to '10'
        |)
      """.stripMargin)
  }

  Seq("oneToTen", "oneToTen_with_schema").foreach { table =>
    sqlTest(
      s"SELECT * FROM $table",
      (1 to 10).map(Row(_)).toSeq)

    sqlTest(
      s"SELECT i FROM $table",
      (1 to 10).map(Row(_)).toSeq)

    sqlTest(
      s"SELECT i FROM $table WHERE i < 5",
      (1 to 4).map(Row(_)).toSeq)

    sqlTest(
      s"SELECT i * 2 FROM $table",
      (1 to 10).map(i => Row(i * 2)).toSeq)

    sqlTest(
      s"SELECT a.i, b.i FROM $table a JOIN $table b ON a.i = b.i + 1",
      (2 to 10).map(i => Row(i, i - 1)).toSeq)
  }


  Seq("oneToTen", "oneToTen_with_schema").foreach { table =>

    test(s"Caching $table") {
      // Cached Query Execution
      cacheTable(s"$table")
      assertCached(sql(s"SELECT * FROM $table"))
      checkAnswer(
        sql(s"SELECT * FROM $table"),
        (1 to 10).map(Row(_)).toSeq)

      assertCached(sql(s"SELECT i FROM $table"))
      checkAnswer(
        sql(s"SELECT i FROM $table"),
        (1 to 10).map(Row(_)).toSeq)

      assertCached(sql(s"SELECT i FROM $table WHERE i < 5"))
      checkAnswer(
        sql(s"SELECT i FROM $table WHERE i < 5"),
        (1 to 4).map(Row(_)).toSeq)

      assertCached(sql(s"SELECT i * 2 FROM $table"))
      checkAnswer(
        sql(s"SELECT i * 2 FROM $table"),
        (1 to 10).map(i => Row(i * 2)).toSeq)

      assertCached(sql(s"SELECT a.i, b.i FROM $table a JOIN $table b ON a.i = b.i + 1"), 2)
      checkAnswer(
        sql(s"SELECT a.i, b.i FROM $table a JOIN $table b ON a.i = b.i + 1"),
        (2 to 10).map(i => Row(i, i - 1)).toSeq)

      // Verify uncaching
      uncacheTable(s"$table")
      assertCached(sql(s"SELECT * FROM $table"), 0)
    }
  }

  test("defaultSource") {
    sql(
      """
        |CREATE TEMPORARY TABLE oneToTenDef
        |USING org.apache.spark.sql.sources
        |OPTIONS (
        |  from '1',
        |  to '10'
        |)
      """.stripMargin)

    checkAnswer(
      sql("SELECT * FROM oneToTenDef"),
      (1 to 10).map(Row(_)).toSeq)
  }
}
