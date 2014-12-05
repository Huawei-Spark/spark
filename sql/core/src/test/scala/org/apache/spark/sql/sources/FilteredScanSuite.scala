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

import scala.language.existentials

import org.apache.spark.sql._

class FilteredScanSource extends RelationProvider {
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: Option[StructType]): BaseRelation = {
    SimpleFilteredScan(
      parameters("from").toInt,
      parameters("to").toInt,
      schema: Option[StructType])(sqlContext)
  }
}

case class SimpleFilteredScan(
    from: Int,
    to: Int,
    _schema: Option[StructType])(@transient val sqlContext: SQLContext)
  extends PrunedFilteredScan {

  override def schema = _schema.getOrElse(
    StructType(
      StructField("a", IntegerType, nullable = false) ::
      StructField("b", IntegerType, nullable = false) :: Nil)
  )

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]) = {
    val rowBuilders = requiredColumns.map {
      case "a" => (i: Int) => Seq(i)
      case "b" => (i: Int) => Seq(i * 2)
    }

    FiltersPushed.list = filters

    val filterFunctions = filters.collect {
      case EqualTo("a", v) => (a: Int) => a == v
      case LessThan("a", v: Int) => (a: Int) => a < v
      case LessThanOrEqual("a", v: Int) => (a: Int) => a <= v
      case GreaterThan("a", v: Int) => (a: Int) => a > v
      case GreaterThanOrEqual("a", v: Int) => (a: Int) => a >= v
      case In("a", values) => (a: Int) => values.map(_.asInstanceOf[Int]).toSet.contains(a)
    }

    def eval(a: Int) = !filterFunctions.map(_(a)).contains(false)

    sqlContext.sparkContext.parallelize(from to to).filter(eval).map(i =>
      Row.fromSeq(rowBuilders.map(_(i)).reduceOption(_ ++ _).getOrElse(Seq.empty)))
  }
}

// A hack for better error messages when filter pushdown fails.
object FiltersPushed {
  var list: Seq[Filter] = Nil
}

class FilteredScanSuite extends DataSourceTest {

  import caseInsensisitiveContext._

  before {
    sql(
      """
        |CREATE TEMPORARY TABLE oneToTenFiltered
        |USING org.apache.spark.sql.sources.FilteredScanSource
        |OPTIONS (
        |  from '1',
        |  to '10'
        |)
      """.stripMargin)

    sql(
      """
        |CREATE TEMPORARY TABLE oneToTenFiltered_with_schema(a int, b int)
        |USING org.apache.spark.sql.sources.FilteredScanSource
        |OPTIONS (
        |  from '1',
        |  to '10'
        |)
      """.stripMargin)
  }
  Seq("oneToTenFiltered", "oneToTenFiltered_with_schema").foreach { table =>

    sqlTest(
      s"SELECT * FROM $table",
      (1 to 10).map(i => Row(i, i * 2)).toSeq)

    sqlTest(
      s"SELECT a, b FROM $table",
      (1 to 10).map(i => Row(i, i * 2)).toSeq)

    sqlTest(
      s"SELECT b, a FROM $table",
      (1 to 10).map(i => Row(i * 2, i)).toSeq)

    sqlTest(
      s"SELECT a FROM $table",
      (1 to 10).map(i => Row(i)).toSeq)

    sqlTest(
      s"SELECT b FROM $table",
      (1 to 10).map(i => Row(i * 2)).toSeq)

    sqlTest(
      s"SELECT a * 2 FROM $table",
      (1 to 10).map(i => Row(i * 2)).toSeq)

    sqlTest(
      s"SELECT A AS b FROM $table",
      (1 to 10).map(i => Row(i)).toSeq)

    sqlTest(
      s"SELECT x.b, y.a FROM $table x JOIN $table y ON x.a = y.b",
      (1 to 5).map(i => Row(i * 4, i)).toSeq)

    sqlTest(
      s"SELECT x.a, y.b FROM $table x JOIN $table y ON x.a = y.b",
      (2 to 10 by 2).map(i => Row(i, i)).toSeq)

    sqlTest(
      s"SELECT * FROM $table WHERE a = 1",
      Seq(1).map(i => Row(i, i * 2)).toSeq)

    sqlTest(
      s"SELECT * FROM $table WHERE a IN (1,3,5)",
      Seq(1,3,5).map(i => Row(i, i * 2)).toSeq)

    sqlTest(
      s"SELECT * FROM $table WHERE A = 1",
      Seq(1).map(i => Row(i, i * 2)).toSeq)

    sqlTest(
      s"SELECT * FROM $table WHERE b = 2",
      Seq(1).map(i => Row(i, i * 2)).toSeq)

    testPushDown(s"SELECT * FROM $table WHERE A = 1", 1)
    testPushDown(s"SELECT a FROM $table WHERE A = 1", 1)
    testPushDown(s"SELECT b FROM $table WHERE A = 1", 1)
    testPushDown(s"SELECT a, b FROM $table WHERE A = 1", 1)
    testPushDown(s"SELECT * FROM $table WHERE a = 1", 1)
    testPushDown(s"SELECT * FROM $table WHERE 1 = a", 1)

    testPushDown(s"SELECT * FROM $table WHERE a > 1", 9)
    testPushDown(s"SELECT * FROM $table WHERE a >= 2", 9)

    testPushDown(s"SELECT * FROM $table WHERE 1 < a", 9)
    testPushDown(s"SELECT * FROM $table WHERE 2 <= a", 9)

    testPushDown(s"SELECT * FROM $table WHERE 1 > a", 0)
    testPushDown(s"SELECT * FROM $table WHERE 2 >= a", 2)

    testPushDown(s"SELECT * FROM $table WHERE a < 1", 0)
    testPushDown(s"SELECT * FROM $table WHERE a <= 2", 2)

    testPushDown(s"SELECT * FROM $table WHERE a > 1 AND a < 10", 8)

    testPushDown(s"SELECT * FROM $table WHERE a IN (1,3,5)", 3)

    testPushDown(s"SELECT * FROM $table WHERE a = 20", 0)
    testPushDown(s"SELECT * FROM $table WHERE b = 1", 10)
  }

  def testPushDown(sqlString: String, expectedCount: Int): Unit = {
    test(s"PushDown Returns $expectedCount: $sqlString") {
      val queryExecution = sql(sqlString).queryExecution
      val rawPlan = queryExecution.executedPlan.collect {
        case p: execution.PhysicalRDD => p
      } match {
        case Seq(p) => p
        case _ => fail(s"More than one PhysicalRDD found\n$queryExecution")
      }
      val rawCount = rawPlan.execute().count()

      if (rawCount != expectedCount) {
        fail(
          s"Wrong # of results for pushed filter. Got $rawCount, Expected $expectedCount\n" +
            s"Filters pushed: ${FiltersPushed.list.mkString(",")}\n" +
            queryExecution)
      }
    }
  }
}

