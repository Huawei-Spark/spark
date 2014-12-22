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
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.types._
import org.scalatest.{BeforeAndAfterAll, FunSuite}

//@Ignore
class CriticalPointsTestSuite extends FunSuite with BeforeAndAfterAll with Logging {
  var sparkConf: SparkConf = _
  var sparkContext: SparkContext = _
  var hbaseContext: HBaseSQLContext = _
  var configuration: Configuration = _
  var catalog: HBaseCatalog = _

  override def beforeAll() = {
    sparkConf = new SparkConf().setAppName("Catalog Test").setMaster("local[4]")
    sparkContext = new SparkContext(sparkConf)
    hbaseContext = new HBaseSQLContext(sparkContext)
    catalog = new HBaseCatalog(hbaseContext)
    configuration = HBaseConfiguration.create()
  }

  test("Generate CP Ranges") {
    val namespace = "testNamespace"
    val tableName = "testTable"
    val hbaseTableName = "hbaseTable"
    val family1 = "family1"
    val family2 = "family2"

    var allColumns = List[AbstractColumn]()
    allColumns = allColumns :+ KeyColumn("column1", IntegerType, 0)
    allColumns = allColumns :+ KeyColumn("column2", StringType, 1)
    allColumns = allColumns :+ NonKeyColumn("column4", FloatType, family2, "qualifier2")
    allColumns = allColumns :+ NonKeyColumn("column3", BooleanType, family1, "qualifier1")

    val lll = AttributeReference("column1", IntegerType)(ExprId(0L), Seq("testTable"))
    val llr = Literal(1023, IntegerType)
    val ll = GreaterThan(lll, llr)

    val lrl = AttributeReference("column1", IntegerType)(ExprId(0L), Seq("testTable"))
    val lrr = Literal(1025, IntegerType)
    val lr = LessThan(lrl, lrr)

    val l = And(ll, lr)

    val rll = AttributeReference("column1", IntegerType)(ExprId(0L), Seq("testTable"))
    val rlr = Literal(2048, IntegerType)
    val rl = GreaterThanOrEqual(rll, rlr)

    val rrl = AttributeReference("column1", IntegerType)(ExprId(0L), Seq("testTable"))
    val rrr = Literal(512, IntegerType)
    val rr = LessThanOrEqual(rrl, rrr)

    val r = Or(rl, rr)

    val mid = Or(l, r)
    val pred = Some(mid)

    val relation = HBaseRelation(tableName, namespace, hbaseTableName, allColumns)(hbaseContext)
    RangeCriticalPoint.generateCriticalPointRanges(relation, pred, 0)
  }
}
