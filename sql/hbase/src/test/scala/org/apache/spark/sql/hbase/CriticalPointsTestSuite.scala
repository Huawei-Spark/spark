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

import scala.collection.mutable.ArrayBuffer

//@Ignore
class CriticalPointsTestSuite extends FunSuite with BeforeAndAfterAll with Logging {
  var sparkConf: SparkConf = _
  var sparkContext: SparkContext = _
  var hbaseContext: HBaseSQLContext = _
  var configuration: Configuration = _
  var catalog: HBaseCatalog = _

  val namespace = "testNamespace"
  val tableName = "testTable"
  val hbaseTableName = "ht"
  val family1 = "family1"
  val family2 = "family2"

  override def beforeAll() = {
    sparkConf = new SparkConf().setAppName("Catalog Test").setMaster("local[4]")
    sparkContext = new SparkContext(sparkConf)
    hbaseContext = new HBaseSQLContext(sparkContext)
    catalog = new HBaseCatalog(hbaseContext)
    configuration = HBaseConfiguration.create()
  }

  test("Generate CP Ranges 0") {
    var allColumns = List[AbstractColumn]()
    allColumns = allColumns :+ KeyColumn("column1", IntegerType, 0)
    allColumns = allColumns :+ NonKeyColumn("column2", BooleanType, family1, "qualifier1")

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
    val rr = EqualTo(rrl, rrr)

    val r = Or(rl, rr)

    val mid = Or(l, r)
    val pred = Some(mid)

    val relation = HBaseRelation(tableName, namespace, hbaseTableName, allColumns)(hbaseContext)
    val result = RangeCriticalPoint.generateCriticalPointRanges(relation, pred)

    assert(result.size == 3)
    assert(result(0).start.get == 512 && result(0).startInclusive
      && result(0).end.get == 512 && result(0).endInclusive)
    assert(result(1).start.get == 1024 && result(1).startInclusive
      && result(1).end.get == 1024 && result(1).endInclusive)
    assert(result(2).start.get == 2048 && result(2).startInclusive
      && result(2).end == None && !result(2).endInclusive)
  }

  test("Generate CP Ranges 1") {
    var allColumns = List[AbstractColumn]()
    allColumns = allColumns :+ KeyColumn("column1", LongType, 0)
    allColumns = allColumns :+ NonKeyColumn("column2", BooleanType, family1, "qualifier1")

    val relation = HBaseRelation(tableName, namespace, hbaseTableName, allColumns)(hbaseContext)

    val lll = relation.output.find(_.name == "column1").get
    val llr = Literal(1023L, LongType)
    val ll = GreaterThan(lll, llr)

    val lrl = lll
    val lrr = Literal(1024L, LongType)
    val lr = LessThanOrEqual(lrl, lrr)

    val l_0 = And(ll, lr)
    val l = Not(l_0)

    val rll = lll
    val rlr = Literal(512L, LongType)
    val rl = LessThanOrEqual(rll, rlr)

    val r = Not(rl)

    val mid = And(l, r)
    val pred = Some(mid)

    val result = RangeCriticalPoint.generateCriticalPointRanges(relation, pred)

    assert(result.size == 2)
    assert(result(0).start.get == 513L && result(0).startInclusive
      && result(0).end.get == 1023L && result(0).endInclusive)
    assert(result(1).start.get == 1025L && result(1).startInclusive
      && result(1).end == None && !result(1).endInclusive)
  }

  test("Generate CP Ranges 2") {
    var allColumns = List[AbstractColumn]()
    allColumns = allColumns :+ KeyColumn("column1", StringType, 0)
    allColumns = allColumns :+ NonKeyColumn("column2", BooleanType, family1, "qualifier1")

    val relation = HBaseRelation(tableName, namespace, hbaseTableName, allColumns)(hbaseContext)

    val lll = relation.output.find(_.name == "column1").get
    val llr = Literal("aaa", StringType)
    val ll = EqualTo(lll, llr)

    val lrl = lll
    val lrr = Literal("bbb", StringType)
    val lr = EqualTo(lrl, lrr)

    val l = Or(ll, lr)

    val rl = lll
    val rr = Literal("abc", StringType)
    val r = LessThanOrEqual(rl, rr)

    val mid = And(l, r)
    val pred = Some(mid)

    val result = RangeCriticalPoint.generateCriticalPointRanges(relation, pred)

    assert(result.size == 1)
    assert(result(0).start.get == "aaa" && result(0).startInclusive
      && result(0).end.get == "aaa" && result(0).endInclusive)
  }

  test("Generate CP Ranges for Multi-Dimension 0") {
    var allColumns = List[AbstractColumn]()
    allColumns = allColumns :+ KeyColumn("column1", StringType, 0)
    allColumns = allColumns :+ KeyColumn("column2", IntegerType, 1)
    allColumns = allColumns :+ KeyColumn("column3", ShortType, 2)
    allColumns = allColumns :+ NonKeyColumn("column4", FloatType, family2, "qualifier2")
    allColumns = allColumns :+ NonKeyColumn("column5", BooleanType, family1, "qualifier1")

    val relation = HBaseRelation(tableName, namespace, hbaseTableName, allColumns)(hbaseContext)

    val lll = relation.output.find(_.name == "column3").get
    val llr = Literal(8.toShort, ShortType)
    val ll = GreaterThan(lll, llr)

    val lrl = relation.output.find(_.name == "column2").get
    val lrr = Literal(2048, IntegerType)
    val lr = EqualTo(lrl, lrr)

    val l = And(ll, lr)

    val rll = relation.output.find(_.name == "column1").get
    val rlr = Literal("abc", StringType)
    val rl = EqualTo(rll, rlr)

    val rrl = rll
    val rrr = Literal("cba", StringType)
    val rr = EqualTo(rrl, rrr)

    val r = Or(rl, rr)

    val mid = And(l, r)
    val pred = Some(mid)

    val result = RangeCriticalPoint.generateCriticalPointRanges(relation, pred)

    val expandedCPRs: Seq[MDCriticalPointRange[_]] =
      result.flatMap(_.flatten(new ArrayBuffer[(Any, NativeType)](relation.dimSize)))

    assert(result.size == 2)
    assert(expandedCPRs.size == 2)
    assert(expandedCPRs(0).prefix.size == 2)
    assert(expandedCPRs(1).prefix.size == 2)
  }

  test("Generate CP Ranges for Multi-Dimension 1") {
    var allColumns = List[AbstractColumn]()
    allColumns = allColumns :+ KeyColumn("column1", StringType, 0)
    allColumns = allColumns :+ KeyColumn("column2", IntegerType, 1)
    allColumns = allColumns :+ NonKeyColumn("column4", FloatType, family2, "qualifier2")
    allColumns = allColumns :+ NonKeyColumn("column5", BooleanType, family1, "qualifier1")

    val relation = HBaseRelation(tableName, namespace, hbaseTableName, allColumns)(hbaseContext)

    val lll = relation.output.find(_.name == "column2").get
    val llr = Literal(8, IntegerType)
    val ll = EqualTo(lll, llr)

    val lrl = lll
    val lrr = Literal(2048, IntegerType)
    val lr = EqualTo(lrl, lrr)

    val l = Or(ll, lr)

    val rll = relation.output.find(_.name == "column1").get
    val rlr = Literal("abc", StringType)
    val rl = EqualTo(rll, rlr)

    val rrl = rll
    val rrr = Literal("cba", StringType)
    val rr = EqualTo(rrl, rrr)

    val r = Or(rl, rr)

    val mid = And(l, r)
    val pred = Some(mid)

    val result = RangeCriticalPoint.generateCriticalPointRanges(relation, pred)

    assert(result.size == 2)
    val expandedCPRs: Seq[MDCriticalPointRange[_]] =
      result.flatMap(_.flatten(new ArrayBuffer[(Any, NativeType)](relation.dimSize)))

    assert(expandedCPRs.size == 4)
  }

  test("Get partitions 0") {
    var allColumns = List[AbstractColumn]()
    allColumns = allColumns :+ KeyColumn("column1", IntegerType, 0)
    allColumns = allColumns :+ KeyColumn("column2", IntegerType, 1)
    allColumns = allColumns :+ NonKeyColumn("column3", FloatType, family2, "qualifier2")
    allColumns = allColumns :+ NonKeyColumn("column4", BooleanType, family1, "qualifier1")

    val relation = HBaseRelation(tableName, namespace, hbaseTableName, allColumns)(hbaseContext)

    val lll = relation.output.find(_.name == "column2").get
    val llr = Literal(8, IntegerType)
    val ll = EqualTo(lll, llr)

    val lrl = lll
    val lrr = Literal(2048, IntegerType)
    val lr = EqualTo(lrl, lrr)

    val l = Or(ll, lr)

    val rll = relation.output.find(_.name == "column1").get
    val rlr = Literal(32, IntegerType)
    val rl = EqualTo(rll, rlr)

    val rrl = rll
    val rrr = Literal(1024, IntegerType)
    val rr = EqualTo(rrl, rrr)

    val r = Or(rl, rr)

    val mid = And(l, r)
    val pred = Some(mid)

    val result = RangeCriticalPoint.generateCriticalPointRanges(relation, pred)

    assert(result.size == 2)

    val expandedCPRs: Seq[MDCriticalPointRange[_]] =
      result.flatMap(_.flatten(new ArrayBuffer[(Any, NativeType)](relation.dimSize)))

    assert(expandedCPRs.size == 4)

    val rowkey0 = HBaseKVHelper.encodingRawKeyColumns(
      Seq((BytesUtils.create(IntegerType).toBytes(0), IntegerType)
        , (BytesUtils.create(IntegerType).toBytes(7), IntegerType))
    )

    val rowkey1 = HBaseKVHelper.encodingRawKeyColumns(
      Seq((BytesUtils.create(IntegerType).toBytes(1), IntegerType)
        , (BytesUtils.create(IntegerType).toBytes(2), IntegerType))
    )

    val rowkey2 = HBaseKVHelper.encodingRawKeyColumns(
      Seq((BytesUtils.create(IntegerType).toBytes(2), IntegerType)
        , (BytesUtils.create(IntegerType).toBytes(2), IntegerType))
    )

    val rowkey3 = HBaseKVHelper.encodingRawKeyColumns(
      Seq((BytesUtils.create(IntegerType).toBytes(3), IntegerType)
        , (BytesUtils.create(IntegerType).toBytes(4), IntegerType))
    )

    val rowkey4 = HBaseKVHelper.encodingRawKeyColumns(
      Seq((BytesUtils.create(IntegerType).toBytes(3), IntegerType)
        , (BytesUtils.create(IntegerType).toBytes(6), IntegerType))
    )

    val p1 = new HBasePartition(0, 0, None, Some(rowkey0), relation = relation)
    val p2 = new HBasePartition(1, 1, Some(rowkey0), Some(rowkey1), relation = relation)
    val p3 = new HBasePartition(2, 2, Some(rowkey1), Some(rowkey2), relation = relation)
    val p4 = new HBasePartition(3, 3, Some(rowkey2), Some(rowkey3), relation = relation)
    val p5 = new HBasePartition(4, 4, Some(rowkey3), Some(rowkey4), relation = relation)
    val p6 = new HBasePartition(5, 5, Some(rowkey4), None, relation = relation)

    relation.partitions = Seq(p1, p2, p3, p4, p5, p6)

    RangeCriticalPoint.prunePartitions(
      expandedCPRs, pred, relation.partitions, relation.partitionKeys.size)
  }

  test("Get partitions 1") {
    var allColumns = List[AbstractColumn]()
    allColumns = allColumns :+ KeyColumn("column1", IntegerType, 0)
    allColumns = allColumns :+ KeyColumn("column2", IntegerType, 1)
    allColumns = allColumns :+ NonKeyColumn("column3", FloatType, family2, "qualifier2")
    allColumns = allColumns :+ NonKeyColumn("column4", BooleanType, family1, "qualifier1")

    val relation = HBaseRelation(tableName, namespace, hbaseTableName, allColumns)(hbaseContext)

    val lll = relation.output.find(_.name == "column2").get
    val llr = Literal(8, IntegerType)
    val ll = EqualTo(lll, llr)

    val lrl = lll
    val lrr = Literal(2048, IntegerType)
    val lr = EqualTo(lrl, lrr)

    val l = Or(ll, lr)

    val rll = relation.output.find(_.name == "column1").get
    val rlr = Literal(32, IntegerType)
    val rl = EqualTo(rll, rlr)

    val rrl = rll
    val rrr = Literal(1024, IntegerType)
    val rr = EqualTo(rrl, rrr)

    val r = Or(rl, rr)

    val mid = And(l, r)
    val pred = Some(mid)

    val result = RangeCriticalPoint.generateCriticalPointRanges(relation, pred)

    assert(result.size == 2)

    val expandedCPRs: Seq[MDCriticalPointRange[_]] =
      result.flatMap(_.flatten(new ArrayBuffer[(Any, NativeType)](relation.dimSize)))

    assert(expandedCPRs.size == 4)

    val rowkey0 = HBaseKVHelper.encodingRawKeyColumns(
      Seq((BytesUtils.create(IntegerType).toBytes(1), IntegerType)
        , (BytesUtils.create(IntegerType).toBytes(1), IntegerType))
    )

    val rowkey1 = HBaseKVHelper.encodingRawKeyColumns(
      Seq((BytesUtils.create(IntegerType).toBytes(8), IntegerType)
        , (BytesUtils.create(IntegerType).toBytes(2), IntegerType))
    )

    val rowkey2 = HBaseKVHelper.encodingRawKeyColumns(
      Seq((BytesUtils.create(IntegerType).toBytes(32), IntegerType)
        , (BytesUtils.create(IntegerType).toBytes(16), IntegerType))
    )

    val rowkey3 = HBaseKVHelper.encodingRawKeyColumns(
      Seq((BytesUtils.create(IntegerType).toBytes(64), IntegerType)
        , (BytesUtils.create(IntegerType).toBytes(128), IntegerType))
    )

    val rowkey4 = HBaseKVHelper.encodingRawKeyColumns(
      Seq((BytesUtils.create(IntegerType).toBytes(1024), IntegerType)
        , (BytesUtils.create(IntegerType).toBytes(256), IntegerType))
    )

    val p1 = new HBasePartition(0, 0, None, Some(rowkey0), relation = relation)
    val p2 = new HBasePartition(1, 1, Some(rowkey0), Some(rowkey1), relation = relation)
    val p3 = new HBasePartition(2, 2, Some(rowkey1), Some(rowkey2), relation = relation)
    val p4 = new HBasePartition(3, 3, Some(rowkey2), Some(rowkey3), relation = relation)
    val p5 = new HBasePartition(4, 4, Some(rowkey3), Some(rowkey4), relation = relation)
    val p6 = new HBasePartition(5, 5, Some(rowkey4), None, relation = relation)

    relation.partitions = Seq(p1, p2, p3, p4, p5, p6)

    RangeCriticalPoint.prunePartitions(
      expandedCPRs, pred, relation.partitions, relation.partitionKeys.size)
  }
}
