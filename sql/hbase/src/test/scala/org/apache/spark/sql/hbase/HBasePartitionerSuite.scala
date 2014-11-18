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

import org.apache.hadoop.hbase.{HColumnDescriptor, TableName, HTableDescriptor}
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.parquet.{OrFilter, AndFilter, ComparisonFilter, ParquetFilters}
import org.scalatest.FunSuite
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.ShuffledRDD

import scala.collection.mutable.{ListBuffer, ArrayBuffer}

class HBasePartitionerSuite extends FunSuite with HBaseTestSparkContext {

  test("test hbase partitioner") {
    val data = (1 to 40).map { r =>
      val rowKey = Bytes.toBytes(r)
      val rowKeyWritable = new ImmutableBytesWritableWrapper(rowKey)
      (rowKeyWritable, r)
    }
    val rdd = sc.parallelize(data, 4)
    val splitKeys = (1 to 40).filter(_ % 5 == 0).filter(_ != 40).map { r =>
      new ImmutableBytesWritableWrapper(Bytes.toBytes(r))
    }
    import org.apache.spark.sql.hbase.HBasePartitioner._
    val partitioner = new HBasePartitioner(rdd)(splitKeys.toArray)
    val shuffled =
      new ShuffledRDD[ImmutableBytesWritableWrapper, Int, Int](rdd, partitioner)

    val groups = shuffled.mapPartitionsWithIndex { (idx, iter) =>
      iter.map(x => (x._2, idx))
    }.collect()
    assert(groups.size == 40)
    assert(groups.map(_._2).toSet.size == 8)
    groups.foreach { r =>
      assert(r._1 > 5 * r._2 && r._1 <= 5 * (1 + r._2))
    }
  }

  test("test HBaseRelation getPrunedPartions") {
    val namespace = "testNamespace"
    val tableName = "testTable"
    val hbaseTableName = "hbaseTable"
    val family1 = "family1"
    val family2 = "family2"

    val rowkey1 = HBaseKVHelper.encodingRawKeyColumns(
      ListBuffer[Byte](),
      Seq(((new BytesUtils).toBytes(1), IntegerType)
        , ((new BytesUtils).toBytes(2), IntegerType))
    )

    val rowkey2 = HBaseKVHelper.encodingRawKeyColumns(
      ListBuffer[Byte](),
      Seq(((new BytesUtils).toBytes(9), IntegerType)
        , ((new BytesUtils).toBytes(2), IntegerType))
    )

    val rowkey3 = HBaseKVHelper.encodingRawKeyColumns(
      ListBuffer[Byte](),
      Seq(((new BytesUtils).toBytes(3), IntegerType)
        , ((new BytesUtils).toBytes(4), IntegerType))
    )

    val rowkey4 = HBaseKVHelper.encodingRawKeyColumns(
      ListBuffer[Byte](),
      Seq(((new BytesUtils).toBytes(3), IntegerType)
        , ((new BytesUtils).toBytes(6), IntegerType))
    )

    val partition1 = new HBasePartition(0, Some(rowkey1),
      Some(rowkey2))
    val partition2 = new HBasePartition(1, Some(rowkey3),
      Some(rowkey4))

    var allColumns = List[AbstractColumn]()
    allColumns = allColumns :+ KeyColumn("column2", IntegerType, 1)
    allColumns = allColumns :+ KeyColumn("column1", IntegerType, 0)
    allColumns = allColumns :+ NonKeyColumn("column4", FloatType, family2, "qualifier2")
    allColumns = allColumns :+ NonKeyColumn("column3", ShortType, family1, "qualifier1")

    val hbr = HBaseRelation(tableName, namespace, hbaseTableName, allColumns)
    val partitions = List[HBasePartition](partition1, partition2)
//    hbr.partitions = partitions

    val attribute1 = hbr.partitionKeys(0)
    val attribute2 = hbr.partitionKeys(1)
    val predicate5 = new GreaterThan(Literal(5,IntegerType), attribute1)

    hbr.getPrunedPartitions(Option(predicate5))
  }
}
