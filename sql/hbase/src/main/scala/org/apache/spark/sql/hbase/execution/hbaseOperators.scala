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

package org.apache.spark.sql.hbasesource.execution

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.log4j.Logger
import org.apache.spark.SparkContext._
import org.apache.spark.TaskContext
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.{ShuffledRDD, RDD}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.RangePartitioning
import org.apache.spark.sql.catalyst.types.DataType
import org.apache.spark.sql.execution.{LeafNode, SparkPlan, UnaryNode}
import org.apache.spark.sql.hbasesource._
import org.apache.spark.sql.hbasesource.HBasePartitioner._

import scala.collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
 * :: DeveloperApi ::
 * The HBase table scan operator.
 */
@DeveloperApi
case class HBaseSQLTableScan(
                        relation: HBaseRelation,
                        output: Seq[Attribute],
                        filterPredicate: Option[Expression],
                        coProcessorPlan: Option[SparkPlan])(@transient context: HBaseSQLContext)
  extends LeafNode {

  override def outputPartitioning = {
    var ordering = List[SortOrder]()
    for (key <- relation.partitionKeys) {
      ordering = ordering :+ SortOrder(key, Ascending)
    }
    RangePartitioning(ordering.toSeq, relation.partitions.size)
  }

  override def execute(): RDD[Row] = {
    new HBaseSQLReaderRDD(
      relation,
      context.codegenEnabled,
      output,
      filterPredicate, // PartitionPred : Option[Expression]
      None, // coprocSubPlan: SparkPlan
      context
    )
  }
}

@DeveloperApi
case class InsertIntoHBaseTable(
                                 relation: HBaseRelation,
                                 child: SparkPlan)
                               (@transient hbContext: HBaseSQLContext)
  extends UnaryNode {

  override def execute() = {
    val childRdd = child.execute()
    assert(childRdd != null)
    saveAsHbaseFile(childRdd, relation)
    childRdd
  }

  override def output = child.output

  private def saveAsHbaseFile(rdd: RDD[Row], relation: HBaseRelation): Unit = {
    //TODO:make the BatchMaxSize configurable
    val BatchMaxSize = 100

    hbContext.sparkContext.runJob(rdd, writeToHbase _)

    def writeToHbase(context: TaskContext, iterator: Iterator[Row]) = {
      val htable = relation.htable
      val bu = Array.fill[BytesUtils](BatchMaxSize, relation.allColumns.length) {
        new BytesUtils
      }
      var rowIndexInBatch = 0
      var colIndexInBatch = 0

      var puts = new ListBuffer[Put]()
      val buffer = ListBuffer[Byte]()
      while (iterator.hasNext) {
        val row = iterator.next()
        val rawKeyCol = relation.keyColumns.map (
          kc => {
            val rowColumn = DataTypeUtils.getRowColumnFromHBaseRawType(
              row, kc.ordinal, kc.dataType, bu(rowIndexInBatch)(colIndexInBatch))
            colIndexInBatch += 1
            (rowColumn, kc.dataType)
          }
        )
        val key = HBaseKVHelper.encodingRawKeyColumns(buffer, rawKeyCol)
        val put = new Put(key)
        relation.nonKeyColumns.foreach (
          nkc => {
            val rowVal = DataTypeUtils.getRowColumnFromHBaseRawType(
              row, nkc.ordinal, nkc.dataType, bu(rowIndexInBatch)(colIndexInBatch))
            colIndexInBatch += 1
            put.add(Bytes.toBytes(nkc.family), Bytes.toBytes(nkc.qualifier), rowVal)
          }
        )

        puts += put
        colIndexInBatch = 0
        rowIndexInBatch += 1
        if (rowIndexInBatch >= BatchMaxSize) {
          htable.put(puts.toList)
          puts.clear()
          rowIndexInBatch = 0
        }
      }
      if (!puts.isEmpty) {
        htable.put(puts.toList)
      }
    }
  }
}

@DeveloperApi
case class InsertValueIntoHBaseTable(relation: HBaseRelation, valueSeq: Seq[String])(
  @transient hbContext: HBaseSQLContext) extends LeafNode {

  override def execute() = {
    val buffer = ListBuffer[Byte]()
    val keyBytes = ListBuffer[(Array[Byte], DataType)]()
    val valueBytes = ListBuffer[(Array[Byte], Array[Byte], Array[Byte])]()
    HBaseKVHelper.string2KV(valueSeq, relation.allColumns, keyBytes, valueBytes)
    val rowKey = HBaseKVHelper.encodingRawKeyColumns(buffer, keyBytes)
    val put = new Put(rowKey)
    valueBytes.foreach { case (family, qualifier, value) =>
      put.add(family, qualifier, value)
    }
    relation.htable.put(put)

    hbContext.sc.parallelize(Seq.empty[Row], 1)
  }

  override def output = Nil
}

@DeveloperApi
case class BulkLoadIntoTable(path: String, relation: HBaseRelation,
                             isLocal: Boolean, delimiter: Option[String])(
                              @transient hbContext: HBaseSQLContext) extends LeafNode {

  val logger = Logger.getLogger(getClass.getName)

  val conf = hbContext.sc.hadoopConfiguration

  val job = Job.getInstance(conf)

  val hadoopReader = if (isLocal) {
    val fs = FileSystem.getLocal(conf)
    val pathString = fs.pathToFile(new Path(path)).getCanonicalPath
    new HadoopReader(hbContext.sparkContext, pathString, delimiter)(relation.allColumns)
  } else {
    new HadoopReader(hbContext.sparkContext, path, delimiter)(relation.allColumns)
  }

  // tmp path for storing HFile
  val tmpPath = Util.getTempFilePath(conf, relation.tableName)

  private[hbasesource] def makeBulkLoadRDD(splitKeys: Array[ImmutableBytesWritableWrapper]) = {
    val ordering = HBasePartitioner.orderingRowKey
      .asInstanceOf[Ordering[ImmutableBytesWritableWrapper]]
    val rdd = hadoopReader.makeBulkLoadRDDFromTextFile
    val partitioner = new HBasePartitioner(rdd)(splitKeys)
    // Todo: fix issues with HBaseShuffledRDD
    val shuffled =
      new ShuffledRDD[ImmutableBytesWritableWrapper, PutWrapper, PutWrapper](rdd, partitioner)
        .setKeyOrdering(ordering)
        //.setHbasePartitions(relation.partitions)
    val bulkLoadRDD = shuffled.mapPartitions { iter =>
      // the rdd now already sort by key, to sort by value
      val map = new java.util.TreeSet[KeyValue](KeyValue.COMPARATOR)
      var preKV: (ImmutableBytesWritableWrapper, PutWrapper) = null
      var nowKV: (ImmutableBytesWritableWrapper, PutWrapper) = null
      val ret = new ArrayBuffer[(ImmutableBytesWritable, KeyValue)]()
      if (iter.hasNext) {
        preKV = iter.next()
        var cellsIter = preKV._2.toPut().getFamilyCellMap.values().iterator()
        while (cellsIter.hasNext()) {
          cellsIter.next().foreach { cell =>
            val kv = KeyValueUtil.ensureKeyValue(cell)
            map.add(kv)
          }
        }
        while (iter.hasNext) {
          nowKV = iter.next()
          if (0 == (nowKV._1 compareTo preKV._1)) {
            cellsIter = nowKV._2.toPut().getFamilyCellMap.values().iterator()
            while (cellsIter.hasNext()) {
              cellsIter.next().foreach { cell =>
                val kv = KeyValueUtil.ensureKeyValue(cell)
                map.add(kv)
              }
            }
          } else {
            ret ++= map.iterator().map((preKV._1.toImmutableBytesWritable(), _))
            preKV = nowKV
            map.clear()
            cellsIter = preKV._2.toPut().getFamilyCellMap.values().iterator()
            while (cellsIter.hasNext()) {
              cellsIter.next().foreach { cell =>
                val kv = KeyValueUtil.ensureKeyValue(cell)
                map.add(kv)
              }
            }
          }
        }
        ret ++= map.iterator().map((preKV._1.toImmutableBytesWritable(), _))
        map.clear()
        ret.iterator
      } else {
        Iterator.empty
      }
    }

    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[KeyValue])
    job.setOutputFormatClass(classOf[HFileOutputFormat])
    job.getConfiguration.set("mapred.output.dir", tmpPath)
    bulkLoadRDD.saveAsNewAPIHadoopDataset(job.getConfiguration)
  }

  override def execute() = {
    val splitKeys = relation.getRegionStartKeys().toArray
    logger.debug(s"Starting makeBulkLoad on table ${relation.htable.getName} ...")
    makeBulkLoadRDD(splitKeys)
    val tablePath = new Path(tmpPath)
    val load = new LoadIncrementalHFiles(conf)
    logger.debug(s"Starting doBulkLoad on table ${relation.htable.getName} ...")
    load.doBulkLoad(tablePath, relation.htable)
    hbContext.sc.parallelize(Seq.empty[Row], 1)
  }

  override def output = Nil

}
