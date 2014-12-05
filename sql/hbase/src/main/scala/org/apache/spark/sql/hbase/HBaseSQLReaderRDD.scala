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
package org.apache.spark.sql.hbasesource

import org.apache.hadoop.hbase.client.Result
import org.apache.spark.sql.hbasesource.catalyst.expressions.PartialPredicateOperations._
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.codegen.GeneratePredicate
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.hbasesource.catalyst.types.PartitionRange
import org.apache.spark.{InterruptibleIterator, Logging, Partition, TaskContext}


/**
 * HBaseSQLReaderRDD
 */
class HBaseSQLReaderRDD(
    relation: HBaseRelation,
    codegenEnabled: Boolean,
    output: Seq[Attribute],
    filterPred: Option[Expression],
    coprocSubPlan: Option[SparkPlan],
    @transient hbaseContext: HBaseSQLContext)
  extends RDD[Row](hbaseContext.sparkContext, Nil) with Logging {

  @transient lazy val logger = Logger.getLogger(getClass.getName)
  private final val cachingSize: Int = 100 // To be made configurable

  override def getPartitions: Array[Partition] = {
    relation.getPrunedPartitions(filterPred).get.toArray
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[HBasePartition].server.map {
      identity
    }.toSeq
  }

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    val filters = relation.buildFilter(output, filterPred, filterPred)
    val scan = relation.buildScan(split, filters, output)
    scan.setCaching(cachingSize)
    logger.debug(s"relation.htable scanner conf="
      + s"${relation.htable.getConfiguration.get("hbase.zookeeper.property.clientPort")}")
    val scanner = relation.htable.getScanner(scan)

    val row = new GenericMutableRow(output.size)
    val projections = output.zipWithIndex
    val bytesUtils = new BytesUtils

    var finished: Boolean = false
    var gotNext: Boolean = false
    var result: Result = null

    val iter = new Iterator[Row] {
      override def hasNext: Boolean = {
        if (!finished) {
          if (!gotNext) {
            result = scanner.next
            finished = result == null
            gotNext = true
          }
        }
        if (finished) {
          close
        }
        !finished
      }

      override def next(): Row = {
        if (hasNext) {
          gotNext = false
          relation.buildRow(projections, result, row, bytesUtils)
        } else {
          null
        }
      }

      def close() = {
        try {
          scanner.close()
        } catch {
          case e: Exception => logWarning("Exception in scanner.close", e)
        }
      }
    }
    new InterruptibleIterator(context, iter)
  }

  // TODO: renamed to compute and add override
  def compute2(split: Partition, context: TaskContext): Iterator[Row] = {
    val (filters, otherFilters) = relation.buildFilter2(output,
      split.asInstanceOf[HBasePartition].filterPred)
    val scan = relation.buildScan(split, filters, output)
    scan.setCaching(cachingSize)
    val scanner = relation.htable.getScanner(scan)
    val otherFilter: (Row) => Boolean = if (otherFilters.isDefined) {
      if (codegenEnabled) {
        GeneratePredicate(otherFilters.get, output)
      } else {
        InterpretedPredicate(otherFilters.get, output)
      }
    } else null

    val row = new GenericMutableRow(output.size)
    val projections = output.zipWithIndex
    val bytesUtils = new BytesUtils

    var finished: Boolean = false
    var gotNext: Boolean = false
    var result: Result = null

    val iter = new Iterator[Row] {
      override def hasNext: Boolean = {
        if (!finished) {
          if (!gotNext) {
            result = scanner.next
            finished = result == null
            gotNext = true
          }
        }
        if (finished) {
          close
        }
        !finished
      }

      override def next(): Row = {
        if (hasNext) {
          gotNext = false
          relation.buildRow(projections, result, row, bytesUtils)
        } else {
          null
        }
      }

      def close() = {
        try {
          scanner.close()
        } catch {
          case e: Exception => logWarning("Exception in scanner.close", e)
        }
      }
    }
    if (otherFilter == null) {
      new InterruptibleIterator(context, iter)
    } else {
      new InterruptibleIterator(context, iter.filter((otherFilter)))
    }
  }

  // For critical-point-based predicate pushdown
  // Identical to compute2 with the addition of partition-specific
  // partial reduction for those partitions mapped to multiple critical point ranges,
  // as indicated by the keyPartialEvalIndex in the partition, where the original
  // filter predicate will be used
  // TODO: renamed to compute and add override
  def compute3(split: Partition, context: TaskContext): Iterator[Row] = {
    val partition = split.asInstanceOf[HBasePartition]
    val pred = if (partition.keyPartialEvalIndex >= 0 && partition.filterPred.isDefined) {
      val oriPred = partition.filterPred.get
      val predRefs = oriPred.references.toSeq
      val boundRef = BindReferences.bindReference(oriPred, predRefs)
      val row = new GenericMutableRow(predRefs.size)
      for (i <- 0 to partition.keyPartialEvalIndex) {
        val keyIndex = predRefs.indexWhere(_.exprId == relation.partitionKeys(i).exprId)
        val range = relation.generateRange(partition, oriPred, keyIndex)
        row.update(keyIndex, range)
      }
      val pr = boundRef.partialReduce(row, predRefs)
      if (pr.isInstanceOf[Expression]) Some(pr.asInstanceOf[Expression])
      else None
    } else partition.filterPred
    val (filters, otherFilters) = relation.buildFilter2(output, pred)
    val scan = relation.buildScan(split, filters, output)
    scan.setCaching(cachingSize)
    val scanner = relation.htable.getScanner(scan)
    val otherFilter: (Row) => Boolean = if (otherFilters.isDefined) {
      if (codegenEnabled) {
        GeneratePredicate(otherFilters.get, output)
      } else {
        InterpretedPredicate(otherFilters.get, output)
      }
    } else null

    val row = new GenericMutableRow(output.size)
    val projections = output.zipWithIndex
    val bytesUtils = new BytesUtils

    var finished: Boolean = false
    var gotNext: Boolean = false
    var result: Result = null

    val iter = new Iterator[Row] {
      override def hasNext: Boolean = {
        if (!finished) {
          if (!gotNext) {
            result = scanner.next
            finished = result == null
            gotNext = true
          }
        }
        if (finished) {
          close
        }
        !finished
      }

      override def next(): Row = {
        if (hasNext) {
          gotNext = false
          relation.buildRow(projections, result, row, bytesUtils)
        } else {
          null
        }
      }

      def close() = {
        try {
          scanner.close()
        } catch {
          case e: Exception => logWarning("Exception in scanner.close", e)
        }
      }
    }
    if (otherFilter == null) {
      new InterruptibleIterator(context, iter)
    } else {
      new InterruptibleIterator(context, iter.filter((otherFilter)))
    }
  }
}
