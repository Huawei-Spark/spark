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

import org.apache.hadoop.hbase.client.Result
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.codegen.GeneratePredicate
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.{InterruptibleIterator, Logging, Partition, TaskContext}


/**
 * HBaseSQLReaderRDD
 * Created by sboesch on 9/16/14.
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
          relation.buildRow(projections, result, row)
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
    val (filters, otherFilters) = relation.buildFilter2(output, filterPred)
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
          relation.buildRow(projections, result, row)
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
