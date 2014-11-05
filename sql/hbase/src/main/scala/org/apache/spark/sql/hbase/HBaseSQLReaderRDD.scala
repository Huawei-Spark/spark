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
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericMutableRow, Expression}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.{Logging, InterruptibleIterator, Partition, TaskContext}


/**
 * HBaseSQLReaderRDD
 * Created by sboesch on 9/16/14.
 */
class HBaseSQLReaderRDD(
    relation: HBaseRelation,
    output: Seq[Attribute],
    rowKeyPred: Option[Expression],
    valuePred: Option[Expression],
    partitionPred: Option[Expression],
    coprocSubPlan: Option[SparkPlan],
    @transient hbaseContext: HBaseSQLContext)
  extends RDD[Row](hbaseContext.sparkContext, Nil) with Logging {

  private final val cachingSize: Int = 100 // To be made configurable

  override def getPartitions: Array[Partition] = {
    relation.getPrunedPartitions(partitionPred).get.toArray
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[HBasePartition].server.map {
      identity
    }.toSeq
  }

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    val filters = relation.buildFilter(output, rowKeyPred, valuePred)
    val scan = relation.buildScan(split, filters, output)
    scan.setCaching(cachingSize)
    val scanner = relation.htable.getScanner(scan)
    var finished: Boolean = false
    var gotNext: Boolean = false
    var result: Result = null
    val row = new GenericMutableRow(output.size)
    val projections = output.zipWithIndex
    val bytesUtils = new BytesUtils
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
}
