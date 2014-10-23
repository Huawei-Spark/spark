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

package org.apache.spark.sql.hbase.execution

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{LeafNode, UnaryNode, SparkPlan}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.hbase.{HBaseRelation, HadoopReader, HBaseSQLContext}
import org.apache.spark.SparkContext._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Put
import org.apache.spark.SerializableWritable

/**
 * :: DeveloperApi ::
 * The HBase table scan operator.
 */
@DeveloperApi
case class HBaseSQLTableScan(
    otherAttributes: Seq[Attribute],
    attributes: Seq[Attribute],
    relation: HBaseRelation,
    projList: Seq[NamedExpression],
    columnPruningPredicates: Seq[Expression],
    rowKeyPredicates: Seq[Expression],
    partitionPruningPredicates: Seq[Expression],
    coProcessorPlan: Option[SparkPlan])
    (@transient context: HBaseSQLContext)
  extends LeafNode {

  override def execute(): RDD[Row] = {
    new HBaseSQLReaderRDD(
      relation,
      projList,
      columnPruningPredicates, // TODO:convert to column pruning preds
      rowKeyPredicates,
      rowKeyPredicates, // PartitionPred : Option[Expression]
      None, // coprocSubPlan: SparkPlan
      context
    )
  }

  override def output = attributes
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
    // YZ: to be implemented using sc.runJob() => SparkContext needed here
    // TODO:WF: use tableoutputformat here to insert into hbase table
    childRdd
  }

  override def output = child.output
}

@DeveloperApi
case class BulkLoadIntoTable(relation: HBaseRelation, path: String)(
  @transient hbContext: HBaseSQLContext)
  extends UnaryNode {

  val jobConf = new JobConf(hbContext.sc.hadoopConfiguration)

  val hadoopReader = new HadoopReader(hbContext.sparkContext, jobConf)

  val regions: Int = ???  // get from the relation/configs

  override def execute() = {
    val rdd = hadoopReader.makeBulkLoadRDD.sortByKey(true, regions)
    val jobConfSer = new SerializableWritable(jobConf)
    saveAsHfile(rdd, jobConfSer)
    // bulk load is like a command, so return null is ok here?
    null
  }

  def saveAsHfile(
    rdd: RDD[(ImmutableBytesWritable, Put)],
    jobConf: SerializableWritable[JobConf]) {
    ???
  }

  override def output = Nil
}
