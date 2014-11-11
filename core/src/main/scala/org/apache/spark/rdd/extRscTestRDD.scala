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

package org.apache.spark.rdd

import java.sql.{Connection, ResultSet}

import scala.reflect.ClassTag

import org.apache.spark.{Logging, Partition, SparkContext, TaskContext}
import org.apache.spark.util.NextIterator


class extRscTestRDD[T: ClassTag](
    sc: SparkContext,
    extRscName: String)
  extends RDD[T](sc, Nil) with Logging {
  override def getPartitions: Array[Partition] = {
    val array = new Array[Partition](4)
    for (i <- 0 until array.size){
      array(i) = new Partition { override def index: Int = i }
    }
    array
  }

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    throw new UnsupportedOperationException("empty RDD")
    //Todo: sma : exception handling
//    if (!context.resources.isDefined) throw new Exception("No available ExtResources")
//
//    val extRsc = context.resources.get.get(extRscName)
//    if (extRsc==null) throw new Exception(s"No such resource : $extRscName")
//    val rsc = extRsc._1
//    println("++++ rdd compute: param size : "+rsc.params.size )
//    println("Object Id =  :" + rsc)
//    //    Thread.dumpStack()
//
//    rsc.getInstance(context.partitionId, context.attemptId).asInstanceOf[String].toIterator

  }
}



