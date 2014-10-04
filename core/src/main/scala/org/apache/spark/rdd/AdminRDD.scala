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

import org.apache.spark._
import org.apache.spark.scheduler.TaskLocation
import org.apache.spark.annotation.DeveloperApi

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
 * A Spark split class that is per executor
 */
private[spark] class ExecutorPartition(idx: Int, @transient val taskLocation: TaskLocation)
  extends Partition {

  override val index: Int = idx
}

/**
 * :: DeveloperApi ::
 * An abstract RDD that provides basics to dispatch executor-specific task
 * (mostly for administration purposes)
 *
 * @param sc The SparkContext to associate the RDD with.
 */
@DeveloperApi
abstract class AdminRDD[T: ClassTag](
    sc: SparkContext)
  extends RDD[T](sc, Nil) with Logging {

  override def getPartitions: Array[Partition] = {
    val executors : Seq[TaskLocation] = sc.getExecutorsAndLocations
    val array = new Array[Partition](executors.size)
    for (i <- 0 until executors.size) {
      array(i) = new ExecutorPartition(i, executors(i))
    }
    array
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val adminSplit = split.asInstanceOf[ExecutorPartition]
    ArrayBuffer[String](adminSplit.taskLocation.host)
  }
}
