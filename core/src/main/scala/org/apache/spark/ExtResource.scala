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

package org.apache.spark

import scala.collection.mutable.{HashMap, ArrayBuffer, HashSet}

case class ExtResourceInfo(slaveHostname: String, executorId: String,
                           name: String, timestamp: Long, sharable: Boolean,
                           partitionAffined: Boolean, instanceCount: Int,
                           instanceUseCount: Int) extends Serializable {
  override def toString = {
    "host: %s\texecutor: %s\tname: %s\ttimestamp: %d\tsharable: %s\tpartitionAffined: %s\tinstanceCount %d\tinstances in use%d".format(name, timestamp, sharable, partitionAffined,
      instanceCount, instanceUseCount)
  }
}


object ExternalResourceManager {
  private lazy val taskToUsedResources = new HashMap[Long, HashSet[ExtResource]]

  def cleanupResourcesPerTask(taskId: Long) = {
    // mark the resources by this task as unused
    synchronized {
      taskToUsedResources.get(taskId).get.foreach(_.putInstances(taskId))
      taskToUsedResources -= taskId
    }
  }

  def addResource(taskId: Long, res: ExtResource) = {
    synchronized {
      taskToUsedResources.getOrElseUpdate(taskId, new HashSet[ExtResource]()) += res
    }
  }
}

/** record of number of uses of a shared resource instance per partition
  */
class ResourceRefCountPerPartition(var refCnt: Int = 0, val instance: Any)

/**
 * An external resource
 */
case class ExtResource(
    name: String,
    shared: Boolean = false,
    params: Seq[Serializable]=ArrayBuffer(),
    init: (Int, Seq[Serializable]) => Any = null,  // Initialization function
    term: (Int, Any, Seq[Serializable]) => Unit = null,  // Termination function
    partitionAffined: Boolean = false, // partition speficication preferred 
    expiration: Int = -1       // optional expiration time, default to none; 
                               // 0 for one-time use
  ) extends Serializable {

  @transient private var instances : Any = {
    if (shared) {
      if (partitionAffined)
        new HashMap[Int, ResourceRefCountPerPartition] // map of partition to (use count, instance) 
      else
        init(-1, params)
    } else if (partitionAffined)
      new HashMap[Int, ArrayBuffer[Any]]
    else 
      // large number of tasks per executor may deterioate modification performance
      ArrayBuffer[Any]()
  }

  @transient private var instancesInUse =
    if (shared) {
      if (partitionAffined)
        new HashMap[Long, Int]() // map from task id to partition
      else
        0                        // use count
    } else if (partitionAffined)
      new HashMap[Long, Pair[Int, ArrayBuffer[Any]]]() // map of task id to (partition, instances in use)
    else
      new HashMap[Long, ArrayBuffer[Any]]()            // map of task id to instances in use

  override def hashCode: Int = name.hashCode

  override def equals(other: Any): Boolean = other match {
    case o: ExtResource =>
      name.equals(o.name)
    case _ =>
      false
  }

  def getResourceInfo(host: String, executorId: String, timestamp: Long)
    : ExtResourceInfo = {
    synchronized {
      if (shared) {
        if (partitionAffined) {
          ExtResourceInfo(host, executorId, name, timestamp, true, true
                          ,instances.asInstanceOf[HashMap[Int, ResourceRefCountPerPartition]].size
                          ,instances.asInstanceOf[HashMap[Int, ResourceRefCountPerPartition]].values.map(_.refCnt).reduce(_+_))
        } else {
          ExtResourceInfo(host, executorId, name, timestamp, true, false, 1, instancesInUse.asInstanceOf[Int])
        }
      } else {
        if (partitionAffined) {
          val usedCount = instancesInUse.asInstanceOf[HashMap[Long, Pair[Int, ArrayBuffer[Any]]]].values.map(_._2.size).reduce(_+_)
          ExtResourceInfo(host, executorId, name, timestamp, false, true
                          ,instances.asInstanceOf[HashMap[Int, ArrayBuffer[Any]]].values.map(_.size).reduce(_+_)+usedCount, usedCount)
        } else {
          val usedCount = instancesInUse.asInstanceOf[HashMap[Long, ArrayBuffer[Any]]].values.map(_.size).reduce(_+_)
          ExtResourceInfo(host, executorId, name, timestamp, false, false
                          ,instances.asInstanceOf[ArrayBuffer[Any]].size+usedCount, usedCount)
        }
      }
    }
  }

  // Grab a newly established instance or from pool
  def getInstance(split: Int, taskId: Long): Any = {
    synchronized {
      // TODO: too conservative a locking: finer granular ones hoped
      var result = {
        if (!shared && !partitionAffined) {
          val l = instances.asInstanceOf[ArrayBuffer[Any]]
          if (l.isEmpty) 
            init(split, params)
          else
            l.remove(0)
        } else if (!shared) {
          val hml = instances.asInstanceOf[HashMap[Int, ArrayBuffer[Any]]]
            var resList = hml.getOrElseUpdate(split, ArrayBuffer(init(split, params)))
            if (resList.isEmpty)
              init(split, params)
            else
              resList.remove(0)
        } else if (partitionAffined) {
          val res = instances.asInstanceOf[HashMap[Int, ResourceRefCountPerPartition]]
                     .getOrElseUpdate(split, new ResourceRefCountPerPartition(instance=init(split, params)))
          res.refCnt += 1
          res.instance
        } else {
          if(instances.asInstanceOf[Option[Any]].isDefined)
            instances
          else
            instances = init(-1, params)
        }
      }

      if (shared) {
        if (partitionAffined) {
          instancesInUse.asInstanceOf[HashMap[Long, Int]].put(taskId, split)
        } else {
          instancesInUse = instancesInUse.asInstanceOf[Int] + 1
        }
      } else if (partitionAffined) {
        // add to the in-use instance list for non-sharable resources
        val hml=instancesInUse.asInstanceOf[HashMap[Long, Pair[Int, ArrayBuffer[Any]]]]
        hml.getOrElseUpdate(taskId, (split, ArrayBuffer[Any]()))._2 += result
      } else {
        val hm = instancesInUse.asInstanceOf[HashMap[Long, ArrayBuffer[Any]]]
        hm.getOrElseUpdate(taskId, ArrayBuffer[Any]()) += result
      }
      ExternalResourceManager.addResource(taskId, this)
      result
    }
  }

  // return instance to the pool; called by executor at task's termination
  def putInstances(taskId: Long) : Unit = {
    synchronized {
      // TODO: too conservative a locking: finer granular ones hoped
      if (shared) {
        if (partitionAffined) {
          instances.asInstanceOf[HashMap[Int, ResourceRefCountPerPartition]]
            .get(instancesInUse.asInstanceOf[HashMap[Long, Int]].get(taskId).get).get.refCnt -= 1
        } else
          instancesInUse = instancesInUse.asInstanceOf[Int] - 1
      } else {
        if (partitionAffined) {
          val hml = instancesInUse.asInstanceOf[HashMap[Long, Pair[Int, ArrayBuffer[Any]]]]
          hml.get(taskId).map(p=>instances.asInstanceOf[HashMap[Int, ArrayBuffer[Any]]]
             .getOrElseUpdate(p._1, ArrayBuffer[Any]()) += p._2)
          hml -= taskId
        } else {
          val hm = instancesInUse.asInstanceOf[HashMap[Long, ArrayBuffer[Any]]]
          hm.get(taskId).map(instances.asInstanceOf[ArrayBuffer[Any]] ++= _)
          hm -= taskId
        }
      }
    }
  }

  def cleanup(slaveHostname: String, executorId: String): String = {
    val errorString
         =  "Executor %s at %s : External Resource %s has instances in use and can't be cleaned up now".format(executorId, slaveHostname, name)
    val successString
         =  "Executor %s at %s : External Resource %s cleanup succeeds".format(executorId, slaveHostname, name)
    synchronized {
      if (shared) {
        if (partitionAffined) {
          // an all-or-nothing cleanup mechanism
          if (instances.asInstanceOf[HashMap[Int, ResourceRefCountPerPartition]].values.exists(_.refCnt >0))
            return errorString
          else {
            instances.asInstanceOf[HashMap[Int, ResourceRefCountPerPartition]]
            .foreach(r=>term(r._1, r._2.instance, params))
            instances.asInstanceOf[HashMap[Int, ResourceRefCountPerPartition]].clear
          }
        } else {
          if (instancesInUse.asInstanceOf[Int] > 0)
            // an all-or-nothing cleanup mechanism
            return errorString
          else {
            term(-1, instances, params)
            instances = None
          }
        }
      } else if (partitionAffined) {
        if (!instancesInUse.asInstanceOf[HashMap[Long, Pair[Int, ArrayBuffer[Any]]]].isEmpty)
          // an all-or-nothing cleanup mechanism
          return errorString
        else {
          instances.asInstanceOf[HashMap[Int, ArrayBuffer[Any]]].foreach(l => l._2.foreach(term(l._1, _, params)))
          instances.asInstanceOf[HashMap[Int, ArrayBuffer[Any]]].clear
        }
      } else {
        if (!instancesInUse.asInstanceOf[HashMap[Long, ArrayBuffer[Any]]].isEmpty)
          // an all-or-nothing cleanup mechanism
          return errorString
        else {
          instances.asInstanceOf[ArrayBuffer[Any]].foreach(term(-1, _, params))
          instances.asInstanceOf[ArrayBuffer[Any]].clear
        }
      }
      successString
    }
  }
}
