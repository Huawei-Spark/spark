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

// import java.io.Serializable


import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.{HashMap, ArrayBuffer, HashSet}

case class ExtResourceInfo(slaveHostname: String, executorId: String,
                           name: String, timestamp: Long, sharable: Boolean,
                           partitionAffined: Boolean, instanceCount: Int,
                           instanceUseCount: Int)  {
  override def toString = {
    ("host: %s\texecutor: %s\tname: %s\ttimestamp: %d\tsharable: %s\tpartitionAffined: " +
      "%s\tinstanceCount %d\tinstances in use%d").format(slaveHostname, executorId,  name, timestamp,
        sharable.toString, partitionAffined.toString, instanceCount, instanceUseCount)
  }
}


object ExternalResourceManager {
  private lazy val taskToUsedResources = new ConcurrentHashMap[Long, HashSet[ExtResource[_]]]

  def cleanupResourcesPerTask(taskId: Long): Unit = {
    // mark the resources by this task as unused
    val res = taskToUsedResources.asScala.get(taskId)
    res.isDefined match {
      case true => {
        res.get.foreach(_.putInstances(taskId))
        taskToUsedResources -= taskId
      }
      case _ =>
    }

  }

  def addResource(taskId: Long, res: ExtResource[_]) = {
    taskToUsedResources.getOrElseUpdate(taskId, new HashSet[ExtResource[_]]()) += res
  }
}

/** record of number of uses of a shared resource instance per partition
  */
class ResourceRefCountPerPartition[T] (var refCnt: Int = 0, val instance: T)

/**
 * An external resource
 */
case class ExtResource[T](
    name: String,
    shared: Boolean = false,
    params: Seq[String],
    init: (Int, Seq[String]) => T = null,  // Initialization function
    term: (Int, T, Seq[String]) => Unit = null,  // Termination function
    partitionAffined: Boolean = false, // partition speficication preferred 
    expiration: Int = -1       // optional expiration time, default to none; 
                               // 0 for one-time use
  ) extends Serializable {


  @transient private var instances: Any = null

  def getInstancesStat(shared: Boolean,
      partitionAffined: Boolean): Any ={

    def instInit(): Any ={
      (shared, partitionAffined) match{
        case (true, true) =>
          // map of partition to (use count, instance)
          instances = new HashMap[Int, ResourceRefCountPerPartition[T]]
        case (true, false) =>
//          instances = init(-1, params)
          instances = new HashSet[T]()
        case (false, true) =>
          instances = new HashMap[Int, ArrayBuffer[T]]
        case (false, false) =>
          // large number of tasks per executor may deterioate modification performance
          instances = ArrayBuffer[T]()
      }
    }

    Option(instances) match {
      case None => instInit()
      case _ => instances
    }
    instances
  }


  @transient private var instancesInUse : Any = null
  def getInstancesInUseStat (shared: Boolean,
    partitionAffined: Boolean) :  Any ={

    def instInUseInit(): Unit ={
      (shared, partitionAffined) match {
        case (true, true) =>
          // map from task id to partition
          instancesInUse = new HashMap[Long, Int]()
        case (true, false) => instancesInUse = 0 // use count
        case (false, true) =>
          // map of task id to (partition, instances in use)
          instancesInUse = new HashMap[Long, Pair[Int, ArrayBuffer[T]]]()
        case (false, false) =>
          // map of task id to instances in use
          instancesInUse = new HashMap[Long, ArrayBuffer[T]]()
      }
    }

    Option(instancesInUse) match{
      case None => instInUseInit()
      case _ => instancesInUse
    }
    instancesInUse
  }


  override def hashCode: Int = name.hashCode

  override def equals(other: Any): Boolean = other match {
    case o: ExtResource[T] =>
      name.equals(o.name)
    case _ =>
      false
  }

  def getResourceInfo(host: String, executorId: String, timestamp: Long)
    : ExtResourceInfo = {
    synchronized {
      instances = getInstancesStat(shared, partitionAffined)
      instancesInUse = getInstancesInUseStat(shared, partitionAffined)

      (shared, partitionAffined) match {
        case (true, true) => {
          val instanceCnt = instances.asInstanceOf[
            HashMap[Int, ResourceRefCountPerPartition[T]]].size
          val instanceUseCnt = instances.asInstanceOf[
            HashMap[Int, ResourceRefCountPerPartition[T]]]
            .values.map(_.refCnt).foldLeft(0)(_ + _)
          ExtResourceInfo(host, executorId, name, timestamp, true, true, instanceCnt, instanceUseCnt)
        }
        case (true, false) => {
          ExtResourceInfo(host, executorId, name, timestamp, true, false, instances.asInstanceOf[HashSet[T]].size, instancesInUse.asInstanceOf[Int])
        }
        case (false, true) =>
          val usedCount = instancesInUse.asInstanceOf[
            HashMap[Long, Pair[Int, ArrayBuffer[T]]]].values.map(_._2.size).foldLeft(0)(_ + _)
          ExtResourceInfo(host, executorId, name, timestamp, false, true,
            instances.asInstanceOf[HashMap[Int, ArrayBuffer[T]]].values.map(_.size)
            .foldLeft(0)(_ + _) + usedCount, usedCount)
        case (false, false) =>
          val usedCount = instancesInUse.asInstanceOf[HashMap[Long, ArrayBuffer[T]]].values.map(_.size).foldLeft(0)(_ + _)
          ExtResourceInfo(host, executorId, name, timestamp, false, false,
            instances.asInstanceOf[ArrayBuffer[T]].size + usedCount, usedCount)
      }
    }
  }

  // Grab a newly established instance or from pool
  def getInstance(split: Int, taskId: Long): Any = {
    synchronized {
      // TODO: too conservative a locking: finer granular ones hoped
      instances = getInstancesStat(shared, partitionAffined)
      instancesInUse = getInstancesInUseStat(shared, partitionAffined)

      var result : T = {
        (shared, partitionAffined) match {
          case (false, false) =>
            val l = instances.asInstanceOf[ArrayBuffer[T]]
            if (l.isEmpty){
              init(split, params)
            }
            else{
              l.remove(0)
            }

          case (false, true) =>{
            val hml = instances.asInstanceOf[HashMap[Int, ArrayBuffer[T]]]
            var resList = hml.getOrElseUpdate(split, ArrayBuffer(init(split, params)))
            if (resList.isEmpty)
              init(split, params)
            else
              resList.remove(0)
          }
          case (true, true) =>{
            val res = instances.asInstanceOf[HashMap[Int, ResourceRefCountPerPartition[T]]]
              .getOrElseUpdate(split, new ResourceRefCountPerPartition[T](instance=init(split, params)))
            res.refCnt += 1
            res.instance
          }
          case (true, false) => {
            val res = instances.asInstanceOf[HashSet[T]]
            if(res.size>0)
              res.head
            else{
              val rsc = init(-1, params)
              res += rsc
              rsc
            }
          }
        }
      }

      (shared, partitionAffined) match {
        case (true, true) =>
          instancesInUse.asInstanceOf[HashMap[Long, Int]].put(taskId, split)
        case (true, false) =>
          instancesInUse = instancesInUse.asInstanceOf[Int] + 1
        case (false, true) => {
          // add to the in-use instance list for non-sharable resources
          val hml=instancesInUse.asInstanceOf[HashMap[Long, Pair[Int, ArrayBuffer[T]]]]
          hml.getOrElseUpdate(taskId, (split, ArrayBuffer[T]()))._2 += result
        }
        case (false, false) => {
          val hm = instancesInUse.asInstanceOf[HashMap[Long, ArrayBuffer[T]]]
          hm.getOrElseUpdate(taskId, ArrayBuffer[T]()) += result
        }
      }

      ExternalResourceManager.addResource(taskId, this)
      result
    }
  }

  // return instance to the pool; called by executor at task's termination
  def putInstances(taskId: Long) : Unit = {
    synchronized {
      // TODO: too conservative a locking: finer granular ones hoped
      instances = getInstancesStat(shared, partitionAffined)
      instancesInUse = getInstancesInUseStat(shared, partitionAffined)

      (shared, partitionAffined) match {
        case (true, true) =>
          instances.asInstanceOf[HashMap[Int, ResourceRefCountPerPartition[T]]]
            .get(instancesInUse.asInstanceOf[HashMap[Long, Int]].get(taskId).get).get.refCnt -= 1
        case (true, false) =>
          instancesInUse = instancesInUse.asInstanceOf[Int] - 1
        case (false, true) => {
          val hml = instancesInUse.asInstanceOf[HashMap[Long, Pair[Int, ArrayBuffer[T]]]]
          hml.get(taskId).map(p => instances.asInstanceOf[HashMap[Int, ArrayBuffer[T]]]
            .getOrElseUpdate(p._1, ArrayBuffer[T]()) ++= p._2)
          hml -= taskId
        }
        case (false, false) => {
          val hm = instancesInUse.asInstanceOf[HashMap[Long, ArrayBuffer[T]]]
          hm.get(taskId).map(instances.asInstanceOf[ArrayBuffer[T]] ++= _)
          hm -= taskId
        }
      }
    }
  }

  def cleanup(slaveHostname: String, executorId: String): String = {
    val errorString
         =  "Executor %s at %s : External Resource %s has instances in use and can't be cleaned up now"
      .format(executorId, slaveHostname, name)
    val successString
         =  "Executor %s at %s : External Resource %s cleanup succeeds".format(executorId, slaveHostname, name)

    synchronized {
      instances = getInstancesStat(shared, partitionAffined)
      instancesInUse = getInstancesInUseStat(shared, partitionAffined)

      (shared, partitionAffined) match {
        case (true, true) =>
          // an all-or-nothing cleanup mechanism
          if (instances.asInstanceOf[HashMap[Int, ResourceRefCountPerPartition[T]]].values.exists(_.refCnt >0))
            return errorString
          else {
            instances.asInstanceOf[HashMap[Int, ResourceRefCountPerPartition[T]]]
              .foreach(r=>term(r._1, r._2.instance, params))
            instances.asInstanceOf[HashMap[Int, ResourceRefCountPerPartition[T]]].clear
          }
        case (true, false) =>
          if (instancesInUse.asInstanceOf[Int] > 0) {
            // an all-or-nothing cleanup mechanism
            return errorString
          }else {
            val res = instances.asInstanceOf[HashSet[T]]
            if (res.size>0)
              term(-1, res.head, params)
            res.clear()
          }
        case (false, true) =>
          if (!instancesInUse.asInstanceOf[HashMap[Long, Pair[Int, ArrayBuffer[T]]]].isEmpty)
          // an all-or-nothing cleanup mechanism
            return errorString
          else {
            instances.asInstanceOf[HashMap[Int, ArrayBuffer[T]]].foreach(l =>l._2.foreach(
              e => term(l._1, e, params)))
            instances.asInstanceOf[HashMap[Int, ArrayBuffer[T]]].clear
          }
        case (false, false) =>
          if (!instancesInUse.asInstanceOf[HashMap[Long, ArrayBuffer[T]]].isEmpty)
          // an all-or-nothing cleanup mechanism
            return errorString
          else {
            instances.asInstanceOf[ArrayBuffer[T]].foreach(term(-1, _, params))
            instances.asInstanceOf[ArrayBuffer[T]].clear
          }
      }
      successString
    }
  }
}
