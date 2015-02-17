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

package org.apache.spark.executor

import java.util.concurrent.ConcurrentHashMap

import akka.actor.Actor
import org.apache.spark.{ExtResource, ExtResourceInfo, Logging}

import org.apache.spark.util.{Utils, ActorLogReceive}
import scala.collection.JavaConversions._

import scala.collection.mutable.ArrayBuffer


/**
 * Driver -> Executor message to trigger a thread dump.
 */
private[spark] case object TriggerThreadDump

private[spark] case object TriggerExtResourceInfoDump

private[spark] case class TriggerExtResourceCleanup(resourceName: Option[String] = None)

/**
 * Actor that runs inside of executors to enable driver -> executor RPC.
 */
private[spark]
class ExecutorActor(executorId: String,
    slaveHostname: String,
    resource: ConcurrentHashMap[String, Pair[ExtResource[_], Long]]
) extends Actor with ActorLogReceive with Logging {

  def getExtResourceInfo(): Map[String, ExtResourceInfo] = {
    val res = resource.map(r => (r._1, r._2._1.getResourceInfo(slaveHostname, executorId, r._2._2))).toMap
    res
  }

  def extResourceCleanup(resourceName: Option[String]): Iterator[String] = {
    synchronized {
      if (resource.size()<=0){
        ArrayBuffer[String]("No external resources registered for Executor %s at %s"
          .format(executorId, slaveHostname)).toIterator
      } else if (resourceName.isDefined) {
        val resName = resourceName.get
        if (resource.containsKey(resName))
          ArrayBuffer[String](resource.get(resName)
            ._1.cleanup(slaveHostname, executorId)).toIterator
        else
          ArrayBuffer[String]("No external resources %s registered for Executor %s at %s"
            .format(resourceName.get, executorId, slaveHostname)).toIterator
      } else {
        resource.map(_._2._1.cleanup(slaveHostname, executorId)).toIterator
      }
    }
  }

  override def receiveWithLogging = {
    case TriggerThreadDump =>
      sender ! Utils.getThreadDump()
    case TriggerExtResourceInfoDump => {
      sender ! getExtResourceInfo()
    }
    case TriggerExtResourceCleanup(resourceName) => {
      sender ! extResourceCleanup(resourceName)
    }
  }

}
