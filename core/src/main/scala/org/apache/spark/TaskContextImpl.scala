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

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConversions.mapAsScalaMap
import scala.collection.mutable.{ArrayBuffer, HashMap}

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.util.{TaskCompletionListener, TaskCompletionListenerException}

import scala.collection.mutable.ArrayBuffer

private[spark] class TaskContextImpl(
    val stageId: Int,
    val partitionId: Int,
    override val taskAttemptId: Long,
    override val attemptNumber: Int,
    val runningLocally: Boolean = false,
    val resources: Option[ConcurrentHashMap[String, Pair[ExtResource[_], Long]]] = None,
    val executorId: Option[String] = None,
    val slaveHostname: Option[String] = None,
    val taskMetrics: TaskMetrics = TaskMetrics.empty)
  extends TaskContext
  with Logging {

  // For backwards-compatibility; this method is now deprecated as of 1.3.0.
  override def attemptId(): Long = taskAttemptId

  // List of callback functions to execute when the task completes.
  @transient private val onCompleteCallbacks = new ArrayBuffer[TaskCompletionListener]

  // Whether the corresponding task has been killed.
  @volatile private var interrupted: Boolean = false

  // Whether the task has completed.
  @volatile private var completed: Boolean = false

  override def addTaskCompletionListener(listener: TaskCompletionListener): this.type = {
    onCompleteCallbacks += listener
    this
  }

  override def addTaskCompletionListener(f: TaskContext => Unit): this.type = {
    onCompleteCallbacks += new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit = f(context)
    }
    this
  }

  @deprecated("use addTaskCompletionListener", "1.1.0")
  override def addOnCompleteCallback(f: () => Unit) {
    onCompleteCallbacks += new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit = f()
    }
  }

  /** Marks the task as completed and triggers the listeners. */
  private[spark] def markTaskCompleted(): Unit = {
    completed = true
    val errorMsgs = new ArrayBuffer[String](2)
    // Process complete callbacks in the reverse order of registration
    onCompleteCallbacks.reverse.foreach { listener =>
      try {
        listener.onTaskCompletion(this)
      } catch {
        case e: Throwable =>
          errorMsgs += e.getMessage
          logError("Error in TaskCompletionListener", e)
      }
    }
    if (errorMsgs.nonEmpty) {
      throw new TaskCompletionListenerException(errorMsgs)
    }
  }

  /** Marks the task for interruption, i.e. cancellation. */
  private[spark] def markInterrupted(): Unit = {
    interrupted = true
  }

  override def isCompleted(): Boolean = completed

  override def isRunningLocally(): Boolean = runningLocally

<<<<<<< HEAD
  override def isInterrupted(): Boolean = interrupted
=======
  override def isInterrupted: Boolean = interrupted
  
  def getExtResourceUsageInfo() : Iterator[ExtResourceInfo] = {
    synchronized {
      if (resources.isDefined && slaveHostname.isDefined
        && executorId.isDefined){
        val smap = mapAsScalaMap(resources.get)
        smap.map(r=>r._2._1.getResourceInfo(slaveHostname.get,
                    executorId.get, r._2._2)).toIterator
      }
      else{
        ArrayBuffer[ExtResourceInfo]().toIterator
      }
    }
  }

  def cleanupResources(resourceName: Option[String]) : Iterator[String] = {
    synchronized {
      if (!resources.isDefined) {
        ArrayBuffer[String]("No external resources available to tasks for Executor %s at %s"
          .format(executorId, slaveHostname)).toIterator
      } else if (resources.get.isEmpty) {
        ArrayBuffer[String]("No external resources registered for Executor %s at %s"
          .format(executorId, slaveHostname)).toIterator
      } else if (resourceName.isDefined) {
        if (resources.get.containsKey(resourceName.get)) {
          ArrayBuffer[String](resources.get.get(resourceName.get)
            ._1.cleanup(slaveHostname.get, executorId.get)).toIterator
        } else {
          ArrayBuffer[String]("No external resources %s registered for Executor %s at %s"
           .format(resourceName.get, executorId.get, slaveHostname.get)).toIterator
        }
      } else {
        resources.get.map(_._2._1.cleanup(slaveHostname.get, executorId.get)).toIterator
      }
    }
  }
}

