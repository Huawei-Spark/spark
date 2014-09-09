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

import java.util.{ArrayQueue, HashMap}

case class ExtResourceInstance(handle : Any, partition : Int)
{
}

/**
 * An external resource
 */
case class ExtResource(
    name: String,
    shared: Boolean = false,
    params: Seq[Serializable],
    init: (Seq[Serializable]) => Any,  // Initialization function
    term: (Seq[Serializable]) => Unit,  // Termination function
    partitionAffined: Boolean, // partition speficication preferred 
    expiration: Int = -1       // optional expiration time, default to none; 
                               // 0 for one-time use
  )

  extends Serializable {

  @transient private lazy instances : Option[Any] = {
    if (shared)
      ExtResourceInstance(init(params), -1, -1)
    else if (partitionAffined) 
      new HashMap[Int, Any]
    else 
      new ArrayQueue[Any] 
  }

  @transient private lazy usedInstances : Option[Any] = {
    if (shared)
      init(params)
    else if (partitionAffined) 
      new HashMap[Long, Any]
    else 
      new ArrayQueue[Any] 
  }

  override def hashCode : Int = name.hashCode

  override def equals(other : ExtResource) :Boolean = name.equals(other.name)

  def getInstance(split : Int, taskId : Long) : Any = {
    val instance : Any = { 
      if (shared)
        instances
      else if (partitionAffined) {
        instance = ((HashMap)instances).get(split).getOrElse(init(params))
      } else {
        if (((ArrayQueue)instances).isEmpty)
          instance = init(params)
        else
          instance = ((ArrayQueue)instances).pop.get
      }
      instance
    }
  }

  def putInstance(split : Int) : Unit = {
    if (!shared) {
      if (partitionAffined) {
        ((HashMap)instances).get(split).getOrElse(init(params))
      } else {
        if (((ArrayQueue)instances).isEmpty)
          init(params)
        else
          ((ArrayQueue)instances).pop.get
      }
    }
  }

  def cleanup() = {
  }
}

object ExtResources {
  resources: 
}
