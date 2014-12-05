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

package org.apache.spark.sql.hbasesource

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}

import org.apache.spark.{Partitioner, SparkEnv}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.util.{CollectionsUtils, Utils}

import scala.reflect.ClassTag

class HBasePartitioner [K : Ordering : ClassTag, V](
    @transient rdd: RDD[_ <: Product2[K,V]])(splitKeys: Array[K])
  extends Partitioner {

  private var ordering = implicitly[Ordering[K]]

  private var rangeBounds: Array[K] = splitKeys

  def numPartitions = rangeBounds.length

  private var binarySearch: ((Array[K], K) => Int) = CollectionsUtils.makeBinarySearch[K]

  def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[K]
    var partition = 0
    if (rangeBounds.length <= 128) {
      // If we have less than 128 partitions naive search
      while (partition < rangeBounds.length && ordering.gt(k, rangeBounds(partition))) {
        partition += 1
      }
    } else {
      // Determine which binary search method to use only once.
      partition = binarySearch(rangeBounds, k)
      // binarySearch either returns the match location or -[insertion point]-1
      if (partition < 0) {
        partition = -partition - 1
      }
      if (partition > rangeBounds.length) {
        partition = rangeBounds.length
      }
    }
    partition
  }

  override def equals(other: Any): Boolean = other match {
    case r: HBasePartitioner[_, _] =>
      r.rangeBounds.sameElements(rangeBounds)
    case _ =>
      false
  }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    var i = 0
    while (i < rangeBounds.length) {
      result = prime * result + rangeBounds(i).hashCode
      i += 1
    }
    result = prime * result
    result
  }

  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream) {
    val sfactory = SparkEnv.get.serializer
    sfactory match {
      case js: JavaSerializer => out.defaultWriteObject()
      case _ =>
        out.writeObject(ordering)
        out.writeObject(binarySearch)

        val ser = sfactory.newInstance()
        Utils.serializeViaNestedStream(out, ser) { stream =>
          stream.writeObject(scala.reflect.classTag[Array[K]])
          stream.writeObject(rangeBounds)
        }
    }
  }

  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream) {
    val sfactory = SparkEnv.get.serializer
    sfactory match {
      case js: JavaSerializer => in.defaultReadObject()
      case _ =>
        ordering = in.readObject().asInstanceOf[Ordering[K]]
        binarySearch = in.readObject().asInstanceOf[(Array[K], K) => Int]

        val ser = sfactory.newInstance()
        Utils.deserializeViaNestedStream(in, ser) { ds =>
          implicit val classTag = ds.readObject[ClassTag[Array[K]]]()
          rangeBounds = ds.readObject[Array[K]]()
        }
    }
  }
}

object HBasePartitioner {
  implicit val orderingRowKey =
    OrderingRowKey.asInstanceOf[Ordering[ImmutableBytesWritableWrapper]]
}

object OrderingRowKey extends Ordering[ImmutableBytesWritableWrapper] {
  def compare(a: ImmutableBytesWritableWrapper, b: ImmutableBytesWritableWrapper) = a.compareTo(b)
}
