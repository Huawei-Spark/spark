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
package org.apache.spark.sql.hbase.catalyst.types

import java.sql.Timestamp
import scala.math.PartialOrdering
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.{TypeTag, runtimeMirror, typeTag}
import org.apache.spark.sql.catalyst.types._
import scala.language.implicitConversions
import org.apache.spark.util.Utils

class Range[T](val start: Option[T], // None for open ends
               val startInclusive: Boolean,
               val end: Option[T], // None for open ends
               val endInclusive: Boolean)(implicit tag: TypeTag[T]) {
  // sanity checks
  lazy val dt: NativeType = PrimitiveType.all.find(_.tag == tag).getOrElse(null)
  require(dt != null && !(start.isDefined && end.isDefined &&
    ((dt.ordering.eq(start.get, end.get) &&
      (!startInclusive || !endInclusive)) ||
      (dt.ordering.gt(start.get.asInstanceOf[dt.JvmType], end.get.asInstanceOf[dt.JvmType])))),
    "Inappropriate range parameters")
  val castStart = if (start.isDefined) start.get.asInstanceOf[dt.JvmType] else null
  val castEnd = if (end.isDefined) end.get.asInstanceOf[dt.JvmType] else null
}

// HBase ranges: start is inclusive and end is exclusive
class HBaseRange[T](start: Option[T], end: Option[T], val id: Int)(implicit tag: TypeTag[T])
  extends Range[T](start, true, end, false)

// A PointRange is a range of a single point. It is used for convenience when
// do comparison on two values of the same type. An alternatively would be to
// use multiple (overloaded) comparison methods, which could be more natural
// but also more codes

class PointRange[T](value: T)(implicit tag: TypeTag[T])
  extends Range[T](Some(value), true, Some(value), true)

object HBasePointRange {
  implicit def toPointRange(s: Any): Any = s match {
    case i: Int => new PointRange[Int](i)
    case l: Long => new PointRange[Long](l)
    case d: Double => new PointRange[Double](d)
    case f: Float => new PointRange[Float](f)
    case b: Byte => new PointRange[Byte](b)
    case s: Short => new PointRange[Short](s)
    case s: String => new PointRange[String](s)
    case b: Boolean => new PointRange[Boolean](b)
    case d: BigDecimal => new PointRange[BigDecimal](d)
    case t: Timestamp => new PointRange[Timestamp](t)
    case _ => null
  }
}

abstract class PartiallyOrderingDataType extends DataType {
  private[sql] type JvmType
  @transient private[sql] val tag: TypeTag[JvmType]

  @transient private[sql] val classTag = {
    // No need to use the ReflectLock for Scala 2.11?
    val mirror = runtimeMirror(Utils.getSparkClassLoader)
    ClassTag[JvmType](mirror.runtimeClass(tag.tpe))
  }
  private[sql] val partialOrdering: PartialOrdering[JvmType]
}

class RangeType[T] extends PartiallyOrderingDataType {
  private[sql] type JvmType = Range[T]
  @transient private[sql] val tag = typeTag[JvmType]
  val partialOrdering = new PartialOrdering[JvmType] {
    // Right now we just support comparisons between a range and a point
    // In the future when more generic range comparisons, these two methods
    // must be functional as expected
    def tryCompare(a: JvmType, b: JvmType): Option[Int] = {
      val p1 = lteq(a, b)
      val p2 = lteq(b, a)
      if (p1) {
        if (p2) Some(0) else Some(-1)
      } else if (p2) Some(1) else None
    }

    def lteq(a: JvmType, b: JvmType): Boolean = {
      // returns TRUE iff a <= b
      // Right now just support PointRange at one end
      require(a.isInstanceOf[PointRange[T]] || b.isInstanceOf[PointRange[T]],
        "Non-point range on both sides of a predicate is not supported")

      var result = false
      if (a.isInstanceOf[PointRange[T]]) {
        val pointValue = a.asInstanceOf[PointRange[T]].start.getOrElse(null)
        val range = b.asInstanceOf[HBaseRange[T]]
        val startValue = range.start.getOrElse(null)

        if (pointValue != null && startValue != null &&
          range.dt.ordering.lteq(pointValue.asInstanceOf[range.dt.JvmType],
            startValue.asInstanceOf[range.dt.JvmType])) {
          result = true
        }
      } else if (b.isInstanceOf[PointRange[T]]) {
        val pointValue = b.asInstanceOf[PointRange[T]].start.getOrElse(null)
        val range = a.asInstanceOf[HBaseRange[T]]
        val endValue = range.start.getOrElse(null)
        if (pointValue != null && endValue != null &&
          range.dt.ordering.lteq(endValue.asInstanceOf[range.dt.JvmType],
            pointValue.asInstanceOf[range.dt.JvmType])) {
          result = true
        }
      }

      result

      /*
      val (point, range, reversed) = if (a.isInstanceOf[PointRange[T]]) {
        (a.asInstanceOf[PointRange[T]], b, false)
      } else {
        (b.asInstanceOf[PointRange[T]], a, true)
      }
      if (!reversed) { `
        if (range.start.isDefined) {
          if (range.startInclusive) {
            if (range.dt.ordering.lteq(point.value, range.start.get)) {
              Some(true)
            } else if (!range.end.isDefined) {
              None
            } else if (range.endInclusive) {
              if (range)
            }
          } else if (range.dt.ordering.lt(point.value, range.start.get)) {
            true
          }
        }
        */
    }
  }
}
