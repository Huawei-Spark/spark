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

import org.apache.hadoop.hbase.util.Bytes
import scala.collection.mutable.{ArrayBuffer, Set}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.hbasesource.catalyst.expressions.PartialPredicateOperations._
import org.apache.spark.sql.hbasesource.catalyst.types.Range
import org.apache.spark.sql.catalyst.types.{IntegralType, NativeType}
import org.apache.spark.sql.hbasesource.CriticalPointType.CriticalPointType

object CriticalPointType extends Enumeration {
  type CriticalPointType = Value
  val upInclusive = Value("Up Inclusive: (...)[...)")
  val lowInclusive = Value("Low Inclusive: (...](...)")
  val bothInclusive = Value("Both Inclusive: (...)[](...)")
}

case class CriticalPoint[T](value: T, ctype: CriticalPointType, dt: NativeType) {
  override def hashCode() = value.hashCode()
  val decreteType: Boolean = dt.isInstanceOf[IntegralType]
  override def equals(other: Any): Boolean = other match {
    case cp: CriticalPoint[T] => value.equals(cp.value)
    case _ => false
  }
}


private [hbasesource] class CriticalPointRange[T](start: Option[T], startInclusive: Boolean,
                            end: Option[T], endInclusive: Boolean, dimIndex:Int,
                            dt: NativeType, var pred: Expression)
  extends Range[T](start, startInclusive, end, endInclusive, dt) {
  var nextDimCriticalPointRanges: Seq[CriticalPointRange[_]] = Nil
  lazy val isPoint: Boolean = startInclusive && endInclusive && start.get == end.get
  var prefixIndex: Int = 0 // # of prefix key dimensions

  private[hbasesource] def convert(prefix: HBaseRawType, origin: CriticalPointRange[_])
                            : Seq[CriticalPointRange[HBaseRawType]]  = {
    val rawStart: Option[HBaseRawType] = if (start.isDefined) {
      if (prefix == null) Some(DataTypeUtils.dataToBytes(start.get, dt))
      else Some(prefix ++ DataTypeUtils.dataToBytes(start.get, dt))
    } else if (prefix == null) {
      None
    } else Some(prefix)
    val rawEnd: Option[HBaseRawType] = if (end.isDefined) {
      if (prefix == null) Some(DataTypeUtils.dataToBytes(end.get, dt))
      else Some(prefix ++ DataTypeUtils.dataToBytes(end.get, dt))
    } else if (prefix == null) None
    else Some(prefix)

    if (nextDimCriticalPointRanges.isEmpty) {
      // Leaf node
      origin.prefixIndex = dimIndex
      Seq(new CriticalPointRange[HBaseRawType](rawStart, startInclusive,
                                               rawEnd, endInclusive, dimIndex, dt, pred))
    } else {
      nextDimCriticalPointRanges.map(_.convert(rawStart.orNull, origin)).reduceLeft(_ ++ _)
    }
  }
}

/**
 * find the critical points in the given expressiona: not really a transformer
 * Must be called before reference binding
 */
object RangeCriticalPoint {
  private[hbasesource] def collect[T](expression: Expression, key: AttributeReference)
                               : Seq[CriticalPoint[T]] = {
    if (key.references.subsetOf(expression.references)) {
      val pointSet = Set[CriticalPoint[T]]()
      val dt: NativeType = expression.dataType.asInstanceOf[NativeType]
      def checkAndAdd(value: Any, ct: CriticalPointType): Unit = {
        val cp = CriticalPoint[T](value.asInstanceOf[T], ct, dt)
        if (!pointSet.add(cp)) {
          val oldCp = pointSet.find(_.value == value).get
          if (oldCp.ctype != ct && oldCp.ctype != CriticalPointType.bothInclusive) {
            pointSet.remove(cp)
            if (ct == CriticalPointType.bothInclusive) {
              pointSet.add(cp)
            } else {
              pointSet.add(CriticalPoint[T](value.asInstanceOf[T],
                CriticalPointType.bothInclusive, dt))
            }
          }
        }
      }
      expression transform {
        case a@EqualTo(AttributeReference(_,_,_,_), Literal(value, _)) =>
          if (a.left.equals(key)) checkAndAdd(value, CriticalPointType.bothInclusive)
          a
        case a@EqualTo(Literal(value, _), AttributeReference(_,_,_,_)) =>
          if (a.right.equals(key)) checkAndAdd(value, CriticalPointType.bothInclusive)
          a
        case a@LessThan(AttributeReference(_,_,_,_), Literal(value, _)) =>
          if (a.left.equals(key)) checkAndAdd(value, CriticalPointType.upInclusive)
          a
        case a@LessThan(Literal(value, _), AttributeReference(_,_,_,_)) =>
          if (a.right.equals(key)) checkAndAdd(value, CriticalPointType.lowInclusive)
          a
        case a@LessThanOrEqual(AttributeReference(_,_,_,_), Literal(value, _)) =>
          if (a.left.equals(key)) checkAndAdd(value, CriticalPointType.lowInclusive)
          a
        case a@LessThanOrEqual(Literal(value, _), AttributeReference(_,_,_,_)) =>
          if (a.right.equals(key)) checkAndAdd(value, CriticalPointType.upInclusive)
          a
        case a@GreaterThanOrEqual(AttributeReference(_,_,_,_), Literal(value, _)) =>
          if (a.left.equals(key)) checkAndAdd(value, CriticalPointType.upInclusive)
          a
        case a@GreaterThanOrEqual(Literal(value, _), AttributeReference(_,_,_,_)) =>
          if (a.right.equals(key)) checkAndAdd(value, CriticalPointType.lowInclusive)
          a
      }
      pointSet.toSeq.sortWith((a: CriticalPoint[T], b: CriticalPoint[T])
      => dt.ordering.lt(a.value.asInstanceOf[dt.JvmType], b.value.asInstanceOf[dt.JvmType]))
    } else Nil
  }
/*
 * create partition ranges on a *sorted* list of critical points
 */
  private[hbasesource] def generateCriticalPointRange[T](cps: Seq[CriticalPoint[T]],
                                                   dimIndex: Int, dt: NativeType)
         : Seq[CriticalPointRange[T]] = {
    if (cps.isEmpty) Nil
    else {
      val discreteType = dt.isInstanceOf[IntegralType]
      val result = new ArrayBuffer[CriticalPointRange[T]](cps.size + 1)
      var prev: CriticalPoint[T] = null
      cps.foreach(cp=> {
        if (prev == null) {
          cp.ctype match {
            case CriticalPointType.lowInclusive =>
              result += new CriticalPointRange[T](None, false, Some(cp.value), true,
                                                  dimIndex, cp.dt, null)
            case CriticalPointType.upInclusive =>
              result += new CriticalPointRange[T](None, false, Some(cp.value), false,
                                                  dimIndex, cp.dt, null)
            case CriticalPointType.bothInclusive =>
              result += (new CriticalPointRange[T](None, false, Some(cp.value), false,
                                                   dimIndex, cp.dt, null),
                new CriticalPointRange[T](Some(cp.value), true, Some(cp.value), true,
                                                   dimIndex, cp.dt, null))
          }
        } else {
          (prev.ctype, cp.ctype) match {
            case (CriticalPointType.lowInclusive, CriticalPointType.lowInclusive) =>
              result += new CriticalPointRange[T](Some(prev.value), false, Some(cp.value), true,
                                                  dimIndex, cp.dt, null)
            case (CriticalPointType.lowInclusive, CriticalPointType.upInclusive) =>
              result += new CriticalPointRange[T](Some(prev.value), false, Some(cp.value), false,
                                                  dimIndex, cp.dt, null)
            case (CriticalPointType.lowInclusive, CriticalPointType.bothInclusive) =>
              result += (new CriticalPointRange[T](Some(prev.value), false, Some(cp.value), false,
                                                  dimIndex, cp.dt, null),
                new CriticalPointRange[T](Some(cp.value), true, Some(cp.value), true,
                                                  dimIndex, cp.dt, null))
            case (CriticalPointType.upInclusive, CriticalPointType.lowInclusive) =>
              result += new CriticalPointRange[T](Some(prev.value), true, Some(cp.value), true,
                                                  dimIndex, cp.dt, null)
            case (CriticalPointType.upInclusive, CriticalPointType.upInclusive) =>
              result += new CriticalPointRange[T](Some(prev.value), true, Some(cp.value), false,
                                                  dimIndex, cp.dt, null)
            case (CriticalPointType.upInclusive, CriticalPointType.bothInclusive) =>
              result += (new CriticalPointRange[T](Some(prev.value), true, Some(cp.value), false,
                                                  dimIndex, cp.dt, null),
                new CriticalPointRange[T](Some(cp.value), true, Some(cp.value), true,
                                                  dimIndex, cp.dt, null))
            case (CriticalPointType.bothInclusive, CriticalPointType.lowInclusive) =>
              result += new CriticalPointRange[T](Some(prev.value), false, Some(cp.value), true,
                                                  dimIndex, cp.dt, null)
            case (CriticalPointType.bothInclusive, CriticalPointType.upInclusive) =>
              result += new CriticalPointRange[T](Some(prev.value), false, Some(cp.value), false,
                                                  dimIndex, cp.dt, null)
            case (CriticalPointType.bothInclusive, CriticalPointType.bothInclusive) =>
              result += (new CriticalPointRange[T](Some(prev.value), false, Some(cp.value), false,
                                                  dimIndex, cp.dt, null),
                new CriticalPointRange[T](Some(cp.value), true, Some(cp.value), true,
                                          dimIndex, cp.dt, null))
          }
        }
        prev = cp
      })
      if (prev != null) {
        result += {
          prev.ctype match {
            case CriticalPointType.lowInclusive =>
              new CriticalPointRange[T](Some(prev.value), false,
                None, false, dimIndex, prev.dt, null)
            case CriticalPointType.upInclusive =>
              new CriticalPointRange[T](Some(prev.value), true,
                None, false, dimIndex, prev.dt, null)
            case CriticalPointType.bothInclusive =>
              new CriticalPointRange[T](Some(prev.value), false,
                None, false, dimIndex, prev.dt, null)
          }
        }
      }
      // remove any redundant ranges for integral type
      if (discreteType) {
        var prev: CriticalPointRange[T] = null
        var prevChanged = false
        var thisChangedUp = false
        var thisChangedDown = false
        var newRange: CriticalPointRange[T] = null
        val newResult = new ArrayBuffer[CriticalPointRange[T]](result.size)
        result.foreach(r=>{
          thisChangedDown = false
          thisChangedUp = false
          if (r.startInclusive && !r.endInclusive && r.end.isDefined
               && r.start.get==
                  dt.ordering.asInstanceOf[Integral[T]].minus(r.end.get, 1.asInstanceOf[T])) {
            thisChangedDown = true
            if (prev != null && prev.startInclusive && prev.endInclusive
                && prev.start.get == prev.end.get && prev.start.get == r.start.get)
            {
              // the previous range is a equivalent point range => merge it with current one
              newRange = null
            } else {
              newRange = new CriticalPointRange[T](r.start, true, r.start, true,
                dimIndex, r.dt, null)
            }
          } else if (!r.startInclusive && r.endInclusive && r.end.isDefined
              && r.start.get==
                 dt.ordering.asInstanceOf[Integral[T]].minus(r.end.get, 1.asInstanceOf[T])) {
            newRange = new CriticalPointRange[T](r.end, true, r.end, true,
              dimIndex, r.dt, null)
            thisChangedUp = true
          } else newRange = r

          // the previous range has been changed up and this one has not changed =>
          // check whether this is mergeable with the (changed) previous
          if (newRange != null && !thisChangedDown && !thisChangedUp && prevChanged) {
            if (r.startInclusive && r.endInclusive && r.start.get == r.end.get &&
                prev.startInclusive && prev.endInclusive
                && prev.start.get == prev.end.get && prev.start.get == r.start.get) {
              newRange = null // merged with the previous range
            }
          }
          if (newRange != null) {
            newResult += newRange
            prev = newRange
            prevChanged = thisChangedUp
          }
        })
        newResult
      } else result
    }
  }

  // Step 1
  private[hbasesource] def generateCriticalpointRanges(relation: HBaseRelation,
                       pred: Option[Expression], dimIndex: Int): Seq[CriticalPointRange[_]] =  {
    if (!pred.isDefined) Nil
    else {
      val predExpr = pred.get
      val predRefs = predExpr.references.toSeq
      val boundPred = BindReferences.bindReference(predExpr, predRefs)
      val row = new GenericMutableRow(predRefs.size)
      // Step 1
      generateCriticalpointRangesHelper(relation, predExpr, dimIndex, row, boundPred, predRefs)
    }
  }

  private[hbasesource] def generateCriticalpointRangesHelper(relation: HBaseRelation,
                 predExpr: Expression, dimIndex: Int, row: MutableRow,
                 boundPred: Expression, predRefs: Seq[Attribute])
                 : Seq[CriticalPointRange[_]] = {
    val keyDim = relation.partitionKeys(dimIndex)
    val dt: NativeType = keyDim.dataType.asInstanceOf[NativeType]
    // Step 1.1
    val criticalPoints: Seq[CriticalPoint[dt.JvmType]]
        = collect(predExpr, relation.partitionKeys(dimIndex))
    if (criticalPoints.isEmpty) Nil
    else {
      val cpRanges: Seq[CriticalPointRange[dt.JvmType]]
      = generateCriticalPointRange[dt.JvmType](criticalPoints, dimIndex, dt)
      // Step 1.2
      val keyIndex = predRefs.indexWhere(_.exprId == relation.partitionKeys(dimIndex).exprId)
      val qualifiedCPRanges = cpRanges.filter(cpr => {
        row.update(keyIndex, cpr)
        val prRes = boundPred.partialReduce(row, predRefs)
        if (prRes.isInstanceOf[Expression]) cpr.pred = prRes.asInstanceOf[Expression]
        prRes.isInstanceOf[Expression] || prRes.asInstanceOf[Boolean]
      })

      // Step 1.3
      if (dimIndex < relation.partitionKeys.size - 1) {
        // For point range, generate CPs for the next dim
        qualifiedCPRanges.foreach(cpr => {
          if (cpr.isPoint) {
            cpr.nextDimCriticalPointRanges = generateCriticalpointRangesHelper(relation,
                cpr.pred, dimIndex + 1, row, boundPred, predRefs)
          }
        })
      }
      qualifiedCPRanges
    }
  }

  // Step 3

  // Find all overlapping ranges on a particular range. The ranges are sorted
  private def getOverlappedRanges(src: Range[HBaseRawType], target: Seq[Range[HBaseRawType]],
                                  startIndex: Int, srcPrefixIndex: Int, dimSize: Int,
                                  srcPartition: Boolean) : (Int, Int) = {
    if (target.isEmpty) (-1, -1)
    else {
      // Essentially bisect the target through binary search
      // We need to either find the biggest "start" <= the source's "end" in target,
      //    as indicated by lowBound being FALSE;
      // or find the smallest "end" >= the source's "start",
      //    as indicated by lowBound being TRUE
      def binarySearch(src: HBaseRawType, srcInclusive: Boolean, lowBound: Boolean): Int = {
        val threshold = 10 // Below this the linear search will be employed
        var left = startIndex
        var right = target.size
        var prevLarger = -1
        var prevSmaller = -1
        var mid = -1
        var cmp: Int = 0
        val empty: HBaseRawType = Array[Byte]()
        var tgtInclusive = false
        if (src == null) {
          // open boundary = +/- Infinity
          require(!srcInclusive && lowBound, "Internal logical error: invalid open boundary")
          startIndex
        } else {
          // Binary search for smallest/largest quality
          def binarySearchEquality(eq: Int, limit: Int): Int = {
            var mid = -1
            var cmp = 0
            var prevEq = eq
            if (lowBound) {
              // downward search
              var left = limit
              var right = eq
              var size = 0
              var inclusive = false
              if (!srcPartition) {
                size = src.size
                inclusive = srcPrefixIndex < dimSize - 1
              }
              while (right > left) {
                if (right - left < threshold) {
                  // linear search
                  var i = right
                  cmp = 0
                  while (i <= left + 1 && cmp == 0) {
                    prevEq = i
                    i = i - 1
                    if (srcPartition) {
                      size = target(i).end.get.size
                      val cpr = target(i).asInstanceOf[CriticalPointRange[HBaseRawType]]
                      inclusive = cpr.prefixIndex < dimSize-1 || target(i).endInclusive
                    }
                    if (srcPartition) size = target(i).end.get.size
                    else src.size
                    cmp = if (inclusive) Bytes.compareTo(target(i).end.get, 0, size, src, 0, size)
                    else 1
                  }
                  right = left
                } else {
                  mid = left + (right - left) / 2
                  if (srcPartition) {
                    size = target(mid).end.get.size
                    val cpr = target(mid).asInstanceOf[CriticalPointRange[HBaseRawType]]
                    inclusive = cpr.prefixIndex < dimSize -1 || target(mid).endInclusive
                  }
                  cmp = Bytes.compareTo(target(mid).end.get, 0, size, src, 0, size)
                  if (cmp == 0) {
                    cmp = if (inclusive) 0
                    else -1 // (target)(source)
                  }
                  if (cmp == 0) {
                    prevEq = mid
                    right = mid - 1
                  } else {
                    left = mid + 1
                  }
                }
              }
            } else {
              // upward search
              var left = eq
              var right = limit
              var inclusive = false
              var size = 0
              if (!srcPartition) {
                size = src.size
                inclusive = srcPrefixIndex < dimSize - 1
              }
              while (right > left) {
                if (right - left < threshold) {
                  // linear search
                  cmp = 0
                  var i = left
                  prevEq = i
                  while (i <= right - 1 && cmp == 0) {
                    prevEq = i
                    i = i + 1
                    if (srcPartition) {
                      size = target(i).start.getOrElse(empty).size
                      val cpr = target(i).asInstanceOf[CriticalPointRange[HBaseRawType]]
                      inclusive = cpr.prefixIndex < dimSize -1 || target(i).startInclusive
                    }
                    if (inclusive) cmp = Bytes.compareTo(target(i).start.getOrElse(empty),
                                                         0, size, src, 0, size)
                    else cmp = -1
                  }
                  right = left
                } else {
                  mid = left + (right - left) / 2
                  if (srcPartition) {
                    size = target(mid).start.getOrElse(empty).size
                    val cpr = target(mid).asInstanceOf[CriticalPointRange[HBaseRawType]]
                    inclusive = cpr.prefixIndex < dimSize -1 || target(mid).startInclusive
                  }
                  cmp = Bytes.compareTo(target(mid).start.getOrElse(empty), 0, size, src, 0, size)
                  if (cmp == 0) {
                    cmp = if (inclusive) 0
                    else 1 // (source)(target)
                  }
                  if (cmp == 0) {
                    prevEq = mid
                    left = mid + 1
                  } else {
                    right = mid - 1
                  }
                }
              }
            }
            prevEq
          }

          var inclusive = false
          var size = 0
          if (!srcPartition) {
            size = src.size
            inclusive = srcPrefixIndex < dimSize - 1
          }
          while (right > left) {
            if (right - left < threshold) {
              // Linear search
              if (lowBound) {
                var i = right
                cmp = 0
                prevLarger = right
                while (i >= left + 1 && cmp >= 0) {
                  prevLarger = i
                  i = i - 1
                  if (srcPartition) {
                    size = target(i).end.get.size
                    val cpr = target(i).asInstanceOf[CriticalPointRange[HBaseRawType]]
                    inclusive = cpr.prefixIndex < dimSize -1 || target(i).endInclusive
                  }
                  cmp = Bytes.compareTo(target(i).end.get, 0, size, src, 0, size)
                  tgtInclusive = inclusive
                  if (cmp == 0) {
                    cmp = if (srcInclusive && tgtInclusive) 0
                    else -1 // (target)(source)
                  }
                }
              } else {
                var i = left
                cmp = 0
                prevSmaller = left
                while (i < right && cmp <= 0) {
                  prevSmaller = i
                  i = i + 1
                  if (srcPartition) {
                    size = target(i).start.getOrElse(empty).size
                    val cpr = target(i).asInstanceOf[CriticalPointRange[HBaseRawType]]
                    inclusive = cpr.prefixIndex < dimSize -1 || target(mid).startInclusive
                  }
                  cmp = Bytes.compareTo(target(i).start.getOrElse(empty), 0, size, src, 0, size)
                  tgtInclusive = inclusive
                  if (cmp == 0) {
                    cmp = if (srcInclusive && tgtInclusive) 0
                    else 1 // (source)(target)
                  }
                }
              }
              right = left
            } else {
              mid = left + (right - left) / 2
              if (lowBound) {
                // looking for smallest "end" >= the source "start"
                if (srcPartition) {
                  size = target(mid).end.size
                  val cpr = target(mid).asInstanceOf[CriticalPointRange[HBaseRawType]]
                  inclusive = cpr.prefixIndex < dimSize -1 || target(mid).endInclusive
                }
                cmp = Bytes.compareTo(target(mid).end.get, 0, size, src, 0, size)
                tgtInclusive = inclusive
              } else {
                // looking for biggest "start" <= the source "end"
                if (srcPartition) {
                  size = target(mid).start.size
                  val cpr = target(mid).asInstanceOf[CriticalPointRange[HBaseRawType]]
                  inclusive = cpr.prefixIndex < dimSize -1 || target(mid).startInclusive
                }
                cmp = Bytes.compareTo(target(mid).start.getOrElse(empty), 0, size, src, 0, size)
                tgtInclusive = inclusive
                if (cmp == 0) {
                  cmp = if (srcInclusive && tgtInclusive) 0
                  else if (lowBound) -1 // (target)(source)
                  else 1 // (source)(target)
                }
                if (cmp == 0) {
                  if (lowBound) prevLarger = binarySearchEquality(mid, prevSmaller)
                  else prevSmaller = binarySearchEquality(mid, prevLarger)
                  right = left // break out the loop
                } else if (cmp > 0) {
                  prevLarger = mid
                  right = mid - 1
                } else {
                  prevSmaller = mid
                  left = mid + 1
                }
              }
            }
          }
          if (lowBound) prevLarger
          else prevSmaller
        }
      }
      val pLargestStart = binarySearch(src.end.orNull, src.endInclusive, false)
      val pSmallestEnd = binarySearch(src.start.orNull, src.startInclusive, true)
      if (pSmallestEnd == -1 || pLargestStart == -1
          || pSmallestEnd > pLargestStart)
      {
        null // no overlapping
      } else {
        (pSmallestEnd, pLargestStart)
      }
    }
  }

  private[hbasesource] def prunePartitions(cprs: Seq[CriticalPointRange[HBaseRawType]],
                                     pred: Option[Expression], partitions: Seq[HBasePartition],
                                     dimSize: Int): Seq[HBasePartition] = {
    if (cprs.isEmpty) {
      partitions.map(p=> new HBasePartition(p.idx, p.mappedIndex, 0,
                                            p.lowerBound, p.upperBound, p.server, pred))
    } else {
      var cprStartIndex = 0
      var pStartIndex = 0
      var done = false
      var pIndex = 0
      val result = Seq[HBasePartition]()
      while (cprStartIndex < cprs.size && pStartIndex < partitions.size && !done) {
        val cpr = cprs(cprStartIndex)
        val qualifiedPartitionIndexes = getOverlappedRanges(cpr, partitions, pStartIndex,
                                                            cpr.prefixIndex, dimSize, false)
        if (qualifiedPartitionIndexes == null) done = true
        else {
          val (pstart, pend) = qualifiedPartitionIndexes
          var p = partitions(pstart)
          val pred = if (cpr.pred == null) None else Some(cpr.pred)
          // for partitions on more than 1 critical ranges, use the original pred
          // and let the slave to do the partial reduction on the leading dimension
          // only. This is a bit conservative to avoid extra complexity
          // Step 3.3
          result :+ new HBasePartition(pIndex, p.idx, 0,
            p.lowerBound, p.upperBound, p.server, pred)
          pIndex += 1
          for (i <- pstart + 1 to pend - 1) {
            p = partitions(i)
            // Step 3.2
            result :+ new HBasePartition(pIndex, p.idx, -1,
                                         p.lowerBound, p.upperBound, p.server, pred)
            pIndex += 1
          }
          if (pend > pstart) {
            p = partitions(pend)
            // for partitions on more than 1 critical ranges, use the original pred
            // and let the slave to do the partial reduction on the leading dimension
            // only. This is a bit conservative to avoid extra complexity
            // Step 3.3
            result :+ new HBasePartition(pIndex, p.idx, 0,
              p.lowerBound, p.upperBound, p.server, pred)
            pIndex += 1
          }
          pStartIndex = pend + 1
          // skip any critical point ranges that possibly are covered by
          // the last of just-qualified partitions
          val qualifiedCPRIndexes = getOverlappedRanges(partitions(pend), cprs, cprStartIndex,
                                                        -1, dimSize, true)
          if (qualifiedCPRIndexes == null) done = true
          else cprStartIndex = qualifiedPartitionIndexes._2
        }
      }
      result
    }
  }
  /*
   * Given a HBase relation, generate a sequence of pruned partitions and their
   * associated filter predicates that are further subject to slave-side refinement
   * The algorithm goes like this:
   * 1. For each dimension key (starting from the primary key dimension)
   *    1.1 collect the critical points and their sorted ranges
   *    1.2 For each range, partial reduce to qualify and generate associated filter predicates
   *    1.3 For each "point range", repeat Step 1 for the next key dimension
   * 2. For each critical point based range, convert to its byte[] representation,
   *    basically collapse multi-dim boundaries into a universal byte[] and potential
   *    expand the original top-level critical point ranges into ones incorporated
   *    lower level nested critical point ranges
   * 3. For each converted critical point based range, map them to partitions to partitions
    *   3.1 start the binary search from the last mapped partition
    *   3.2 For each partition mapped to only one critical point range, assign the latter's filter
    *       predicate to the partition
    *   3.3 For each partition mapped to multiple critical point ranges, use the original
    *       predicate so the slave will
   */
  private[hbasesource] def generatePrunedPartitions(
      relation: HBaseRelation,
      pred: Option[Expression]): Seq[HBasePartition] = {
    // Step 1
    val cprs: Seq[CriticalPointRange[_]] = generateCriticalpointRanges(relation, pred, 0)
    // Step 2
    val expandedCPRs: Seq[CriticalPointRange[HBaseRawType]] = Nil
    cprs.foreach(cpr=>expandedCPRs ++ cpr.convert(null, cpr))
    // Step 3
    prunePartitions(expandedCPRs, pred, relation.partitions, relation.partitionKeys.size)
  }
}
