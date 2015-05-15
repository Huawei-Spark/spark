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

package org.apache.spark.sql.hbase

import java.io._

import org.apache.hadoop.hbase.exceptions.DeserializationException
import org.apache.hadoop.hbase.filter.Filter.ReturnCode
import org.apache.hadoop.hbase.filter.FilterBase
import org.apache.hadoop.hbase.util.{Bytes, Writables}
import org.apache.hadoop.hbase.{Cell, CellUtil, KeyValue}
import org.apache.hadoop.io.Writable
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.hbase.catalyst.expressions.PartialPredicateOperations._
import org.apache.spark.sql.hbase.util.{BytesUtils, DataTypeUtils, HBaseKVHelper}
import org.apache.spark.sql.types.{DataType, NativeType, StringType}

/**
 * the serializer to serialize / de-serialize the objects,
 * may use some serializer provided by Spark in the future.
 */
private[hbase] object Serializer {
  /**
   * serialize the input object to byte array
   * @param obj the input object
   * @return the serialized byte array
   */
  def serialize(obj: Any): Array[Byte] = {
    val b = new ByteArrayOutputStream()
    val o = new ObjectOutputStream(b)
    o.writeObject(obj)
    b.toByteArray
  }

  /**
   * de-serialize the byte array to the original object
   * @param bytes the input byte array
   * @return the de-serialized object
   */
  def deserialize(bytes: Array[Byte]): Any = {
    val b = new ByteArrayInputStream(bytes)
    val o = new ObjectInputStream(b)
    o.readObject()
  }
}

/**
 * the custom filter, it will skip the scan to the proper next position based on predicate
 * this filter will only deal with the predicate which has key columns inside
 */
private[hbase] class HBaseSkipScanFilter extends FilterBase with Writable {
  private var relation: HBaseRelation = null
  private var predExpr: Expression = null

  // the total size of the key dimension
  private var sizeOfDimension: Int = 0

  // the next hint
  private var nextReturnCode: ReturnCode = null

  // the next key hint
  private var nextKeyValue: Cell = null

  // the next possible row key
  private var nextRowKey: HBaseRawType = null

  // the next key position was generated by key hint
  private var hasAdjusted: Boolean = false

  // the current row key
  private var currentRowKey: HBaseRawType = null

  // the current row key values
  private var currentValues: Seq[Any] = null

  // the currently considering dimension in the loop
  private var currentDim = 0

  // the flag to determine whether to filter the remaining or not
  private var filterAllRemainingSetting: Boolean = false

  // the cache for recording the current value, current cpr and cprs in each dimension
  private var cprsCache: Array[SearchRange[_]] = null

  // the start dimension based on predicate
  private var minimumDimension: Int = -1

  // the end dimension based on predicate
  private var maximumDimension: Int = sizeOfDimension - 1

  // number of key columns in the predicate, if it is zero, we won't be able to optimize
  private var numOfKeyColumns: Int = -1

  /**
   * the model to hold the information for each dimension
   * @tparam T the data type of the dimension
   */
  private class SearchRange[T]() {
    // the current value in the dimension
    var currentValue: T = _

    // the current cpr in the dimension
    var currentCPR: CriticalPointRange[T] = null

    // the critical point range list in this dimension
    var cprs: Seq[CriticalPointRange[T]] = null

    // the data type of current dimension
    var dt: DataType = null

    /**
     * clear the cprs and current cpr
     */
    def clear() = {
      this.currentCPR = null
      this.cprs = null
    }

    /**
     * set the current value to the input value
     * @param input the input value
     */
    def setCurrentValue(input: Any) = {
      this.currentValue = input.asInstanceOf[T]
    }

    /**
     * compare the current value with the input value
     * @param input the input value
     * @return true if they are identical, otherwise false
     */
    def compareValue(input: Any): Boolean = {
      if (this.currentValue == input.asInstanceOf[T]) {
        true
      } else {
        false
      }
    }
  }

  /**
   * initialize the variables based on the relation and predicate,
   * we also initialize the cpr cache for each dimension
   */
  private def initialize() = {
    // size of the key column
    sizeOfDimension = relation.keyColumns.size

    // find the start / end dimension and number of key columns from the predicate
    minimumDimension = -1
    maximumDimension = -1
    var count: Int = 0
    val map = predExpr.references.toSeq.map(a => a.name)
    for (i <- 0 to relation.keyColumns.size - 1) {
      val key = relation.keyColumns(i).sqlName
      if (map.contains(key)) {
        count = count + 1
        if (minimumDimension == -1) {
          minimumDimension = i
        }
        if (i > maximumDimension) {
          maximumDimension = i
        }
      }
    }
    if (minimumDimension == -1) {
      minimumDimension = 0
    }
    if (maximumDimension == -1) {
      maximumDimension = sizeOfDimension - 1
    }

    // number of key columns in the predicates
    numOfKeyColumns = count

    // initialize cprs cache
    cprsCache = new Array[SearchRange[_]](maximumDimension + 1)
    for (i <- 0 to maximumDimension) {
      // set data type of each dimension wth an empty search range
      val dataType = relation.keyColumns(i).dataType.asInstanceOf[NativeType]
      cprsCache(i) = new SearchRange[dataType.JvmType]
      cprsCache(i).dt = dataType

      // dimension [0..minimum-1] will be full range of dimension since predicate does not
      // have any information
      if (i < minimumDimension) {
        val cpr = new CriticalPointRange[dataType.JvmType](None, false, None, false,
          dataType, predExpr)
        val qualifiedCPRanges: Seq[(CriticalPointRange[dataType.JvmType])] = Seq(cpr)
        cprsCache(i).asInstanceOf[SearchRange[dataType.JvmType]].cprs = qualifiedCPRanges
        cprsCache(i).asInstanceOf[SearchRange[dataType.JvmType]].currentCPR = cpr
      }
    }
  }

  /**
   * constructor method
   * @param relation the relation
   * @param predExpr the predicate
   */
  def this(relation: HBaseRelation, predExpr: Expression) = {
    this()
    this.relation = relation
    this.predExpr = predExpr
  }

  /**
   * construct the row key based on the current currentValue of each dimension,
   * from dimension 0 to the dimIndex
   */
  private def buildRowKey(dimIndex: Int): HBaseRawType = {
    var list: List[(HBaseRawType, DataType)] = List[(HBaseRawType, DataType)]()
    for (i <- 0 to dimIndex) {
      val dataType = cprsCache(i).dt
      val value = BytesUtils.create(dataType).toBytes(cprsCache(i).currentValue)
      list = list :+ (value, dataType)
    }
    HBaseKVHelper.encodingRawKeyColumns(list.toSeq)
  }

  /**
   * Given the input kv (cell), filter it out, or keep it, or give the next hint
   * the decision is based on input predicate
   */
  override def filterKeyValue(kv: Cell): ReturnCode = {
    // if it only has one key column, we won't do optimization;
    // if the predicate does not have key columns, we won't do it either.
    if (sizeOfDimension < 2 || numOfKeyColumns == 0) {
      ReturnCode.INCLUDE
    } else {
      // initialize the state
      currentDim = -1
      filterAllRemainingSetting = false

      currentRowKey = CellUtil.cloneRow(kv)
      currentValues = relation.nativeKeyConvert(Some(currentRowKey))
      for (i <- 0 to maximumDimension) {
        // find the first dimension that does not match to the currentValue in the cache
        // which means this dimension needs re-calculate the cprs and currentCPR
        if (i >= minimumDimension &&
          (cprsCache(i).currentCPR == null || !cprsCache(i).compareValue(currentValues(i)))) {
          if (currentDim == -1) {
            // set the current dimension to be the first dimension that has the difference
            currentDim = i
          }
        }
        // put the value into currentValue of each dimension
        cprsCache(i).setCurrentValue(currentValues(i))
      }

      // current value(s) was not changed by input value,
      // we do not need to re-calculate, always include this
      if (currentDim == -1) {
        nextReturnCode = ReturnCode.INCLUDE
      } else {
        // clear up the cprs from currentDim + 1 to maximumDimension,
        // because we want to keep the cprs in the current dimension for reusing its result
        for (i <- currentDim + 1 to maximumDimension) {
          cprsCache(i).clear()
        }

        // deal with other dimension, set default value first
        hasAdjusted = false

        // get the result in tuple
        val result = findNextHint(currentRowKey)
        nextReturnCode = result._1

        if (result._1 == ReturnCode.SEEK_NEXT_USING_HINT) {
          // construct the next hint
          nextKeyValue = new KeyValue(result._2, CellUtil.cloneFamily(kv),
            Array[Byte](), Array[Byte]())
        } else if (result._1 == ReturnCode.NEXT_ROW && currentDim == minimumDimension) {
          // if it is the minimum dimension and it is already skipped, which means we can
          // skip the remaining
          filterAllRemainingSetting = true
        }
      }
      nextReturnCode
    }
  }

  /**
   * find the next hint based on the input byte array (row key as byte array)
   * @param input the raw row key
   * @tparam Any the data type
   * @return the tuple (ReturnCode, the improved row key)
   */
  private def findNextHint[Any](input: HBaseRawType): (ReturnCode, HBaseRawType) = {
    // the stop condition is current dimension is already beyond the maximum dimension
    if (currentDim > maximumDimension) {
      // the last dimension
      if (hasAdjusted) {
        // if the input position has been changed
        (ReturnCode.SEEK_NEXT_USING_HINT, nextRowKey)
      } else {
        // the input position is included in the result
        (ReturnCode.INCLUDE, nextRowKey)
      }
    } else {
      // if in certain dimension the predicate associated with certain cpr is null,
      // we do not need to go further, it will always be included
      if (currentDim > 0 && cprsCache(currentDim - 1).currentCPR.pred == null) {
        (ReturnCode.INCLUDE, input)
      } else {
        if (cprsCache(currentDim).cprs == null) {
          generateCPRs()
        }
        // get the current value for this dimension
        val v = cprsCache(currentDim).currentValue
        // find its position in the cprs
        val result = findPositionInRanges(v)
        // if we cannot find a position in certain dimension,
        // then we have to skip the current input
        if (result._1 == null) {
          (ReturnCode.NEXT_ROW, input)
        } else if (result._1 == ReturnCode.INCLUDE) {
          // the input value is already within one of the cprs
          nextRowKey = buildRowKey(currentDim)
          currentDim = currentDim + 1
          findNextHint(nextRowKey)
        } else {
          // the input value has to be adjusted to fit into one of the cprs
          hasAdjusted = true
          nextRowKey = buildRowKey(currentDim)
          currentDim = currentDim + 1
          findNextHint(nextRowKey)
        }
      }
    }
  }

  /**
   * generate cprs for the current dimension
   */
  private def generateCPRs() = {
    // only generate cprs when current dimension is great than or equal to minimum dimension,
    // because the dimension(s) prior to minimum dimension are all full range
    if (currentDim >= minimumDimension) {
      val dt: NativeType = cprsCache(currentDim).dt.asInstanceOf[NativeType]
      type t = dt.JvmType

      // handle the first dimension, the predicate is the input
      if (currentDim == 0) {
        val keyDim = relation.partitionKeys(0)
        val criticalPoints: Seq[CriticalPoint[t]] = RangeCriticalPoint.collect(predExpr, keyDim)
        val predRefs = predExpr.references.toSeq
        val boundPred = BindReferences.bindReference(predExpr, predRefs)
        val row = new GenericMutableRow(predRefs.size)

        if (criticalPoints.nonEmpty) {
          // partial reduce
          val cpRanges: Seq[CriticalPointRange[t]] =
            RangeCriticalPoint.generateCriticalPointRange[t](criticalPoints, 0, dt)
          val keyIndex = predRefs.indexWhere(_.exprId == relation.partitionKeys(0).exprId)
          val qualifiedCPRanges = cpRanges.filter(cpr => {
            row.update(keyIndex, cpr)
            val prRes = boundPred.partialReduce(row, predRefs)
            if (prRes._1 == null) cpr.pred = prRes._2
            prRes._1 == null || prRes._1.asInstanceOf[Boolean]
          })
          cprsCache(0).asInstanceOf[SearchRange[t]].cprs = qualifiedCPRanges
        } else {
          // cpr is the whole range
          val cpr = new CriticalPointRange[t](None, false, None, false, dt, predExpr)
          val qualifiedCPRanges: Seq[(CriticalPointRange[t])] = Seq(cpr)
          cprsCache(0).asInstanceOf[SearchRange[t]].cprs = qualifiedCPRanges
          cprsCache(0).asInstanceOf[SearchRange[t]].currentCPR = cpr
        }
      } else {
        // the following dimension, the predicate is the previous one in the cpr
        val keyDim = relation.partitionKeys(currentDim)
        val newPredExpr = cprsCache(currentDim - 1).currentCPR.pred
        val criticalPoints: Seq[CriticalPoint[t]] = RangeCriticalPoint.collect(newPredExpr, keyDim)
        val predRefs = newPredExpr.references.toSeq
        val boundPred = BindReferences.bindReference(newPredExpr, predRefs)

        val row = new GenericMutableRow(predRefs.size)

        if (criticalPoints.nonEmpty) {
          // partial reduce
          val cpRanges: Seq[CriticalPointRange[t]] =
            RangeCriticalPoint.generateCriticalPointRange[t](criticalPoints, currentDim, dt)
          val keyIndex = predRefs.indexWhere(_.exprId == relation.partitionKeys(currentDim).exprId)
          val qualifiedCPRanges = cpRanges.filter(cpr => {
            for (i <- 0 to currentDim - 1) {
              val newKeyIndex = predRefs.indexWhere(_.exprId == relation.partitionKeys(i).exprId)
              if (newKeyIndex != -1) {
                row.update(newKeyIndex, cprsCache(i).currentValue)
              }
            }
            row.update(keyIndex, cpr)
            val prRes = boundPred.partialReduce(row, predRefs)
            if (prRes._1 == null) cpr.pred = prRes._2
            prRes._1 == null || prRes._1.asInstanceOf[Boolean]
          })
          cprsCache(currentDim).asInstanceOf[SearchRange[t]].cprs = qualifiedCPRanges
        } else {
          // cpr is the whole range
          val cpr = new CriticalPointRange[t](None, false, None, false, dt, predExpr)
          val qualifiedCPRanges: Seq[(CriticalPointRange[t])] = Seq(cpr)
          cprsCache(currentDim).asInstanceOf[SearchRange[t]].cprs = qualifiedCPRanges
          cprsCache(currentDim).asInstanceOf[SearchRange[t]].currentCPR = cpr
        }
      }
    }
  }

  /**
   * find the proper position in the critical point ranges list by
   * using binary search
   *
   * @param input the input currentRowKey in its data type
   * @return (null, null) if cannot find a position;
   *         (ReturnCode.INCLUDE, input) if the input is within a range;
   *         (ReturnCode.SEEK_NEXT_USING_HINT, output) if the next position is found
   */
  private def findPositionInRanges[T](input: T): (ReturnCode, T) = {
    val dt: NativeType = cprsCache(currentDim).dt.asInstanceOf[NativeType]
    val ordering = dt.ordering
    type t = dt.JvmType
    val cprs: Seq[CriticalPointRange[T]] = cprsCache(currentDim).asInstanceOf[SearchRange[T]].cprs

    // the low value is based on the current cpr, or 0 if current cpr is null
    var low: Int = {
      val currentCPR = cprsCache(currentDim).asInstanceOf[SearchRange[T]].currentCPR
      if (currentCPR == null) {
        0
      } else {
        cprs.indexOf(currentCPR)
      }
    }
    var high: Int = cprs.size - 1
    // the flag to exit the while loop
    var breakFlag: Boolean = false
    var result: Int = -1
    while (high >= low && !breakFlag) {
      val middle: Int = (low + high) / 2
      // get the compare result
      val compare: Int = compareWithinRange(dt, input, cprs, middle)
      if (compare == 0) {
        // find the value in the range
        result = middle
        breakFlag = true
      } else if (low != high) {
        if (compare == 1) {
          // increase the low value
          low = middle + 1
        } else if (compare == -1) {
          // decrease the high value
          high = middle - 1
        }
      } else {
        // if high equals low, means we have to stop search with no result
        breakFlag = true
      }
    }
    if (result == -1) {
      // no position found in the range
      (null, input)
    } else {
      val start: Option[T] = cprs(result).start
      val startInclusive: Boolean = cprs(result).startInclusive
      cprsCache(currentDim).asInstanceOf[SearchRange[T]].currentCPR = cprs(result)
      if (start.isEmpty ||
        (startInclusive && ordering.gteq(input.asInstanceOf[t], start.get.asInstanceOf[t])) ||
        (!startInclusive && ordering.gt(input.asInstanceOf[t], start.get.asInstanceOf[t]))) {
        // the input is within the range, does not have to adjust
        cprsCache(currentDim).asInstanceOf[SearchRange[T]].currentValue = input
        (ReturnCode.INCLUDE, input)
      } else {
        if (dt == StringType || startInclusive) {
          // have to adjust the input to the beginning of the range
          // for string, it will always be the beginning, even startInclusive is false
          cprsCache(currentDim).setCurrentValue(start.get)
          (ReturnCode.SEEK_NEXT_USING_HINT, start.get)
        } else {
          // for other data types, increase the start value by adding one bit
          // since its startInclusive is false
          val key = BytesUtils.addOne(DataTypeUtils.dataToBytes(start.get, dt))
          val value = DataTypeUtils.bytesToData(key, 0, key.length, dt).asInstanceOf[T]
          cprsCache(currentDim).setCurrentValue(value)
          (ReturnCode.SEEK_NEXT_USING_HINT, value)
        }
      }
    }
  }

  /**
   * compare the input with a range [(previousRange.end, range.start)]
   * @param dt the data type
   * @param input the input value
   * @param cprs the sequence of the ranges
   * @param index the indexed range to be compared
   * @tparam T the type template
   * @return 0 within the range, -1 less than the range, 1 great than the range
   */
  private def compareWithinRange[T](dt: NativeType, input: T,
                                    cprs: Seq[CriticalPointRange[T]], index: Int): Int = {
    val ordering = dt.ordering
    type t = dt.JvmType

    // make the range continuous, by calculating the start and startInclusive using
    // the previous cpr information
    val start: Option[T] = {
      if (index == 0) {
        None
      } else {
        cprs(index - 1).end
      }
    }
    val end: Option[T] = cprs(index).end
    val startInclusive: Boolean = {
      if (index == 0) {
        false
      } else {
        !cprs(index - 1).endInclusive
      }
    }
    val endInclusive: Boolean = cprs(index).endInclusive

    if (end.isEmpty) {
      // always in the range, [(Any, None)
      0
    } else if ((endInclusive && ordering.gt(input.asInstanceOf[t], end.get.asInstanceOf[t]))
      || (!endInclusive && ordering.gteq(input.asInstanceOf[t], end.get.asInstanceOf[t]))) {
      // (Any, _) where input > Any; or [Any, _] where input >= Any
      1
    } else if (start.isEmpty) {
      // always in the range (None, Any)], where input < or <= Any
      0
    } else if ((startInclusive && ordering.gteq(input.asInstanceOf[t], start.get.asInstanceOf[t]))
      || (!startInclusive && ordering.gt(input.asInstanceOf[t], start.get.asInstanceOf[t]))) {
      // [Any, _) where input >= Any; or (Any, _) where input > Any, in the range
      0
    } else {
      // all other cases, input < range
      -1
    }
  }

  /**
   * decide whether to skip all the remaining or not
   * @return
   */
  override def filterAllRemaining() = {
    filterAllRemainingSetting
  }

  /**
   * determine where to skip to if filterKeyValue() returns SEEK_NEXT_USING_HINT
   * @param currentKV the current key value
   * @return the next possible key value
   */
  override def getNextCellHint(currentKV: Cell): Cell = {
    nextKeyValue
  }

  /**
   * convert the relation / predicate to byte array, used by framework
   * @param dataOutput the output to write
   */
  override def write(dataOutput: DataOutput) = {
    val relationArray = Serializer.serialize(relation)
    Bytes.writeByteArray(dataOutput, relationArray)
    val predicateArray = Serializer.serialize(predExpr)
    Bytes.writeByteArray(dataOutput, predicateArray)
  }

  /**
   * convert byte array to relation / predicate, used by framework
   * @param dataInput the input to read
   */
  override def readFields(dataInput: DataInput) = {
    val relationArray = Bytes.readByteArray(dataInput)
    this.relation = Serializer.deserialize(relationArray).asInstanceOf[HBaseRelation]
    val predicateArray = Bytes.readByteArray(dataInput)
    this.predExpr = Serializer.deserialize(predicateArray).asInstanceOf[Expression]
    initialize()
  }

  override def toByteArray: Array[Byte] = {
    Writables.getBytes(this)
  }
}

object HBaseSkipScanFilter {
  def parseFrom(pbBytes: Array[Byte]): HBaseSkipScanFilter = {
    try {
      Writables.getWritable(pbBytes, new HBaseSkipScanFilter()).asInstanceOf[HBaseSkipScanFilter]
    } catch {
      case e: IOException => throw new DeserializationException(e)
    }
  }
}
