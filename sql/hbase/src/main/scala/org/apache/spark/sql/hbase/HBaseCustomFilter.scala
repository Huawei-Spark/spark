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

import org.apache.hadoop.hbase.{KeyValue, CellUtil, Cell}
import org.apache.hadoop.hbase.exceptions.DeserializationException
import org.apache.hadoop.hbase.filter.Filter.ReturnCode
import org.apache.hadoop.hbase.filter.FilterBase
import org.apache.hadoop.hbase.util.{Bytes, Writables}
import org.apache.hadoop.io.Writable
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.hbase.util.{HBaseKVHelper, DataTypeUtils, BytesUtils}
import org.apache.spark.sql.types.{DataType, NativeType, StringType}
import org.apache.spark.sql.hbase.catalyst.expressions.PartialPredicateOperations._

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
private[hbase] class HBaseCustomFilter extends FilterBase with Writable {

  private class Node() {
    // the current selected node index with cpr
    var currentNodeIndex: Int = 0

    // the current value of this dimension
    var currentValue: Any = null

    // the data type of this dimension
    var dt: NativeType = null

    // the cpr associated with this node, at root level, the cpr will be null
    var cpr: CriticalPointRange[_] = null

    // the current dimension index
    var dimension: Int = -1

    // the parent node reference
    var parent: Node = null

    // the child nodes associated with this dimension
    var children: Seq[Node] = null
  }

  private var relation: HBaseRelation = null
  private var predExpr: Expression = null
  private var predReferences: Seq[Attribute] = null
  private var boundPredicate: Expression = null
  private var predicateMap: Seq[(String, Int)] = null

  // the root node reference
  private var root: Node = null

  // the current node in the iteration
  private var currentNode: Node = null

  // the current row key
  private var currentRowKey: HBaseRawType = null

  // the current row key values
  private var currentValues: Seq[Any] = null

  // the next hint
  private var nextReturnCode: ReturnCode = null

  // the next key hint
  private var nextKeyValue: Cell = null

  // the next possible row key
  private var nextRowKey: HBaseRawType = null

  // the flag to determine whether to filter the remaining or not
  private var filterAllRemainingSetting: Boolean = false

  // flag of the position moved or not
  private var hasSeeked: Boolean = false

  // flag of row change
  private var nextColFlag: Boolean = false

  // flag of filter row
  private var filterRowFlag: Boolean = false

  // the remaining predicate
  private var remainingPredicate: Expression = null

  // the working row
  private var workingRow: GenericMutableRow = null

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

  /**
   * initialize the variables based on the relation and predicate,
   * we also initialize the cpr cache for each dimension
   */
  private def initialize() = {
    predReferences = predExpr.references.toSeq
    // remainingRow = new GenericMutableRow(predReferences.size)
    workingRow = new GenericMutableRow(predReferences.size)
    boundPredicate = BindReferences.bindReference(predExpr, predReferences)
    predicateMap = predReferences.map(a => a.name).zipWithIndex

    root = new Node()
    val dataType = relation.keyColumns.head.dataType.asInstanceOf[NativeType]
    root.dt = dataType
    root.dimension = 0

    filterAllRemainingSetting = false
    generateCPRs(root)
  }

  /**
   * reset the current value and current index of each level
   * @param level the start level, it will also reset its children
   * @param dim the level to start to reset
   * @param start the current level, used by this recursive call
   */
  def resetEachLevel(level: Node, dim: Int, start: Int): Unit = {
    if (start >= dim) {
      level.currentNodeIndex = 0
      level.currentValue = null
    }
    if (level.children != null) {
      for (item <- level.children) {
        resetEachLevel(item, dim, start + 1)
      }
    }
  }

  /**
   * Given the input kv (cell), filter it out, or keep it, or give the next hint
   * the decision is based on input predicate
   */
  override def filterKeyValue(kv: Cell): ReturnCode = {
    if (!nextColFlag) {
      // reset the index of each level
      currentRowKey = CellUtil.cloneRow(kv)
      val inputValues = relation.nativeKeyConvert(Some(currentRowKey))
      if (currentValues == null) {
        resetEachLevel(root, 0, 0)
      } else {
        var dim = -1
        for (i <- inputValues.indices if dim >= 0) {
          if (inputValues(i) != currentValues(i)) {
            dim = i
          }
        }
        resetEachLevel(root, dim, 0)
      }

      nextColFlag = true

      hasSeeked = false
      currentNode = root
      currentValues = inputValues
      currentNode.currentValue = currentValues.head

      val result = findNextHint()
      nextReturnCode = result._1
      if (nextReturnCode == ReturnCode.SEEK_NEXT_USING_HINT) {
        nextRowKey = result._2
        nextKeyValue = new KeyValue(nextRowKey, CellUtil.cloneFamily(kv),
          Array[Byte](), Array[Byte]())
      } else if (nextReturnCode == ReturnCode.SKIP) {
        filterAllRemainingSetting = true
      }
    }

    nextReturnCode
  }

  /**
   * find the proper position in the critical point ranges list by
   * using binary search
   *
   * @param input the input currentRowKey in its data type
   * @param node the node be considered
   * @return (null, null) if cannot find a position;
   *         (ReturnCode.INCLUDE, input) if the input is within a range;
   *         (ReturnCode.SEEK_NEXT_USING_HINT, output) if the next position is found
   */
  private def findPositionInRanges[T](input: T, node: Node): (ReturnCode, T) = {
    val dt: NativeType = node.dt
    val ordering = dt.ordering
    type t = dt.JvmType
    val cprs: Seq[CriticalPointRange[T]] =
      node.children.map(item => item.cpr.asInstanceOf[CriticalPointRange[T]])

    var low: Int = node.currentNodeIndex
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
      node.currentNodeIndex = result
      if (start.isEmpty ||
        (startInclusive && ordering.gteq(input.asInstanceOf[t], start.get.asInstanceOf[t])) ||
        (!startInclusive && ordering.gt(input.asInstanceOf[t], start.get.asInstanceOf[t]))) {
        // the input is within the range, does not have to adjust
        (ReturnCode.INCLUDE, input)
      } else {
        if (dt == StringType || startInclusive) {
          // have to adjust the input to the beginning of the range
          // for string, it will always be the beginning, even startInclusive is false
          node.currentValue = start.get
          (ReturnCode.SEEK_NEXT_USING_HINT, start.get)
        } else {
          // for other data types, increase the start value by adding one bit
          // since its startInclusive is false
          val key = BytesUtils.addOne(DataTypeUtils.dataToBytes(start.get, dt))
          val value = DataTypeUtils.bytesToData(key, 0, key.length, dt).asInstanceOf[T]
          node.currentValue = value
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
   * find the next hint based on the input byte array (row key as byte array)
   * @tparam Any the data type
   * @return the tuple (ReturnCode, the improved row key)
   */
  private def findNextHint[Any](): (ReturnCode, HBaseRawType) = {
    // find the position for the current value in the dimension
    val result = findPositionInRanges(currentNode.currentValue, currentNode)
    if (result._1 == ReturnCode.SEEK_NEXT_USING_HINT) {
      nextRowKey = buildRowKey(currentNode)
      (ReturnCode.SEEK_NEXT_USING_HINT, nextRowKey)
    } else if (result._1 == ReturnCode.INCLUDE) {
      if (currentNode.dimension == relation.keyColumns.size - 1) {
        if (hasSeeked) {
          nextRowKey = buildRowKey(currentNode)
          (ReturnCode.SEEK_NEXT_USING_HINT, nextRowKey)
        } else {
          remainingPredicate = currentNode.children(currentNode.currentNodeIndex).cpr.pred
          (ReturnCode.INCLUDE, nextRowKey)
        }
      } else {
        val dimension = currentNode.dimension + 1
        currentNode = currentNode.children(currentNode.currentNodeIndex)
        currentNode.dimension = dimension
        currentNode.dt = relation.keyColumns(dimension).dataType.asInstanceOf[NativeType]

        val parent = currentNode.parent
        parent.children(parent.currentNodeIndex).cpr.pred
        if (parent.children(parent.currentNodeIndex).cpr.pred == null) {
          (ReturnCode.INCLUDE, nextRowKey)
        } else {
          if (currentNode.children == null) {
            generateCPRs(currentNode)
          }

          var exit: Boolean = false
          if (hasSeeked) {
            if (currentNode.children != null && currentNode.children.nonEmpty) {
              val start = currentNode.children.head.cpr.start
              if (start.isDefined) {
                currentNode.currentValue = start.get
                currentNode.currentNodeIndex = 0
              } else {
                nextRowKey = buildRowKey(currentNode.parent)
                exit = true
              }
            } else {
              var levelNode = parent
              var found = false
              while (levelNode != null && !found) {
                if (levelNode.currentNodeIndex < levelNode.children.length - 1) {
                  found = true
                  levelNode.currentNodeIndex = levelNode.currentNodeIndex + 1
                  levelNode.currentValue =
                    levelNode.children(levelNode.currentNodeIndex).cpr.start.get
                } else {
                  levelNode = levelNode.parent
                }
              }
              if (found) {
                nextRowKey = buildRowKey(levelNode)
                currentNode = levelNode
              } else {
                nextRowKey = buildRowKey(parent)
                currentNode = parent
              }
              exit = true
            }
          } else {
            currentNode.currentValue = currentValues(dimension)
          }
          if (exit) {
            (ReturnCode.SEEK_NEXT_USING_HINT, nextRowKey)
          } else {
            findNextHint()
          }
        }
      }
    } else {
      // cannot find a position
      currentNode = currentNode.parent
      if (currentNode == null) {
        (ReturnCode.SKIP, null)
      } else {
        val dt = currentNode.dt
        val value = currentNode.currentValue
        var canAddOne: Boolean = true
        if (dt == StringType) {
          val newString = BytesUtils.addOneString(BytesUtils.create(dt).toBytes(value))
          val newValue = DataTypeUtils.bytesToData(newString, 0, newString.length, dt)
          currentNode.currentValue = newValue
        } else {
          val newArray = BytesUtils.addOne(BytesUtils.create(dt).toBytes(value))
          if (newArray == null) {
            canAddOne = false
          } else {
            val newValue = DataTypeUtils.bytesToData(newArray, 0, newArray.length, dt)
            currentNode.currentValue = newValue
          }
        }
        if (canAddOne) {
          hasSeeked = true
          findNextHint()
        } else {
          (ReturnCode.NEXT_ROW, nextRowKey)
        }
      }
    }
  }

  override def reset() = {
    nextColFlag = false
    filterRowFlag = false
  }

  /**
   * reset all the value in the row to be null
   * @param row the row to be reset
   */
  private def resetRow(row: GenericMutableRow) = {
    // reset the row
    for (i <- 0 to row.length - 1) {
      row.update(i, null)
    }
  }

  /**
   * construct the row key based on the current currentValue of each dimension,
   * from dimension 0 to the dimIndex
   */
  private def buildRowKey(node: Node): HBaseRawType = {
    var list: List[(HBaseRawType, DataType)] = List[(HBaseRawType, DataType)]()
    var levelNode = node
    do {
      val dt = levelNode.dt
      val value = BytesUtils.create(dt).toBytes(levelNode.currentValue)
      list = (value, dt) +: list
      levelNode = levelNode.parent
    } while (levelNode != null)
    HBaseKVHelper.encodingRawKeyColumns(list.toSeq)
  }

  /**
   * generate cprs for the current dimension
   */
  private def generateCPRs(node: Node) = {
    val dt: NativeType = node.dt
    type t = dt.JvmType
    val index = node.dimension
    if (node.parent == null) {
      // this is root node
      val keyDim = relation.partitionKeys.head
      val criticalPoints: Seq[CriticalPoint[t]] = RangeCriticalPoint.collect(predExpr, keyDim)
      val predRefs = predExpr.references.toSeq
      val boundPred = BindReferences.bindReference(predExpr, predRefs)

      resetRow(workingRow)

      if (criticalPoints.nonEmpty) {
        // partial reduce
        val cpRanges: Seq[CriticalPointRange[t]] =
          RangeCriticalPoint.generateCriticalPointRange[t](criticalPoints, 0, dt)
        val keyIndex = predRefs.indexWhere(_.exprId == relation.partitionKeys.head.exprId)
        val qualifiedCPRanges = cpRanges.filter(cpr => {
          workingRow.update(keyIndex, cpr)
          val prRes = boundPred.partialReduce(workingRow, predRefs)
          if (prRes._1 == null) cpr.pred = prRes._2
          prRes._1 == null || prRes._1.asInstanceOf[Boolean]
        })

        node.children = qualifiedCPRanges.map(range => {
          val child = new Node()
          child.dimension = index + 1
          child.cpr = range
          child.parent = node
          child
        })
      } else {
        val cpr = new CriticalPointRange[t](None, false, None, false, dt, predExpr)
        val qualifiedCPRanges: Seq[(CriticalPointRange[t])] = Seq(cpr)
        node.children = qualifiedCPRanges.map(range => {
          val child = new Node()
          child.dimension = index + 1
          child.cpr = range
          child.parent = node
          child
        })
      }
    }
    else {
      // the following dimension, the predicate is the previous one in the cpr
      val keyDim = relation.partitionKeys(node.dimension)
      val parent = node.parent
      val newPredExpr = parent.children(parent.currentNodeIndex).cpr.pred
      val criticalPoints: Seq[CriticalPoint[t]] = RangeCriticalPoint.collect(newPredExpr, keyDim)
      val predRefs = newPredExpr.references.toSeq
      val boundPred = BindReferences.bindReference(newPredExpr, predRefs)

      resetRow(workingRow)

      if (criticalPoints.nonEmpty) {
        // partial reduce
        val cpRanges: Seq[CriticalPointRange[t]] =
          RangeCriticalPoint.generateCriticalPointRange[t](criticalPoints, index, dt)
        val keyIndex = predRefs.indexWhere(_.exprId == relation.partitionKeys(index).exprId)
        val qualifiedCPRanges = cpRanges.filter(cpr => {
          var levelNode = root
          for (i <- 0 to index - 1) {
            val value = levelNode.currentValue
            levelNode = levelNode.children(levelNode.currentNodeIndex)
            val newKeyIndex = predRefs.indexWhere(_.exprId == relation.partitionKeys(i).exprId)
            if (newKeyIndex != -1) {
              workingRow.update(newKeyIndex, value)
            }
          }
          workingRow.update(keyIndex, cpr)
          //val prRes = boundPred.partialReduce(row, predRefs)
          val prRes = boundPred.partialReduce(workingRow, predRefs)
          if (prRes._1 == null) cpr.pred = prRes._2
          prRes._1 == null || prRes._1.asInstanceOf[Boolean]
        })
        node.children = qualifiedCPRanges.map(range => {
          val child = new Node()
          child.dimension = index + 1
          child.cpr = range
          child.parent = node
          child
        })
      } else {
        val cpr = new CriticalPointRange[t](None, false, None, false, dt, node.cpr.pred)
        val qualifiedCPRanges: Seq[(CriticalPointRange[t])] = Seq(cpr)
        node.children = qualifiedCPRanges.map(range => {
          val child = new Node()
          child.dimension = index + 1
          child.cpr = range
          child.parent = node
          child
        })
      }
    }
  }

  /**
   * do a full evaluation for the remaining predicate based on all the cell values
   * @param kvs the list of cell
   */
  def fullEvalution(kvs: java.util.List[Cell]) = {
    resetRow(workingRow)

    var cellMap: Map[NonKeyColumn, Any] = Map[NonKeyColumn, Any]()
    for (i <- 0 to kvs.size() - 1) {
      val item = kvs.get(i)
      val family = CellUtil.cloneFamily(item)
      val qualifier = CellUtil.cloneQualifier(item)
      val data = CellUtil.cloneValue(item)
      val nkc = relation.nonKeyColumns.find(a =>
        Bytes.compareTo(a.familyRaw, family) == 0 &&
          Bytes.compareTo(a.qualifierRaw, qualifier) == 0).get
      if (data.nonEmpty) {
        val value = DataTypeUtils.bytesToData(data, 0, data.length, nkc.dataType)
        cellMap += (nkc -> value)
      }
    }
    for (item <- remainingPredicate.references.toSeq) {
      relation.columnMap.get(item.name).get match {
        case nkc: NonKeyColumn =>
          val result = predicateMap.find(a => a._1 == nkc.sqlName).get
          val value = cellMap.get(nkc)
          if (value.isDefined) {
            workingRow.update(result._2, value.get)
          }
        case keyColumn: Int =>
          val keyIndex =
            predReferences.indexWhere(_.exprId == relation.partitionKeys(keyColumn).exprId)
          workingRow.update(keyIndex, currentValues(keyColumn))
      }
    }

    val result =
      BindReferences.bindReference(remainingPredicate, predReferences).eval(workingRow)
    if (result != null && result.asInstanceOf[Boolean]) {
      filterRowFlag = false
    } else {
      filterRowFlag = true
    }
  }

  override def filterRowCells(kvs: java.util.List[Cell]) = {
    if (remainingPredicate != null) {
      fullEvalution(kvs)
    }
  }

  override def hasFilterRow(): Boolean = {
    if (remainingPredicate != null) true else false
  }

  override def filterRow(): Boolean = {
    filterRowFlag
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
}

object HBaseCustomFilter {
  def parseFrom(pbBytes: Array[Byte]): HBaseCustomFilter = {
    try {
      Writables.getWritable(pbBytes, new HBaseCustomFilter()).asInstanceOf[HBaseCustomFilter]
    } catch {
      case e: IOException => throw new DeserializationException(e)
    }
  }
}
