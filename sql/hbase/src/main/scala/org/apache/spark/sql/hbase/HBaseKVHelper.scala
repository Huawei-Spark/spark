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

import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.catalyst.expressions.{Row, Attribute}
import org.apache.spark.sql.catalyst.types._

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object HBaseKVHelper {
  private val delimiter: Byte = 0

  /**
   * create row key based on key columns information
   * @param buffer an input buffer
   * @param rawKeyColumns sequence of byte array and data type representing the key columns
   * @return array of bytes
   */
  def encodingRawKeyColumns(buffer: ListBuffer[Byte],
                            rawKeyColumns: Seq[(HBaseRawType, DataType)]): HBaseRawType = {
    var listBuffer = buffer
    listBuffer.clear()
    for (rawKeyColumn <- rawKeyColumns) {
      listBuffer = listBuffer ++ rawKeyColumn._1
      if (rawKeyColumn._2 == StringType) {
        listBuffer += delimiter
      }
    }
    listBuffer.toArray
  }

//  /**
//   * get the sequence of key columns from the byte array
//   * @param buffer an input buffer
//   * @param rowKey array of bytes
//   * @param keyColumns the sequence of key columns
//   * @return sequence of byte array
//   */
//  def decodingRawKeyColumns(buffer: ListBuffer[HBaseRawType], arrayBuffer: ArrayBuffer[Byte],
//                            rowKey: HBaseRawType, keyColumns: Seq[KeyColumn]): Seq[HBaseRawType] = {
//    buffer.clear()
//    var index = 0
//    for (keyColumn <- keyColumns) {
//      arrayBuffer.clear()
//      val dataType = keyColumn.dataType
//      if (dataType == StringType) {
//        while (index < rowKey.length && rowKey(index) != delimiter) {
//          arrayBuffer += rowKey(index)
//          index = index + 1
//        }
//        index = index + 1
//      }
//      else {
//        val length = NativeType.defaultSizeOf(dataType.asInstanceOf[NativeType])
//        for (i <- 0 to (length - 1)) {
//          arrayBuffer += rowKey(index)
//          index = index + 1
//        }
//      }
//      buffer += arrayBuffer.toArray
//    }
//    buffer.toSeq
//  }

  def decodingRawKeyColumns(rowKey: HBaseRawType, keyColumns: Seq[KeyColumn]): Seq[(Int, Int)] = {
    var index = 0
    keyColumns.map {
      case c => {
        if (index >= rowKey.length) (-1, -1)
        else {
          val start = index
          if (c.dataType == StringType) {
            val pos = rowKey.indexOf(delimiter, index)
            index = pos + 1
            (start, pos - start)
          } else {
            val length = NativeType.defaultSizeOf(c.dataType.asInstanceOf[NativeType])
            index += length
            (start, length)
          }
        }
      }
    }
  }

  /**
   * Takes a record, translate it into HBase row key column and value by matching with metadata
   * @param values record that as a sequence of string
   * @param relation HBaseRelation
   * @param keyBytes  output parameter, array of (key column and its type);
   * @param valueBytes array of (column family, column qualifier, value)
   */
  def string2KV(values: Seq[String],
                relation: HBaseRelation,
                lineBuffer: Array[BytesUtils],
                keyBytes: Array[(Array[Byte], DataType)],
                valueBytes: Array[(Array[Byte], Array[Byte], Array[Byte])]) = {
    assert(values.length == relation.output.length,
      s"values length ${values.length} not equals columns length ${relation.output.length}")

    relation.keyColumns.foreach(kc => {
      val ordinal = kc.ordinal
      keyBytes(kc.order) = (string2Bytes(values(ordinal), lineBuffer(ordinal)),
        relation.output(ordinal).dataType)
    })
    for (i <- 0 until relation.nonKeyColumns.size) {
      val nkc = relation.nonKeyColumns(i)
      val family = nkc.familyRaw
      val qualifier = nkc.qualifierRaw
      val bytes = string2Bytes(values(nkc.ordinal), lineBuffer(nkc.ordinal))
      valueBytes(i) = (family, qualifier, bytes)
    }
  }

  private def string2Bytes(v: String, bu: BytesUtils): Array[Byte] = {
    bu.dataType match {
      // todo: handle some complex types
      case BooleanType => bu.toBytes(v.toBoolean)
      case ByteType => bu.toBytes(v)
      case DoubleType => bu.toBytes(v.toDouble)
      case FloatType => bu.toBytes(v.toFloat)
      case IntegerType => bu.toBytes(v.toInt)
      case LongType => bu.toBytes(v.toLong)
      case ShortType => bu.toBytes(v.toShort)
      case StringType => bu.toBytes(v)
    }
  }

  /**
   * create a array of buffer that to be used for creating HBase Put object
   * @param schema the schema of the line buffer
   * @return
   */
  private[hbase] def createLineBuffer(schema: Seq[Attribute]): Array[BytesUtils] = {
    val buffer = ArrayBuffer[BytesUtils]()
    schema.foreach { x =>
      buffer.append(BytesUtils.create(x.dataType))
    }
    buffer.toArray
  }

  /**
   * create a row key
   * @param row the generic row
   * @param dataTypeOfKeys sequence of data type
   * @return the row key
   */
  def makeRowKey(row: Row, dataTypeOfKeys: Seq[DataType]): HBaseRawType = {
    val rawKeyCol = dataTypeOfKeys.zipWithIndex.map {
      case (dataType, index) =>
        (DataTypeUtils.getRowColumnFromHBaseRawType(row, index, dataType), dataType)
    }

    val buffer = ListBuffer[Byte]()
    encodingRawKeyColumns(buffer, rawKeyCol)
  }
}

