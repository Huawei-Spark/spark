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
import org.apache.spark.sql.catalyst.types._

import scala.collection.mutable
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

  /**
   * get the sequence of key columns from the byte array
   * @param buffer an input buffer
   * @param rowKey array of bytes
   * @param keyColumns the sequence of key columns
   * @return sequence of byte array
   */
  def decodingRawKeyColumns(buffer: ListBuffer[HBaseRawType],
                            rowKey: HBaseRawType, keyColumns: Seq[KeyColumn]): Seq[HBaseRawType] = {
    var listBuffer = buffer
    listBuffer.clear()
    var arrayBuffer = ArrayBuffer[Byte]()
    var index = 0
    for (keyColumn <- keyColumns) {
      arrayBuffer.clear()
      val dataType = keyColumn.dataType
      if (dataType == StringType) {
        while (index < rowKey.length && rowKey(index) != delimiter) {
          arrayBuffer += rowKey(index)
          index = index + 1
        }
        index = index + 1
      }
      else {
        val length = NativeType.defaultSizeOf(dataType.asInstanceOf[NativeType])
        for (i <- 0 to (length - 1)) {
          arrayBuffer += rowKey(index)
          index = index + 1
        }
      }
      listBuffer += arrayBuffer.toArray
    }
    listBuffer.toSeq
  }

  /**
   * Takes a record, translate it into HBase row key column and value by matching with metadata
   * @param values record that as a sequence of string
   * @param columns metadata that contains KeyColumn and NonKeyColumn
   * @param keyBytes  output paramater, array of (key column and its type);
   * @param valueBytes array of (column family, column qualifier, value)
   */
  def string2KV(values: Seq[String],
                columns: Seq[AbstractColumn],
                keyBytes: ListBuffer[(Array[Byte], DataType)],
                valueBytes: ListBuffer[(Array[Byte], Array[Byte], Array[Byte])]) = {
    assert(values.length == columns.length,
      s"values length ${values.length} not equals columns length ${columns.length}")
    keyBytes.clear()
    valueBytes.clear()
    val map = mutable.HashMap[Int, (Array[Byte], DataType)]()
    var index = 0
    for (i <- 0 until values.length) {
      val value = values(i)
      val column = columns(i)
      val bytes = string2Bytes(value, column.dataType, new BytesUtils)
      if (column.isKeyColumn()) {
        map(column.asInstanceOf[KeyColumn].order) = ((bytes, column.dataType))
        index = index + 1
      } else {
        val realCol = column.asInstanceOf[NonKeyColumn]
        valueBytes += ((Bytes.toBytes(realCol.family), Bytes.toBytes(realCol.qualifier), bytes))
      }
    }

    (0 until index).foreach(k => keyBytes += map.get(k).get)
    keyBytes
  }

  private def string2Bytes(v: String, dataType: DataType, bu: BytesUtils): Array[Byte] = {
    dataType match {
      // todo: handle some complex types
      case BooleanType => bu.toBytes(v.toBoolean)
      case ByteType => bu.toBytes(v)
      case DoubleType => bu.toBytes(v.toDouble)
      case FloatType => bu.toBytes((v.toFloat))
      case IntegerType => bu.toBytes(v.toInt)
      case LongType => bu.toBytes(v.toLong)
      case ShortType => bu.toBytes(v.toShort)
      case StringType => bu.toBytes(v)
    }
  }
}

