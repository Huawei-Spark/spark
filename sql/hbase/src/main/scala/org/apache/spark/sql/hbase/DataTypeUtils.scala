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
import org.apache.spark.sql.catalyst.expressions.{MutableRow, Row}
import org.apache.spark.sql.catalyst.types._

/**
 * Data Type conversion utilities
 *
 */
object DataTypeUtils {
  //  TODO: more data types support?
  def setRowColumnFromHBaseRawType(row: MutableRow,
                                   index: Int,
                                   src: HBaseRawType,
                                   dt: DataType,
                                   bu: BytesUtils): Any = {
    dt match {
      case StringType => row.setString(index, bu.toString(src))
      case IntegerType => row.setInt(index, bu.toInt(src))
      case BooleanType => row.setBoolean(index, bu.toBoolean(src))
      case ByteType => row.setByte(index, src(0))
      case DoubleType => row.setDouble(index, bu.toDouble(src))
      case FloatType => row.setFloat(index, bu.toFloat(src))
      case LongType => row.setLong(index, bu.toLong(src))
      case ShortType => row.setShort(index, bu.toShort(src))
      case _ => throw new Exception("Unsupported HBase SQL Data Type")
    }
  }

  def getRowColumnFromHBaseRawType(row: Row,
                                   index: Int,
                                   dt: DataType,
                                   bu: BytesUtils): HBaseRawType = {
    dt match {
      case StringType => bu.toBytes(row.getString(index))
      case IntegerType => bu.toBytes(row.getInt(index))
      case BooleanType => bu.toBytes(row.getBoolean(index))
      case ByteType => bu.toBytes(row.getByte(index))
      case DoubleType => bu.toBytes(row.getDouble(index))
      case FloatType => bu.toBytes(row.getFloat(index))
      case LongType => bu.toBytes(row.getLong(index))
      case ShortType => bu.toBytes(row.getShort(index))
      case _ => throw new Exception("Unsupported HBase SQL Data Type")
    }
  }
}
