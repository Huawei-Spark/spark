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
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.types._

import scala.collection.mutable.ArrayBuffer

/**
 * Helper class for scanning files stored in Hadoop - e.g., to read text file when bulk loading.
 */
private[hbase]
class HadoopReader(@transient sc: SparkContext, @transient job: Job,
                   path: String)(columns: Seq[AbstractColumn]) {
  // make RDD[(SparkImmutableBytesWritable, SparkKeyValue)] from text file
  private[hbase] def makeBulkLoadRDDFromTextFile = {

    val rdd = sc.textFile(path)
    val splitRegex = sc.getConf.get("spark.sql.hbase.bulkload.textfile.splitRegex", ",")
    // use to fix serialize issue
    val cls = columns
    // Todo: use mapPartitions more better
    rdd.map { line =>
      val (keyBytes, valueBytes) = HadoopReader.string2KV(line, splitRegex, cls)
      val rowKeyData = HadoopReader.encodingRawKeyColumns(keyBytes)
      val rowKey = new ImmutableBytesWritableWrapper(rowKeyData)
      val put = new PutWrapper(rowKeyData)
      valueBytes.foreach { case (family, qualifier, value) =>
        put.add(family, qualifier, value)
      }
      (rowKey, put)
    }
  }
}

object HadoopReader {
  /**
   * create row key based on key columns information
   * @param rawKeyColumns sequence of byte array representing the key columns
   * @return array of bytes
   */
  def encodingRawKeyColumns(rawKeyColumns: Seq[(HBaseRawType, DataType)]): HBaseRawType = {
    var buffer = ArrayBuffer[Byte]()
    val delimiter: Byte = 0
    var index = 0
    for (rawKeyColumn <- rawKeyColumns) {
      buffer = buffer ++ rawKeyColumn._1
      if (rawKeyColumn._2 == StringType) {
        buffer += delimiter
      }
      index = index + 1
    }
    buffer.toArray
  }


  def string2KV(value: String, splitRegex: String, columns: Seq[AbstractColumn]):
  (Seq[(Array[Byte], DataType)], Seq[(Array[Byte], Array[Byte], Array[Byte])]) = {
    val keyBytes = new ArrayBuffer[(Array[Byte], DataType)]()
    val valueBytes = new ArrayBuffer[(Array[Byte], Array[Byte], Array[Byte])]()
    value.split(splitRegex).zip(columns).foreach { case (value, column) =>
      val bytes = string2Bytes(value, column.dataType)
      if (column.isKeyColum()) {
        keyBytes += ((bytes, column.dataType))
      } else {
        val realCol = column.asInstanceOf[NonKeyColumn]
        valueBytes += ((Bytes.toBytes(realCol.family), Bytes.toBytes(realCol.qualifier), bytes))
      }
    }
    (keyBytes, valueBytes)
  }

  def string2Bytes(v: String, dataType: DataType): Array[Byte] = dataType match {
    // todo: handle some complex types
    case ArrayType(elemType, _) => Bytes.toBytes(v)
    case StructType(fields) => Bytes.toBytes(v)
    case MapType(keyType, valueType, _) => Bytes.toBytes(v)
    case BinaryType => Bytes.toBytes(v)
    case BooleanType => Bytes.toBytes(v.toBoolean)
    case ByteType => Bytes.toBytes(v)
    case DoubleType => Bytes.toBytes(v.toDouble)
    case FloatType => Bytes.toBytes((v.toFloat))
    case IntegerType => Bytes.toBytes(v.toInt)
    case LongType => Bytes.toBytes(v.toLong)
    case ShortType => Bytes.toBytes(v.toShort)
    case StringType => Bytes.toBytes(v)
    case DecimalType => Bytes.toBytes(v)
    case DateType => Bytes.toBytes(v)
    case TimestampType => Bytes.toBytes(v)
    case NullType => Bytes.toBytes(v)
  }
}
