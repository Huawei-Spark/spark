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

import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.types._

import scala.collection.mutable.ArrayBuffer

/**
 * Helper class for scanning files stored in Hadoop - e.g., to read text file when bulk loading.
 */
private[hbase]
class HadoopReader(
    @transient sc: SparkContext,
    @transient job: Job,
    path: String,
    delimiter: Option[String])(columns: Seq[AbstractColumn]) {
  // make RDD[(SparkImmutableBytesWritable, SparkKeyValue)] from text file
  private[hbase] def makeBulkLoadRDDFromTextFile = {

    val rdd = sc.textFile(path)
    val splitRegex = delimiter.getOrElse(",")
    // use to fix serialize issue
    val cls = columns
    // Todo: use mapPartitions more better
    val buffer = ArrayBuffer[Byte]()
    val keyBytes = ArrayBuffer[(Array[Byte], DataType)]()
    val valueBytes = ArrayBuffer[(Array[Byte], Array[Byte], Array[Byte])]()
    rdd.map { line =>
      HBaseKVHelper.string2KV(line.split(splitRegex), cls, keyBytes, valueBytes)
      val rowKeyData = HBaseKVHelper.encodingRawKeyColumns(buffer, keyBytes)
      val rowKey = new ImmutableBytesWritableWrapper(rowKeyData)
      val put = new PutWrapper(rowKeyData)
      valueBytes.foreach { case (family, qualifier, value) =>
        put.add(family, qualifier, value)
      }
      (rowKey, put)
    }
  }
}
