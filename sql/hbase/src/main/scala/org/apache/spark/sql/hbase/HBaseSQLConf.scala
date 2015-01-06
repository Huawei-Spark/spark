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

import org.apache.spark.sql.SQLConf

private[hbase] object HBaseSQLConf {
  val PARTITION_EXPIRATION = "spark.sql.hbase.partition"
  val COMPRESS_CACHED = "spark.sql.inMemoryColumnarStorage.compressed"
  val COLUMN_BATCH_SIZE = "spark.sql.inMemoryColumnarStorage.batchSize"
  val IN_MEMORY_PARTITION_PRUNING = "spark.sql.inMemoryColumnarStorage.partitionPruning"
  val AUTO_BROADCASTJOIN_THRESHOLD = "spark.sql.autoBroadcastJoinThreshold"
  val DEFAULT_SIZE_IN_BYTES = "spark.sql.defaultSizeInBytes"
  val SHUFFLE_PARTITIONS = "spark.sql.shuffle.partitions"
  val CODEGEN_ENABLED = "spark.sql.codegen"
  val DIALECT = "spark.sql.dialect"

  val PARQUET_BINARY_AS_STRING = "spark.sql.parquet.binaryAsString"
  val PARQUET_CACHE_METADATA = "spark.sql.parquet.cacheMetadata"
  val PARQUET_COMPRESSION = "spark.sql.parquet.compression.codec"
  val PARQUET_FILTER_PUSHDOWN_ENABLED = "spark.sql.parquet.filterPushdown"

  val COLUMN_NAME_OF_CORRUPT_RECORD = "spark.sql.columnNameOfCorruptRecord"

  // Options that control which operators can be chosen by the query planner.  These should be
  // considered hints and may be ignored by future versions of Spark SQL.
  val EXTERNAL_SORT = "spark.sql.planner.externalSort"

  // This is only used for the thriftserver
  val THRIFTSERVER_POOL = "spark.sql.thriftserver.scheduler.pool"
}

/**
 * A trait that enables the setting and getting of mutable config parameters/hints.
 *
 */
private[hbase] trait HBaseSQLConf extends SQLConf {
  import org.apache.spark.sql.hbase.HBaseSQLConf._

  /** The expiration of cached partition (i.e., region) info; defaults to 10 minutes . */
  private[spark] def partitionExpiration: Long = getConf(PARTITION_EXPIRATION, "600").toLong
}
