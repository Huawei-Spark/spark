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

import org.apache.spark.Partition
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.types.BinaryType
import org.apache.spark.sql.hbasesource.catalyst.types.Range

private[hbasesource] class HBasePartition(
    val idx: Int, val mappedIndex: Int,
    val keyPartialEvalIndex: Int = -1,
    val lowerBound: Option[HBaseRawType] = None,
    val upperBound: Option[HBaseRawType] = None,
    val server: Option[String] = None,
    val filterPred: Option[Expression] = None) extends Range[HBaseRawType](lowerBound, true,
               upperBound, false, BinaryType) with Partition with IndexMappable {

  override def index: Int = idx

  override def hashCode(): Int = idx
}
