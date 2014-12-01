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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HTable
import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LeafNode

import scala.collection.JavaConverters._

import scala.collection.mutable.ArrayBufferg

private[hbase] case class HBaseRelation(
    tableName: String,
    hbaseNamespace: String,
    hbaseTableName: String,
    allColumns: Seq[AbstractColumn],
   @transient optConfiguration: Option[Configuration] = None)
  extends LeafNode {

  @transient lazy val logger = Logger.getLogger(getClass.getName)

  @transient lazy val keyColumns = allColumns.filter(_.isInstanceOf[KeyColumn])
    .asInstanceOf[Seq[KeyColumn]].sortBy(_.order)

  @transient lazy val nonKeyColumns = allColumns.filter(_.isInstanceOf[NonKeyColumn])
    .asInstanceOf[Seq[NonKeyColumn]]

  @transient lazy val columnMap = allColumns.map {
    case key: KeyColumn => (key.sqlName, key.order)
    case nonKey: NonKeyColumn => (nonKey.sqlName, nonKey)
  }.toMap

  // Read the configuration from (a) the serialized version if available
  //  (b) the constructor parameter if available
  //  (c) otherwise create a default one using HBaseConfiguration.create
  private var serializedConfiguration: Array[Byte] = optConfiguration.map
    { conf => Util.serializeHBaseConfiguration(conf)}.orNull
  @transient private var config: Configuration = _

  def configuration() = getConf()

  private def getConf(): Configuration = {
    if (config == null) {
      config = if (serializedConfiguration != null) {
        Util.deserializeHBaseConfiguration(serializedConfiguration)
      } else {
        optConfiguration.getOrElse {
          HBaseConfiguration.create
        }
      }
    }
    config
  }

  logger.debug(s"HBaseRelation config has zkPort="
    + s"${getConf.get("hbase.zookeeper.property.clientPort")}")

  @transient lazy val htable: HTable = new HTable(getConf, hbaseTableName)

  lazy val attributes = nonKeyColumns.map(col =>
    AttributeReference(col.sqlName, col.dataType, nullable = true)())

  //  lazy val colFamilies = nonKeyColumns.map(_.family).distinct
  //  lazy val applyFilters = false

  def isNonKey(attr: AttributeReference): Boolean = {
    attributes.exists(_.exprId == attr.exprId)
  }

  def closeHTable() = htable.close

  val output: Seq[Attribute] = {
    allColumns.map {
      case column =>
        (partitionKeys union attributes).find(_.name == column.sqlName).get
    }
  }

  def keyIndex(attr: AttributeReference): Int = {
    // -1 if nonexistent
    partitionKeys.indexWhere(_.exprId == attr.exprId)
  }

  lazy val partitions: Seq[HBasePartition] = {
    val regionLocations = htable.getRegionLocations.asScala.toSeq
    log.info(s"Number of HBase regions for " +
      s"table ${htable.getName.getNameAsString}: ${regionLocations.size}")
    regionLocations.zipWithIndex.map {
      case p =>
        new HBasePartition(
          p._2, p._2,
          Some(p._1._1.getStartKey),
          Some(p._1._1.getEndKey),
          Some(p._1._2.getHostname))
    }
  }

  @transient lazy val partitionKeys: Seq[AttributeReference] = keyColumns.map(col =>
    AttributeReference(col.sqlName, col.dataType, nullable = false)())

  /**
   * Return the start keys of all of the regions in this table,
   * as a list of SparkImmutableBytesWritable.
   */
  def getRegionStartKeys() = {
    val byteKeys: Array[Array[Byte]] = htable.getStartKeys
    val ret = ArrayBuffer[ImmutableBytesWritableWrapper]()
    for (byteKey <- byteKeys) {
      ret += new ImmutableBytesWritableWrapper(byteKey)
    }
    ret
  }
}
