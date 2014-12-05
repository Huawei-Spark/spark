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

import java.io._
import scala.Some

import org.apache.hadoop.hbase.{HColumnDescriptor, TableName, HTableDescriptor, HBaseConfiguration}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

import org.apache.spark.sql.catalyst.types.DataType
import org.apache.spark.Logging

/**
 * Column represent the sql column
 * sqlName the name of the column
 * dataType the data type of the column
 */
sealed abstract class AbstractColumn {
  val sqlName: String
  val dataType: DataType

  def isKeyColum(): Boolean = false

  override def toString: String = {
    s"$sqlName , $dataType.typeName"
  }
}

case class KeyColumn(val sqlName: String, val dataType: DataType, val order: Int)
  extends AbstractColumn {
  override def isKeyColum() = true
}

case class NonKeyColumn(
                         val sqlName: String,
                         val dataType: DataType,
                         val family: String,
                         val qualifier: String) extends AbstractColumn {
  @transient lazy val familyRaw = Bytes.toBytes(family)
  @transient lazy val qualifierRaw = Bytes.toBytes(qualifier)

  override def toString = {
    s"$sqlName , $dataType.typeName , $family:$qualifier"
  }
}

private[hbasesource] class HBaseMetadata extends Logging with Serializable {

  lazy val configuration = HBaseConfiguration.create()

  lazy val admin = new HBaseAdmin(configuration)

  logDebug(s"HBaseAdmin.configuration zkPort="
    + s"${admin.getConfiguration.get("hbase.zookeeper.property.clientPort")}")

  private def createHBaseUserTable(tableName: String, allColumns: Seq[AbstractColumn]) {
    val tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))
    allColumns.map(x =>
      if (x.isInstanceOf[NonKeyColumn]) {
        val nonKeyColumn = x.asInstanceOf[NonKeyColumn]
        tableDescriptor.addFamily(new HColumnDescriptor(nonKeyColumn.family))
      })

    admin.createTable(tableDescriptor, null);
  }

  def createTable(
      tableName: String,
      hbaseTableName: String,
      allColumns: Seq[AbstractColumn]) = {
    // create a new hbase table for the user if not exist
    if (!checkHBaseTableExists(hbaseTableName)) {
      createHBaseUserTable(hbaseTableName, allColumns)
    }
    // check hbase table contain all the families
    val nonKeyColumns = allColumns.filter(_.isInstanceOf[NonKeyColumn])
    nonKeyColumns.foreach {
      case NonKeyColumn(_, _, family, _) =>
        if (!checkFamilyExists(hbaseTableName, family)) {
          throw new Exception(s"The HBase table doesn't contain the Column Family: $family")
        }
    }

    HBaseRelation(tableName, "", hbaseTableName, allColumns, Some(configuration))
  }

  private[hbasesource] def checkHBaseTableExists(hbaseTableName: String): Boolean = {
    admin.tableExists(hbaseTableName)
  }

  private[hbasesource] def checkFamilyExists(hbaseTableName: String, family: String): Boolean = {
    val tableDescriptor = admin.getTableDescriptor(TableName.valueOf(hbaseTableName))
    tableDescriptor.hasFamily(Bytes.toBytes(family))
  }
}
