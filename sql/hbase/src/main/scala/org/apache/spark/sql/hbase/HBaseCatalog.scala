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

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.analysis.SimpleCatalog
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.hbase.HBaseCatalog._

import scala.collection.mutable.{HashMap, SynchronizedMap}

/**
 * Column represent the sql column
 * @param sqlName the name of the column
 * @param dataType the data type of the column
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

private[hbase] class HBaseCatalog(@transient hbaseContext: HBaseSQLContext)
  extends SimpleCatalog(false) with Logging with Serializable {

  lazy val configuration = hbaseContext.optConfiguration
    .getOrElse(HBaseConfiguration.create())

  {
    val hbaseAdmin = new HBaseAdmin(configuration)
    println(s"test catalog ${hbaseAdmin.tableExists("wf")}")
    println("hhahah")
  }


  lazy val relationMapCache = new HashMap[String, HBaseRelation]
    with SynchronizedMap[String, HBaseRelation]

  private def processTableName(tableName: String): String = {
    if (!caseSensitive) {
      tableName.toLowerCase
    } else {
      tableName
    }
  }

  private def createHBaseUserTable(tableName: String,
                                   allColumns: Seq[AbstractColumn]): Unit ={
    val hBaseAdmin = new HBaseAdmin(configuration)
    val tableDescriptor = new HTableDescriptor(tableName);
    allColumns.map(x =>
      if (x.isInstanceOf[NonKeyColumn]) {
        val nonKeyColumn = x.asInstanceOf[NonKeyColumn]
        tableDescriptor.addFamily(new HColumnDescriptor(nonKeyColumn.family))
      })
    hBaseAdmin.createTable(tableDescriptor);
  }

  def createTable(tableName: String, hbaseNamespace: String, hbaseTableName: String,
                  allColumns: Seq[AbstractColumn]): Unit = {
    if (checkLogicalTableExist(tableName)) {
      throw new Exception(s"The logical table: $tableName already exists")
    }

    // create a new hbase table for the user if not exist
    if (!checkHBaseTableExists(hbaseTableName)) {
      createHBaseUserTable(hbaseTableName, allColumns)
    }

    val nonKeyColumns = allColumns.filter(_.isInstanceOf[NonKeyColumn])
      .asInstanceOf[Seq[NonKeyColumn]]
    nonKeyColumns.foreach {
      case NonKeyColumn(_, _, family, _) =>
        if (!checkFamilyExists(hbaseTableName, family)) {
          throw new Exception(s"The HBase table doesn't contain the Column Family: $family")
        }
    }

    val admin = new HBaseAdmin(configuration)
    val avail = admin.isTableAvailable(MetaData)

    if (!avail) {
      // create table
      createMetadataTable(admin)
    }

    val table = new HTable(configuration, MetaData)
    table.setAutoFlushTo(false)

    val get = new Get(Bytes.toBytes(tableName))
    if (table.exists(get)) {
      throw new Exception(s"row key $tableName exists")
    }
    else {
      /*
      // construct key columns
      val result = new StringBuilder()
      for (column <- keyColumns) {
        result.append(column.sqlName)
        result.append(",")
        result.append(column.dataType.typeName)
        result.append(";")
      }
      put.add(ColumnFamily, QualKeyColumns, Bytes.toBytes(result.toString))

      // construct non-key columns
      result.clear()
      for (column <- nonKeyColumns) {
        result.append(column.sqlName)
        result.append(",")
        result.append(column.dataType.typeName)
        result.append(",")
        result.append(column.family)
        result.append(",")
        result.append(column.qualifier)
        result.append(";")
      }
      put.add(ColumnFamily, QualNonKeyColumns, Bytes.toBytes(result.toString))

      // construct all columns
      result.clear()
      for (column <- allColumns) {
        result.append(column.sqlName)
        result.append(",")
        result.append(column.dataType.typeName)
        result.append(";")
      }
      put.add(ColumnFamily, QualAllColumns, Bytes.toBytes(result.toString))

      // construct HBase table name and namespace
      result.clear()
      result.append(hbaseNamespace)
      result.append(",")
      result.append(hbaseTableName)
      put.add(ColumnFamily, QualHbaseName, Bytes.toBytes(result.toString))
      */

      val hbaseRelation = HBaseRelation(tableName, hbaseNamespace, hbaseTableName, allColumns)
      hbaseRelation.configuration = configuration

      writeObjectToTable(hbaseRelation)

      relationMapCache.put(processTableName(tableName), hbaseRelation)
    }
  }

  def alterTableDropNonKey(tableName: String, columnName: String) = {
    val result = getTable(tableName)
    if (result.isDefined) {
      val relation = result.get
      val allColumns = relation.allColumns.filter(!_.sqlName.equals(columnName))
      val hbaseRelation = HBaseRelation(relation.tableName,
        relation.hbaseNamespace, relation.hbaseTableName, allColumns)
      hbaseRelation.configuration = configuration

      writeObjectToTable(hbaseRelation)

      relationMapCache.put(processTableName(tableName), hbaseRelation)
    }
  }

  def alterTableAddNonKey(tableName: String, column: NonKeyColumn) = {
    val result = getTable(tableName)
    if (result.isDefined) {
      val relation = result.get
      val allColumns = relation.allColumns :+ column
      val hbaseRelation = HBaseRelation(relation.tableName,
        relation.hbaseNamespace, relation.hbaseTableName, allColumns)
      hbaseRelation.configuration = configuration

      writeObjectToTable(hbaseRelation)

      relationMapCache.put(processTableName(tableName), hbaseRelation)
    }
  }

  private def writeObjectToTable(hbaseRelation: HBaseRelation) = {
    val tableName = hbaseRelation.tableName
    val table = new HTable(configuration, MetaData)

    val put = new Put(Bytes.toBytes(tableName))
    val byteArrayOutputStream = new ByteArrayOutputStream()
    val objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)
    objectOutputStream.writeObject(hbaseRelation)

    put.add(ColumnFamily, QualData, byteArrayOutputStream.toByteArray)

    // write to the metadata table
    table.put(put)
    table.flushCommits()
    table.close()
  }

  def getTable(tableName: String): Option[HBaseRelation] = {
    var result = relationMapCache.get(processTableName(tableName))
    if (result.isEmpty) {
      val table = new HTable(configuration, MetaData)

      val get = new Get(Bytes.toBytes(tableName))
      val values = table.get(get)
      table.close()
      if (values == null || values.isEmpty) {
        result = None
      } else {
        /*
        // get HBase table name and namespace
        val hbaseName = Bytes.toString(values.getValue(ColumnFamily, QualHbaseName))
        val hbaseNameArray = hbaseName.split(",")
        val hbaseNamespace = hbaseNameArray(0)
        val hbaseTableName = hbaseNameArray(1)

        // get all of the columns
        var allColumns = Bytes.toString(values.getValue(ColumnFamily, QualAllColumns))
        if (allColumns.length > 0) {
          allColumns = allColumns.substring(0, allColumns.length - 1)
        }
        val allColumnArray = allColumns.split(";")
        var allColumnList = List[KeyColumn]()
        for (allColumn <- allColumnArray) {
          val index = allColumn.indexOf(",")
          val sqlName = allColumn.substring(0, index)
          val dataType = getDataType(allColumn.substring(index + 1))
          val column = KeyColumn(sqlName, dataType)
          allColumnList = allColumnList :+ column
        }

        // get the key columns
        var keyColumns = Bytes.toString(values.getValue(ColumnFamily, QualKeyColumns))
        if (keyColumns.length > 0) {
          keyColumns = keyColumns.substring(0, keyColumns.length - 1)
        }
        val keyColumnArray = keyColumns.split(";")
        var keyColumnList = List[KeyColumn]()
        for (keyColumn <- keyColumnArray) {
          val index = keyColumn.indexOf(",")
          val sqlName = keyColumn.substring(0, index)
          val dataType = getDataType(keyColumn.substring(index + 1))
          val column = KeyColumn(sqlName, dataType)
          keyColumnList = keyColumnList :+ column
        }

        // get the non-key columns
        var nonKeyColumns = Bytes.toString(values.getValue(ColumnFamily, QualNonKeyColumns))
        if (nonKeyColumns.length > 0) {
          nonKeyColumns = nonKeyColumns.substring(0, nonKeyColumns.length - 1)
        }
        var nonKeyColumnList = List[NonKeyColumn]()
        val nonKeyColumnArray = nonKeyColumns.split(";")
        for (nonKeyColumn <- nonKeyColumnArray) {
          val nonKeyColumnInfo = nonKeyColumn.split(",")
          val sqlName = nonKeyColumnInfo(0)
          val dataType = getDataType(nonKeyColumnInfo(1))
          val family = nonKeyColumnInfo(2)
          val qualifier = nonKeyColumnInfo(3)

          val column = NonKeyColumn(sqlName, dataType, family, qualifier)
          nonKeyColumnList = nonKeyColumnList :+ column
        }
        */
        val value = values.getValue(ColumnFamily, QualData)
        val byteArrayInputStream = new ByteArrayInputStream(value)
        val objectInputStream = new ObjectInputStream(byteArrayInputStream)
        val hbaseRelation: HBaseRelation
        = objectInputStream.readObject().asInstanceOf[HBaseRelation]
        hbaseRelation.configuration = configuration
        result = Some(hbaseRelation)
      }
    }
    result
  }

  override def lookupRelation(namespace: Option[String],
                              tableName: String,
                              alias: Option[String] = None): LogicalPlan = {
    val hbaseRelation = getTable(tableName)
    if (hbaseRelation.isEmpty) {
      throw new IllegalArgumentException(
        s"Table $namespace:$tableName does not exist in the catalog")
    }
    hbaseRelation.get
  }

  def deleteTable(tableName: String): Unit = {
    if (!checkLogicalTableExist(tableName)) {
      throw new IllegalStateException(s"The logical table $tableName does not exist")
    }
    val table = new HTable(configuration, MetaData)

    val delete = new Delete((Bytes.toBytes(tableName)))
    table.delete(delete)
    table.close()

    relationMapCache.remove(processTableName(tableName))
  }

  def createMetadataTable(admin: HBaseAdmin) = {
    val descriptor = new HTableDescriptor(TableName.valueOf(MetaData))
    val columnDescriptor = new HColumnDescriptor(ColumnFamily)
    descriptor.addFamily(columnDescriptor)
    admin.createTable(descriptor)
  }

  private[hbase] def checkHBaseTableExists(hbaseTableName: String): Boolean = {
    val admin = new HBaseAdmin(configuration)
    admin.tableExists(hbaseTableName)
  }

  private[hbase] def checkLogicalTableExist(tableName: String): Boolean = {
    val admin = new HBaseAdmin(configuration)
    if (!admin.tableExists(MetaData)) {
      // create table
      createMetadataTable(admin)
    }

    val table = new HTable(configuration, MetaData)
    val get = new Get(Bytes.toBytes(tableName))
    val result = table.get(get)

    result.size() > 0
  }

  private[hbase] def checkFamilyExists(hbaseTableName: String, family: String): Boolean = {
    val admin = new HBaseAdmin(configuration)
    val tableDescriptor = admin.getTableDescriptor(TableName.valueOf(hbaseTableName))
    tableDescriptor.hasFamily(Bytes.toBytes(family))
  }

  def getDataType(dataType: String): DataType = {
    if (dataType.equalsIgnoreCase(StringType.typeName)) {
      StringType
    } else if (dataType.equalsIgnoreCase(ByteType.typeName)) {
      ByteType
    } else if (dataType.equalsIgnoreCase(ShortType.typeName)) {
      ShortType
    } else if (dataType.equalsIgnoreCase(IntegerType.typeName)) {
      IntegerType
    } else if (dataType.equalsIgnoreCase(LongType.typeName)) {
      LongType
    } else if (dataType.equalsIgnoreCase(FloatType.typeName)) {
      FloatType
    } else if (dataType.equalsIgnoreCase(DoubleType.typeName)) {
      DoubleType
    } else if (dataType.equalsIgnoreCase(BooleanType.typeName)) {
      BooleanType
    } else {
      throw new IllegalArgumentException(s"Unrecognized data type: $dataType")
    }
  }
}

object HBaseCatalog {
  private final val MetaData = "metadata"
  private final val ColumnFamily = Bytes.toBytes("colfam")
  //  private final val QualKeyColumns = Bytes.toBytes("keyColumns")
  //  private final val QualNonKeyColumns = Bytes.toBytes("nonKeyColumns")
  //  private final val QualHbaseName = Bytes.toBytes("hbaseName")
  //  private final val QualAllColumns = Bytes.toBytes("allColumns")
  private final val QualData = Bytes.toBytes("data")
}
