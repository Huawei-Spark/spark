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

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.log4j.Logger
import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.analysis.SimpleCatalog
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.hbasesource.HBaseCatalog._

import scala.collection.mutable.{ArrayBuffer, HashMap, ListBuffer, SynchronizedMap}

/**
 * Column represent the sql column
 * sqlName the name of the column
 * dataType the data type of the column
 */
sealed abstract class AbstractColumn extends Serializable {
  val sqlName: String
  val dataType: DataType
  var ordinal: Int = -1

  def isKeyColumn(): Boolean = false

  override def toString: String = {
    s"$sqlName , $dataType.typeName"
  }
}

case class KeyColumn(val sqlName: String, val dataType: DataType, val order: Int)
  extends AbstractColumn {
  override def isKeyColumn() = true
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

private[hbasesource] class HBaseCatalog(@transient hbaseContext: HBaseSQLContext)
  extends SimpleCatalog(false) with Logging with Serializable {

  lazy val logger = Logger.getLogger(getClass.getName)
  lazy val configuration = hbaseContext.optConfiguration
    .getOrElse(HBaseConfiguration.create())

  lazy val relationMapCache = new HashMap[String, HBaseRelation]
    with SynchronizedMap[String, HBaseRelation]

  lazy val admin = new HBaseAdmin(configuration)

  private def processTableName(tableName: String): String = {
    if (!caseSensitive) {
      tableName.toLowerCase
    } else {
      tableName
    }
  }

  //Todo: This function is used to fake the rowkey. Just for test purpose
  def makeRowKey(row: Row, dataTypeOfKeys: Seq[DataType]) = {
    //    val row = new GenericRow(Array(col7, col1, col3))
    val rawKeyCol = dataTypeOfKeys.zipWithIndex.map {
      case (dataType, index) => {
        (DataTypeUtils.getRowColumnFromHBaseRawType(row, index, dataType, new BytesUtils),
          dataType)
      }
    }

    val buffer = ListBuffer[Byte]()
    HBaseKVHelper.encodingRawKeyColumns(buffer, rawKeyCol)
  }

  // Use a single HBaseAdmin throughout this instance instad of creating a new one in
  // each method
  var hBaseAdmin = new HBaseAdmin(configuration)
  logger.debug(s"HBaseAdmin.configuration zkPort="
    + s"${hBaseAdmin.getConfiguration.get("hbase.zookeeper.property.clientPort")}")

  private def createHBaseUserTable(tableName: String,
                                   allColumns: Seq[AbstractColumn]): Unit = {
    val tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))
    allColumns.map(x =>
      if (x.isInstanceOf[NonKeyColumn]) {
        val nonKeyColumn = x.asInstanceOf[NonKeyColumn]
        tableDescriptor.addFamily(new HColumnDescriptor(nonKeyColumn.family))
      })

    //    val splitKeys: Array[Array[Byte]] = Array(
    //        new GenericRow(Array(1024.0, "Upen", 128: Short)),
    //        new GenericRow(Array(1024.0, "Upen", 256: Short)),
    //        new GenericRow(Array(4096.0, "SF", 512: Short))
    //      ).map(makeRowKey(_, Seq(DoubleType, StringType, ShortType)))
    //    hBaseAdmin.createTable(tableDescriptor, splitKeys);

    admin.createTable(tableDescriptor, null);
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

    val avail = admin.isTableAvailable(MetaData)

    if (!avail) {
      // create table
      createMetadataTable()
    }

    val table = new HTable(configuration, MetaData)
    table.setAutoFlushTo(false)

    val get = new Get(Bytes.toBytes(tableName))
    if (table.exists(get)) {
      throw new Exception(s"row key $tableName exists")
    }
    else {
      val hbaseRelation = HBaseRelation(tableName, hbaseNamespace, hbaseTableName,
        allColumns)
      hbaseRelation.setConfig(configuration)

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
      hbaseRelation.setConfig(configuration)

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
      hbaseRelation.setConfig(configuration)

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
        result = Some(getRelationFromResult(values))
      }
    }
    result
  }

  private def getRelationFromResult(result: Result): HBaseRelation = {
    val value = result.getValue(ColumnFamily, QualData)
    val byteArrayInputStream = new ByteArrayInputStream(value)
    val objectInputStream = new ObjectInputStream(byteArrayInputStream)
    val hbaseRelation: HBaseRelation
    = objectInputStream.readObject().asInstanceOf[HBaseRelation]
    hbaseRelation.setConfig(configuration)
    hbaseRelation
  }

  def getAllTableName(): Seq[String] = {
    val tables = new ArrayBuffer[String]()
    val table = new HTable(configuration, MetaData)
    val scanner = table.getScanner(ColumnFamily)
    var result = scanner.next()
    while (result != null) {
      val relation = getRelationFromResult(result)
      tables.append(relation.tableName)
      result = scanner.next()
    }
    tables.toSeq
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

  def createMetadataTable() = {
    val descriptor = new HTableDescriptor(TableName.valueOf(MetaData))
    val columnDescriptor = new HColumnDescriptor(ColumnFamily)
    descriptor.addFamily(columnDescriptor)
    admin.createTable(descriptor)
  }

  private[hbasesource] def checkHBaseTableExists(hbaseTableName: String): Boolean = {
    admin.tableExists(hbaseTableName)
  }

  private[hbasesource] def checkLogicalTableExist(tableName: String): Boolean = {
    if (!admin.tableExists(MetaData)) {
      // create table
      createMetadataTable()
    }

    val table = new HTable(configuration, MetaData)
    val get = new Get(Bytes.toBytes(tableName))
    val result = table.get(get)

    result.size() > 0
  }

  private[hbasesource] def checkFamilyExists(hbaseTableName: String, family: String): Boolean = {
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
    } else if (dataType.equalsIgnoreCase(IntegerType.typeName) ||
      dataType.equalsIgnoreCase("int")) {
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
