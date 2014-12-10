package org.apache.spark.sql.hbase

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor}
import org.apache.log4j.Logger

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

/**
 * CreateTableAndLoadData
 *
 */
trait CreateTableAndLoadData {

  private val logger = Logger.getLogger(getClass.getName)
  val DefaultStagingTableName = "StageTable"
  val DefaultTableName = "TestTable"
  val DefaultHbaseStagingTableName = s"Hb$DefaultStagingTableName"
  val DefaultHbaseTabName = s"Hb$DefaultTableName"
  val DefaultHbaseColFamiles = Seq("cf1", "cf2")

  val CsvPath = "src/test/resources"
  val DefaultLoadFile = s"$CsvPath/testTable.csv"

  var AvoidRowkeyBug = false

  def createTableAndLoadData(hbc: HBaseSQLContext) = {
    createTables(hbc, DefaultStagingTableName, DefaultTableName,
      DefaultHbaseStagingTableName, DefaultHbaseTabName)
    loadData(hbc, DefaultStagingTableName, DefaultTableName, DefaultLoadFile)
  }

  def createTables(hbc: HBaseSQLContext) : Unit = {
    createTables(hbc, DefaultStagingTableName, DefaultTableName,
      DefaultHbaseStagingTableName, DefaultHbaseTabName)
  }

  def createNativeHbaseTable(hbc: HBaseSQLContext, tableName: String, families: Seq[String]) = {
    val hbaseAdmin = hbc.catalog.hBaseAdmin
    val hdesc = new HTableDescriptor(TableName.valueOf(tableName))
    families.foreach { f => hdesc.addFamily(new HColumnDescriptor(f))}
    hbaseAdmin.createTable(hdesc)
  }

  def createTables(hbc: HBaseSQLContext, stagingTableName: String, tableName: String,
                   hbaseStagingTable: String, hbaseTable: String) = {

    val hbaseAdmin = hbc.catalog.hBaseAdmin
    if (!hbaseAdmin.tableExists(TableName.valueOf(hbaseStagingTable))) {
      createNativeHbaseTable(hbc, hbaseStagingTable, DefaultHbaseColFamiles)
    }
    if (!hbaseAdmin.tableExists(TableName.valueOf(hbaseTable))) {
      createNativeHbaseTable(hbc, hbaseTable, DefaultHbaseColFamiles)
    }

    if (hbc.catalog.checkLogicalTableExist(stagingTableName)) {
      val dropSql = s"drop table $stagingTableName"
      println(dropSql)
      val execsql = hbc.executeSql(dropSql)
      execsql.toRdd.collect().foreach(println)
    }

    if (hbc.catalog.checkLogicalTableExist(tableName)) {
      val dropSql = s"drop table $tableName"
      println(dropSql)
      val execsql = hbc.executeSql(dropSql)
      execsql.toRdd.collect().foreach(println)
    }

    val (stagingSql, tabSql) =
      ( s"""CREATE TABLE $stagingTableName(strcol STRING, bytecol String, shortcol String, intcol String,
            longcol string, floatcol string, doublecol string, PRIMARY KEY(doublecol, strcol, intcol))
            MAPPED BY ($hbaseStagingTable, COLS=[bytecol=cf1.hbytecol,
            shortcol=cf1.hshortcol, longcol=cf2.hlongcol, floatcol=cf2.hfloatcol])"""
        .stripMargin
        ,
        s"""CREATE TABLE $tableName(strcol STRING, bytecol BYTE, shortcol SHORT, intcol INTEGER,
            longcol LONG, floatcol FLOAT, doublecol DOUBLE, PRIMARY KEY(doublecol, strcol, intcol))
            MAPPED BY ($hbaseTable, COLS=[bytecol=cf1.hbytecol,
            shortcol=cf1.hshortcol, longcol=cf2.hlongcol, floatcol=cf2.hfloatcol])"""
          .stripMargin
        )
    println(stagingSql)
    var executeSql1 = hbc.executeSql(stagingSql)
    executeSql1.toRdd.collect().foreach(println)

    logger.debug(s"Created table $tableName: " +
      s"isTableAvailable= ${hbaseAdmin.isTableAvailable(s2b(hbaseStagingTable))}" +
      s" tableDescriptor= ${hbaseAdmin.getTableDescriptor(s2b(hbaseStagingTable))}")


    println(tabSql)
    executeSql1 = hbc.executeSql(tabSql)
    executeSql1.toRdd.collect().foreach(println)

  }

  def loadData(hbc: HBaseSQLContext, stagingTableName: String, tableName: String,
               loadFile: String) = {
    // then load data into table
    val hbaseAdmin = hbc.catalog.hBaseAdmin
    val loadSql = s"LOAD DATA LOCAL INPATH '$loadFile' INTO TABLE $tableName"
    val result3 = hbc.executeSql(loadSql).toRdd.collect()
    val insertSql = s"""insert into $tableName select cast(strcol as string),
    cast(bytecol as tinyint), cast(shortcol as smallint), cast(intcol as int),
    cast (longcol as bigint), cast(floatcol as float), cast(doublecol as double)
    from $stagingTableName"""
  }

  def s2b(s: String) = Bytes.toBytes(s)

}
