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

import java.text.DecimalFormat

import org.apache.commons.codec.binary.Hex
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.{HTable, Scan}
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.hbase.util.HBaseKVHelper
import org.apache.spark.sql.types.IntegerType

import scala.collection.JavaConversions._


/**
 * Query test suite against HBase mini-cluster.
 */
class FourDimensionQueriesTestSuite extends HBaseIntegrationTestBase {

  private val tableName = "four_dims"
  private val hbaseTableName = "four_dims_htable"
  private val hbaseFamilies = Seq("f")

  private val csvPaths = Array("src/test/resources", "sql/hbase/src/test/resources")
  private val csvFile = "fourDimensions.txt"
  private val tpath = for (csvPath <- csvPaths if new java.io.File(csvPath).exists()) yield {
    logInfo(s"Following path exists $csvPath\n")
    csvPath
  }
  private[hbase] val csvPath = tpath(0)

  override protected def beforeAll() = {
    val hbaseAdmin = TestHbase.hbaseAdmin

    /**
     * create hbase table if it does not exists
     */
    if (!hbaseAdmin.tableExists(TableName.valueOf(hbaseTableName))) {
      val tblDescriptor = new HTableDescriptor(TableName.valueOf(hbaseTableName))
      val colDescriptor = new HColumnDescriptor(hbaseFamilies(0))
      colDescriptor.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF)
      tblDescriptor.addFamily(colDescriptor)
      try {
        hbaseAdmin.createTable(tblDescriptor, splitKeys)
      } catch {
        case e: TableExistsException =>
          logError(s"Table already exists $tableName", e)
      }
    }

    /**
     * drop the existing logical table if it exists
     */
    if (TestHbase.catalog.checkLogicalTableExist(tableName)) {
      val dropSql = "DROP TABLE " + tableName
      try {
        runSql(dropSql)
      } catch {
        case e: IllegalStateException =>
          logError(s"Error occurs while dropping the table $tableName", e)
      }
    }

    /**
     * create table
     */
    val ddl =
      s"""CREATE TABLE four_dims (
        d1 INTEGER,
        d2 INTEGER,
        d3 INTEGER,
        d4 INTEGER,
        v1 BOOLEAN,
        v2 STRING,
        v3 STRING,
        v4 INTEGER,
        PRIMARY KEY(d1,d2,d3,d4))
        MAPPED BY
        (four_dims_htable, COLS=[
         v1=f.v1,
         v2=f.v2,
         v3=f.v3,
         v4=f.v4])""".stripMargin

    try {
      runSql(ddl)
    } catch {
      case e: IllegalStateException =>
        logError(s"Error occurs while creating the table $tableName", e)
    }

    /**
     * load the data
     */
    val loadSql = "LOAD DATA LOCAL INPATH '" + s"$csvPath/$csvFile" +
      "' INTO TABLE four_dims"
    try {
      runSql(loadSql)
    } catch {
      case e: IllegalStateException =>
        logError(s"Error occurs while loading the data $tableName", e)
    }


    // Print region stats and verify data distribution across 4 regions.
    printTableStats(hbaseTableName, hbaseFamilies(0))
  }

  override protected def afterAll() = {
    runSql("DROP TABLE " + tableName)
  }


  test("Query (-1)") {
    val sql = "SELECT * FROM four_dims"
    val rows = runSql(sql)
    //printRows(sql, rows)
    assert(rows.size == 256)
  }

  test("Query 00") {
    val sql = "SELECT count(1) FROM four_dims"
    val rows = runSql(sql)
    assert(rows(0).get(0) == 256)
  }

  test("Query 01.0") {
    val sql =
      s"""SELECT d1,d2,d3,d4,v2
         |FROM four_dims
         |WHERE d1 = 1 OR d1 = 2"""
        .stripMargin

    val rows = runSql(sql)
    printRows(sql, rows)
    assert(rows.size == 128)
  }


  // TODO fails: returns 64 rows, instead of 128
  test("Query 01.10") {
    val sql =
      s"""SELECT d1,d2,d3,d4,v2
         |FROM four_dims
         |WHERE d1 >= 1 AND d1 <= 2"""
        .stripMargin
    val rows = runSql(sql)
    printRows(sql, rows)
    assert(rows.size == 128)
  }

  // TODO fails: returns 64 rows, instead of 128
  test("Query 01.11") {
    val sql =
      s"""SELECT d1,d2,d3,d4,v2
         |FROM four_dims
         |WHERE d1 >= 2 AND d1 <= 3"""
        .stripMargin
    val rows = runSql(sql)
    printRows(sql, rows)
    assert(rows.size == 128)
  }

  // TODO fails: returns 32, instead of 64
  test("Query 01.12") {
    val sql =
      s"""SELECT d1,d2,d3,d4,v2
         |FROM four_dims
         |WHERE d1 >= 2 AND d1 <= 3
         |AND (d2 = 3 OR d2 = 4)"""
        .stripMargin
    val rows = runSql(sql)
    printRows(sql, rows)
    assert(rows.size == 64)
  }

  // TODO fails: returns 16, instead of 64
  test("Query 01.13") {
    val sql =
      s"""SELECT d1,d2,d3,d4,v2
         |FROM four_dims
         |WHERE d1 >= 2 AND d1 <= 3
         |AND (d3 = 3 OR d3 = 4)"""
        .stripMargin
    val rows = runSql(sql)
    printRows(sql, rows)
    assert(rows.size == 64)
  }

  // TODO fails: returns 16, instead of 64
  test("Query 01.14") {
    val sql =
      s"""SELECT d1,d2,d3,d4,v2
         |FROM four_dims
         |WHERE d1 >= 2 AND d1 <= 3
         |AND (d4 = 1 OR d4 = 4)"""
        .stripMargin
    val rows = runSql(sql)
    printRows(sql, rows)
    assert(rows.size == 64)
  }

  test("Query 01.20") {
    val sql =
      s"""SELECT d1,d2,d3,d4,v2
         |FROM four_dims
         |WHERE d2 >= 1 AND d2 <= 2"""
        .stripMargin
    val rows = runSql(sql)
    printRows(sql, rows)
    assert(rows.size == 128)
  }

  test("Query 01.21") {
    val sql =
      s"""SELECT d1,d2,d3,d4,v2
         |FROM four_dims
         |WHERE d2 >= 1 AND d2 <= 2
         |AND (d3 = 2 OR d3 = 4)"""
        .stripMargin
    val rows = runSql(sql)
    printRows(sql, rows)
    assert(rows.size == 64)
  }

  test("Query 01.22") {
    val sql =
      s"""SELECT d1,d2,d3,d4,v2
         |FROM four_dims
         |WHERE d2 >= 1 AND d2 <= 2
         |AND (d3 = 2 OR d4 = 4)"""
        .stripMargin
    val rows = runSql(sql)
    printRows(sql, rows)
    assert(rows.size == 56)
  }

  test("Query 01.23") {
    val sql =
      s"""SELECT d1,d2,d3,d4,v2
         |FROM four_dims
         |WHERE d2 >= 1 AND d2 <= 2
         |AND (d1 = 2 OR d1 = 4)"""
        .stripMargin
    val rows = runSql(sql)
    printRows(sql, rows)
    assert(rows.size == 64)
  }

  // TODO fails: returns 80, instead of 96
  test("Query 01.24") {
    val sql =
      s"""SELECT d1,d2,d3,d4,v3
         |FROM four_dims
         |WHERE d2 >= 1 AND d2 <= 2
         |AND ((d1 = 2 OR d1 = 4) OR (d3 = 1 OR d3 = 2))"""
        .stripMargin
    val rows = runSql(sql)
    printRows(sql, rows)
    assert(rows.size == 96)
  }

  // TODO fails: returns 32, instead of 112
  test("Query 01.25") {
    val sql =
      s"""SELECT d1,d2,d3,d4,v4
         |FROM four_dims
         |WHERE d2 >= 2 AND d2 <= 3
         |AND ((d1 >= 3 OR d1 = 1) OR (d3 <= 1 OR d3 = 4))"""
        .stripMargin
    val rows = runSql(sql)
    printRows(sql, rows)
    assert(rows.size == 112)
  }

  // TODO fails: returns 8, instead of 28
  test("Query 01.26") {
    val sql =
      s"""SELECT d1,d2,d3,d4,v4
         |FROM four_dims
         |WHERE d2 >= 2 AND d2 <= 3
         |AND ((d1 >= 3 OR d1 = 1) OR (d3 <= 1 OR d3 = 3))
         |AND d4 = 2"""
        .stripMargin
    val rows = runSql(sql)
    printRows(sql, rows)
    assert(rows.size == 28)
  }

  // TODO fails: returns 2, instead of 14
  test("Query 01.27") {
    val sql =
      s"""SELECT d1,d2,d3,d4,v1,v4
         |FROM four_dims
         |WHERE d2 >= 2 AND d2 <= 3
         |AND ((d1 >= 3 OR d1 = 1) OR (d3 <= 1 OR d3 = 3))
         |AND d4 >= 4
         |AND v1 = true"""
        .stripMargin
    val rows = runSql(sql)
    printRows(sql, rows)
    assert(rows.size == 14)
  }

  // TODO fails: returns 20, instead of 24
  test("Query 01.28") {
    val sql =
      s"""SELECT d1,d2,d3,d4,v2
         |FROM four_dims
         |WHERE d2 >= 2 AND d2 <= 3
         |AND ((d1 >= 3 OR d1 = 0) OR (d3 <= 1 OR d3 = 3))
         |AND (d4 >= 2 OR d4 <= 3)
         |AND v2 = 'Honda'"""
        .stripMargin
    val rows = runSql(sql)
    printRows(sql, rows)
    assert(rows.size == 24)
  }

  // TODO fails: returns 15, instead of 20
  test("Query 01.29") {
    val sql =
      s"""SELECT d1,d2,d3,d4,v1,v2
         |FROM four_dims
         |WHERE d2 >= 2 AND d2 <= 3
         |AND ((d1 >= 3 OR d1 = 0) OR (d3 <= 1 OR d3 = 3))
         |AND (d4 >= 2 OR d4 <= 3)
         |AND ((v2 = 'Honda' OR v2 = 'Toyota') AND v1 = false)"""
        .stripMargin
    val rows = runSql(sql)
    printRows(sql, rows)
    assert(rows.size == 20)
  }

  test("Query 01.30") {
    val sql =
      s"""SELECT d1,d2,d3,d4,v2
         |FROM four_dims
         |WHERE d3 >= 2 AND d3 <= 3"""
        .stripMargin
    val rows = runSql(sql)
    printRows(sql, rows)
    assert(rows.size == 128)
  }

  test("Query 01.4") {
    val sql =
      s"""SELECT d1,d2,d3,d4,v2
         |FROM four_dims
         |WHERE d4 >= 3 AND d4 <= 4"""
        .stripMargin
    val rows = runSql(sql)
    printRows(sql, rows)
    assert(rows.size == 128)
  }

  test("Query 02.0") {
    val sql =
      s"""SELECT d1,d2,d3,d4,v1
         |FROM four_dims
         |WHERE (d1 > 1 and d1 < 3)
         |AND (d2 = 2 OR d2 = 3)
         |AND d3 = 3
         |AND d4 = 4"""
        .stripMargin
    val rows = runSql(sql)

    printRows(sql, rows)

    assert(rows.size == 2)

    val exparr =
      Array(
        Array(2, 2, 3, 4, true),
        Array(2, 3, 3, 4, false))
    var res = {
      for (rx <- 0 until rows.size)
        yield compareWithTol(rows(rx).toSeq, exparr(rx), s"Row$rx failed")
    }.foldLeft(true) { case (res1, newres) => res1 && newres }
    assert(res, "One or more rows did not match expected")
  }


  // There are 4 regions.  Return 3 split keys.
  private def splitKeys: Array[Array[Byte]] = {
    val splitKeys: Array[Array[Byte]] = {
      Array(
        new GenericRow(Array[Any](2)),
        new GenericRow(Array[Any](3)),
        new GenericRow(Array[Any](4))
      ).map(HBaseKVHelper.makeRowKey(_, Seq(IntegerType)))
    }
    splitKeys
  }


  def printTableStats(htableName: String, colFamilyName: String) = {
    val conf = TestHbase.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)
    val hbaseAdmin = TestHbase.hbaseAdmin
    val htable = new HTable(conf, htableName)
    val regions = TestHbase.cluster.getRegions(htableName.getBytes("UTF-8"))
    var tableRowCount = 0
    var tableSize = 0L

    println("Region Stats for HTable " + htableName + ": \n")
    printRegionStatsColumnLabels

    var start = System.currentTimeMillis
    for (region <- regions) {
      val startKey: Option[String] = Option(region.getStartKey).map(Hex.encodeHexString)
      val endKey: Option[String] = Option(region.getEndKey).map(Hex.encodeHexString)
      val rfs = region.getRegionFileSystem
      val regionPath = rfs.getRegionDir
      val cSummary = fs.getContentSummary(regionPath)
      val regionSize = cSummary.getLength
      tableSize += regionSize

      val scan = new Scan(region.getStartKey, region.getEndKey)
      scan.setCaching(1000)
      scan.setCacheBlocks(true)
      scan.setFilter(new FirstKeyOnlyFilter())

      val scanner = htable.getScanner(scan)
      val scanIterator = scanner.iterator
      var regionRowCount = 0

      while (scanIterator.hasNext) {
        var result = scanIterator.next
        regionRowCount += 1
        tableRowCount += 1
      }

      assert(regionRowCount == 64, "Incorrect region row count.  Test data not evenly distributed across regions.")

      printRegionStats(region.getRegionInfo.getEncodedName, startKey.get, endKey.get, regionRowCount, regionSize)
    }
    val end = System.currentTimeMillis
    val execTime = end - start
    printTableSummary(tableRowCount, tableSize, execTime)
  }


  private def printRegionStatsColumnLabels = {
    val statsColumnLabels = "%-34s %-18s %-18s %-18s %-18s"
      .format("ENCODED-REGION-NAME", "START-KEY", "END-KEY", "ROW-COUNT", "SIZE")
    println(statsColumnLabels)
  }


  private def printRegionStats(
                                encodedName: String,
                                startKey: String,
                                endKey: String,
                                rowCount: Int,
                                regionSize: Long) = {
    val sizeInMB = bytesToHuman(regionSize)
    val regionStats = "%-34s %-18s %-18s %-18d %-18s".format(
      new String(encodedName), startKey, endKey, rowCount, sizeInMB)
    println(regionStats)
  }

  private def printTableSummary(rowCount: Int, tableSize: Long, execTime: Long) = {
    val summary = "%-34s %-18s %-18s %-18d %-18s%n%n".format("TOTAL", "", "", rowCount, bytesToHuman(tableSize))
    println(summary)
  }


  private def printRows(sql: String, rows: Array[Row]) = {
    println("========= QUERY ============")
    println(sql)
    println("======= QUERY RESULTS ======")
    for (i <- 0 until rows.size) {
      println(rows(i).mkString(" | "))
    }
    println("Row Count: " + rows.size)
    println("============================")
  }


  private def floatForm(d: Double): String = {
    new DecimalFormat("#.##").format(d);
  }

  private def bytesToHuman(size: Long): String = {
    val Kb: Long = 1 * 1024
    val Mb: Long = Kb * 1024
    val Gb: Long = Mb * 1024
    val Tb: Long = Gb * 1024
    val Pb: Long = Tb * 1024
    val Eb: Long = Pb * 1024

    if (size < Kb) return floatForm(size) + " byte";
    if (size >= Kb && size < Mb) return floatForm(size.toDouble / Kb) + " Kb"
    if (size >= Mb && size < Gb) return floatForm(size.toDouble / Mb) + " Mb"
    if (size >= Gb && size < Tb) return floatForm(size.toDouble / Gb) + " Gb"
    if (size >= Tb && size < Pb) return floatForm(size.toDouble / Tb) + " Tb"
    if (size >= Pb && size < Eb) return floatForm(size.toDouble / Pb) + " Pb"
    if (size >= Eb) return floatForm(size.toDouble / Eb) + " Eb"
    "???"
  }
}
