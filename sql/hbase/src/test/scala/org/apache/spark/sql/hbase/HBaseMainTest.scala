package org.apache.spark.sql.hbase

import java.io.{ByteArrayOutputStream, DataOutputStream}

import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.Logger
import org.apache.spark.Logging
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.catalyst.expressions.{GenericRow, Row}
import org.apache.spark.sql.catalyst.types._

import scala.collection.mutable.ListBuffer

/**
 * HBaseMainTest
 * create HbTestTable and metadata table, and insert some data
 */
object HBaseMainTest extends HBaseIntegrationTestBase(false) with CreateTableAndLoadData
with Logging {
  @transient val logger = Logger.getLogger(getClass.getName)

  val TabName_a: String = "ta"
  val TabName_b: String = "tb"
  val HbaseTabName: String = "ht"

  def tableSetup() = {
    createTable()
  }

  def createTable() = {
    try {
      var allColumns = List[AbstractColumn]()
      allColumns = allColumns :+ KeyColumn("col1", StringType, 1)
      allColumns = allColumns :+ NonKeyColumn("col2", ByteType, "cf1", "cq11")
      allColumns = allColumns :+ KeyColumn("col3", ShortType, 2)
      allColumns = allColumns :+ NonKeyColumn("col4", IntegerType, "cf1", "cq12")
      allColumns = allColumns :+ NonKeyColumn("col5", LongType, "cf2", "cq21")
      allColumns = allColumns :+ NonKeyColumn("col6", FloatType, "cf2", "cq22")
      allColumns = allColumns :+ KeyColumn("col7", DoubleType, 0)

      val splitKeys: Array[Array[Byte]] = Array(
        new GenericRow(Array(1024.0, "Upen", 128: Short)),
        new GenericRow(Array(1024.0, "Upen", 256: Short)),
        new GenericRow(Array(4096.0, "SF", 512: Short))
      ).map(HBaseKVHelper.makeRowKey(_, Seq(DoubleType, StringType, ShortType)))
      // val splitKeys = null

      catalog = new HBaseCatalog(hbc)
      catalog.createTable(TabName_a, null, HbaseTabName, allColumns, splitKeys)

      hbc.sql( s"""CREATE TABLE $TabName_b(col1 STRING, col2 BYTE, col3 SHORT, col4 INTEGER,
          col5 LONG, col6 FLOAT, col7 DOUBLE, PRIMARY KEY(col7, col1, col3))
          MAPPED BY ($HbaseTabName, COLS=[col2=cf1.cq11, col4=cf1.cq12, col5=cf2.cq21,
          col6=cf2.cq22])""".stripMargin)

      if (!hbaseAdmin.tableExists(HbaseTabName)) {
        throw new IllegalArgumentException("where is our table?")
      }
    }
  }

  def checkHBaseTableExists(hbaseTable: String) = {
    val tname = TableName.valueOf(hbaseTable)
    hbaseAdmin.tableExists(tname)
  }

  def insertTestData() = {
    if (!checkHBaseTableExists(HbaseTabName)) {
      throw new IllegalStateException(s"Unable to find table $HbaseTabName")
    }
    val htable = new HTable(config, HbaseTabName)

    var row = new GenericRow(Array(1024.0, "Upen", 128: Short))
    var key = makeRowKey(row, Seq(DoubleType, StringType, ShortType))
    var put = new Put(key)
    Seq((64.toByte, ByteType, "cf1", "cq11"),
      (12345678, IntegerType, "cf1", "cq12"),
      (12345678901234L, LongType, "cf2", "cq21"),
      (1234.5678F, FloatType, "cf2", "cq22")).foreach {
      case (rowValue, rowType, colFamily, colQualifier) =>
        addRowVals(put, rowValue, rowType, colFamily, colQualifier)
    }
    htable.put(put)

    row = new GenericRow(Array(2048.0, "Michigan", 256: Short))
    key = makeRowKey(row, Seq(DoubleType, StringType, ShortType))
    put = new Put(key)
    Seq((32.toByte, ByteType, "cf1", "cq11"),
      (456789012, IntegerType, "cf1", "cq12"),
      (4567890123446789L, LongType, "cf2", "cq21"),
      (456.78901F, FloatType, "cf2", "cq22")).foreach {
      case (rowValue, rowType, colFamily, colQualifier) =>
        addRowVals(put, rowValue, rowType, colFamily, colQualifier)
    }
    htable.put(put)

    row = new GenericRow(Array(4096.0, "SF", 512: Short))
    key = makeRowKey(row, Seq(DoubleType, StringType, ShortType))
    put = new Put(key)
    Seq((16.toByte, ByteType, "cf1", "cq11"),
      (98767, IntegerType, "cf1", "cq12"),
      (987563454423454L, LongType, "cf2", "cq21"),
      (987.645F, FloatType, "cf2", "cq22")).foreach {
      case (rowValue, rowType, colFamily, colQualifier) =>
        addRowVals(put, rowValue, rowType, colFamily, colQualifier)
    }
    htable.put(put)
    htable.close()
  }

  def testQuery() {
    ctxSetup()
    createTable()

    if (!checkHBaseTableExists(HbaseTabName)) {
      throw new IllegalStateException(s"Unable to find table $HbaseTabName")
    }

    insertTestData()
  }

  def printResults(msg: String, results: SchemaRDD) =
    results match {
      case rdd: TestingSchemaRDD =>
        val data = rdd.collectPartitions()
        println(s"For test [$msg]: Received data length=${data(0).length}: ${
          data(0).mkString("RDD results: {", "],[", "}")
        }")
      case _ =>
        val data = results.collect()
        println(s"For test [$msg]: Received data length=${data.length}: ${
          data.mkString("RDD results: {", "],[", "}")
        }")
    }

  def makeRowKey(row: Row, dataTypeOfKeys: Seq[DataType]) = {
    val rawKeyCol = dataTypeOfKeys.zipWithIndex.map {
      case (dataType, index) =>
        (DataTypeUtils.getRowColumnFromHBaseRawType(row, index, dataType),
          dataType)
    }

    val buffer = ListBuffer[Byte]()
    HBaseKVHelper.encodingRawKeyColumns(buffer, rawKeyCol)
  }

  def addRowVals(put: Put, rowValue: Any, rowType: DataType,
                 colFamily: String, colQulifier: String) = {
    val bos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(bos)
    val bu = BytesUtils.create(rowType)
    rowType match {
      case StringType => dos.write(bu.toBytes(rowValue.asInstanceOf[String]))
      case IntegerType => dos.write(bu.toBytes(rowValue.asInstanceOf[Int]))
      case BooleanType => dos.write(bu.toBytes(rowValue.asInstanceOf[Boolean]))
      case ByteType => dos.write(bu.toBytes(rowValue.asInstanceOf[Byte]))
      case DoubleType => dos.write(bu.toBytes(rowValue.asInstanceOf[Double]))
      case FloatType => dos.write(bu.toBytes(rowValue.asInstanceOf[Float]))
      case LongType => dos.write(bu.toBytes(rowValue.asInstanceOf[Long]))
      case ShortType => dos.write(bu.toBytes(rowValue.asInstanceOf[Short]))
      case _ => throw new Exception("Unsupported HBase SQL Data Type")
    }
    put.add(Bytes.toBytes(colFamily), Bytes.toBytes(colQulifier), bos.toByteArray)
  }

  def testHBaseScanner() = {
    val scan = new Scan
    val htable = new HTable(config, HbaseTabName)
    val scanner = htable.getScanner(scan)
    var res: Result = null
    do {
      res = scanner.next
      if (res != null) println(s"Row ${res.getRow} has map=${res.getNoVersionMap.toString}")
    } while (res != null)
  }

  def main(args: Array[String]) = {
    testQuery()
  }
}
