package org.apache.spark.sql.hbasesource

import java.io.{ByteArrayOutputStream, DataOutputStream}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.log4j.Logger
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.{Logging, SparkConf}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.mutable.ArrayBuffer

/**
 * HBaseIntegrationTest
 * Created by sboesch on 9/27/14.
 */
object HBaseMainTest extends HBaseIntegrationTestBase(false) with CreateTableAndLoadData
    with Logging {
  @transient val logger = Logger.getLogger(getClass.getName)

  val useMiniCluster = false

  val TabName = DefaultTableName
  val HbaseTabName = DefaultHbaseTabName

  def tableSetup() = {
    createTable()
  }

  def createTable() = {

    val createTable = !useMiniCluster
    if (createTable) {
      try {
        createTables(hbc)
      } catch {
        case e: TableExistsException =>
          e.printStackTrace
      }
    }

    if (!hbaseAdmin.tableExists(HbaseTabName)) {
      throw new IllegalArgumentException("where is our table?")
    }

  }

  def checkHBaseTableExists(hbaseTable: String) = {
    hbaseAdmin.listTableNames.foreach { t => println(s"table: $t")}
    val tname = TableName.valueOf(hbaseTable)
    hbaseAdmin.tableExists(tname)
  }

  def insertTestData() = {
    if (!checkHBaseTableExists(HbaseTabName)) {
      throw new IllegalStateException(s"Unable to find table ${HbaseTabName}")
    }
    val htable = new HTable(config, HbaseTabName)

    var row = new GenericRow(Array(1024.0, "Upen", 128:Short))
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
    row = new GenericRow(Array(2048.0, "Michigan", 256:Short))
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
    row = new GenericRow(Array(4096.0, "SF", 512:Short))
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
    htable.close
    //    addRowVals(put, (123).toByte, 12345678, 12345678901234L, 1234.5678F)
  }

  val runMultiTests: Boolean = false

  def testQuery() {
    ctxSetup()
    createTable()
    //    testInsertIntoTable
    //    testHBaseScanner

    if (!checkHBaseTableExists(HbaseTabName)) {
      throw new IllegalStateException(s"Unable to find table ${HbaseTabName}")
    }

    insertTestData

  }

  def printResults(msg: String, results: SchemaRDD) = {
    if (results.isInstanceOf[TestingSchemaRDD]) {
      val data = results.asInstanceOf[TestingSchemaRDD].collectPartitions
      println(s"For test [$msg]: Received data length=${data(0).length}: ${
        data(0).mkString("RDD results: {", "],[", "}")
      }")
    } else {
      val data = results.collect
      println(s"For test [$msg]: Received data length=${data.length}: ${
        data.mkString("RDD results: {", "],[", "}")
      }")
    }

  }

  val allColumns: Seq[AbstractColumn] = Seq(
    KeyColumn("col1", StringType, 1),
    NonKeyColumn("col2", ByteType, "cf1", "cq11"),
    KeyColumn("col3", ShortType, 2),
    NonKeyColumn("col4", IntegerType, "cf1", "cq12"),
    NonKeyColumn("col5", LongType, "cf2", "cq21"),
    NonKeyColumn("col6", FloatType, "cf2", "cq22"),
    KeyColumn("col7", DoubleType, 0)
  )

  val keyColumns = allColumns.filter(_.isInstanceOf[KeyColumn])
    .asInstanceOf[Seq[KeyColumn]].sortBy(_.order)


  def makeRowKey(row: Row, dataTypeOfKeys: Seq[DataType]) = {
    //    val row = new GenericRow(Array(col7, col1, col3))
    val rawKeyCol = dataTypeOfKeys.zipWithIndex.map {
      case (dataType, index) => {
        DataTypeUtils.getRowColumnFromHBaseRawType(row, index, dataType, new BytesUtils)
      }
    }

    encodingRawKeyColumns(rawKeyCol)
  }

  /**
   * create row key based on key columns information
   * @param rawKeyColumns sequence of byte array representing the key columns
   * @return array of bytes
   */
  def encodingRawKeyColumns(rawKeyColumns: Seq[HBaseRawType]): HBaseRawType = {
    var buffer = ArrayBuffer[Byte]()
    val delimiter: Byte = 0
    var index = 0
    for (rawKeyColumn <- rawKeyColumns) {
      val keyColumn = keyColumns(index)
      buffer = buffer ++ rawKeyColumn
      if (keyColumn.dataType == StringType) {
        buffer += delimiter
      }
      index = index + 1
    }
    buffer.toArray
  }

  def addRowVals(put: Put, rowValue: Any, rowType: DataType, colFamily: String, colQulifier: String) = {
    val bos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(bos)
    rowType match {
      case StringType => dos.writeChars(rowValue.asInstanceOf[String])
      case IntegerType => dos.writeInt(rowValue.asInstanceOf[Int])
      case BooleanType => dos.writeBoolean(rowValue.asInstanceOf[Boolean])
      case ByteType => dos.writeByte(rowValue.asInstanceOf[Byte])
      case DoubleType => dos.writeDouble(rowValue.asInstanceOf[Double])
      case FloatType => dos.writeFloat(rowValue.asInstanceOf[Float])
      case LongType => dos.writeLong(rowValue.asInstanceOf[Long])
      case ShortType => dos.writeShort(rowValue.asInstanceOf[Short])
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
    testQuery
  }

}
