package org.apache.spark.sql.hbase

import java.io.{ObjectOutputStream, ByteArrayOutputStream, DataOutputStream}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.log4j.Logger
import org.apache.spark
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.hbase.DataTypeUtils._
import org.apache.spark.sql.hbase.HBaseCatalog._
import org.apache.spark.sql.hbase.KeyColumn
import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql.test.TestSQLContext._
import org.apache.spark.{Logging, SparkConf, sql}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.mutable.ArrayBuffer

/**
 * HBaseIntegrationTest
 * Created by sboesch on 9/27/14.
 */
object HBaseMainTest extends FunSuite with BeforeAndAfterAll with Logging {
  @transient val logger = Logger.getLogger(getClass.getName)

  val useMiniCluster: Boolean = false

  val NMasters = 1
  val NRegionServers = 1
  // 3
  val NDataNodes = 0

  val NWorkers = 1

  @transient var cluster: MiniHBaseCluster = null
  @transient var config: Configuration = null
  @transient var hbaseAdmin: HBaseAdmin = null
  @transient var hbContext: HBaseSQLContext = null
  @transient var catalog: HBaseCatalog = null
  @transient var testUtil: HBaseTestingUtility = null

  case class MyTable(col1: String, col2: Byte, col3: Short, col4: Int, col5: Long,
                     col6: Float, col7: Double)

  val DbName = "mynamespace"
  val TabName = "myTable"
  val HbaseTabName = "hbaseTableName"

  def ctxSetup() {
    if (useMiniCluster) {
      logger.info(s"Spin up hbase minicluster w/ $NMasters mast, $NRegionServers RS, $NDataNodes dataNodes")
      testUtil = new HBaseTestingUtility
      config = testUtil.getConfiguration
    } else {
      config = HBaseConfiguration.create
    }
    //    cluster = HBaseTestingUtility.createLocalHTU.
    //      startMiniCluster(NMasters, NRegionServers, NDataNodes)
    //    config = HBaseConfiguration.create
    config.set("hbase.regionserver.info.port", "-1")
    config.set("hbase.master.info.port", "-1")
    config.set("dfs.client.socket-timeout", "240000")
    config.set("dfs.datanode.socket.write.timeout", "240000")
    config.set("zookeeper.session.timeout", "240000")
    config.set("zookeeper.minSessionTimeout", "10")
    config.set("zookeeper.tickTime", "10")
    config.set("hbase.rpc.timeout", "240000")
    config.set("ipc.client.connect.timeout", "240000")
    config.set("dfs.namenode.stale.datanode.interva", "240000")
    config.set("hbase.rpc.shortoperation.timeout", "240000")
//    config.set("hbase.regionserver.lease.period", "240000")

    if (useMiniCluster) {
      cluster = testUtil.startMiniCluster(NMasters, NRegionServers)
      println(s"# of region servers = ${cluster.countServedRegions}")
    }

    @transient val conf = new SparkConf
    val SparkPort = 11223
    conf.set("spark.ui.port", SparkPort.toString)
    //    @transient val sc = new SparkContext(s"local[$NWorkers]", "HBaseTestsSparkContext", conf)
    hbContext = new HBaseSQLContext(TestSQLContext.sparkContext)

    catalog = hbContext.catalog
    hbaseAdmin = new HBaseAdmin(config)

  }

  def tableSetup() = {
    createTable()
  }

  def createTable() = {

    val createTable = !useMiniCluster
    if (createTable) {
      try {
        hbContext.sql( s"""CREATE TABLE $TabName(col1 STRING, col2 BYTE, col3 SHORT, col4 INTEGER,
          col5 LONG, col6 FLOAT, col7 DOUBLE, PRIMARY KEY(col7, col1, col3))
          MAPPED BY ($HbaseTabName, COLS=[col2=cf1.cq11,
          col4=cf1.cq12, col5=cf2.cq21, col6=cf2.cq22])"""
          .stripMargin)
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

//    var results: SchemaRDD = null
//    var data: Array[sql.Row] = null
//
//    results = hbContext.sql( s"""SELECT * FROM $TabName """.stripMargin)
//    printResults("Star* operator", results)
//    data = results.collect
//    assert(data.size >= 2)
//
//    results = hbContext.sql(
//      s"""SELECT col3, col1, col7 FROM $TabName LIMIT 1
//             """.stripMargin)
//    printResults("Limit Op", results)
//    data = results.collect
//    assert(data.size == 1)
//
//    results = hbContext.sql(
//      s"""SELECT col3, col2, col1, col4, col7 FROM $TabName order by col7 desc
//             """.stripMargin)
//    printResults("Ordering with nonkey columns", results)
//    data = results.collect
//    assert(data.size >= 2)
//
//    try {
//      results = hbContext.sql(
//        s"""SELECT col3, col1, col7 FROM $TabName LIMIT 1
//             """.stripMargin)
//      printResults("Limit Op", results)
//    } catch {
//      case e: Exception => "Query with Limit failed"
//        e.printStackTrace
//    }
//
//    results = hbContext.sql( s"""SELECT col3, col1, col7 FROM $TabName ORDER  by col7 DESC
//      """.stripMargin)
//    printResults("Order by", results)
//
//    if (runMultiTests) {
//      results = hbContext.sql( s"""SELECT col3, col2, col1, col7, col4 FROM $TabName
//          WHERE col1 ='Michigan'
//          """.stripMargin)
//      printResults("Where/filter on rowkey", results)
//      data = results.collect
//      assert(data.size >= 1)
//
//      results = hbContext.sql( s"""SELECT col7, col3, col2, col1, col4 FROM $TabName
//          WHERE col1 ='Michigan' and col7 >= 2500.0 and col3 >= 3500 and col3 <= 5000
//          """.stripMargin)
//      printResults("Where/filter on rowkeys change", results)
//
//      results = hbContext.sql( s"""SELECT col3, col2, col1, col7, col4 FROM $TabName
//          WHERE col1 ='Michigan' and col7 >= 2500.0 and col3 >= 3500 and col3 <= 5000
//          """.stripMargin)
//      printResults("Where/filter on rowkeys", results)
//
//
//      results = hbContext.sql( s"""SELECT col1, col3, col7 FROM $TabName
//          WHERE col1 ='Michigan' and col7 >= 2500.0 and col3 >= 35 and col3 <= 50 and col3 != 7.0
//          """.stripMargin)
//      printResults("Where with notequal", results)
//
//      results = hbContext.sql( s"""SELECT col1, col2, col3, col7 FROM $TabName
//          WHERE col1 ='Michigan' and col7 >= 2500.0 and col3 >= 35 and col3 <= 50 and cast(col2 as double) != 7.0
//          """.stripMargin)
//      printResults("Include non-rowkey cols in project", results)
//    }
//    if (runMultiTests) {
//      results = hbContext.sql( s"""SELECT col1, col2, col3, col7 FROM $TabName
//        WHERE col1 ='Michigan' and col7 >= 2500.0 and col3 >= 35 and col3 <= 50 and col2 != 7.0
//        """.stripMargin)
//      printResults("Include non-rowkey cols in filter", results)
//
//      results = hbContext.sql( s"""SELECT sum(col3) as col3sum, col1, col3 FROM $TabName
//        WHERE col1 ='Michigan' and col7 >= 2500.0 and col3 >= 35 and col3 <= 50 and col2 != 7.0
//        group by col1,  col3
//        """.stripMargin)
//      printResults("Aggregates on rowkeys", results)
//
//
//      results = hbContext.sql( s"""SELECT sum(col2) as col2sum, col4, col1, col3, col2 FROM $TabName
//        WHERE col1 ='Michigan' and col7 >= 2500.0 and col3 >= 35 and col3 <= 50
//        group by col1, col2, col4, col3
//        """.stripMargin)
//      printResults("Aggregates on non-rowkeys", results)
//    }
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
    //put: Put, col2: Byte, col4: Int, col5: Long, col6: Float) = {
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
    //      val barr = new Array[Byte](size)
    //    var bos = new ByteArrayOutputStream()
    //    var dos = new DataOutputStream(bos)
    //    dos.writeByte(col2)
    //    put.add(Bytes.toBytes("cf1"), Bytes.toBytes("cq11"), bos.toByteArray)
    //    bos = new ByteArrayOutputStream()
    //    dos = new DataOutputStream(bos)
    //    dos.writeInt(col4)
    //    put.add(Bytes.toBytes("cf1"), Bytes.toBytes("cq12"), bos.toByteArray)
    //    bos = new ByteArrayOutputStream()
    //    dos = new DataOutputStream(bos)
    //    dos.writeLong(col5)
    //    put.add(Bytes.toBytes("cf2"), Bytes.toBytes("cq21"), bos.toByteArray)
    //    bos = new ByteArrayOutputStream()
    //    dos = new DataOutputStream(bos)
    //    dos.writeFloat(col6)
    //    put.add(Bytes.toBytes("cf2"), Bytes.toBytes("cq22"), bos.toByteArray)
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
