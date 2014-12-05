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

import java.util.{Random, Date}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, Logging, SparkContext}
import org.scalatest.{Suite, BeforeAndAfterAll, FunSuite}

class HBaseMiniClusterBase(
                            useMiniCluster: Boolean = true,
                            nRegionServers: Int = 2,
                            nDataNodes: Int = 2,
                            nMasters: Int = 1) extends FunSuite with BeforeAndAfterAll with Logging {
  self: Suite =>

  @transient var sc: SparkContext = null
  @transient var cluster: MiniHBaseCluster = null
  @transient var config: Configuration = null
  @transient var hbaseAdmin: HBaseAdmin = null
  @transient var sqlContext: SQLContext = null
  @transient var testUtil: HBaseTestingUtility = null

  def sparkContext: SparkContext = sc

  val startTime = (new Date).getTime
  val sparkUiPort = 0xc000 + new Random().nextInt(0x3f00)
  println(s"SparkUIPort = $sparkUiPort")

  override def beforeAll(): Unit = {
    ctxSetup()
  }

  //  def createNativeHbaseTable(tableName: String, families: Seq[String]) = {
  //    val hdesc = new HTableDescriptor(tableName)
  //    families.foreach { f => hdesc.addFamily(new HColumnDescriptor(f))}
  //    hbaseAdmin.createTable(hdesc)
  //  }

  def ctxSetup() {
    if (useMiniCluster) {
      logDebug(s"Spin up hbase minicluster w/ $nMasters mast, $nRegionServers RS, $nDataNodes dataNodes")
      testUtil = new HBaseTestingUtility
      config = testUtil.getConfiguration
    } else {
      config = HBaseConfiguration.create
    }

    if (useMiniCluster) {
      cluster = testUtil.startMiniCluster(nMasters, nRegionServers, nDataNodes)
      println(s"# of region servers = ${cluster.countServedRegions}")
    }
    // Need to retrieve zkPort AFTER mini cluster is started
    val zkPort = config.get("hbase.zookeeper.property.clientPort")
    logDebug(s"After testUtil.getConfiguration the hbase.zookeeper.quorum="
      + s"${config.get("hbase.zookeeper.quorum")} port=$zkPort")

    val sconf = new SparkConf()

    sconf.set("spark.hadoop.hbase.zookeeper.property.clientPort", zkPort)
    sconf.set("spark.hadoop.hbase.zookeeper.quorum",
      "%s:%s".format(config.get("hbase.zookeeper.quorum"), zkPort))
    sconf.set("spark.ui.port", sparkUiPort.toString)
    sconf.set("spark.hadoop.hbase.regionserver.info.port", "-1")
    sconf.set("spark.hadoop.hbase.master.info.port", "-1")

    sconf.set("spark.hadoop.dfs.client.socket-timeout", "480000")
    sconf.set("spark.hadoop.dfs.datanode.socket.write.timeout", "480000")
    sconf.set("spark.hadoop.zookeeper.session.timeout", "480000")
    sconf.set("spark.hadoop.zookeeper.minSessionTimeout", "10")
    sconf.set("spark.hadoop.zookeeper.tickTime", "10")
    sconf.set("spark.hadoop.hbase.rpc.timeout", "480000")
    sconf.set("spark.hadoop.ipc.client.connect.timeout", "480000")
    sconf.set("spark.hadoop.dfs.namenode.stale.datanode.interval", "480000")
    sconf.set("spark.hadoop.hbase.rpc.shortoperation.timeout", "480000")
    sconf.set("spark.hadoop.hbase.regionserver.lease.period", "480000")
    sconf.set("spark.hadoop.hbase.client.scanner.timeout.period", "480000")
    sc = new SparkContext("local[2]", "TestSQLContext", sconf)

    hbaseAdmin = testUtil.getHBaseAdmin
    sqlContext = new SQLContext(sc)
    logDebug(s"In testbase: HBaseAdmin.configuration zkPort="
      + s"${hbaseAdmin.getConfiguration.get("hbase.zookeeper.property.clientPort")}")
  }

  override def afterAll(): Unit = {
    var msg = s"Test ${getClass.getName} completed at ${(new java.util.Date).toString} duration=${((new java.util.Date).getTime - startTime) / 1000}"
    logInfo(msg)
    try {
      sqlContext.sparkContext.stop()
    } catch {
      case e: Throwable =>
        logError(s"Exception shutting down sparkContext: ${e.getMessage}")
    }
    sqlContext = null
    msg = "SQLContext was shut down"

    try {
      testUtil.shutdownMiniCluster()
    } catch {
      case e: Throwable =>
        logError(s"Exception shutting down HBaseMiniCluster: ${e.getMessage}")
    }
  }

}
