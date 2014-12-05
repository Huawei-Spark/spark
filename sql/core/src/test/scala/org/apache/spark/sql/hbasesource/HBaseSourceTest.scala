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

package org.apache.spark.sql.hbasesource.source

import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor}
import org.apache.spark.sql.hbasesource.HBaseMiniClusterBase

class HBaseSourceTest extends HBaseMiniClusterBase {


  def createNativeHbaseTable(tableName: String, families: Seq[String]) = {
    val hdesc = new HTableDescriptor(tableName)
    families.foreach { f => hdesc.addFamily(new HColumnDescriptor(f))}
    hbaseAdmin.createTable(hdesc)
  }

  test("test mini cluster") {
    createNativeHbaseTable("hbase_table1", Seq("cf1", "cf2"))
    println(s"1: ${hbaseAdmin.tableExists("wf")}")
    println(s"1: ${hbaseAdmin.tableExists("hbase_table1")}")

    val desc = new HTableDescriptor("wf")
    val farmily = Bytes.toBytes("fam")
    val hcd = new HColumnDescriptor(farmily)
      .setMaxVersions(10)
      .setTimeToLive(1)
    desc.addFamily(hcd)

    hbaseAdmin.createTable(desc)
    println(s"2: ${hbaseAdmin.tableExists("wf")}")
  }



  test("ddl for hbase source test") {
    val ddl =
      """
        |CREATE TEMPORARY TABLE test_sql_table(a int, b String)
        |USING org.apache.spark.sql.hbase.source
        |OPTIONS (
        |  hbase_table 'hbase_table1',
        |  mapping 'a=cf1.column1',
        |  primary_key 'b'
        |)
      """.stripMargin

    sqlContext.sql(ddl)

    sqlContext.sql("select * from source_test").collect.foreach(println)

  }

}
