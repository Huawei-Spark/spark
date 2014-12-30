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

import org.apache.spark.sql.hbase.TestHbase._

/**
 * Test insert / query against the table created by HBaseMainTest
 */
class HBaseBasicOperationSuite extends QueryTest {

  test("create table") {
    sql( """CREATE TABLE tb (col1 STRING, col2 BYTE, col3 SHORT, col4 INTEGER,
      col5 LONG, col6 FLOAT, col7 DOUBLE, PRIMARY KEY(col7, col1, col3))
      MAPPED BY (hbaseTableName1, COLS=[col2=cf1.cq11,
      col4=cf1.cq12, col5=cf2.cq21, col6=cf2.cq22])"""
    )
  }

  test("create table1") {
    sql( """CREATE TABLE testTable (column2 INTEGER, column1 INTEGER, column4 FLOAT,
        column3 SHORT, PRIMARY KEY(column1, column2))
        MAPPED BY (testNamespace.hbaseTable, COLS=[column3=family1.qualifier1,
        column4=family2.qualifier2])"""
    )
  }

  test("create table2") {
    sql( """CREATE TABLE test (column1 INTEGER,
        PRIMARY KEY(column1))
        MAPPED BY (testTable, COLS=[])"""
    )
  }

  test("create table3") {
    sql( """CREATE TABLE tb3 (column1 INTEGER, column2 STRING,
        PRIMARY KEY(column2))
        MAPPED BY (htb3, COLS=[column1=cf.cq])"""
    )
  }

  test("Insert Into table0") {
        sql( """INSERT INTO testTable SELECT col4,col4,col6,col3 FROM ta""")
  }

  test("Insert Into table") {
    //    sql("""CREATE TABLE t1 (t1c1 STRING, t1c2 STRING)
    //      MAPPED BY (ht1, KEYS=[t1c1], COLS=[t1c2=cf1.cq11])""".stripMargin
    //    )
    //    sql("""CREATE TABLE t2 (t2c1 STRING, t2c2 STRING)
    //      MAPPED BY (ht2, KEYS=[t2c1], COLS=[t2c2=cf2.cq21])""".stripMargin
    //    )
    sql( """INSERT INTO tableName SELECT * FROM testTable""")
  }

  test("Insert Into table1") {
//    sql( """INSERT INTO testTable VALUES (1024, 2048, 13.6, 2)""")
    sql( """SELECT * FROM testTable""").foreach(println)
  }

  test("Insert Into table3") {
    sql( """INSERT INTO tb3 VALUES (1024, "abc")""")
  }

  test("Select test 0") {
    sql( """SELECT ta.col2 FROM ta join tb on ta.col1=tb.col1""").foreach(println)
  }

  test("Select test 1") {
    sql( """SELECT * FROM ta""").foreach(println)
    assert(sql( """SELECT * FROM ta WHERE col7 > 1024""").count() == 2)
    assert(sql( """SELECT * FROM ta WHERE (col7 - 10 > 1024) AND col1 = 'SF'""").count() == 1)
  }

  test("Select test 2") {
    sql( """SELECT col6, col7 FROM ta ORDER BY col6 DESC""").foreach(println)
  }

  test("Select test 3") {
    sql( """SELECT * FROM tb""").foreach(println)
  }

  test("Select test 4") {
    sql( """SELECT * FROM ta WHERE col7 = 1024 OR col7 = 2048""").foreach(println)
  }

  test("Select test 5") {
    sql( """SELECT * FROM ta WHERE col7 < 1025 AND col1 ='Upen'""").foreach(println)
  }

  test("Alter Add column") {
    sql( """ALTER TABLE ta ADD col8 STRING MAPPED BY (col8 = cf1.cf13)""")
  }

  test("Alter Drop column") {
    sql( """ALTER TABLE ta DROP col6""")
  }

  test("Drop table") {
    sql( """DROP TABLE tb3""")
  }

  test("SPARK-3176 Added Parser of SQL ABS()") {
    checkAnswer(
      sql("SELECT ABS(-1.3)"),
      1.3)
  }
}
