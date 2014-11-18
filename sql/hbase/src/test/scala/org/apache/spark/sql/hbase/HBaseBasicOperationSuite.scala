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

import org.apache.spark.sql.catalyst.SqlParser
import org.apache.spark.sql.catalyst.types.{IntegerType, NativeType}
import org.apache.spark.sql.hbase.TestHbase._

import scala.collection.immutable.HashMap

class HBaseBasicOperationSuite extends QueryTest {

  test("create table") {
    sql( """CREATE TABLE tableName (col1 STRING, col2 BYTE, col3 SHORT, col4 INTEGER,
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

  test("Insert Into table0") {
        sql( """INSERT INTO testTable SELECT col4,col4,col6,col3 FROM myTable""")
  }

  test("Insert Into table") {
    //    sql("""CREATE TABLE t1 (t1c1 STRING, t1c2 STRING)
    //      MAPPED BY (ht1, KEYS=[t1c1], COLS=[t1c2=cf1.cq11])""".stripMargin
    //    )
    //    sql("""CREATE TABLE t2 (t2c1 STRING, t2c2 STRING)
    //      MAPPED BY (ht2, KEYS=[t2c1], COLS=[t2c2=cf2.cq21])""".stripMargin
    //    )
    sql( """INSERT INTO tableName SELECT * FROM myTable""")
  }

  test("Select test 0") {
    sql( """SELECT * FROM tableName ORDER BY col7 DESC""").foreach(println)
  }

  test("Select test 1") {
    sql( """SELECT * FROM myTable WHERE col7 = 1024.0""").foreach(println)
  }

  test("Select test 2") {
    sql( """SELECT col6, col7 FROM tableName ORDER BY col6 DESC""").foreach(println)
  }

  test("Select test 3") {
    sql( """SELECT col6, col6 FROM myTable""").foreach(println)
  }

  test("Select test 4") {
    sql( """SELECT * FROM myTable WHERE col7 = 1024 OR col7 = 2048""").foreach(println)
  }

  test("Select test 5") {
    sql( """SELECT * FROM myTable WHERE col7 < 1025 AND col1 ='Upen'""").foreach(println)
  }

  test("Alter Add column") {
    sql( """ALTER TABLE tableName ADD col8 STRING MAPPED BY (col8 = cf1.cf13)""")
  }

  test("Alter Drop column") {
    sql( """ALTER TABLE tableName DROP col6""")
  }

  test("Drop table") {
    sql( """DROP TABLE myTable""")
  }

  test("SPARK-3176 Added Parser of SQL ABS()") {
    checkAnswer(
      sql("SELECT ABS(-1.3)"),
      1.3)
  }
}
