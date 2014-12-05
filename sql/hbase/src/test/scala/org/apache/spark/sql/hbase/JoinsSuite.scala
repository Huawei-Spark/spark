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

import org.apache.log4j.Logger

class JoinsSuite extends JoinsSuiteBase {

  private val logger = Logger.getLogger(getClass.getName)


  def run(testName: String, sql: String, exparr: Array[Array[Any]]) = {
    val execQuery1 = hbc.executeSql(sql)
    val result1 = execQuery1.toRdd.collect()
    assert(result1.size == 2, s"$testName failed on size")

    var res = {
      for (rx <- 0 until exparr.size)
      yield compareWithTol(result1(rx).toSeq, exparr(rx), s"Row$rx failed")
    }.foldLeft(true) { case (res1, newres) => res1 && newres}
    assert(res, "One or more rows did not match expected")

    println(s"$sql came back with ${result1.size} results")
    println(result1.mkString)

    println(s"Test $testName completed successfully")
  }

  var testnm = "Basic Join: Two Way"
  test(testnm) {
    val query = makeJoin(2)
    val exparr = Array(
      Array(1, 23456783, 45657.83F, "Row3", 'c', 12343, 45657.83F, 5678912.345683, 3456789012343L),
      Array(1, 23456782, 45657.82F, "Row2", 'b', 12342, 45657.82F, 5678912.345682, 3456789012342L))
    run(testnm, query, exparr)
  }


//  testnm = "Basic Join: Three Way"
//  test(testnm) {
//    makeJoin(3)
//  }
//
//  testnm = "Basic Join: Four Way"
//  test(testnm) {
//    makeJoin(4)
//  }

//  testnm = "Join with GroupBy: Two Way"
//  test(testnm) {
//    makeGroupByJoin(2)
//  }

    private val joinCols = Array(
      Array(("t1.bytecol", "t2.bytecol"), ("t1.intcol", "t2.intcol"), ("t1.longcol", "t2.longcol"), ("t1.floatcol", "t2.floatcol"), ("t1.doublecol", "t2.doublecol")),
      Array(("t2.bytecol", "t3.bytecol"), ("t2.intcol", "t3.intcol"), ("t2.longcol", "t3.longcol"), ("t2.floatcol", "t3.floatcol"), ("t2.doublecol", "t3.doublecol")),
      Array(("t3.bytecol", "t4.bytecol"), ("t3.intcol", "t4.intcol"), ("t3.longcol", "t4.longcol"), ("t3.floatcol", "t4.floatcol"), ("t3.doublecol", "t4.doublecol")))

  def makeJoin(ntabs: Int = 2) = {
    val TNameBase = "JoinTable"

    val (allcols, alljoins) = (1 to ntabs).foldLeft("", "") { case ((cols, joins), px) =>
      val tn = s"t$px"
      val newcols = s"""$tn.intcol ${tn}intcol, $tn.floatcol ${tn}floatcol, $tn.strcol ${tn}strcol, $tn.bytecol  ${tn}bytecol,
         | $tn.shortcol  ${tn}shortcol,
         | $tn.floatcol  ${tn}floatcol2, $tn.doublecol  ${tn}doublecol,
         | $tn.longcol  ${tn}longcol"""

      val newjoins = {
        val tname = TNameBase + px
        if (px == 1) {
          tname
        } else {
          val newjoinCols = joinCols(px-1).foldLeft("") { case (cumjoins, (cola, colb)) =>
            cumjoins + (if (cumjoins.length > 0) " AND " else "") + s"$cola=$colb"
          }
          s"$joins JOIN $tname ON $newjoinCols"
        }
      }

      val newgroups =
        s"""${tn}strcol,${tn}bytecol,${tn}shortcol,${tn}intcol,${tn}floatcol,
           |${tn}doublecol""".stripMargin
      ((if (cols.length > 0) s"$cols,\n" else "") + newcols,
        newjoins)
    }
    val query1 =
      s"""select
         |$allcols
         | from $alljoins
         |  where t1.strcol like '%Row%' and t2.shortcol < 12345 and t3.doublecol > 5678912.345681
         |  and t3.doublecol < 5678912.345684
                 | order by t1strcol"""
        //         | order by t1strcol desc"""  // Potential bug with DESC ??
        .stripMargin
    query1
  }

  def makeGroupByJoin(ntabs: Int = 3) = {
    val TNameBase = "JoinTable"

    val (allcols, alljoins, groupcols) = (1 to ntabs).foldLeft("", "", "") { case ((cols, joins, groups), px) =>
      val tn = s"t$px"
      val newcols = s"""$tn.intcol ${tn}intcol, $tn.floatcol ${tn}floatcol, $tn.strcol ${tn}strcol, max($tn.bytecol)  ${tn}bytecol,
         | max($tn.shortcol)  ${tn}shortcol,
         | max($tn.floatcol)  ${tn}floatcolmax, max($tn.doublecol)  ${tn}doublecol,
         | max($tn.longcol)  ${tn}longcol"""

      val newjoins = {
        val tname = TNameBase + px
        if (px == 1) {
          tname
        } else if (px < ntabs) {
          val newjoinCols = joinCols(px - 1).foldLeft("") { case (cumjoins, (cola, colb)) =>
            cumjoins + (if (cumjoins.length > 0) " AND " else "") + s"$cola=$colb"
          }
          s"$joins JOIN $tname ON $newjoinCols"
        } else {
          joins
        }
      }

      val newgroups =
        s"""${tn}strcol,${tn}bytecol,${tn}shortcol,${tn}intcol,${tn}floatcol,
           |${tn}doublecol""".stripMargin
      ((if (cols.length > 0) s"$cols,\n" else "") + newcols,
        newjoins,
        (if (groups.length > 0) s"$groups,\n" else "") + newgroups)

    }
    val query1 =
      s"""select count(1) as cnt,
         |$allcols
         | from $alljoins
         |  where t1.strcol like '%Row%' and t2.shortcol < 12345 and t3.doublecol > 5678912.345681
         |  and t3.doublecol < 5678912.345684
         | group by $groupcols"""
        //         | order by t1strcol"""
        //         | order by t1strcol desc"""  // Potential bug with DESC ??
        .stripMargin
      query1
    }


}

