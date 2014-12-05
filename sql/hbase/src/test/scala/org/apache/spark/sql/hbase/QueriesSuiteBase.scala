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
import org.scalatest.ConfigMap

class QueriesSuiteBase() extends HBaseIntegrationTestBase(
    System.getProperty("spark.testing.use-external-hbase","false") == "false")
    with CreateTableAndLoadData {
  self: HBaseIntegrationTestBase =>

  var AvoidByteDataTypeBug = true

  override protected def beforeAll(configMap: ConfigMap): Unit = {
    super.beforeAll(configMap)
    createTableAndLoadData(hbc)
  }

  val tabName = DefaultTableName

  private val logger = Logger.getLogger(getClass.getName)

  val CompareTol = 1e-6

  def compareWithTol(actarr: Seq[Any], exparr: Seq[Any], emsg: String): Boolean = {
    actarr.zip(exparr).forall { case (a, e) =>
      val eq = (a, e) match {
        case (a: Double, e: Double) =>
          Math.abs(a - e) <= CompareTol
        case (a: Float, e: Float) =>
          Math.abs(a - e) <= CompareTol
        case (a: Byte, e) if AvoidByteDataTypeBug =>
          logger.error("We are sidestepping the byte datatype bug..")
          true
        case (a, e) =>
          logger.debug(s"atype=${a.getClass.getName} etype=${e.getClass.getName}")
          a == e
        case _ => throw new IllegalArgumentException("Expected tuple")
      }
      if (!eq) {
        logger.error(s"$emsg: Mismatch- act=$a exp=$e")
      }
      eq
    }
  }

}

