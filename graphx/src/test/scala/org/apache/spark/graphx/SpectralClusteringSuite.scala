package org.apache.spark.graphx

import org.apache.spark.SparkContext
import org.scalatest.FunSuite

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

/**
 * SpectralClusteringSuite
 *
 */
class SpectralClusteringSuite extends FunSuite with LocalSparkContext {

  test("tenVerticesTest") {
    val vertFile = "../data/graphx/new_lr_data.10.txt"
    val sigma = 1.0
    withSpark { sc =>
      val out = SpectralClustering.fromFile(sc, vertFile, sigma)
      println(printMatrix(out, 10, 10))
    }
  }

  def printMatrix(darr: Array[Double], numRows: Int, numCols: Int): String = {
    val stride = (darr.length / numCols).toInt
    val sb = new StringBuilder
    def leftJust(s: String, len: Int) = {
      "         ".substring(0, len - s.length) + s
    }

    for (r <- 0 until numRows) {
      for (c <- 0 until numCols) {
        sb.append(leftJust(f"${darr(c * stride + r)}%.6f", 9) + " ")
      }
      sb.append("\n")
    }
    sb.toString
  }

}
