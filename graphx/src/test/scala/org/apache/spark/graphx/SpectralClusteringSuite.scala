package org.apache.spark.graphx

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
    val vertFile = "../data/graphx/new_lr_data.15.txt"
    val sigma = 1.0
    val nIterations = 5
    withSpark { sc =>
      val vertices = SpectralClustering.readVerticesfromFile(vertFile)
      val nVertices = vertices.length
      val out = SpectralClustering.cluster(sc, vertices, sigma, nIterations)
      val collectedRdd = out.map{ _._2}.collect
      println(printMatrix(collectedRdd, nVertices, nVertices))
    }
  }

  def printMatrix(darr: Array[Double], numRows: Int, numCols: Int): String =
    SpectralClustering.printMatrix(darr, numRows, numCols)

  def printMatrix(darr: Array[Array[Double]], numRows: Int, numCols: Int): String =
    SpectralClustering.printMatrix(darr, numRows, numCols)
}
