package org.apache.spark.graphx

import org.scalatest.FunSuite

import scala.reflect.ClassTag

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

  test("fifteenVerticesTest") {
    val vertFile = "../data/graphx/new_lr_data.15.txt"
    val sigma = 1.0
    val nIterations = 20
    val nClusters = 3
    withSpark { sc =>
      val vertices = SpectralClustering.readVerticesfromFile(vertFile)
      val nVertices = vertices.length
      val (rddOut, lambdas, eigens) = SpectralClustering.cluster(sc, vertices, nClusters, sigma, nIterations)
      val collectedRdd = rddOut.map {
        _._2
      }.collect
      println(s"DegreeMatrix:\n${printMatrix(collectedRdd, nVertices, nVertices)}")
      val collectedEigens = eigens.collect
      println(s"Eigenvalues = ${lambdas.mkString(",")} EigenVectors:\n${printMatrix(collectedEigens, nClusters, nVertices)}")
    }
  }

  test("VectorProjection") {
    //    def A[T : ClassTag](ts: T*) = Array(ts:_*)
    type A = Array[Double]
    val A = Array
    val dat = A(
      A(1., 2., 3.),
      A(1.5, 2., 2.5),
      A(2., 3.8, 5.6),
      A(2.5, 3.0, 3.5),
      A(3.1, 3.7, 4.3),
      A(3., 6., 9.))
    val firstEigen = SpectralClustering.subtractProject(dat(0), dat(5))
    println(s"firstEigen: ${firstEigen.mkString(",")}")
  }

  def printMatrix(darr: Array[Double], numRows: Int, numCols: Int): String =
    SpectralClustering.printMatrix(darr, numRows, numCols)

  def printMatrix(darr: Array[Array[Double]], numRows: Int, numCols: Int): String =
    SpectralClustering.printMatrix(darr, numRows, numCols)
}
