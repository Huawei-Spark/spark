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

  import SpectralClusteringUsingRdd._

  val SP = SpectralClusteringUsingRdd
  val LA = SP.Linalg
  val A = Array
  test("VectorProjection") {
    //    def A[T : ClassTag](ts: T*) = Array(ts:_*)
    //    type A = Array[Double]
    var dat = A(
      A(1.0, 2.0, 3.0),
      A(3.0, 6.0, 9.0)
    )
    var firstEigen = LA.subtractProjection(dat(0), dat(1)) // collinear
    assert(firstEigen.forall(LA.withinTol(_, 1e-11)))
    dat = A(
      A(1.0, 2.0, 3.0),
      A(-3.0, -6.0, -9.0),
      A(2.0, 4.0, 6.0)
    )
    val proj = LA.project(dat(0), dat(1)) // orthog
    firstEigen = LA.subtractProjection(dat(0), dat(1)) // orthog
    val subVect = LA.subtract(firstEigen, dat(2))
    println(s"subVect: ${LA.printVect(subVect)}")
    assert(subVect.forall(LA.withinTol(_, 1e-11)))
  }

  def compareVectors(v1: Array[Double], v2: Array[Double]) = {
    v1.zip(v2).forall{ case (v1v, v2v) => LA.withinTol(v1v - v2v) }
  }

  type DMatrix = Array[Array[Double]]
  def compareMatrices(m1: DMatrix, m2: DMatrix) = {
    m1.zip(m2).forall { case (m1v, m2v) =>
      m1v.zip(m2v).forall { case (m1vv, m2vv) => LA.withinTol(m1vv - m2vv)}
    }
  }

  test("eigensTest") {
    val dat1 = A(
      A(3.0, 2.0, 4.0),
      A(2.0, 0.0, 2.0),
      A(4.0, 2.0, 3.0)
    )
    val expDat1 =
      (A(8.0, -1.0, 1.0),
      A(
        A(0.6666667,  0.7453560,  0.0000000),
        A(0.3333333, -0.2981424, -0.8944272),
        A(0.6666667, -0.5962848,  0.4472136)
      ))
    val dat2 = A(
      A(1.0, 1, -2.0),
      A(-1.0, 2.0, 1.0),
      A(0.0, 1.0, -1.0)
    )
//    val dat2 = A(
//      A(1.0, -1.0, 0.0),
//      A(1.0, 2.0, 1.0),
//      A(-2.0, 1.0, -1.0)
//    )
    val expDat2 =
      (A(8.0, -1.0, 1.0),
      A(
        A(-0.5773503, -0.1360828, -7.071068e-01),
        A( 0.5773503, -0.2721655, -3.188873e-16),
        A( 0.5773503,  0.9525793,  7.071068e-01)
      ))
    val dats = Array((dat2,expDat2), (dat1,expDat1))
//    val dats = Array((dat1,expDat1), (dat2,expDat2))
    for ( (dat,expdat) <- dats) {
      val sigma = 1.0
      val nIterations = 10 // 20
      val nClusters = 3
      withSpark { sc =>
        var datRdd = LA.transpose(
          sc.parallelize((0 until dat.length).zip(dat).map { case (ix, dvect) =>
          (ix, dvect)
        }.toSeq))
        val datRddCollected = datRdd.collect()
        val (eigvals, eigvects) = LA.eigens(sc, datRdd, nClusters, nIterations)
        val collectedEigens = eigvects.collect
        val printedEigens = LA.printMatrix(collectedEigens, 3, 3)
        println(s"eigvals=${eigvals.mkString(",")} eigvects=\n$printedEigens}")

        assert(compareVectors(eigvals,expdat._1))
        assert(compareMatrices(eigvects.collect,expdat._2))
      }
    }
  }

  test("fifteenVerticesTest") {
    val vertFile = "../data/graphx/new_lr_data.15.txt"
    val sigma = 1.0
    val nIterations = 20
    val nClusters = 3
    withSpark { sc =>
      val vertices = SpectralClusteringUsingRdd.readVerticesfromFile(vertFile)
      val nVertices = vertices.length
      val (lambdas, eigens) = SpectralClusteringUsingRdd.cluster(sc, vertices, nClusters, sigma, nIterations)
      val collectedRdd = eigens.collect
      println(s"DegreeMatrix:\n${printMatrix(collectedRdd, nVertices, nVertices)}")
      val collectedEigens = eigens.collect
      println(s"Eigenvalues = ${lambdas.mkString(",")} EigenVectors:\n${printMatrix(collectedEigens, nClusters, nVertices)}")
    }
  }

  def printMatrix(darr: Array[Double], numRows: Int, numCols: Int): String =
    LA.printMatrix(darr, numRows, numCols)

  def printMatrix(darr: Array[Array[Double]], numRows: Int, numCols: Int): String =
    LA.printMatrix(darr, numRows, numCols)
}

