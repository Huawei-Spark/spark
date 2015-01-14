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
  val A = Array
  test("VectorProjection") {
    //    def A[T : ClassTag](ts: T*) = Array(ts:_*)
//    type A = Array[Double]
    var dat = A(
      A(1., 2., 3.),
      A(3., 6., 9.)
    )
    var firstEigen = subtractProjection(dat(0), dat(1)) // collinear
    assert(firstEigen.forall(withinTol(_, 1e-11)))
    dat = A(
      A(1., 2., 3.),
      A(-3., -6., -9.),
      A(2., 4., 6.)
    )
    val proj  = SP.project(dat(0), dat(1)) // orthog
    firstEigen = SP.subtractProjection(dat(0), dat(1)) // orthog
    val subVect = SP.subtract(firstEigen,dat(2))
    println(s"subVect: ${SP.printVect(subVect)}")
    assert(subVect.forall(SP.withinTol(_, 1e-11)))
  }

  test("OneEigen") {
    var dat = A(
      A(1., 0., 0.),
      A(0., 1., 0.),
      A(0., 0., 1.)
    )
    withSpark { sc =>
      var datRdd = sc.parallelize((0 until dat.length).zip(dat))
      val (eigval, eigvect) = SP.getPrincipalEigen(sc, datRdd, None,
          10, -1.0)
      println(s"eigval=$eigval eigvect=${SP.printVect(eigvect)}")
    }
  }

  test("simpleMatrixTest") {
    val dat1 = A(
      A(3., 2., 4.),
      A(2., 0., 2.),
      A(4., 2., 3.)
    )
    val dat2 = A(
      A(1., 1, -2.),
      A(-1.,2., 1.),
      A(0., 1., -1.)
    )
    val dats = Array(dat1, dat2)
    for (dat <- dats) {
      val sigma = 1.0
      val nIterations = 20
      val nClusters = 2
      withSpark { sc =>
        var datRdd = /* sc.parallelize( */(0 until dat.length).zip(dat).map{ case (ix, dvect) =>
          (ix.toString, dvect)}.toSeq /*) */
        val (rdd, eigvals, eigvect) = SP.cluster(sc, datRdd, 2, sigma, nIterations)
        println(s"eigvals=${eigvals.mkString(",")} eigvects=${eigvect.collect.foreach{e => SP.printVect(e)}}")
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
      val (rddOut, lambdas, eigens) = SpectralClusteringUsingRdd.cluster(sc, vertices, nClusters, sigma, nIterations)
      val collectedRdd = rddOut.map {
        _._2
      }.collect
      println(s"DegreeMatrix:\n${printMatrix(collectedRdd, nVertices, nVertices)}")
      val collectedEigens = eigens.collect
      println(s"Eigenvalues = ${lambdas.mkString(",")} EigenVectors:\n${printMatrix(collectedEigens, nClusters, nVertices)}")
    }
  }

  def printMatrix(darr: Array[Double], numRows: Int, numCols: Int): String =
    SP.printMatrix(darr, numRows, numCols)

  def printMatrix(darr: Array[Array[Double]], numRows: Int, numCols: Int): String =
    SP.printMatrix(darr, numRows, numCols)
}

