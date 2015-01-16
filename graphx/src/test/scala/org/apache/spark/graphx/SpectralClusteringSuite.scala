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
  test("VectorArithmeticAndProjections") {
    //    def A[T : ClassTag](ts: T*) = Array(ts:_*)
    //    type A = Array[Double]
    var dat = A(
      A(1.0, 2.0, 3.0),
      A(3.0, 6.0, 9.0)
    )
    var firstEigen = LA.subtractProjection(dat(0), dat(1)) // collinear
    println(s"firstEigen should be all 0's ${firstEigen.mkString(",")}")
    assert(firstEigen.forall(LA.withinTol(_, 1e-8)))
    dat = A(
      A(1.0, 2.0, 3.0),
      A(-3.0, -6.0, -9.0),
      A(2.0, 4.0, 6.0),
      A(-1.0, 1.0, -.33333333333333),
      A(-2.0, 5.0, 2.0)
    )
    var proj = LA.subtractProjection(dat(0), dat(3)) // orthog
    println(s"proj after subtracting orthogonal vector should  be same " +
      s"as input (1.,2.,3.) ${proj.mkString(",")}")
    assert(proj.zip(dat(0)).map { case (a, b) => a - b}.forall(LA.withinTol(_, 1e-11)))

    val addMultVect = LA.add(dat(0), LA.mult(dat(3), 3.0))
    assert(addMultVect.zip(dat(4)).map { case (a, b) => a - b}.forall(LA.withinTol(_, 1e-11)),
      "AddMult was not within tolerance")
    proj = LA.subtractProjection(addMultVect, dat(3)) // orthog
    println(s"proj should  be same as parallel input (1.,2.,3.) ${proj.mkString(",")}")
    assert(proj.zip(dat(0)).map { case (a, b) => a - b}.forall(LA.withinTol(_, 1e-11)))
  }

  def compareVectors(v1: Array[Double], v2: Array[Double]) = {
    v1.zip(v2).forall { case (v1v, v2v) => LA.withinTol(v1v - v2v)}
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
      (A(8.0, -1.0, -1.0),
        A(
          A(0.6666667, 0.7453560, 0.0000000),
          A(0.3333333, -0.2981424, -0.8944272),
          A(0.6666667, -0.5962848, 0.4472136)
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
      (A(2.0, -1.0, 1.0),
        A(
          A(-0.5773503, -0.1360828, -7.071068e-01),
          A(0.5773503, -0.2721655, -3.188873e-16),
          A(0.5773503, 0.9525793, 7.071068e-01)
        ))
    val dats = Array((dat2, expDat2), (dat1, expDat1))
    //    val dats = Array((dat1,expDat1), (dat2,expDat2))
    for ((dat, expdat) <- dats) {
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

        assert(compareVectors(eigvals, expdat._1))
        assert(compareMatrices(eigvects.collect, expdat._2))
      }
    }
  }

  def makeMat(nrows: Int, dvect: Array[Double]) = {
    dvect.toSeq.grouped(nrows).map(_.toArray).toArray
  }

  test("manualPowerIt") {
    // R code
    //
    //> x = t(matrix(c(-2,-.5,2,.5,-3,.5,-1.,.5,4),nrow=3,ncol=3))
    //> x
    //     [,1] [,2] [,3]
    //[1,] -2.0 -0.5  2.0
    //[2,]  0.5 -3.0  0.5
    //[3,] -1.0  0.5  4.0
    //
    //> eigen(x)
    //$values
    //[1]  3.708394 -2.639960 -2.068434
    //
    //$vectors
    //           [,1]       [,2]        [,3]
    //[1,] 0.32182428 0.56847491 -0.85380536
    //[2,] 0.09420476 0.82235947 -0.51117379
    //[3,] 0.94210116 0.02368917 -0.09857872
    //

//    var dat2r = A(
//      A(-2.0, -0.5, 2.0),
//      A(0.5, -3.0, 0.5),
//      A(-0.5, 0.5, 4.0)
//    )

    var dat2r = makeMat(3, A(-2.,-.5,2,.5,-3,.5,-1.,.5,4.))

    var expdat = LA.transpose(makeMat(3, A(0.32182428, 0.56847491, -0.85380536, 0.09420476,
      0.82235947, -0.51117379, 0.94210116, 0.02368917, -0.09857872)))
    val nClusters = 3
    val nIterations = 30
    val numVects =3

    var mat = dat2r.map(identity)
    var eigen = Array.fill(numVects){1.0 / Math.sqrt(numVects)}

    var cnorm = -1.0
    for (k <- 0 until nClusters) {
      for (iter <- 0 until nIterations) {
        eigen = mat.map { dvect =>
          val d = LA.dot(dvect, eigen)
          d
        }
        cnorm = LA.makeNonZero(LA.norm(eigen))
        eigen = eigen.map(_  / cnorm)
      }
      val lambda = LA.dot(mat.take(1)(0), eigen) / eigen(0)
      println(s"lambda=$lambda eigen=${LA.printVect(eigen)}")
      val compareVect = eigen.zip(expdat(k)).map{ case (a,b) => a / b}
      println(s"Ratio  to expected: ${compareVect.mkString(",")}")
      if (k < nClusters - 1) {
        mat = LA.transpose(LA.transpose(mat).map(LA.subtractProjection(_, eigen)))
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

