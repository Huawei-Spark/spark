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
package org.apache.spark.mllib.clustering


import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Matrix, Matrices, Vectors}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import breeze.linalg.{DenseVector => BDV, DenseMatrix => BDM}
import org.apache.spark.mllib.rdd.RDDFunctions
import scala.math.Ordering
import org.apache.spark.mllib.rdd.RDDFunctions._

/**
 * SpectralClustering
 */
class SpectralClustering {
  val logger = Logger.getLogger(getClass.getName)

}

object SpectralClustering {

  val log = Logger.getLogger(SpectralClustering.getClass)

//  def gaussians(nPoints: Int, mean: Double, sttdev: Double) = {
//    import org.apache.spark.mllib.stat.Statistics
//
//  }

  def testSpectral = {

  }

  def testData(sc: SparkContext) = {
    import java.util.Random
    val xrand = new Random(37)
    val yrand = new Random(41)
    val zrand = new Random(43)
    val SignalScale = 5.0
    val NoiseScale = 8.0
    val npoints = 50000
    val XScale = SignalScale / npoints
    val YScale = 2.0 * SignalScale / npoints
    val ZScale = 3.0 * SignalScale / npoints
    val randRdd = sc.parallelize({
      for (p <- Range(0, npoints))
      yield
        (NoiseScale * xrand.nextGaussian + XScale * p,
          NoiseScale * yrand.nextGaussian + YScale * p,
          NoiseScale * zrand.nextGaussian + ZScale * p)
    }.toSeq, 2)
    randRdd
  }

  def euclid(c1: Array[Double], c2: Array[Double]) = {
    Math.sqrt(c1.zip(c2).foldLeft(0.0) { case (dist: Double, (c1: Double, c2: Double)) =>
      //         println(c1 - c2)
      dist + Math.pow(c1 - c2, 2)
    })
  }

  protected def logWarning(msg: => String) {
    //    if (log.isWarnEnabled) log.warn(msg)
  }


  /**
   * Fills a full square matrix from its upper triangular part.
   */
  private def triuToFull(n: Int, U: Array[Double]): Matrix = {
    val G = new BDM[Double](n, n)

    var row = 0
    var col = 0
    var idx = 0
    var value = 0.0
    while (col < n) {
      row = 0
      while (row < col) {
        value = U(idx)
        G(row, col) = value
        G(col, row) = value
        idx += 1
        row += 1
      }
      G(col, col) = U(idx)
      idx += 1
      col += 1
    }

    Matrices.dense(n, n, G.data)
  }

  import org.apache.spark._

  def createTestRdd(sc: SparkContext, NRows: Int, NCols: Int) = {

    import org.apache.spark.mllib._
    import org.apache.spark.mllib.linalg.distributed._
    import org.apache.spark.mllib.linalg._
    val rdd = sc.parallelize {
      Array.tabulate(NRows) { row =>
        Vectors.dense(Array.tabulate[Double](NCols) { col =>
          row * 10000.0 + col * 5.0
        })
      }
    }
    rdd
  }

  import org.apache.spark.rdd._
  import org.apache.spark.mllib.linalg._
  import org.apache.spark.mllib.linalg.distributed._

  def createTestRowMatrix(sc: SparkContext) = {
    new RowMatrix(createTestRdd(sc, NRows, NCols))
  }

  val NCols = 100
  val NRows = 10000

  def gaussianDist(c1: Array[Double], c2: Array[Double], sigma: Double) = {
    Math.exp((-1.0 / Math.pow(sigma, 2.0)) * c1.zip(c2).foldLeft(0.0) {
      case (dist: Double, (c1: Double, c2: Double)) =>
        //         println(c1 - c2)
        dist + Math.pow(c1 - c2, 2)
    })
  }

  def computeGaussianSimilarity(mat: RowMatrix, sigma: Double): Matrix = {
    val n = mat.numCols().toInt

    def checkNumColumns(cols: Int): Unit = {
      if (cols > 65535) {
        throw new IllegalArgumentException(s"Argument with more than 65535 cols: $cols")
      }
      if (cols > 10000) {
        val mem = cols * cols * 8
        println(s"$cols columns will require at least $mem bytes of memory!")
      }
    }

    checkNumColumns(n)
    // Computes n*(n+1)/2, avoiding overflow in the multiplication.
    // This succeeds when n <= 65535, which is checked above
    val nt: Int = if (n % 2 == 0) ((n / 2) * (n + 1)) else (n * ((n + 1) / 2))

    // Compute the upper triangular part of the gram matrix.

    /*
        val GU = rows.treeAggregate(new BDV[Double](new Array[Double](nt)))(
         seqOp = (U, v) => {
           RowMatrix.dspr(1.0, v, U.data)
           U
         }, combOp = (U1, U2) => U1 += U2)
     */

    val rcntr = new java.util.concurrent.atomic.AtomicInteger
    val GU = mat.rows. /*zipWithIndex.*/ treeAggregate(new BDV[Double](new Array[Double](nt)))(
      seqOp = (U, v) => {
        val vect = v
        val rx = rcntr.getAndIncrement
        for (col <- 0 until vect.size) {
          val arr1 = Array[Double](vect.size)
          System.arraycopy(U, (rx * vect.size).toInt, arr1, 0, arr1.length)
          val arr2 = Array[Double](vect.size)
          System.arraycopy(U, (col * vect.size).toInt, arr2, 0, arr2.length)
//                    U(rx, col) = gaussianDist(U(rx), U(col), sigma)
          System.arraycopy(U, (rx * vect.size).toInt, gaussianDist(arr1, arr2, sigma), 0, vect.size)
        }
        //        RowMatrix.dspr(1.0, v, U.data)

        U
      }, combOp = (U1, U2) => U1 += U2)

    //    triuToFull(n, GU.data)
    Matrices.dense(n, n, GU.data)
  }

  def testGaussianSimilarity(sc: SparkContext) = {
    val Sigma = 1.0
    computeGaussianSimilarity(createTestRowMatrix(sc), Sigma)
  }


  def testEuclid(sc: SparkContext) = {
    def euclid(c1: Array[Double], c2: Array[Double]) = {
      Math.sqrt(c1.zip(c2).foldLeft(0.0) { case (dist: Double, (c1: Double, c2: Double)) =>
        //         println(c1 - c2)
        dist + Math.pow(c1 - c2, 2)
      })
    }

    val rdd = createTestRdd(sc, NRows, NCols)

    val PrintFreq = 100
    val dmat = sc.broadcast(rdd.collect)
    val distRdd = rdd.mapPartitions { points =>
      var nRows = 0
      val otherPoints = dmat.value
      points.map { point =>
        //      println(s"point = $point")
        val dists = otherPoints.map { otherPoint =>
          euclid(point.toArray, otherPoint.toArray)
        }
        if (nRows % PrintFreq == 0) {
          println(s"${(new java.util.Date).toString}: nRows=$nRows")
        }
        nRows += 1
        dists
      }
    }
    val output = distRdd.collect
  }

  def main(args: Array[String]) {
    val sc = new SparkContext("local[2]","GSTest")
    testGaussianSimilarity(sc)
  }
}
