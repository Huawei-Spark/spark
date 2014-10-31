package org.apache.spark.mllib.clustering

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Matrix, Matrices}
import breeze.linalg.{DenseMatrix =>BDM}

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
 * EuclidTest
 *
 */
object EuclidTest {

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

  def createTestRdd(@transient sc: SparkContext, nRows: Int, nCols: Int) = {

    import org.apache.spark.mllib.linalg._
    println("CreateTestRDD")
    val rdd = sc.parallelize {
      Array.tabulate(nRows) { row =>
        Vectors.dense(Array.tabulate[Double](nCols) { col =>
          row * 0.5 + col * 0.21
        })
      }
    }
    rdd
  }

   val NCols = 3
  // 100
  val NRows = 8 // 10000

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
}
