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


import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV}
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}

/**
 * SpectralClustering
 */
class SpectralClustering {
  val logger = Logger.getLogger(getClass.getName)

}

object SpectralClustering {

  val log = Logger.getLogger(SpectralClustering.getClass)

  def computeGaussianSimilarity(sc: SparkContext, mat: IndexedRowMatrix, sigma: Double): IndexedRowMatrix = {

    val bcMat = sc.broadcast(mat.rows.collect)
    val gmat = mat.rows.mapPartitions { rows =>

      def gaussianDist(c1arr: Array[Double], c2arr: Array[Double], sigma: Double) = {
        val c1c2 = c1arr.zip(c2arr)
        val dist = Math.exp((0.5 / Math.pow(sigma, 2.0)) * c1c2.foldLeft(0.0) {
          case (dist: Double, (c1: Double, c2: Double)) =>
            dist + Math.pow(c1 - c2, 2)
        })
        dist
      }
      def myAssert(b: Boolean, msg: String) = if (!b) {
        throw new IllegalStateException(msg)
      }

      val bcMatLocal = bcMat.value
      val allRows = bcMatLocal
      myAssert(bcMatLocal.isInstanceOf[Array[IndexedRow]], "bc mat should be indexedrowmatrix")

      val nr = bcMatLocal.size
      println(s"inside mapPartitions: bcMatLocal.size=${bcMatLocal.size}")

      val output = new BDV[Double](new Array[Double](nr * nr))
      val out = rows.map { indexedRow =>
        //        println(s"inside mapPartitions.map: row=${indexedRow.toString}")
        val rx = indexedRow.index
        val vect = indexedRow.vector
        val rowOutput = new Array[Double](nr)
        for (ox <- 0 until nr) {
          rowOutput(ox) = if (ox > rx) {
            Double.NaN
          } else {
            val otherRow = allRows(ox)

            val distVal = gaussianDist(vect.toArray, otherRow.vector.toArray, sigma)
            distVal
          }
          println(s"($rx,$ox) ${rowOutput(ox)}")
        }
        IndexedRow(rx.toLong, Vectors.dense(rowOutput))
      }
      out.toIterator
    }

    val localRows = gmat.collect
    val outVects = gmat.map { irow =>
      //    val bcGmat = sc.broadcast(gmat.collect)
      val veclen = irow.vector.size
      val varr = irow.vector.toArray
      val rx = irow.index.toInt
      for (lowerColx <- rx until veclen) {
        varr(lowerColx) = if (lowerColx == rx) {
          0.0
        } else {
          println(s"lowercolx=$lowerColx rx=$rx")
          localRows(lowerColx).vector(rx)
        }
      }
      irow
    }
    val ret = new IndexedRowMatrix(outVects)
    ret

  }

  import org.apache.spark.mllib.linalg.DenseMatrix

  def printMatrix(mat: DenseMatrix): String = {
    val darr = mat.toArray
    val stride = (darr.length / mat.numCols).toInt
    val sb = new StringBuilder
    def leftJust(s: String, len: Int) = {
      "     ".substring(0, len - s.length) + s
    }

    for (r <- 0 until mat.numRows) {
      for (c <- 0 until mat.numCols) {
        sb.append(leftJust(f"${darr(c * stride + r)}%.2f ", 6))
      }
      sb.append("\n")
    }
    sb.toString
  }

  def printMatrix(mat: IndexedRowMatrix): String = {
    println(s"outmat rows=${mat.rows.count}")
    printMatrix(new DenseMatrix(mat.numRows.toInt, mat.numCols.toInt, mat.toBreeze.toArray))
  }


}
