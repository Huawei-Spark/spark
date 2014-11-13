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
import org.apache.spark.mllib.linalg.{EigenValueDecomposition, Vectors}
import org.apache.spark.mllib.linalg.distributed.{RowMatrix, IndexedRow, IndexedRowMatrix}

/**
 * SpectralClustering
 */
class SpectralClustering {
  val logger = Logger.getLogger(getClass.getName)
}

object SpectralClustering {
  val log = Logger.getLogger(SpectralClustering.getClass)

  def computeGaussianSimilarity(sc: SparkContext, mat: IndexedRowMatrix): IndexedRowMatrix = {
    computeGaussianSimilarity(sc: SparkContext, mat: IndexedRowMatrix, 1.0)
  }

  /**
   * @param nRows
   * @param nCols
   * @param arr  Input array in Column Major format
   * @param nIters
   * @return
   */
  def powerIt(nRows: Int, nCols: Int, arr: Array[Double], nIters: Int = 500) = {
    val rand = new java.util.Random
    val oarr = Array.tabulate(nCols) { x => rand.nextDouble}
    for (iter <- 0 until nIters) {
      (0 until nCols).foreach { r =>
        (0 until nCols).foreach { c =>
          oarr(r) = arr(r * nCols + c) * oarr(r)
        }
        //        println(s"iter=$iter oarr(r) = ${oarr(r)}%.2f")
      }
      val norm = Math.sqrt(oarr.reduceLeft((sum, cval) => sum + cval * cval))
      (0 until nRows).foreach { rx =>
        oarr(rx) /= norm
      }
      println(s"oarr = ${oarr.mkString(",")}")
    }
    oarr
  }

  def printMatrix(nRows: Int, nCols: Int, arr: Array[Double]) = {
    val sb = new StringBuilder
    for (rx <- 0 until nRows) {
      for (cx <- 0 until nCols) {
        sb.append(s"${arr(rx * nCols + cx) % 7.2f} ")
      }
      sb.append("\n")
    }
    sb.toString
  }

  def computeGaussianSimilarity(sc: SparkContext, mat: IndexedRowMatrix, sigma: Double):
      IndexedRowMatrix = {

    def gaussianDist(c1arr: Array[Double], c2arr: Array[Double], sigma: Double) = {
      val c1c2 = c1arr.zip(c2arr)
      val dist = Math.exp((0.5 / Math.pow(sigma, 2.0)) * c1c2.foldLeft(0.0) {
        case (dist: Double, (c1: Double, c2: Double)) =>
          dist - Math.pow(c1 - c2, 2)
      })
      dist
    }

    val bcMat = sc.broadcast(mat.rows.collect)
    val gmat = mat.rows.mapPartitions { rows =>

      val bcMatLocal = bcMat.value
      val allRows = bcMatLocal
      assert(bcMatLocal.isInstanceOf[Array[IndexedRow]], "bc mat should be indexedrowmatrix")

      val nr = bcMatLocal.size

      val out = rows.map { indexedRow =>
        val rx = indexedRow.index
        val vect = indexedRow.vector
        val rowOutput = new Array[Double](nr)
        for (ox <- 0 until nr) {
          rowOutput(ox) = if (ox < rx) {
            Double.NaN
          } else if (ox == rx) {
            0.0
          } else {
            val otherRow = allRows(ox)
            val distVal = gaussianDist(vect.toArray, otherRow.vector.toArray, sigma)
            distVal
          }
          //          println(s"($rx,$ox) ${rowOutput(ox)}")
        }
        IndexedRow(rx.toLong, Vectors.dense(rowOutput))
      }
      out.toIterator
    }
    val localRows = gmat.collect
    val outVects = gmat.map { irow =>
      // val bcGmat = sc.broadcast(gmat.collect)
      val veclen = irow.vector.size
      val varr = irow.vector.toArray
      val rx = irow.index.toInt
      for (lowerColx <- 0 until rx) {
        varr(lowerColx) = if (lowerColx == rx) {
          0.0
        } else {
          //          println(s"lowercolx=$lowerColx rx=$rx")
          localRows(lowerColx).vector(rx)
        }
      }
      irow
    }
    val ret = new IndexedRowMatrix(outVects)
    ret
  }

  import org.apache.spark.mllib.linalg.DenseMatrix

  def computeUnnormalizedDegreeMatrix(inMat: BDM[Double]): BDM[Double] = {
    val inarr = inMat.toArray
    val K = inMat.cols
    val darr = (0 until K).
      foldLeft(new Array[Double](K * K)) { case (arr, col) =>
      arr(col * inMat.cols + col) = (0 until K).foldLeft(0.0) { case (sum, row) =>
        sum + inarr(row * K + col)
      }
      //      (0 until K).reduceLeft {
      //        (sum : Double, rowx : Int) =>
      //          sum + inarr(rowx * K + col)
      //      }
      arr
    }
    new BDM(K, K, darr)
  }

  def computeDegreeMatrix(inMat: BDM[Double], negative: Boolean = false): BDM[Double] = {
    val umat = computeUnnormalizedDegreeMatrix(inMat)
    val mult = if (negative) -1.0 else 1.0
    (umat - inMat) * mult
  }

  def computeEigenVectors(mat: BDM[Double],
                          k: Int,
                          tol: Double = 1e-6,
                          maxIterations: Int = 200): (BDV[Double], BDM[Double]) = {
    val eig = EigenValueDecomposition.symmetricEigs(
      v => mat * v, mat.cols, k, tol, maxIterations)
    val evectarr = (Seq(0.0) ++ eig._1.toArray.toSeq).toArray
    val oneslen = eig._2.rows
    val emarr = new Array[Double](oneslen + eig._2.size)
    val arr1 = Array.fill(oneslen)(1.0)
    System.arraycopy(arr1, 0, emarr, 0, oneslen)
    //    System.arraycopy(BDV.ones(oneslen).toArray,0,emarr,0,oneslen)
    System.arraycopy(eig._2.toArray, 0, emarr, oneslen, eig._2.size)
    //    val emarr = (Seq(BDV.ones(eig._1.length+1).toArray ++ eig._2.toArray)).toArray
    (new BDV[Double](evectarr), new BDM[Double]
    (eig._2.rows, eig._2.cols + 1, emarr, 0, eig._2.rows, false))
  }

  def eigsToKmeans(sc: SparkContext, eigs: (BDV[Double], BDM[Double])) = {
    val dm = eigs._2
    val darr = eigs._2.toArray
    val vecs = (0 until dm.rows).map { row =>
      org.apache.spark.mllib.linalg.Vectors
          .dense((0 until dm.cols).foldLeft(Array.ofDim[Double](dm.cols)) { case (arr, colx) =>
        arr(colx) = darr(row + colx * dm.rows)
        arr
      })
    }
    val rddVecs = sc.parallelize(vecs)
    //    val model = KMeans.train(rddVecs,2,10)
    val model = KMeans.train(rddVecs, 2, 10)
    (rddVecs, model)
  }

  import org.apache.spark.mllib.linalg.DenseMatrix

  def printMatrix(mat: DenseMatrix): String = {
    val darr = mat.toArray
    val stride = (darr.length / mat.numCols).toInt
    val sb = new StringBuilder
    def leftJust(s: String, len: Int) = {
      "         ".substring(0, len - s.length) + s
    }

    for (r <- 0 until mat.numRows) {
      for (c <- 0 until mat.numCols) {
        sb.append(leftJust(f"${darr(c * stride + r)}%.6f", 9) + " ")
      }
      sb.append("\n")
    }
    sb.toString
  }

  def printMatrix(mat: IndexedRowMatrix): String = {
    println(s"outmat rows=${mat.rows.count}")
    val outmat = new DenseMatrix(mat.numRows.toInt, mat.numCols.toInt, mat.toBreeze.toArray)
    printMatrix(outmat)

  }

  def printMatrix(mat: BDM[Double]): String = {
    println(s"outmat rows=${mat.rows}")
    val outmat = new DenseMatrix(mat.rows.toInt, mat.cols.toInt, mat.toArray)
    printMatrix(outmat)

  }

  def printVector(vect: BDV[Double]) = {
    val sb = new StringBuilder
    def leftJust(s: String, len: Int) = {
      "         ".substring(0, len - s.length) + s
    }

    for (c <- 0 until vect.length) {
      sb.append(leftJust(f"${vect(c)}%.6f", 9) + " ")
    }
    sb.toString

  }

}
