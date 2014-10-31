package org.apache.spark.mllib.clustering

import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, RowMatrix}
import org.apache.spark.mllib.util.LocalSparkContext
import org.apache.spark.rdd.{ZippedWithIndexRDDPartition, ParallelCollectionPartition, RDD}
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

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV}
import org.apache.spark._
import org.apache.spark.mllib.linalg.{Matrices, Matrix, Vectors, Vector => MVector}

class SpectralClusteringSuite extends FunSuite with LocalSparkContext {

  def computeGaussianSimilarity(mat: IndexedRowMatrix, sigma: Double): Matrix = {
    if (sc == null) {
      val conf = new SparkConf()
        .setMaster("local")
        .setAppName("test")
      sc = new SparkContext(conf)
    }
    val n = mat.numCols().toInt
    val nr = mat.numRows.toInt

    val cachedRows = mat.rows.cache
    println(s"CachedRows size=${cachedRows.count}")

    /*
        val bcIdf = dataset.context.broadcast(idf)
        dataset.mapPartitions { iter =>
          val thisIdf = bcIdf.value

     */
    val crows = mat.rows.collect
    assert(crows.isInstanceOf[Array[IndexedRow]])
    val bcRows = sc.broadcast(crows)

    //    mat.rows.cache
    type RddInput = IndexedRow
    type RddOutput = IndexedRow
    val rowsRdd = mat.rows.cache

    val parentRdd = rowsRdd
    type ParentRddInput = IndexedRow
    val parentPartitions = parentRdd.partitions

    class MyPartition(idx: Int) extends Partition with Serializable {
      override val index: Int = idx

      override def hashCode(): Int = idx
    }

    val myRdd = new RDD[RddOutput](parentRdd) {
      val copyOfParentPartitions = parentPartitions

      /* lazy */ val allRows = parentRdd.collect
      println(s"allrows.size=${allRows.size}")

      override protected def getPartitions: Array[Partition] = copyOfParentPartitions

      override def compute(split: Partition, context: TaskContext): Iterator[RddOutput] = {
        val output = new BDV[Double](new Array[Double](n * nr))
        val pvalues = split.asInstanceOf[ParallelCollectionPartition[ParentRddInput]].values
        println(pvalues.mkString(","))
        val out = pvalues.map {
          case indexedRow =>
            val rx = indexedRow.index
            val vect = indexedRow.vector
            val rowOutput = new Array[Double](nr)
            for (ox <- 0 until nr) {
              println(s"($rx,$ox)")
              if (ox > rx) {
                rowOutput(ox) = Double.NaN
              } else {
                //              println(s"ox=$ox rx=$rx vect(0)=${vect(0)}")
                val otherRow = allRows(ox)

                val distVal = gaussianDist(vect.toArray, otherRow.vector.toArray, sigma)
                rowOutput(ox) = distVal
              }
            }
            IndexedRow(rx.toLong, Vectors.dense(rowOutput))
        }
        val outIter = out.toIterator
        outIter
      }

      def gaussianDist(c1arr: Array[Double], c2arr: Array[Double], sigma: Double) = {
        val c1c2 = c1arr.zip(c2arr)
        val dist = Math.exp((0.5 / Math.pow(sigma, 2.0)) * c1c2.foldLeft(0.0) {
          case (dist: Double, (c1: Double, c2: Double)) =>
            dist + Math.pow(c1 - c2, 2)
        })
        dist
      }

    }

    //    /* lazy */ val out1 = myRdd.sortBy(_._2).map(_._1)
    val outVects = myRdd.collect
    val localVects = outVects.sortBy(_.index)
    val veclen = localVects(0).vector.size
    val outArr = localVects.foldLeft(new Array[Double](veclen * localVects.size)) {
      case (arr, irow) =>
        System.arraycopy(irow.vector.toArray, 0, arr, irow.index.toInt * veclen, veclen)
        arr
    }
    Matrices.dense(localVects.size, veclen, outArr)
  }

  def printMatrix(mat: Matrix) = {
    val darr = mat.toArray
    val stride = (darr.length / mat.numCols).toInt
    val sb = new StringBuilder
    def leftJust(s:String, len: Int) = {
      "     ".substring(0,len - s.length) + s
    }

    for (r <- 0 until mat.numRows) {
      for (c <- 0 until mat.numCols) {
        sb.append(leftJust(f"${darr(c * stride + r)}%.2f ",6))
      }
      sb.append("\n")
    }
    sb.toString
  }

  import org.apache.spark._

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

  def createIndexedMatTestRdd(sc: SparkContext, nRows: Int, nCols: Int): RDD[IndexedRow] = {

    import org.apache.spark.mllib.linalg._
    println("CreateIndexedMatTestRDD")
    val rdd = sc.parallelize {
      Array.tabulate(nRows) { row =>
        IndexedRow(row,Vectors.dense(Array.tabulate[Double](nCols) { col =>
          row * 0.5 + col * 0.21
        }))
      }
    }
    rdd
  }


  import org.apache.spark.mllib.linalg.distributed._

  def createTestRowMatrix(@transient sc: SparkContext) = {
    new IndexedRowMatrix(createIndexedMatTestRdd(sc, NRows, NCols))
  }

  val NCols = 3
  // 100
  val NRows = 8 // 10000


  def testGaussianSimilarity(@transient sc: SparkContext) = {
    val Sigma = 2.0
    val out = computeGaussianSimilarity(createTestRowMatrix(sc), Sigma)
    println(printMatrix(out))
  }

  test("main") {
    SpectralClusteringSuite.main(null)
  }

}

object SpectralClusteringSuite {
  def main(args: Array[String]) {
    val sc = new SparkContext("local[2]", "GSTest")
    val testee = new SpectralClusteringSuite
    testee.testGaussianSimilarity(sc)
  }

}
