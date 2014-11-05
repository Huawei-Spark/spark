package org.apache.spark.mllib.clustering
/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements. See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.mllib.util.LocalSparkContext
import org.apache.spark.rdd.{ZippedWithIndexRDDPartition, ParallelCollectionPartition, RDD}
import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV}
import org.apache.spark._
import org.apache.spark.mllib.linalg.{Vector => MVector, DenseMatrix, Matrices, Matrix, Vectors}
import org.scalatest.FunSuite
object SpectralClusteringSuite {
  def main(args: Array[String]) {
    val sc = new SparkContext("local[2]", "GSTest")
    val testee = new SpectralClusteringSuite
    // testee.testBroadcast
    testee.testGaussianSimilarity(sc)
  }
}
class SpectralClusteringSuite extends FunSuite with LocalSparkContext {
  val NCols = 3 // 100
  val NRows = 8 // 10000
  def testGaussianSimilarity(@transient sc: SparkContext) = {
    val Sigma = 1.0
    val out = SpectralClustering.computeGaussianSimilarity(sc, createTestRowMatrix(sc), Sigma)
    println(SpectralClustering.printMatrix(out))
  }
  import org.apache.spark._
  def testBroadcast = {
    val NRows = 8
    val NCols = 3
    val conf = new SparkConf()
      .setMaster("local[1]")
      .setAppName("test")
    val sc = new SparkContext(conf)
    System.err.println("testBroadcast before parallelize")
    val data = sc.parallelize {
      0 until NRows
    }.map { ix => IndexedRow(ix.toLong,
      Vectors.dense(Array.tabulate(NCols) { cx => ix * 0.5 + cx.toDouble * 0.21})
    )
    }
    System.err.println("testBroadcast after parallelize")
    def rmatrix = new IndexedRowMatrix(data, NRows, NCols)
    val bcMat = sc.broadcast(rmatrix.rows.collect)
    // val bcMatLocal = bcMat.value
    // println(s"bcMatLocal size = ${bcMatLocal.size}")
    // assert(bcMatLocal.size == NRows)
    val data1 = rmatrix.rows.collect
    val rdd = rmatrix.rows.mapPartitions { part =>
      val bcInnerMat = bcMat.value
      part.zip(bcInnerMat.toIterator).map { case (prow, brow) =>
        println(s"row=$prow brow=$brow")
        prow
      }
    }
    println(rdd.collect)
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
  def createIndexedMatTestRdd(sc: SparkContext, nRows: Int, nCols: Int): RDD[IndexedRow] = {
    import org.apache.spark.mllib.linalg._
    println("CreateIndexedMatTestRDD")
    val rdd = sc.parallelize {
      Array.tabulate(nRows) { row =>
        IndexedRow(row, Vectors.dense(Array.tabulate[Double](nCols) { col =>
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
  test("main") {
    SpectralClusteringSuite.main(null)
  }
}