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
    import SpectralClustering._
    val Sigma = 1.0
    val outmat = SpectralClustering.computeGaussianSimilarity(sc, createTestRowMatrix(sc), Sigma)
    println(s"GaussianSim: \n${SpectralClustering.printMatrix(outmat)}")
    val normmat = SpectralClustering.computeUnnormalizedDegreeMatrix(outmat.toBreeze)
    println(s"unnormalized degmat: \n${SpectralClustering.printMatrix(normmat)}")
    for (x <- 0 to 0) {
      val degMat = if (x==0) {
        SpectralClustering.computeDegreeMatrix(outmat.toBreeze)
      } else {
        SpectralClustering.computeDegreeMatrix(outmat.toBreeze, negative=true)
      }
      println(s"Degreematrix: \n${SpectralClustering.printMatrix(degMat)}")
      val eigs = computeEigenVectors(degMat, 4)
      println(s"eigenvalues: \n${SpectralClustering.printVector(eigs._1)}")
      println(s"eigenvectors: \n${printMatrix(eigs._2)}")
      val (rddVecs, model) = eigsToKmeans(sc, eigs)
      println(s"cluster centers BABY! ${model.clusterCenters.mkString(",")}")
      val predictions = model.predict(rddVecs)
      println(s"Predictions=${predictions.collect.mkString(",")}")
    }
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
          Math.pow(row,1.1)*0.2 + Math.pow(col,1.1) * 0.11
        }))
      }
    }
//    val rdd = sc.parallelize {
//      Array(
//        IndexedRow(0,Vectors.dense(Array(7.0,7.1,7.3))),
//        IndexedRow(1,Vectors.dense(Array(6.1,6.3,6.7))),
//        IndexedRow(2,Vectors.dense(Array(1.0,1.2,1.3))),
//        IndexedRow(3,Vectors.dense(Array(1.2,2.5,2.8))),
//        IndexedRow(4,Vectors.dense(Array(1.9,2.9,3.0)))
//      )
//    }
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