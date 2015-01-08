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

package org.apache.spark.graphx

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkContext}

/**
 * SpectralClustering
 *
 */
object SpectralClustering {

  type DVector = Array[Double]

  type LabeledVector = (String, DVector)

  type Vertices = Seq[LabeledVector]

  def gaussianDist(c1arr: DVector, c2arr: DVector, sigma: Double) = {
    val c1c2 = c1arr.zip(c2arr)
    val dist = Math.exp((0.5 / Math.pow(sigma, 2.0)) * c1c2.foldLeft(0.0) {
      case (dist: Double, (c1: Double, c2: Double)) =>
        dist - Math.pow(c1 - c2, 2)
    })
    dist
  }

  def readVerticesfromFile(verticesFile: String): Vertices = {

    import scala.io.Source
    val vertices = Source.fromFile(verticesFile).getLines.map { l =>
      val toks = l.split("\t")
      val arr = toks.slice(1, toks.length).map(_.toDouble)
      (toks(0), arr)
    }.toList
    println(s"Read in ${vertices.length} from $verticesFile")
    println(vertices.map { case (x, arr) => s"($x,${arr.mkString(",")})"}.mkString("[", ",\n", "]"))
    vertices
  }

  def cluster(sc: SparkContext, vertices: Vertices, sigma: Double) = {
    val nVertices = vertices.length
    val gaussRdd = createGaussianRdd(sc, vertices, sigma).cache()

    var ix = 0
    val indexedGaussRdd = gaussRdd.map { d =>
      ix += 1
      (ix, d)
    }

    val (columnsRdd, colSumsRdd) = createColumnsRdds(sc, vertices, indexedGaussRdd)
    val degreesRdd = createDegreesRdd(sc, vertices, indexedGaussRdd, colSumsRdd)
    val principalEigen = getPrincipalEigen(sc, vertices, indexedGaussRdd, columnsRdd, colSumsRdd)
    degreesRdd
  }

  def createGaussianRdd(sc: SparkContext, vertices: Vertices, sigma: Double) = {
    val nVertices = vertices.length
    val gaussRdd = sc.parallelize({
      val dvect = new Array[DVector](nVertices)
      for (i <- 0 until vertices.size) {
        dvect(i) = new DVector(nVertices)
        for (j <- 0 until vertices.size) {
          dvect(i)(j) = if (i != j) {
            gaussianDist(vertices(i)._2, vertices(j)._2, sigma)
          } else {
            0.0
          }
        }
      }
      dvect
    }, nVertices)
    gaussRdd
  }

  def createColumnsRdds(sc: SparkContext, vertices: Vertices, indexedGaussRdd: RDD[(Int, DVector)]) = {
    val nVertices = vertices.length
    val ColsPartitioner = new Partitioner() {
      override def numPartitions: Int = nVertices

      override def getPartition(key: Any): Int = {
        val index = key.asInstanceOf[Int]
        index % nVertices
      }
    }
    // Needed for the PairRDDFunctions implicits
    import org.apache.spark.SparkContext._
    val columnsRdd = indexedGaussRdd.partitionBy(ColsPartitioner) // Is this giving the desired # partitions
      .mapPartitions({ iter =>
      var cntr = -1
      iter.map { case (rowIndex, dval) =>
        cntr += 1
        (cntr, dval)
      }
    }, preservesPartitioning = true)

    val colSums = columnsRdd.mapPartitions { iter =>
      iter.map { case (rowIndex, darr) =>
        (rowIndex, darr.foldLeft(0.0) { case (sum, dval) =>
          sum + dval
        })
      }
    }

    (columnsRdd, colSums)
  }

  def createDegreesRdd(sc: SparkContext,
                       vertices: Vertices,
                       indexedGaussRdd: RDD[(Int, DVector)],
                       colSums: RDD[(Int, Double)]) = {
    val nVertices = vertices.length
    val bcNumVertices = sc.broadcast(nVertices)
    val bcColSums = sc.broadcast(colSums.collect)
    val degreeRdd = indexedGaussRdd.mapPartitionsWithIndex({ (partIndex, iter) =>
      val localNumVertices = bcNumVertices.value
      val localColSums = bcColSums.value
      var rowctr = -1
      iter.toList.map { case (dindex, dval) =>
        for (ix <- 0 until dval.length) {
          if (ix != partIndex) {
            dval(ix) = -1.0 * dval(ix)
          } else {
            dval(ix) = localColSums(partIndex)._2
          }
        }
        (rowctr, dval)
      }.iterator
    }, preservesPartitioning = true)

    degreeRdd
  }

  def getPrincipalEigen(sc: SparkContext,
                        vertices: Vertices,
                        indexedGaussRdd: RDD[(Int, DVector)],
                        columnsRdd: RDD[(Int, DVector)],
                        colSumsRdd: RDD[(Int, Double)]) = {


    //    val labelsRdd = sc.parallelize(vertices)
    //    val verticesVals = vertices.map{ _._2}
    //    val bcFirstCol = sc.broadcast(verticesVals)
    //    val firstColumnProductsRdd = gaussRdd.mapPartitions ({ row =>
    //      val firstCol = bcFirstCol.value // .asInstanceOf[DVector]
    //      var sum = 0.0
    //      row.zipWithIndex.foreach { case (rowval, ix) =>
    //        sum += rowval * firstCol(ix)
    //      }
    //      Iterator(sum)
    //    }, preservesPartitioning = true)
    //
    //    val prodResults = firstColumnProductsRdd.take(5)
    //    val prodResults = firstColumnProductsRdd.collect
  }

  def printMatrix(darr: Array[DVector], numRows: Int, numCols: Int): String = {
    val flattenedArr = darr.zipWithIndex.foldLeft(new DVector(numRows * numCols)) { case (flatarr, (row, indx)) =>
      System.arraycopy(row, 0, flatarr, indx * numCols, numCols)
      flatarr
    }
    printMatrix(flattenedArr, numRows, numCols)
  }

  def printMatrix(darr: DVector, numRows: Int, numCols: Int): String = {
    val stride = (darr.length / numCols).toInt
    val sb = new StringBuilder
    def leftJust(s: String, len: Int) = {
      "         ".substring(0, len - s.length) + s
    }

    for (r <- 0 until numRows) {
      for (c <- 0 until numCols) {
        sb.append(leftJust(f"${darr(c * stride + r)}%.6f", 9) + " ")
      }
      sb.append("\n")
    }
    sb.toString
  }

  def main(args: Array[String]) {
    val sc = new SparkContext("local[2]", "TestSpark")
    val vertFile = "../data/graphx/new_lr_data.10.txt"
    val sigma = 1.0
    val vertices = readVerticesfromFile(vertFile)
    SpectralClustering.cluster(sc, vertices, sigma)
  }

}
