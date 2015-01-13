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

import org.apache.spark.{Partitioner, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * SpectralClustering
 *
 */
object SpectralClustering {

  type RCId = Int

  type DVector = Array[Double]

  type LabeledVector = (String, DVector)

  type IndexedVector = (Int, DVector)

  type Vertices = Seq[LabeledVector]

  val DefaultMinNormChange: Double = 1e-11

  val DefaultIterations: Int = 20

  def someNewMethod(i: Int) = ???

  private val twoTo30 = 1 << 30;
  def rowOrColIdFromVertexId(vid: VertexId) = if (vid >= (1<<30) ) {
    vid - (1 << 30)
  } else {
    vid
  }

  sealed trait RowOrCol
  case object Row extends RowOrCol
  case object Col extends RowOrCol

  def vertexIdFromRowOrColId(id: Int, rowOrCol: RowOrCol) = if (rowOrCol == Row) {
    id
  } else {
    id + (1<<30)
  }

  def graphFromRowsAndColsRdds(rowsRdd: RDD[IndexedVector],
                                 colsRdd: RDD[IndexedVector]) = {
    val rowIds = rowsRdd.map { case (rx, dvect) =>
      vertexIdFromRowOrColId(rx, Row)
    }

    Edge
    colsRdd.cartesian(rowIds).map { case ((cx, dvect), rx) =>
      val edge = Edge(vertexIdFromRowOrColId(cx, Col), rx,
        dvect)
      edge
    }
//    Graph.fromEdgeTuples(colsRdd)
  }

  def cluster(sc: SparkContext, vertices: Vertices, nClusters: Int, sigma: Double,
              nPowerIterations: Int) = {
    val nVertices = vertices.length


    val rowsRdd = createGaussianRdd(sc, vertices, sigma)
    val (colsRdd, colSums) = createColumnsRdd(sc, rowsRdd)
//    val graph = edgesRddFromRowsAndColsRdds(rowsRdd, colsRdd)

//    val vertRdd = Graph.fromEdges()
//    //    val gaussRdd = createGaussianRdd(sc, vertices, sigma).cache()
//
//    var ix = 0
//    var indexedGaussRdd = gaussRdd.map { d =>
//      ix += 1
//      (ix, d)
//    }
  }

  //   val vertexRdd : RDD[(Int,DVector)] = sc.parallelize(

  def readVerticesfromFile(verticesFile: String): Vertices = {

    import scala.io.Source
    val vertices = Source.fromFile(verticesFile).getLines.map { l =>
      val toks = l.split("\t")
      val arr = toks.slice(1, toks.length).map(_.toDouble)
      (toks(0), arr)
    }.toList
    println(s"Read in ${vertices.length} from $verticesFile")
    //    println(vertices.map { case (x, arr) => s"($x,${arr.mkString(",")})"}
    // .mkString("[", ",\n", "]"))
    vertices
  }

  def gaussianDist(c1arr: DVector, c2arr: DVector, sigma: Double) = {
    val c1c2 = c1arr.zip(c2arr)
    val dist = Math.exp((0.5 / Math.pow(sigma, 2.0)) * c1c2.foldLeft(0.0) {
      case (dist: Double, (c1: Double, c2: Double)) =>
        dist - Math.pow(c1 - c2, 2)
    })
    dist
  }

  def createGaussianRdd(sc: SparkContext, vertices: Vertices, sigma: Double) = {
    val nVertices = vertices.length
    val gaussRdd = sc.parallelize({
      val ivect = new Array[IndexedVector](nVertices)
      for (i <- 0 until vertices.size) {
        ivect(i) = new IndexedVector(i, new DVector(nVertices))
        for (j <- 0 until vertices.size) {
          ivect(i)._2(j) = if (i != j) {
            gaussianDist(vertices(i)._2, vertices(j)._2, sigma)
          } else {
            0.0
          }
        }
      }
      ivect
    }, nVertices)
    gaussRdd
  }

  def norm(vect: DVector): Double = {
    Math.sqrt(vect.foldLeft(0.0) { case (sum, dval) => sum + Math.pow(dval, 2)})
  }

  def onesVector(len: Int): DVector = {
    Array.fill(len)(1.0)
  }

  val calcEigenDiffs = false

  def withinTol(d: Double, tol: Double) = Math.abs(d) <= tol

  def makeNonZero(dval: Double, tol: Double = 1.0e-8) = {
    if (Math.abs(dval) < tol) {
      Math.signum(dval) * tol
    } else {
      dval
    }
  }

  def createColumnsRdd(sc: SparkContext, rowsRdd: RDD[IndexedVector]) = {
    val nVertices = rowsRdd.count.toInt
    val ColsPartitioner = new Partitioner() {
      override def numPartitions: Int = nVertices

      override def getPartition(key: Any): Int = {
        val index = key.asInstanceOf[Int]
        index % nVertices
      }
    }
    // Needed for the PairRDDFunctions implicits
    import org.apache.spark.SparkContext._
    // TODO:  does the following yield the desired # partitions?
    val columnsRdd = rowsRdd.partitionBy(ColsPartitioner)
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

  def project(basisVector: DVector, inputVect: DVector) = {
    val dotprod = basisVector.zip(inputVect).foldLeft(0.0) {
      case (sum, (b, p)) => sum + b * p
    }
    val pnorm = makeNonZero(norm(basisVector))
    val projectedVect = basisVector.map(_ * (dotprod / Math.pow(pnorm, 2)))
    projectedVect
  }

  def subtract(v1: DVector, v2: DVector) = {
    val subvect = v1.zip(v2).map { case (v1val, v2val) => v1val - v2val}
    subvect
  }

  def subtractProjection(sc: SparkContext, vectorsRdd: RDD[IndexedVector], vect: DVector):
  RDD[IndexedVector] = {
    val bcVect = sc.broadcast(vect)
    val subVectRdd = vectorsRdd.mapPartitions { iter =>
      val localVect = bcVect.value
      iter.map { case (ix, row) =>
        val subproj = subtractProjection(row, localVect)
        //        println(s"Subproj for ${row.mkString(",")} =\n${subproj.mkString(",")}")
        (ix, subproj)
      }
    }
    println(s"Subtracted VectorsRdd\n${printMatrix(subVectRdd.collect.map(_._2), vect.length, 
      vect.length)}")
    subVectRdd
  }

  def subtractProjection(vect: DVector, basisVect: DVector): DVector = {
    val proj = project(basisVect, vect)
    val subVect = subtract(vect, proj)
    subVect
  }

  def createColumnsRdds(sc: SparkContext, vertices: Vertices, 
                        indexedDegreesRdd: RDD[IndexedVector]) = {
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
    // Does the following give the desired # partitions
    val columnsRdd = indexedDegreesRdd.partitionBy(ColsPartitioner)
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

  def printMatrix(darr: Array[DVector], numRows: Int, numCols: Int): String = {
    val flattenedArr = darr.zipWithIndex.foldLeft(new DVector(numRows * numCols)) {
      case (flatarr, (row, indx)) =>
        System.arraycopy(row, 0, flatarr, indx * numCols, numCols)
      flatarr
    }
    printMatrix(flattenedArr, numRows, numCols)
  }

  def printMatrix(darr: DVector, numRows: Int, numCols: Int): String = {
    val stride = (darr.length / numCols)
    val sb = new StringBuilder
    def leftJust(s: String, len: Int) = {
      "         ".substring(0, len - Math.min(len, s.length)) + s
    }

    for (r <- 0 until numRows) {
      for (c <- 0 until numCols) {
        sb.append(leftJust(f"${darr(c * stride + r)}%.6f", 9) + " ")
      }
      sb.append("\n")
    }
    sb.toString
  }

  def printVect(dvect: DVector) = {
    dvect.mkString(",")
  }

}
