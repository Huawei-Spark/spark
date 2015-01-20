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

  type DEdge = Edge[DVector]
  type LabeledVector = (VertexId, DVector)

  type IndexedVector = (Int, DVector)

  type Vertices = Seq[LabeledVector]

  type DGraph = Graph[DVector, DVector]

  val DefaultMinNormChange: Double = 1e-11

  val DefaultIterations: Int = 20

  def cluster(sc: SparkContext, vertices: Vertices, nClusters: Int, sigma: Double,
              nPowerIterations: Int) = {
    val nVertices = vertices.length
    val edgesRdd = createGaussianEdgesRdd(sc, vertices, sigma)
    val G = createGraphFromEdges(edgesRdd)

  }

  def createGraphFromEdges(edgesRdd: RDD[DEdge]) = {
    val G = Graph.fromEdges(edgesRdd, -1L)
  }

  def getPrincipalEigen(sc: SparkContext,
                        G: DGraph,
//                        optGraphSize: Option[Int] = None,
                        nIterations: Int = DefaultIterations,
                        minNormChange: Double = DefaultMinNormChange
                         ): (Double, DVector) = {

//    val graphSize = optGraphSize.getOrElse(G.edges.count().toInt)
    val graphSize = G.edges.count().toInt
    var eigenRdd: RDD[(Int, Double)] = null
    var eigEst: Seq[(Int, Double)] = for (ix <- 0 until graphSize)
      yield (ix, 1.0 / Math.sqrt(graphSize))
    val norms = new DVector(graphSize)
    var normChange = Int.MaxValue
    for (iter <- 0 until nIterations;
         if Math.abs(normChange) > minNormChange) {
      val bcEigEst = sc.broadcast(eigEst)
      //   *   aggregateMessages[Int](ctx => ctx.sendToDst(1), _ + _)
      val tmpEigen = G.aggregateMessages[Double]( ctx => ctx.sendToSrc(
        scalarDot(ctx.attr, ctx.srcAttr)),  (_,_) => Double.NaN)
      val collectedVerts = tmpEigen.collect.map(_._2)
      val vnorm = norm(collectedVerts)
      print(s"Vertices[$iter]:\n${printMatrix(collectedVerts,graphSize,graphSize)}")
    }
    G.
  }

  def printGraph(G: DGraph) = {

  }

//  def normDot(d1: DVector, d2: DVector) = {
//    var sum = 0.0
//    var tmpOutv = d1.zip(d2).map { case (d1v, d2v) =>
//      val v = d1v * d2v
//      sum += v
//      v
//    }
//    val norm = Math.sqrt(sum)
//    for (tx <- 0 until tmpOutv.length) {
//      tmpOutv(tx) /= norm
//    }
//    (norm, tmpOutv)
//  }

  def scalarDot(d1: DVector, d2: DVector) = {
    Math.sqrt(d1.zip(d2).foldLeft(0.0) { case (sum, (d1v, d2v)) =>
      sum + d1v * d2v
    })
  }

  def vectorDot(d1: DVector, d2: DVector) = {
    d1.zip(d2).map { case (d1v, d2v) =>
      d1v * d2v
    }
  }

  def normVect(d1: DVector, d2: DVector) = {
    val scaldot = scalarDot(d1, d2)
    vectorDot(d1, d2).map {
      _ / scaldot
    }
  }

  def readVerticesfromFile(verticesFile: String): Vertices = {

    import scala.io.Source
    val vertices = Source.fromFile(verticesFile).getLines.map { l =>
      val toks = l.split("\t")
      val arr = toks.slice(1, toks.length).map(_.toDouble)
      (toks(0).toLong, arr)
    }.toList
    println(s"Read in ${vertices.length} from $verticesFile")
    //    println(vertices.map { case (x, arr) => s"($x,${arr.mkString(",")})"}
    // .mkString("[", ",\n", "]"))
    vertices
  }

  def verticesToEdges(vertices: Vertices): Seq[DEdge] = {
    val nVertices = vertices.length
    vertices.zipWithIndex.map { case ((vid, darr), ix) =>
      new DEdge(vid, vertices((ix + 1) % nVertices)._1, darr)
    }
  }

  def gaussianDist(c1arr: DVector, c2arr: DVector, sigma: Double) = {
    val c1c2 = c1arr.zip(c2arr)
    val dist = Math.exp((0.5 / Math.pow(sigma, 2.0)) * c1c2.foldLeft(0.0) {
      case (dist: Double, (c1: Double, c2: Double)) =>
        dist - Math.pow(c1 - c2, 2)
    })
    dist
  }

  def createGaussianEdgesRdd(sc: SparkContext, vertices: Vertices, sigma: Double) = {
    val nVertices = vertices.length
    val coordinateEdges = verticesToEdges(vertices)
    val bcCoordinateEdges = sc.broadcast(coordinateEdges)
    val edgesRdd = sc.parallelize(coordinateEdges, nVertices)
    edgesRdd.mapPartitions { iter =>
      val localEdgesRdd = bcCoordinateEdges.value
      iter.map { case edge =>
        val darr = new DVector(nVertices)
        for (j <- 0 until nVertices) {
          darr(j) = if (edge.srcId != localEdgesRdd(j).srcId) {
            gaussianDist(darr, localEdgesRdd(j).attr, sigma)
          } else {
            0.0
          }
        }
        val newEdge = new DEdge(edge.srcId, edge.dstId, darr)
        newEdge
      }
    }
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
    println(s"Subtracted VectorsRdd\n${
      printMatrix(subVectRdd.collect.map(_._2), vect.length,
        vect.length)
    }")
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
