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
object SpectralClusteringUsingRdd {

  type DVector = Array[Double]

  type LabeledVector = (String, DVector)

  type IndexedVector = (Int, DVector)

  type Vertices = Seq[LabeledVector]

  val DefaultMinNormAccel: Double = 1e-11

  val DefaultIterations: Int = 20

  def cluster(sc: SparkContext, vertices: Vertices, nClusters: Int, sigma: Double,
              nPowerIterations: Int) = {
    val nVertices = vertices.length
    val gaussRdd = createGaussianRdd(sc, vertices, sigma).cache()

    var ix = 0
    var indexedGaussRdd = gaussRdd.map { d =>
      ix += 1
      (ix, d)
    }

    val (columnsRdd, colSumsRdd) = createColumnsRdds(sc, vertices, indexedGaussRdd)
    val indexedDegreesRdd = createLaplacianRdd(sc, vertices, indexedGaussRdd, colSumsRdd)

    var eigensRemovedRdd = indexedDegreesRdd
    val collectedIndexedDegreesRdd = indexedDegreesRdd.collect
    println(s"Degrees Matrix:\n${
      printMatrix(collectedIndexedDegreesRdd.map(_._2),
        nVertices, nVertices)
    }")

    val lambdas = new Array[Double](nClusters)
    val eigens = new Array[RDD[Array[Double]]](nClusters)
    for (ex <- 0 until nClusters) {
      val (lambda, eigen) = getPrincipalEigen(sc, eigensRemovedRdd, Some(vertices.length),
        nPowerIterations)
      val collectedEigen = eigen
      println(s"collectedEigen=\n${collectedEigen.mkString(",")}")
      eigensRemovedRdd = subtractProjection(sc, eigensRemovedRdd, collectedEigen)
      val eigensRemovedRddCollected = eigensRemovedRdd.collect
      eigensRemovedRdd = sc.parallelize(eigensRemovedRddCollected, nVertices)
      println(s"EigensRemovedRDDCollected=\n${
        printMatrix(eigensRemovedRddCollected.map {
          _._2
        } , nVertices, nVertices)
      }")
      val arrarr = new Array[Array[Double]](1)
      arrarr(0) = new Array[Double](nVertices)
      System.arraycopy(eigen, 0, arrarr(0), 0, nVertices)
      lambdas(ex) = lambda
      eigens(ex) = sc.parallelize(arrarr, 1)
      println(s"Lambda=$lambda Eigen=${printMatrix(eigen, 1, nVertices)}")
    }
    val combinedEigens = eigens.reduceLeft(_.union(_))
    gaussRdd.unpersist()
    (indexedDegreesRdd, lambdas, combinedEigens)
  }

  def readVerticesfromFile(verticesFile: String): Vertices = {

    import scala.io.Source
    val vertices = Source.fromFile(verticesFile).getLines.map { l =>
      val toks = l.split("\t")
      val arr = toks.slice(1, toks.length).map(_.toDouble)
      (toks(0), arr)
    }.toList
    println(s"Read in ${vertices.length} from $verticesFile")
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

  def norm(vect: DVector): Double = {
    Math.sqrt(vect.foldLeft(0.0) { case (sum, dval) => sum + Math.pow(dval, 2)})
  }

  def manhattanNorm(vect: DVector): Double = {
    vect.foldLeft(0.0) { case (sum, dval) => sum + dval * dval}
  }

  def dot(v1: DVector, v2: DVector)  = {
    v1.zip(v2).foldLeft(0.0) {
      case (sum, (b, p)) => sum + b * p
    }
  }

  def project(basisVector: DVector, inputVect: DVector) = {
    val pnorm = makeNonZero(norm(basisVector))
    val projectedVect = basisVector.map(
      _ * dot(basisVector, inputVect) / dot(basisVector, basisVector))
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
      printMatrix(subVectRdd.collect.map(_._2),
        vect.length, vect.length)
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

  def createLaplacianRdd(sc: SparkContext,
                         vertices: Vertices,
                         indexedDegreesRdd: RDD[IndexedVector],
                         colSums: RDD[(Int, Double)]) = {
    val nVertices = vertices.length
    val bcNumVertices = sc.broadcast(nVertices)
    val bcColSums = sc.broadcast(colSums.collect)
    val laplaceRdd = indexedDegreesRdd.mapPartitionsWithIndex({ (partIndex, iter) =>
      val localNumVertices = bcNumVertices.value
      val localColSums = bcColSums.value
      var rowctr = -1
      iter.toList.map { case (dindex, dval) =>
        for (ix <- 0 until dval.length) {
          dval(ix) = (1.0 / localColSums(partIndex)._2) *
            (if (ix != partIndex) {
              -1.0 * dval(ix)
            } else {
              1.0
            }
              )
        }
        (rowctr, dval)
      }.iterator
    }, preservesPartitioning = true)

    laplaceRdd
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

  def getPrincipalEigen(sc: SparkContext,
                        vectRdd: RDD[IndexedVector],
                        rddSize: Option[Int] = None,
                        nIterations: Int = DefaultIterations,
                        minNormAccel: Double = DefaultMinNormAccel
                         ): (Double, DVector) = {

    vectRdd.cache()
    val numVects = rddSize.getOrElse(vectRdd.count().toInt)
    var eigenRdd: RDD[(Int, Double)] = null
    var eigenRddCollected: Seq[(Int, Double)] = for (ix <- 0 until numVects)
    yield (ix, 1.0 / numVects)

    var eigenRddPrior = onesVector(numVects)
    var priorNorm = manhattanNorm(onesVector(numVects))
    var cnorm = 0.0
    var normDiffVelocity = Double.MaxValue
    var priorNormDiffVelocity = 0.0
    var normDiffAccel = Double.MaxValue
    for (iter <- 0 until nIterations
         if Math.abs(normDiffAccel) >= minNormAccel
           || iter < nIterations / 2) {
      val bcEigenRdd = sc.broadcast(eigenRddCollected)

      eigenRdd = vectRdd.mapPartitions { iter =>
        val localEigenRdd = bcEigenRdd.value
        iter.map { case (ix, dvect) =>
          //  println(s"computing inner product from ${dvect.length}
          //   items per row on ${dvect.mkString(",")}")
          val rand = new java.util.Random().nextInt(1000)
          (ix,
            dvect.zip(localEigenRdd).foldLeft(0.0) { case (sum, (ax, (index, ex))) =>
              val newsum = sum + ax * ex
//              println(s"rand=$rand sum=$newsum ax=$ax ex=$ex")
              newsum
            })
        }
      }
      eigenRddCollected = eigenRdd.collect()
      println(s"eigenRddCollected=\n${eigenRddCollected.mkString(",")}")
      cnorm = manhattanNorm(eigenRddCollected.map(_._2).toArray)
      eigenRddCollected = eigenRddCollected.map { case (ix, dval) =>
        (ix, dval / makeNonZero(cnorm))
      }
      normDiffVelocity = cnorm - priorNorm
      normDiffAccel = normDiffVelocity - priorNormDiffVelocity
      println(s"Norm is $cnorm NormDiffVel=$normDiffVelocity NormDiffAccel=$normDiffAccel}")
      if (calcEigenDiffs) {
        val eigenDiff = eigenRddCollected.zip(eigenRddPrior).map { case ((ix, enew), eold) =>
          enew - eold
        }
        println(s"Norm is $cnorm NormDiff=$normDiffVelocity EigenRddCollected: "
          + s"${eigenRddCollected.mkString(",")} EigenDiffs: ${eigenDiff.mkString(",")}")
        System.arraycopy(eigenRddCollected, 0, eigenRddPrior, 0, eigenRddCollected.length)
      }
      priorNorm = cnorm

    }
    vectRdd.unpersist()

    val darr = new DVector(numVects)
    val collectedEigenRdd = eigenRdd.collect.map(_._2)
    println(s"eigenRdd: ${collectedEigenRdd.mkString(",")}")
    System.arraycopy(collectedEigenRdd, 0, darr, 0, darr.length)
    (cnorm, darr)
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

  def main(args: Array[String]) {
    val sc = new SparkContext("local[2]", "TestSpark")
    val vertFile = "../data/graphx/new_lr_data.10.txt"
    val sigma = 1.0
    val nIterations = 3
    val nClusters = 3
    val vertices = readVerticesfromFile(vertFile)
    SpectralClusteringUsingRdd.cluster(sc, vertices, nClusters, sigma, nIterations)
  }

}

