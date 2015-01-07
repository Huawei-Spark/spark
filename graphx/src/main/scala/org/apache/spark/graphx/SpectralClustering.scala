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


import org.apache.spark.SparkContext

/**
 * SpectralClustering
 *
 */
object SpectralClustering {

  def gaussianDist(c1arr: Array[Double], c2arr: Array[Double], sigma: Double) = {
    val c1c2 = c1arr.zip(c2arr)
    val dist = Math.exp((0.5 / Math.pow(sigma, 2.0)) * c1c2.foldLeft(0.0) {
      case (dist: Double, (c1: Double, c2: Double)) =>
        dist - Math.pow(c1 - c2, 2)
    })
    dist
  }

  def fromFile(sc: SparkContext, verticesFile: String, sigma: Double) = {

    import org.apache.spark.graphx._

    //    val G = GraphLoader.edgeListFile(sc,edgeFile)

    // val G = GraphGenerator.logNormalGraph(20)

    //G.vertices.cache()
    //val vBc = G.vertices.broadcast

    import scala.io.Source
//    println(s"Contents of working dir: ${new java.io.File(".").getAbsolutePath}"
//      + s" ${new java.io.File(".").list.mkString(",")}")
    val vertices = Source.fromFile(verticesFile).getLines.map { l =>
      val toks = l.split("\t")
      val arr = toks.slice(1, toks.length).map(_.toDouble)
      (toks(0).toInt, arr)
    }.toList
    println(vertices.map { case (x, arr) => s"($x,${arr.mkString(",")})"}.mkString("[", ",\n", "]"))

    //    val darr : Seq[Double] = for (i <- vertices.size;
    //                    j <- vertices.size
    //    ) yield {
    //      val gdist = gaussianDist(vertices(i)._2, vertices(j)._2, sigma)
    //      gdist
    //    }

//    println(darr.toList.mkString(","))
//    printMatrix(darr, vertices.size, vertices.size)

    val gaussRdd = sc.parallelize( {
      val darr = new Array[Double](vertices.size * vertices.size)
      for (i <- 0 until vertices.size) {
        for (j <- 0 until vertices.size) {
          darr(i * vertices.size + j) = if (i != j) {
            gaussianDist(vertices(i)._2, vertices(j)._2, sigma)
          } else {
            0.0
          }
        }
      }
      darr
    }, vertices.length)
    gaussRdd.collect()

    // Create degree matrix
    // This is by summing the columns

//    val labelsRdd = sc.parallelize(vertices)
//    val verticesVals = vertices.map{ _._2}
//    val bcFirstCol = sc.broadcast(verticesVals)
//    val firstColumnProductsRdd = gaussRdd.mapPartitions ({ row =>
//      val firstCol = bcFirstCol.value // .asInstanceOf[Array[Double]]
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

  def main(args: Array[String]) {
    val sc = new SparkContext("local[2]", "TestSpark")
    val vertFile = "../data/graphx/new_lr_data.10.txt"
    val sigma = 1.0
    SpectralClustering.fromFile(sc, vertFile, sigma)
  }


}
