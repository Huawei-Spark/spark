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


import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Matrix, Matrices, Vectors}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import breeze.linalg.{DenseVector => BDV, DenseMatrix => BDM}
import org.apache.spark.mllib.rdd.RDDFunctions
import scala.math.Ordering
import org.apache.spark.mllib.rdd.RDDFunctions._

/**
 * SpectralClustering
 */
class SpectralClustering {
  val logger = Logger.getLogger(getClass.getName)

}

object SpectralClustering {

  val log = Logger.getLogger(SpectralClustering.getClass)

//  def gaussians(nPoints: Int, mean: Double, sttdev: Double) = {
//    import org.apache.spark.mllib.stat.Statistics
//
//  }

}
