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

package org.apache.spark

import org.apache.spark.rdd.RDD
import org.scalatest.{FunSuite, PrivateMethodTester}
import org.apache.spark.scheduler._

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._

/**
 * Test add and remove behavior of ExtResource.
 * 
 */
class ExtResourceSuite extends FunSuite with LocalSparkContext {
  import ExtResourceSuite._

  test("ExtResource") {
    val cf = new SparkConf().setAppName("test-ExtResource").setMaster("local") //.setMaster("spark://127.0.0.1:7077") //
    val sc = new SparkContext(cf)

    //create simple resource (string)
    val arr = new ArrayBuffer[String]()
    arr += "ABC"

    //1. create external resource [shared : false, partitionAffined: true]
    val eSrc1 = new ExtResource[String]("TestResource:String1", false, arr, myInit , myterm, true)
    sc.addOrReplaceResource(eSrc1)
    retrieveExtRsc(sc, 4)
    val res1 = getExtRscInfo (sc)
    assert(res1.size === 1)

    //2. create external resource [shared : true, partitionAffined: true]
    val eSrc2 = new ExtResource[String]("TestResource:String2", true, arr, myInit , myterm, true)
    sc.addOrReplaceResource(eSrc2)
    retrieveExtRsc(sc, 4)
    val res2 = getExtRscInfo (sc)
    assert(res2.size === 2)

    //3. create external resource [shared : false, partitionAffined: false]
    val eSrc3 = new ExtResource[String]("TestResource:String3", false, arr, myInit , myterm, false)
    sc.addOrReplaceResource(eSrc3)
    retrieveExtRsc(sc, 4)
    val res3 = getExtRscInfo (sc)
    assert(res3.size === 3)


    //4. create external resource [shared : true, partitionAffined: false]
    val eSrc4 = new ExtResource[String]("TestResource:String4", true, arr, myInit , myterm, false)
    sc.addOrReplaceResource(eSrc4)

    retrieveExtRsc(sc, 4)
    val res4 = getExtRscInfo (sc)
    assert(res4.size === 4)
    //before cleanup
    assert(res4.get("TestResource:String1").get.instanceCount === 4)
    assert(res4.get("TestResource:String2").get.instanceCount === 4)
    assert(res4.get("TestResource:String3").get.instanceCount === 1)
    assert(res4.get("TestResource:String4").get.instanceCount === 1)

    cleanupExtRsc (sc, Option("TestResource:String1"))
    val res5 = getExtRscInfo (sc)
    //after cleanup
    assert(res5.get("TestResource:String1").get.instanceCount === 0)
    assert(res5.get("TestResource:String2").get.instanceCount === 4)
    assert(res5.get("TestResource:String3").get.instanceCount === 1)
    assert(res5.get("TestResource:String4").get.instanceCount === 1)

    cleanupExtRsc (sc)
    getExtRscInfo (sc)
    val res6 = getExtRscInfo (sc)
    assert(res6.get("TestResource:String1").get.instanceCount === 0)
    assert(res6.get("TestResource:String2").get.instanceCount === 0)
    assert(res6.get("TestResource:String3").get.instanceCount === 0)
    assert(res6.get("TestResource:String4").get.instanceCount === 0)

    sc.stop()
  }


}






/**
 *
 *
 */
private object ExtResourceSuite extends PrivateMethodTester {
  val myInit = (split: Int, params: Seq[String]) => {
    println("++++ myInit success")
    "success"
  }

  val myterm =  (split: Int, extRsc: String, params: Seq[String]) => {
    println("++++ myterm " +extRsc)
  }

  def retrieveExtRsc (sc: SparkContext, numOfPartitions: Int): Unit = {
    new MyTestRDD(sc, 4).collect()
  }


  def getExtRscInfo (sc: SparkContext): Map[String, ExtResourceInfo] = {
    val exc = sc.getExecutorsAndLocations()
    var res = Map[String, ExtResourceInfo]()
    for (e <- exc) {
      e match {
        case tl: ExecutorCacheTaskLocation => {
          println("location: "+tl.host + "          executorid: "+tl.executorId)
          val resInfos = sc.getExtResourceInfo(tl.executorId).get
          resInfos.foreach(e => {
            println("++++ resource Name: "+ e._1)
            println("++++ resourceInfo: "+ e._2.toString)
          } )
          res ++= resInfos
        }
      }
    }
    res
  }

  def cleanupExtRsc (sc: SparkContext, res: Option[String] = None) = {
    val exc = sc.getExecutorsAndLocations()
    for (e <- exc) {
      e match {
        case tl: ExecutorCacheTaskLocation => {
          println("location: "+tl.host + "          executorid: "+tl.executorId)
          val message = sc.cleanupExtResourceInfo(tl.executorId, res).get
          message.foreach(e => {
            println("++++ cleanupExtRsc: "+ e)
          } )
        }
      }
    }
  }


  class MyTestRDD(
                   sc: SparkContext,
                   numPartitions: Int) extends RDD[(Long, Int)](sc, Nil) with Serializable {
    override def compute(split: Partition, context: TaskContext): Iterator[(Long, Int)] ={
      val cont = TaskContext.get().asInstanceOf[TaskContextImpl]
      if (cont.resources.isDefined) {
        val m = cont.resources.get.asScala
        //retrieve / init each and every external resource from each partition
        m.map(e => {
          val rsc = e._2._1
          rsc.getInstance(cont.partitionId, cont.attemptId)
          println("resources name: " +rsc.name + "   in partition : "
            +cont.partitionId + "   task: "+cont.attemptId)
          (cont.attemptId, cont.partitionId)
        }).toIterator
      }else{
        Map(1L -> 0).toIterator
      }
    }
    override def getPartitions = (0 until numPartitions).map(i => new Partition {
      override def index = i
    }).toArray
  }



  //  def main (args: Array[String]) {
  //    val cf = new SparkConf().setAppName("test-ExtResource").setMaster("local") //.setMaster("spark://127.0.0.1:7077") //
  //    val sc = new SparkContext(cf)
  //
  //    //    println("++++ sc.executorAllocationManager.isDefined: "+sc.executorAllocationManager.isDefined)
  //
  //    val arr = new ArrayBuffer[String]()
  //    arr += "ABC"
  //
  //    //1. create external resource [shared : false, partitionAffined: true]
  //    val eSrc1 = new ExtResource[String]("TestResource:String1", false, arr, myInit , myterm, true)
  //    sc.addOrReplaceResource(eSrc1)
  //    retrieveExtRsc(sc, 4)
  //    getExtRscInfo (sc)
  //
  //    //2. create external resource [shared : true, partitionAffined: true]
  //    val eSrc2 = new ExtResource[String]("TestResource:String2", true, arr, myInit , myterm, true)
  //    sc.addOrReplaceResource(eSrc2)
  //    retrieveExtRsc(sc, 4)
  //    getExtRscInfo (sc)
  //
  //    //3. create external resource [shared : false, partitionAffined: false]
  //    val eSrc3 = new ExtResource[String]("TestResource:String3", false, arr, myInit , myterm, false)
  //    sc.addOrReplaceResource(eSrc3)
  //    retrieveExtRsc(sc, 4)
  //    getExtRscInfo (sc)
  //
  //
  //    //4. create external resource [shared : true, partitionAffined: false]
  //    val eSrc4 = new ExtResource[String]("TestResource:String4", true, arr, myInit , myterm, false)
  //    sc.addOrReplaceResource(eSrc4)
  //
  //    retrieveExtRsc(sc, 4)
  //    getExtRscInfo (sc)
  //
  //    cleanupExtRsc (sc, Option("TestResource:String1"))
  //
  //    getExtRscInfo (sc)
  //
  //    cleanupExtRsc (sc)
  //
  //    getExtRscInfo (sc)
  //
  //    sc.stop()
  //  }

}




