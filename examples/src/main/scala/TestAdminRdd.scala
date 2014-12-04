package org.apache.spark.examples


import org.apache.spark.rdd.{RDD, ExtResourceCleanupRDD, ExtResourceListRDD}
import org.apache.spark.scheduler.{StageInfo, TaskInfo, TaskLocality}
import org.apache.spark._

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._

/**
 * Test creating, initializing and removing 4 types of external resource
 * retrieving external resources status by AdminRdd in different stage
 * Created by ken on 11/11/14.
 */



object TestAdminRdd {

  def main (args: Array[String]) {
//  test("AddminRdd: executorAlloctionManager"){
    val cf = new SparkConf().setAppName("test adminRDD").setMaster("local") //.setMaster("spark://127.0.0.1:7077") //
    val sc = new SparkContext(cf)
//    val manager = sc.executorAllocationManager.get

    println("++++ sc.executorAllocationManager.isDefined: "+sc.executorAllocationManager.isDefined)

    val arr = new ArrayBuffer[String]()
    arr += "ABC"


    val eSrc1 = new ExtResource[String]("TestResource:String1", false, arr, myInit , myterm, true)
    sc.addOrReplaceResource(eSrc1)


    //get size
//    println("++++1 executorIds : "+executorIds(manager).size)

    val adRdd = new ExtResourceListRDD(sc)
    val v1 = adRdd.collect().size
    print(s"\nsize of adminRdd (before retrieve external resource) : $v1")

    retrieveExtRsc(sc, 4)

    val adRdd1_2 = new ExtResourceListRDD(sc)
    val v1_2 = adRdd1_2.collect()
    println("\n\n++++ list ExtResourceInfo")
    v1_2.foreach(e => println(e.toString))




    /////////////////////////////// 2nd external resource
    val eSrc2 = new ExtResource[String]("TestResource:String2", true, arr, myInit , myterm, true)
    sc.addOrReplaceResource(eSrc2)

    retrieveExtRsc(sc, 4)

    val adRdd2 = new ExtResourceListRDD(sc)
    val v2 = adRdd2.collect()
    println("\n\n++++ list ExtResourceInfo")
    v2.foreach(e => println(e.toString))



    /////////////////////////////// 3rd external resource
    val eSrc3 = new ExtResource[String]("TestResource:String3", false, arr, myInit , myterm, false)
    sc.addOrReplaceResource(eSrc3)

    retrieveExtRsc(sc, 4)

    val adRdd3 = new ExtResourceListRDD(sc)
    val v3 = adRdd3.collect()
    println("\n\n++++ list ExtResourceInfo")
    v3.foreach(e => println(e.toString))

    /////////////////////////////// 4th external resource
    val eSrc4 = new ExtResource[String]("TestResource:String4", true, arr, myInit , myterm, false)
    sc.addOrReplaceResource(eSrc4)

    retrieveExtRsc(sc, 4)

    val adRdd4 = new ExtResourceListRDD(sc)
    val v4 = adRdd4.collect()
    println("\n\n++++ list ExtResourceInfo")
    v4.foreach(e => println(e.toString))



    /////////////////////////////// remove 4th external resource
    val clsRsc4 = new ExtResourceCleanupRDD(sc, Option("TestResource:String4"))
    val v4c = clsRsc4.collect()
    v4c.foreach(e => println("++++ external resource clean up: "+e))

    val clsadRdd4 = new ExtResourceListRDD(sc)
    val c4 = clsadRdd4.collect()
    println("\n\n++++ list ExtResourceInfo")
    c4.foreach(e => println(e.toString))


    /////////////////////////////// remove all external resource (if any)
    val clsRscAll = new ExtResourceCleanupRDD(sc)
    val v4a = clsRscAll.collect()
    v4a.foreach(e => println("++++ external resource clean up: "+e))

    val clsadRddall = new ExtResourceListRDD(sc)
    val clsall = clsadRddall.collect()
    println("\n\n++++ list ExtResourceInfo")
    clsall.foreach(e => println(e.toString))

    sc.stop()
  }








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

}
