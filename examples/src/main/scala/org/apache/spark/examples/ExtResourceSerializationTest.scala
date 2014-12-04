package org.apache.spark.examples

import java.nio.ByteBuffer
import java.sql.{DriverManager, Connection}

import org.apache.spark.serializer.JavaSerializer
import org.apache.spark._
import org.apache.spark.scheduler.{TaskLocation, Task}

import scala.collection.mutable.HashMap

/**
 * Created by ken on 11/11/14.
 *
 * To test retrieving External resource (which is mysql jdbc connection)
 * created with spark context and deserialize from task
 */
object ExtResourceSerializationTest {
  //variable for creating mysql DB connection
  //this test case assume there is a mysql DB setup, and user is able to access the 'mysql' schema
  val driver = "com.mysql.jdbc.Driver"
  val url = "jdbc:mysql://127.0.0.1/mysql"
  val username = "ken"
  val password = "km"

  def main(args: Array[String]) {
    val currentExtResources: HashMap[ExtResource[_], Long] = new HashMap[ExtResource[_], Long]()
    val er = getExtRsc()
    currentExtResources(er)=12345


    val serializedTask = Task.serializeWithDependencies(
      new MyFakeTask(1), new HashMap[String, Long](), new HashMap[String, Long](),
      currentExtResources, SparkEnv.get.closureSerializer.newInstance())

    val (taskFiles, taskJars, in, dataIn) = Task.deserializeWithDependencies(serializedTask)

    val (taskResources, bytes) = Task.deserializeExtResourceWithDependencies( serializedTask, in, dataIn)

    for ((resource_, timestamp) <- taskResources) {
      val resource = resource_.asInstanceOf[ExtResource[_]]
      println("++++ updateDependencies : currentResources: "+ resource.name)


      val conn= resource.init(1, resource.params).asInstanceOf[Connection]

      try{
        val statement = conn.createStatement()
        val resultSet = statement.executeQuery("SELECT host, user FROM user")
        while ( resultSet.next() ) {
          val host = resultSet.getString("host")
          val user = resultSet.getString("user")
          println("host, user = " + host + ", " + user)
        }
      }catch{
        case e: Throwable => e.printStackTrace
      }finally {
        if (conn != null) conn.close()
      }
    }
  }



  class MyFakeTask(stageId: Int, prefLocs: Seq[TaskLocation] = Nil) extends Task[Int](stageId, 0) {
    override def runTask(context: TaskContext): Int = 0

    override def preferredLocations: Seq[TaskLocation] = prefLocs
  }


  def getExtRsc(): ExtResource[Connection] ={
    val cf = new SparkConf().setAppName("TestSerializationOfExtResource").setMaster("local")
    val sc = new SparkContext(cf)

    val myparams: Array[String] = Array(driver, url, username, password)

    def myinit(split: Int, params: Seq[String]): Connection = {
      require(params.size>3, s"parameters error, current param size: "+ params.size)
      var connection:Connection = null
      try {
        //        val loader = Option(Thread.currentThread().getContextClassLoader).getOrElse(getClass.getClassLoader)
        val x = Class.forName(params(0))
        println(x.toString)
        println("get driver class "+ x)
        connection = DriverManager.getConnection(params(1), params(2), params(3))
      } catch {
        case e: Throwable => e.printStackTrace
      }
      connection
    }


    val myterm =  (split: Int, conn: Connection, params: Seq[String]) => {
      require(Option(conn) != None, "Connection error")
      try{
        conn.close()
      }catch {
        case e: Throwable => e.printStackTrace
      }
    }

    val eSrc = new ExtResource[Connection]("ExtRsc: mysql jdbc connection",
      false, myparams, myinit , myterm, false)

    eSrc
  }
}
