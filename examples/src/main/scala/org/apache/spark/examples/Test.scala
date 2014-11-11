package org.apache.spark.examples

import java.net.{URLClassLoader, URL}
import java.sql.{Driver, ResultSet, DriverManager, Connection}
import scala.collection.mutable.{ArrayBuffer, HashMap}
import org.apache.spark.rdd._
import org.apache.spark._
/**
  * Created by ken on 11/10/14.
 */
object Test {
  def main (args: Array[String]) {
    val cf = new SparkConf().setAppName("wfwfwf").setMaster("local")
    val sc = new SparkContext(cf)

    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://127.0.0.1/mysql"
    val username = "ken"
    val password = "km"

    var myparams=Array(driver, url, username, password)

    def myinit(split: Int, params: Seq[_]): Connection = {
      require(params.size>3, s"parameters error, current param size: "+ params.size)
      val p = params
      val driver = p(0).toString
      val url = p(1).toString
      val username = p(2).toString
      val password = p(3).toString
//
//      var connection:Connection = null
//      try {
//        val loader = Option(Thread.currentThread().getContextClassLoader).getOrElse(getClass.getClassLoader)
//
////        val x = Class.forName(driver, true, loader)
//        val x = "wf"
//        println(x.toString)
//        println("get driver class "+ x)
//        connection = DriverManager.getConnection(url, username, password)
//      } catch {
//        case e: Throwable => e.printStackTrace
//      }
//      connection
      null
    }


//    val myterm =  (split: Int, conn: Any, params: Seq[_]) => {
//      require(Option(conn) != None, "Connection error")
//      try{
//        val c = conn.asInstanceOf[Connection]
//        c.close()
//      }catch {
//        case e: Throwable => e.printStackTrace
//      }
//    }

    val eSrc = new ExtResource("mysql ExtRsc test", false, myparams, null , null, false)

    sc.addJar("file:///usr/share/java/mysql-connector-java.jar")
    Thread.sleep(1000)
    //1. add extRsc to sparkContext
    sc.addOrReplaceResource(eSrc)



    //2. create rdd
    val rdd = new JdbcRDDExtRsc(
      sc,
      eSrc.name,
      "SELECT host, user FROM user limit ?, ?",
      1, 6, 3,
      (r: ResultSet) => { r.getString(1) } ).cache()

    //3. output
    println("# of rows (rdd.count): "+rdd.count)
  }

}
