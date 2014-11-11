package org.apache.spark.examples

import java.sql.{ResultSet, DriverManager, Connection}

import org.apache.spark.examples.Test._
import org.apache.spark.rdd.JdbcRDDExtRsc
import org.apache.spark.util.Utils
import org.apache.spark.{ExtResource, SparkContext, SparkConf}

/**
 * Created by ken on 11/10/14.
 */
object Test1 {
  def main (args: Array[String]) {
    val cf = new SparkConf().setAppName("wfwfwf").setMaster("local")
    val sc = new SparkContext(cf)

    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://127.0.0.1/mysql"
    val username = "ken"
    val password = "km"

    sc.addJar("file:///usr/share/java/mysql-connector-java.jar")
    Thread.sleep(1000)


    sc.parallelize((1 to 40), 4).foreach { iter =>
      val x = Class.forName(driver, true, Utils.getContextOrSparkClassLoader)
      println(x.toString)
      println("get driver class " + x)
      var connection: Connection = null
      try {
        connection = DriverManager.getConnection(url, username, password)
        println("successfully create connection: " + connection)
      } catch {
        case e: Throwable => e.printStackTrace
      } finally {
        if (connection != null) connection.close()
      }
    }
//    val rdd = new JdbcRDDExtRsc(
//      sc,
//      "ken",
//      "SELECT host, user FROM user limit ?, ?",
//      1, 6, 3,
//      (r: ResultSet) => { r.getString(1) } ).cache()

  }
}
