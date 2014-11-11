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

package org.apache.spark.rdd

import java.net.{URLClassLoader, URL}
import java.sql.{Driver, ResultSet, DriverManager, Connection}

import org.apache.spark.util.Utils._

import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import org.scalatest.FunSuite

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.util.Utils

import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.rdd.RDDSuiteUtils._

class RDDSuiteKen extends FunSuite with SharedSparkContext {
  test("AdminRdd operation") {
    val arr = new ArrayBuffer[String]()
    arr += "ABC"

    val eSrc = new ExtResource("Ken's test", true, arr, null , null, true)
    val rsInfo = eSrc.getResourceInfo("Hostname", "executorId", 12345)
    println(s"ResourceInfo instanceCount: "+ rsInfo.instanceCount
      + "\nResourceInfo instanceUseCount: "+ rsInfo.instanceUseCount
    )

    sc.addOrReplaceResource(eSrc)

    val adRdd = new ExtResourceListRDD(sc)
    val v = adRdd.collect().size
    print(s"\nsize of adminRdd : $v")

    val cnt = adRdd.count()
    print(s"\nCount of adminRdd : $cnt")


    adRdd.collect().foreach{

      e => println("admin RDD : "+e.name + " : "+ e.slaveHostname )
    }

  }

  test("add mysql jdbc connection as ExtResource") {

    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://127.0.0.1/mysql"
    val username = "ken"
    val password = "km"

    var myparams=Array(driver, url, username, password)

    val myinit = (split: Int, params: Seq[_]) =>  {

      require(params.size>3, s"parameters error, current param size: "+ params.size)
      val p = params
      val driver = p(0).toString
      val url = p(1).toString
      val username = p(2).toString
      val password = p(3).toString

      var connection:Connection = null
      try {
        // make the connection
        val cl = Thread.currentThread.getContextClassLoader
        println("++++ cl.loadClass : "+cl.loadClass(driver).newInstance())

        val loader = Option(Thread.currentThread().getContextClassLoader).getOrElse(getClass.getClassLoader)

        val clV = driver.replaceAll("\\.", "/")+".class"
        println(cl.getResource(clV))
//        Class.forName(driver)
//        val sqlCl = Class.forName(driver, true, Thread.currentThread.getContextClassLoader).newInstance()
          val sqlCl = Class.forName(driver, true, loader)
//        val sqlCl = cl.loadClass(driver).newInstance()
        DriverManager.registerDriver(sqlCl.asInstanceOf[Driver])
        connection = DriverManager.getConnection(url, username, password)
      } catch {
        case e: Throwable => e.printStackTrace
      }
      connection
    }


    val myterm =  (split: Int, conn: Any, params: Seq[_]) => {
      require(Option(conn) != None, "Connection error")
      try{
        val c = conn.asInstanceOf[Connection]
        c.close()
      }catch {
        case e: Throwable => e.printStackTrace
      }
    }

    val eSrc = new ExtResource("mysql ExtRsc test",
      shared=false, params = myparams, init=myinit , term=myterm, partitionAffined=false)

    //1. add extRsc to sparkContext
    sc.addOrReplaceResource(eSrc)
    sc.addJar("file:///usr/share/java/mysql-connector-java.jar")



    //2. create rdd
    val rdd = new JdbcRDDExtRsc(
      sc,
      eSrc.name,
      "SELECT host, user FROM user limit ?, ?",
      1, 6, 3,
      (r: ResultSet) => { r.getString(1) } ).cache()

    //3. output
    println("# of rows (rdd.count): "+rdd.count)
//    println("# of row (rdd.reduce(_+_)): "+rdd.count)
//    assert(rdd.count === 100)
//    assert(rdd.reduce(_+_) === 10100)

  }

  test("test ..."){

  }

  test("test add jar") {
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://127.0.0.1/mysql"
    val username = "ken"
    val password = "km"

    sc.addJar("file:///usr/share/java/mysql-connector-java.jar")
    sc.parallelize((1 to 40), 4).foreach { iter =>
      val x = Class.forName(driver, true, Thread.currentThread.getContextClassLoader).newInstance()
//      val x = Class.forName(driver).newInstance()
      println(x.toString)
      x.isInstanceOf[Driver] match {
        case true => {
          println("get driver class "+ x)

          var connection:Connection = null
          try {
//            DriverManager.registerDriver(x.asInstanceOf[Driver])
            connection = DriverManager.getConnection(url, username, password)
            println("successfully create connection: "+ connection)
          } catch {
            case e: Throwable => e.printStackTrace
          }finally {
            if (connection !=null) connection.close()
          }
        }
        case _ => println("get driver class fail! "+ x)
      }
    }

  }

  test("jdbc test"){
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost/mysql"
    val username = "ken"
    val password = "km"

    // there's probably a better way to do this
    var connection:Connection = null

    try {
      // make the connection
      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)

      // create the statement, and run the select query
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("SELECT host, user FROM user")
      while ( resultSet.next() ) {
        val host = resultSet.getString("host")
        val user = resultSet.getString("user")
        println("host, user = " + host + ", " + user)
      }
    } catch {
      case e: Throwable => e.printStackTrace
    }
    connection.close()
  }


  test("Ken : basic operations") {
    val nums = sc.makeRDD(Array(1, 2, 3, 4), 2)
    assert(nums.collect().toList === List(1, 2, 3, 4))
    assert(nums.toLocalIterator.toList === List(1, 2, 3, 4))
  }


}
