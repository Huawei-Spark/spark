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

import java.sql.{DriverManager, Connection}

object TestExternal {
  def main (args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("test external")
    val sc = new SparkContext(sparkConf)
    args(0) match {
      case "1" => test1(sc)
      case "2" => test2(sc)
      case _ => println("error paras")
    }
    sc.stop()
  }

  def test1(sc: SparkContext) = {
    def init(a: Int, b: Seq[Any]): String = {
      b(2).asInstanceOf[String]
    }
    def term(a: Int, b: String, c: Seq[Any]): Unit = {

    }
    val external = new ExternalResource[String]("test external", true, Seq(1, 2, "wf"), init _, term _)

    val wf = sc.broadcast(external)

    sc.parallelize(1 to 40, 4).foreachPartition { iter =>

      val external = wf.value

      val seq = external.params

      val init = external.init

      println(init(1, seq))
    }
  }

  def test2(sc: SparkContext) = {

    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://127.0.0.1/mysql"
    val username = "ken"
    val password = "km"
    var myparams=Array(driver, url, username, password)

    def myinit(split: Int, params: Seq[_]): Connection = {
      require(params.size > 3, s"parameters error, current param size: " + params.size)
      val p = params
      val driver = p(0).toString
      val url = p(1).toString
      val username = p(2).toString
      val password = p(3).toString

      var connection:Connection = null
      try {
        val loader = Option(Thread.currentThread().getContextClassLoader
        ).getOrElse(getClass.getClassLoader)
        val x = Class.forName(driver, true, loader)
        println(x.toString)
        println("get driver class " + x)
        connection = DriverManager.getConnection(url, username, password)
      } catch {
        case e: Throwable => e.printStackTrace
      }
      connection
    }

    def myterm(split: Int, conn: Any, params: Seq[_]) = {
      require(Option(conn) != None, "Connection error")
      try{
        val c = conn.asInstanceOf[Connection]
        c.close()
      }catch {
        case e: Throwable => e.printStackTrace
      }
    }

    val external = new ExternalResource[Connection]("mysql ExtRsc test", false, myparams, myinit _ ,
      myterm _)

    val wf = sc.broadcast(external)
    sc.addJar("file:///home/kf/Downloads/mysql-connector-java-5.1.18.jar")
    sc.parallelize(1 to 40, 4).foreachPartition { iter =>

      val external = wf.value

      val seq = external.params

      val init = external.init

      println(init(1, seq))
    }
  }

}
