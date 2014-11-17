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

package org.apache.spark.sql.hbase

import java.io.File

import jline.{ConsoleReader, History}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SchemaRDD

/**
 * HBaseSQLCliDriver
 *
 */
object HBaseSQLCLIDriver {
  private val prompt = "spark-hbaseql"
  private val continuedPrompt = "".padTo(prompt.length, ' ')
  private val conf = new SparkConf()
  private val sc = new SparkContext(conf)
  private val hbaseCtx = new HBaseSQLContext(sc)

  def main(args: Array[String]) {

    val reader = new ConsoleReader()
    reader.setBellEnabled(false)

    val historyDirectory = System.getProperty("user.home")

    try {
      if (new File(historyDirectory).exists()) {
        val historyFile = historyDirectory + File.separator + ".hbaseqlhistory"
        reader.setHistory(new History(new File(historyFile)))
      } else {
        System.err.println("WARNING: Directory for hbaseql history file: " + historyDirectory +
          " does not exist.   History will not be available during this session.")
      }
    } catch {
      case e: Exception =>
        System.err.println("WARNING: Encountered an error while trying to initialize hbaseql's " +
          "history file.  History will not be available during this session.")
        System.err.println(e.getMessage)
    }

    println("Welcome to hbaseql CLI")
    var prefix = ""

    def promptPrefix = s"$prompt"
    var currentPrompt = promptPrefix
    var line = reader.readLine(currentPrompt + "> ")
    var ret = 0

    while (line != null) {
      if (prefix.nonEmpty) {
        prefix += '\n'
      }

      if (line.trim().endsWith(";") && !line.trim().endsWith("\\;")) {
        line = prefix + line
        ret = processLine(line, true)
        prefix = ""
        currentPrompt = promptPrefix
      } else {
        prefix = prefix + line
        currentPrompt = continuedPrompt
      }

      line = reader.readLine(currentPrompt + "> ")
    }

    System.exit(0)
  }

  private def processLine(line: String, allowInterrupting: Boolean): Int = {
    processCmd(line)
    println(s"processing line: $line")

    // TODO: install interrupt handler
    // TODO: support multiple cmd in a line separated by ;

    try {

      // Since we are using SqlParser to handle 'select' clause, and it does not handle ';',
      // just work around to omit the ';'
      val statement =
        if (line.trim.toLowerCase.startsWith("select")) line.substring(0, line.length - 1)
        else line


      val start = System.currentTimeMillis()
      val rdd = hbaseCtx.sql(statement)
      val end = System.currentTimeMillis()
      printResult(rdd)

      val timeTaken: Double = (end - start) / 1000.0
      println(s"Time taken: $timeTaken seconds")
      0
    } catch {
      case e: Exception =>
        e.printStackTrace()
        1
    }
  }

  private def printResult(result: SchemaRDD) = {
    println("===================")
    println("      result")
    println("===================")
    result.collect().foreach(println)
  }

  private def processCmd(line: String) = {
    val cmd = line.trim.toLowerCase
    if (cmd.startsWith("quit") || cmd.startsWith("exit"))
      System.exit(0)

    // TODO: support for "!" to exec bash cmd
  }

}
