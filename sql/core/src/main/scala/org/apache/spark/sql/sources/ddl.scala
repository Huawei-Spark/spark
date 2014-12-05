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

package org.apache.spark.sql.sources

import scala.language.implicitConversions
import scala.util.parsing.combinator.syntactical.StandardTokenParsers
import scala.util.parsing.combinator.PackratParsers

import org.apache.spark.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.util.Utils
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.SqlLexical

/**
 * A parser for foreign DDL commands.
 */
private[sql] class DDLParser extends StandardTokenParsers with PackratParsers with Logging {

  def apply(input: String): Option[LogicalPlan] = {
    phrase(ddl)(new lexical.Scanner(input)) match {
      case Success(r, x) => Some(r)
      case x =>
        logDebug(s"Not recognized as DDL: $x")
        None
    }
  }

  protected case class Keyword(str: String)

  protected implicit def asParser(k: Keyword): Parser[String] =
    lexical.allCaseVersions(k.str).map(x => x : Parser[String]).reduce(_ | _)

  protected val STRING = Keyword("STRING")
  protected val SHORT = Keyword("SHORT")
  protected val DOUBLE = Keyword("DOUBLE")
  protected val BOOLEAN = Keyword("BOOLEAN")
  protected val BYTE = Keyword("BYTE")
  protected val FLOAT = Keyword("FLOAT")
  protected val INT = Keyword("INT")
  protected val INTEGER = Keyword("INTEGER")
  protected val LONG = Keyword("LONG")
  protected val CREATE = Keyword("CREATE")
  protected val TEMPORARY = Keyword("TEMPORARY")
  protected val TABLE = Keyword("TABLE")
  protected val USING = Keyword("USING")
  protected val OPTIONS = Keyword("OPTIONS")

  // Use reflection to find the reserved words defined in this class.
  protected val reservedWords =
    this.getClass
      .getMethods
      .filter(_.getReturnType == classOf[Keyword])
      .map(_.invoke(this).asInstanceOf[Keyword].str)

  override val lexical = new SqlLexical(reservedWords)

  protected lazy val ddl: Parser[LogicalPlan] = createTable

  /**
   * `CREATE TEMPORARY TABLE avroTable
   * USING org.apache.spark.sql.avro
   * OPTIONS (path "../hive/src/test/resources/data/files/episodes.avro")`
   * or
   * `CREATE TEMPORARY TABLE avroTable(intField int, stringField string...)
   * USING org.apache.spark.sql.avro
   * OPTIONS (path "../hive/src/test/resources/data/files/episodes.avro")`
   */
  protected lazy val createTable: Parser[LogicalPlan] =
  (  CREATE ~ TEMPORARY ~ TABLE ~> ident ~ (USING ~> className) ~ (OPTIONS ~> options) ^^ {
      case tableName ~ provider ~ opts =>
        CreateTableUsing(tableName, Seq.empty, provider, opts)
    }
  |
    CREATE ~ TEMPORARY ~ TABLE ~> ident
      ~ tableCols  ~ (USING ~> className) ~ (OPTIONS ~> options) ^^ {
      case tableName ~ tableColumns ~ provider ~ opts =>
      CreateTableUsing(tableName, tableColumns, provider, opts)
    }
  )
  protected lazy val tableCol: Parser[(String, String)] =
    ident ~ (STRING | BYTE | SHORT | INT | INTEGER | LONG | FLOAT | DOUBLE | BOOLEAN) ^^ {
      case e1 ~ e2 => (e1, e2)
    }

  protected lazy val tableCols: Parser[Seq[(String, String)]] =
    "(" ~> repsep(tableCol, ",") <~ ")"


  protected lazy val options: Parser[Map[String, String]] =
    "(" ~> repsep(pair, ",") <~ ")" ^^ { case s: Seq[(String, String)] => s.toMap }

  protected lazy val className: Parser[String] = repsep(ident, ".") ^^ { case s => s.mkString(".")}

  protected lazy val pair: Parser[(String, String)] = ident ~ stringLit ^^ { case k ~ v => (k,v) }
}

private[sql] case class CreateTableUsing(
    tableName: String,
    tableCols: Seq[(String, String)],
    provider: String,
    options: Map[String, String]) extends RunnableCommand {

  def run(sqlContext: SQLContext) = {
    val loader = Utils.getContextOrSparkClassLoader
    val clazz: Class[_] = try loader.loadClass(provider) catch {
      case cnf: java.lang.ClassNotFoundException =>
        try loader.loadClass(provider + ".DefaultSource") catch {
          case cnf: java.lang.ClassNotFoundException =>
            sys.error(s"Failed to load class for data source: $provider")
        }
    }
    val dataSource = clazz.newInstance().asInstanceOf[org.apache.spark.sql.sources.RelationProvider]
    val relation = dataSource.createRelation(sqlContext, options, toSchema(tableCols))

    sqlContext.baseRelationToSchemaRDD(relation).registerTempTable(tableName)
    Seq.empty
  }

  def toSchema(tableColumns: Seq[(String, String)]): Option[StructType] = {
    val fields: Seq[StructField] = tableColumns.map { tableColumn =>
      val columnName = tableColumn._1
      val columnType = tableColumn._2
      // todo: support more complex data type
      columnType.toLowerCase match {
        case "string" => StructField(columnName, StringType)
        case "byte" => StructField(columnName, ByteType)
        case "short" => StructField(columnName, ShortType)
        case "int" => StructField(columnName, IntegerType)
        case "integer" => StructField(columnName, IntegerType)
        case "long" => StructField(columnName, LongType)
        case "double" => StructField(columnName, DoubleType)
        case "float" => StructField(columnName, FloatType)
        case "boolean" => StructField(columnName, BooleanType)
      }
    }
    if (fields.isEmpty) {
      return None
    }
    Some(StructType(fields))
  }
}
