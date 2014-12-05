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

package org.apache.spark.sql.hbasesource

import org.apache.spark.Logging
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, Expression, Row}
import org.apache.spark.sql.catalyst.types.StructType
import org.apache.spark.sql.sources.{BaseRelation, CatalystScan, RelationProvider}

/**
 * Allows creation of parquet based tables using the syntax
 * `CREATE TEMPORARY TABLE table_name(field1 filed1_type, filed2 filed2_type...)
 *  USING org.apache.spark.sql.hbase
 *  OPTIONS (
 *    hbase_table "hbase_table_name",
 *    mapping "filed1=cf1.column1, filed2=cf2.column2...",
 *    primary_key "filed_name1, field_name2"
 *  )`.
 */
class DefaultSource extends RelationProvider with Logging {
  /** Returns a new base relation with the given parameters. */
  override def createRelation(
       sqlContext: SQLContext,
       parameters: Map[String, String],
       schema: Option[StructType]): BaseRelation = {

    assert(schema.nonEmpty, "schema can not be empty for hbase rouce!")
    assert(parameters.get("hbase_table").nonEmpty, "no option for hbase.table")
    assert(parameters.get("mapping").nonEmpty, "no option for mapping")
    assert(parameters.get("primary_key").nonEmpty, "no option for mapping")

    val hbaseTableName = parameters.getOrElse("hbase_table", "").toLowerCase
    val mapping = parameters.getOrElse("mapping", "").toLowerCase
    val primaryKey = parameters.getOrElse("primary_key", "").toLowerCase()
    val partValue = "([^=]+)=([^=]+)".r

    val fieldByHbaseColumn = mapping.split(",").map {
      case partValue(key, value) => (key, value)
    }
    val keyColumns = primaryKey.split(",").map(_.trim)

    // check the mapping is legal
    val fieldSet = schema.get.fields.map(_.name).toSet
    fieldByHbaseColumn.iterator.map(_._1).foreach { field =>
      assert(fieldSet.contains(field), s"no field named $field in table")
    }
    HBaseScanBuilder("", hbaseTableName, keyColumns, fieldByHbaseColumn, schema.get)(sqlContext)
  }
}

@DeveloperApi
case class HBaseScanBuilder(
    tableName: String,
    hbaseTableName: String,
    keyColumns: Seq[String],
    fieldByHbaseColumn: Seq[(String, String)],
    schema: StructType)(context: SQLContext) extends CatalystScan with Logging {

  val hbaseMetadata = new HBaseMetadata

  val filedByHbaseFamilyAndColumn = fieldByHbaseColumn.toMap

  def allColumns() = schema.fields.map{ field =>
    val fieldName = field.name
    if(keyColumns.contains(fieldName)) {
      KeyColumn(fieldName, field.dataType, keyColumns.indexOf(fieldName))
    } else {
      val familyAndQuilifier = filedByHbaseFamilyAndColumn.getOrElse(fieldName, "").split("\\.")
      assert(familyAndQuilifier.size == 2, "illegal mapping")
      NonKeyColumn(fieldName, field.dataType, familyAndQuilifier(0), familyAndQuilifier(1))
    }
  }

  val relation = hbaseMetadata.createTable(tableName, hbaseTableName, allColumns)

  override def sqlContext: SQLContext = context

  // todo: optimization for predict push down
  override def buildScan(output: Seq[Attribute], predicates: Seq[Expression]): RDD[Row] = {
    new HBaseSQLReaderRDD(
      relation,
      schema.toAttributes,
      None,
      None,
      predicates.reduceLeftOption(And),
      None
    )(sqlContext)
  }
}
