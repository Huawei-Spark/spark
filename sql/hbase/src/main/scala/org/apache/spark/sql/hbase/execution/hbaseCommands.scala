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
package org.apache.spark.sql.hbase.execution

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{Command, LeafNode}
import org.apache.spark.sql.hbase.{HBaseRelation, NonKeyColumn, KeyColumn, HBaseSQLContext}

case class CreateHBaseTableCommand(
                                    tableName: String,
                                    nameSpace: String,
                                    hbaseTable: String,
                                    colsSeq: Seq[String],
                                    keyCols: Seq[(String, String)],
                                    nonKeyCols: Seq[(String, String, String, String)])
                                  (@transient context: HBaseSQLContext)
  extends LeafNode with Command {

  override protected[sql] lazy val sideEffectResult = {
    val catalog = context.catalog

    val keyMap = keyCols.toMap
    val allColumns = colsSeq.map {
      case name => {
        if (keyMap.contains(name)) {
          KeyColumn(
            name,
            catalog.getDataType(keyMap.get(name).get),
            keyCols.indexWhere(_._1 == name))
        } else {
          val nonKeyCol = nonKeyCols.find(_._1 == name).get
          NonKeyColumn(
            name,
            catalog.getDataType(nonKeyCol._2),
            nonKeyCol._3,
            nonKeyCol._4
          )
        }
      }
    }

    catalog.createTable(tableName, nameSpace, hbaseTable, allColumns)
    Seq.empty[Row]
  }

  override def output: Seq[Attribute] = Seq.empty
}

case class AlterDropColCommand(tableName: HBaseRelation, columnName: String)
                              (@transient context: HBaseSQLContext)
  extends LeafNode with Command {

  override protected[sql] lazy val sideEffectResult = {
    context.catalog.alterTableDropNonKey(tableName.tableName, columnName)
    Seq.empty[Row]
  }

  override def output: Seq[Attribute] = Seq.empty
}

case class AlterAddColCommand(tableName: HBaseRelation,
                              colName: String,
                              colType: String,
                              colFamily: String,
                              colQualifier: String)
                             (@transient context: HBaseSQLContext)
  extends LeafNode with Command {

  override protected[sql] lazy val sideEffectResult = {
    context.catalog.alterTableAddNonKey(tableName.tableName,
      NonKeyColumn(
        colName, context.catalog.getDataType(colType), colFamily, colQualifier)
    )
    Seq.empty[Row]
  }

  override def output: Seq[Attribute] = Seq.empty
}

case class DropHbaseTableCommand(tableName: HBaseRelation)
                                (@transient context: HBaseSQLContext)
  extends LeafNode with Command {

  override protected[sql] lazy val sideEffectResult = {
    context.catalog.deleteTable(tableName.tableName)
    Seq.empty[Row]
  }

  override def output: Seq[Attribute] = Seq.empty
}
