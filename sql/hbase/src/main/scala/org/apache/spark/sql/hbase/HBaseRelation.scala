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

import java.util.ArrayList
import org.apache.spark.sql.hbase.BytesUtils

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Scan, HTable, Put, Get, Result}
import org.apache.hadoop.hbase.filter.{Filter, FilterList}
import org.apache.hadoop.hbase.HBaseConfiguration

import org.apache.spark.Partition
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.catalyst.types._

import scala.collection.JavaConverters._

private[hbase] case class HBaseRelation(
                                         tableName: String,
                                         hbaseNamespace: String,
                                         hbaseTableName: String,
                                         allColumns: Seq[AbstractColumn])
  extends LeafNode {

  @transient lazy val htable: HTable = new HTable(getConf, hbaseTableName)
  @transient lazy val keyColumns = allColumns.filter(_.isInstanceOf[KeyColumn])
    .asInstanceOf[Seq[KeyColumn]].sortBy(_.order)
  @transient lazy val nonKeyColumns = allColumns.filter(_.isInstanceOf[NonKeyColumn])
    .asInstanceOf[Seq[NonKeyColumn]]
  @transient lazy val partitionKeys = keyColumns.map(col =>
    AttributeReference(col.sqlName, col.dataType, nullable = false)())
  @transient lazy val columnMap = allColumns.map {
    case key: KeyColumn => (key.sqlName, key.order)
    case nonKey: NonKeyColumn => (nonKey.sqlName, nonKey)
  }.toMap

  @transient var configuration: Configuration = null

  private def getConf: Configuration = if (configuration == null) HBaseConfiguration.create
  else configuration

  lazy val attributes = nonKeyColumns.map(col =>
    AttributeReference(col.sqlName, col.dataType, nullable = true)())

  //  lazy val colFamilies = nonKeyColumns.map(_.family).distinct
  //  lazy val applyFilters = false

  def closeHTable() = htable.close

  override def output: Seq[Attribute] = {
    allColumns.map {
      case column =>
        (partitionKeys union attributes).find(_.name == column.sqlName).get
    }
  }

  lazy val partitions: Seq[HBasePartition] = {
    val regionLocations = htable.getRegionLocations.asScala.toSeq
    regionLocations.zipWithIndex.map(p =>
      new HBasePartition(p._2, Some(p._1._1.getStartKey),
        Some(p._1._1.getEndKey),
        Some(p._1._2.getHostname))
    )
  }

  def getPrunedPartitions(partionPred: Option[Expression] = None): Option[Seq[HBasePartition]] = {
    //TODO-XY:Use the input parameter
    Option(partitions)
  }


  /**
   * Return the start keys of all of the regions in this table,
   * as a list of SparkImmutableBytesWritable.
   */
  def getRegionStartKeys() = {
    val byteKeys: Array[Array[Byte]] = htable.getStartKeys
    val ret = ArrayBuffer[ImmutableBytesWritableWrapper]()
    for (byteKey <- byteKeys) {
      ret += new ImmutableBytesWritableWrapper(byteKey)
    }
    ret
  }

  def buildFilter(
                   projList: Seq[NamedExpression],
                   rowKeyPredicate: Option[Expression],
                   valuePredicate: Option[Expression]) = {
    val filters = new ArrayList[Filter]
    // TODO: add specific filters
    Option(new FilterList(filters))
  }

  def buildPut(row: Row): Put = {
    // TODO: revisit this using new KeyComposer
    val rowKey: HBaseRawType = null
    new Put(rowKey)
  }

  def buildScan(
                 split: Partition,
                 filters: Option[FilterList],
                 projList: Seq[NamedExpression]): Scan = {
    val hbPartition = split.asInstanceOf[HBasePartition]
    val scan = {
      (hbPartition.lowerBound, hbPartition.upperBound) match {
        case (Some(lb), Some(ub)) => new Scan(lb, ub)
        case (Some(lb), None) => new Scan(lb)
        case _ => new Scan
      }
    }
    if (filters.isDefined && !filters.get.getFilters.isEmpty) {
      scan.setFilter(filters.get)
    }
    // TODO: add add Family to SCAN from projections
    scan
  }

  def buildGet(projList: Seq[NamedExpression], rowKey: HBaseRawType) {
    new Get(rowKey)
    // TODO: add columns to the Get
  }

  /**
   * create row key based on key columns information
   * @param rawKeyColumns sequence of byte array representing the key columns
   * @return array of bytes
   */
  def encodingRawKeyColumns(rawKeyColumns: Seq[HBaseRawType]): HBaseRawType = {
    var buffer = ArrayBuffer[Byte]()
    val delimiter: Byte = 0
    var index = 0
    for (rawKeyColumn <- rawKeyColumns) {
      val keyColumn = keyColumns(index)
      buffer = buffer ++ rawKeyColumn
      if (keyColumn.dataType == StringType) {
        buffer += delimiter
      }
      index = index + 1
    }
    buffer.toArray
  }

  /**
   * get the sequence of key columns from the byte array
   * @param rowKey array of bytes
   * @return sequence of byte array
   */
  def decodingRawKeyColumns(rowKey: HBaseRawType): Seq[HBaseRawType] = {
    var rowKeyList = List[HBaseRawType]()
    val delimiter: Byte = 0
    var index = 0
    for (keyColumn <- keyColumns) {
      var buffer = ArrayBuffer[Byte]()
      val dataType = keyColumn.dataType
      if (dataType == StringType) {
        while (index < rowKey.length && rowKey(index) != delimiter) {
          buffer += rowKey(index)
          index = index + 1
        }
        index = index + 1
      }
      else {
        val length = NativeType.defaultSizeOf(dataType.asInstanceOf[NativeType])
        for (i <- 0 to (length - 1)) {
          buffer += rowKey(index)
          index = index + 1
        }
      }
      rowKeyList = rowKeyList :+ buffer.toArray
    }
    rowKeyList
  }

  //  /**
  //   * Trait for RowKeyParser's that convert a raw array of bytes into their constituent
  //   * logical column values
  //   *
  //   */
  //  trait AbstractRowKeyParser {
  //
  ////    def createKey(rawBytes: Seq[HBaseRawType], version: Byte): HBaseRawType
  ////
  ////    def parseRowKey(rowKey: HBaseRawType): Seq[HBaseRawType]
  ////
  ////    def parseRowKeyWithMetaData(rkCols: Seq[KeyColumn], rowKey: HBaseRawType)
  ////    : SortedMap[TableName, (KeyColumn, Any)] // TODO change Any
  //  }
  //
  //  case class RowKeySpec(offsets: Seq[Int], version: Byte = RowKeyParser.Version1)
  //
  //  // TODO(Bo): replace the implementation with the null-byte terminated string logic
  //  object RowKeyParser extends AbstractRowKeyParser with Serializable {
  //    val Version1 = 1.toByte
  //    val VersionFieldLen = 1
  //    // Length in bytes of the RowKey version field
  //    val DimensionCountLen = 1
  //    // One byte for the number of key dimensions
  //    val MaxDimensions = 255
  //    val OffsetFieldLen = 2
  //
  //    // Two bytes for the value of each dimension offset.
  //    // Therefore max size of rowkey is 65535.  Note: if longer rowkeys desired in future
  //    // then simply define a new RowKey version to support it. Otherwise would be wasteful
  //    // to define as 4 bytes now.
  //    def computeLength(keys: Seq[HBaseRawType]) = {
  //      VersionFieldLen + keys.map(_.length).sum +
  //        OffsetFieldLen * keys.size + DimensionCountLen
  //    }
  //
  //    override def createKey(keys: Seq[HBaseRawType], version: Byte = Version1): HBaseRawType = {
  //      val barr = new Array[Byte](computeLength(keys))
  //      val arrayx = new AtomicInteger(0)
  //      barr(arrayx.getAndAdd(VersionFieldLen)) = version // VersionByte
  //
  //      // Remember the starting offset of first data value
  //      val valuesStartIndex = new AtomicInteger(arrayx.get)
  //
  //      // copy each of the dimension values in turn
  //      keys.foreach { k => copyToArr(barr, k, arrayx.getAndAdd(k.length))}
  //
  //      // Copy the offsets of each dim value
  //      // The valuesStartIndex is the location of the first data value and thus the first
  //      // value included in the Offsets sequence
  //      keys.foreach { k =>
  //        copyToArr(barr,
  //          short2b(valuesStartIndex.getAndAdd(k.length).toShort),
  //          arrayx.getAndAdd(OffsetFieldLen))
  //      }
  //      barr(arrayx.get) = keys.length.toByte // DimensionCountByte
  //      barr
  //    }
  //
  //    def copyToArr[T](a: Array[T], b: Array[T], aoffset: Int) = {
  //      b.copyToArray(a, aoffset)
  //    }
  //
  //    def short2b(sh: Short): Array[Byte] = {
  //      val barr = Array.ofDim[Byte](2)
  //      barr(0) = ((sh >> 8) & 0xff).toByte
  //      barr(1) = (sh & 0xff).toByte
  //      barr
  //    }
  //
  //    def b2Short(barr: Array[Byte]) = {
  //      val out = (barr(0).toShort << 8) | barr(1).toShort
  //      out
  //    }
  //
  //    def createKeyFromCatalystRow(schema: StructType, keyCols: Seq[KeyColumn], row: Row) = {
  //      //      val rawKeyCols = DataTypeUtils.catalystRowToHBaseRawVals(schema, row, keyCols)
  //      //      createKey(rawKeyCols)
  //      null
  //    }
  //
  //    def getMinimumRowKeyLength = VersionFieldLen + DimensionCountLen
  //
  //    override def parseRowKey(rowKey: HBaseRawType): Seq[HBaseRawType] = {
  //      assert(rowKey.length >= getMinimumRowKeyLength,
  //        s"RowKey is invalid format - less than minlen . Actual length=${rowKey.length}")
  //      assert(rowKey(0) == Version1, s"Only Version1 supported. Actual=${rowKey(0)}")
  //      val ndims: Int = rowKey(rowKey.length - 1).toInt
  //      val offsetsStart = rowKey.length - DimensionCountLen - ndims * OffsetFieldLen
  //      val rowKeySpec = RowKeySpec(
  //        for (dx <- 0 to ndims - 1)
  //        yield b2Short(rowKey.slice(offsetsStart + dx * OffsetFieldLen,
  //          offsetsStart + (dx + 1) * OffsetFieldLen))
  //      )
  //
  //      val endOffsets = rowKeySpec.offsets.tail :+ (rowKey.length - DimensionCountLen - 1)
  //      val colsList = rowKeySpec.offsets.zipWithIndex.map { case (off, ix) =>
  //        rowKey.slice(off, endOffsets(ix))
  //      }
  //      colsList
  //    }
  //
  ////    //TODO
  ////    override def parseRowKeyWithMetaData(rkCols: Seq[KeyColumn], rowKey: HBaseRawType):
  ////    SortedMap[TableName, (KeyColumn, Any)] = {
  ////      import scala.collection.mutable.HashMap
  ////
  ////      val rowKeyVals = parseRowKey(rowKey)
  ////      val rmap = rowKeyVals.zipWithIndex.foldLeft(new HashMap[ColumnName, (Column, Any)]()) {
  ////        case (m, (cval, ix)) =>
  ////          m.update(rkCols(ix).toColumnName, (rkCols(ix),
  ////            hbaseFieldToRowField(cval, rkCols(ix).dataType)))
  ////          m
  ////      }
  ////      TreeMap(rmap.toArray: _*)(Ordering.by { cn: ColumnName => rmap(cn)._1.ordinal})
  ////        .asInstanceOf[SortedMap[ColumnName, (Column, Any)]]
  ////    }
  //
  //    def show(bytes: Array[Byte]) = {
  //      val len = bytes.length
  //      //      val out = s"Version=${bytes(0).toInt} NumDims=${bytes(len - 1)} "
  //    }
  //
  //  }

  def buildRow(projections: Seq[(Attribute, Int)],
               result: Result,
               row: MutableRow,
               bytesUtils: BytesUtils): Row = {
    assert(projections.size == row.length, "Projection size and row size mismatched")
    // TODO: replaced with the new Key method
    val rowKeys = decodingRawKeyColumns(result.getRow)
    projections.foreach { p =>
      columnMap.get(p._1.name).get match {
        case column: NonKeyColumn => {
          val colValue = result.getValue(column.familyRaw, column.qualifierRaw)
          DataTypeUtils.setRowColumnFromHBaseRawType(row, p._2, colValue,
            column.dataType, bytesUtils)
        }
        case ki => {
          val keyIndex = ki.asInstanceOf[Int]
          val rowKey = rowKeys(keyIndex)
          DataTypeUtils.setRowColumnFromHBaseRawType(row, p._2, rowKey,
            keyColumns(keyIndex).dataType, bytesUtils)
        }
      }
    }
    row
  }
}
