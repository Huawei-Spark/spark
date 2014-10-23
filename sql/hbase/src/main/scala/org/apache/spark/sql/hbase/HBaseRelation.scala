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

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Row => HRow, _}
import org.apache.hadoop.hbase.filter.{FilterBase, FilterList}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.log4j.Logger
import org.apache.spark.Partition
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.hbase.DataTypeUtils._
import org.apache.spark.sql.{SchemaRDD, StructType}

import scala.collection.SortedMap
import scala.collection.immutable.TreeMap

/**
 * HBaseRelation
 *
 * Created by stephen.boesch@huawei.com on 9/8/14
 */
private[hbase] case class HBaseRelation(
                                         @transient var configuration: Configuration,
                                         @transient var hbaseContext: HBaseSQLContext,
                                         catalogTable: HBaseCatalogTable)
  extends LeafNode {

  self: Product =>

  import org.apache.spark.sql.hbase.HBaseRelation._

  // TODO: use external resource or HConnectionManager.createConnection
  @transient lazy val handle: HTable = {
    val tab = new HTable(configuration, getTableName)
    tab
  }

  def getHTable() = handle

  def closeHTable() = {
    def close = handle.close
  }

  def isPartitioned = true

  def tableName = getTableName

  def getTableName() = {
    catalogTable.hbaseTableName.tableName.getNameAsString
  }

  def buildFilter(rowKeyPredicates: Seq[Expression],
                  colPredicates: Seq[Expression]) = {
    var colFilters: Option[FilterList] = None
    if (HBaseStrategies.PushDownPredicates) {
      // Now process the projection predicates
      // TODO: rewrite the predicates based on Catalyst Expressions

      // TODO: Do column pruning based on only the required colFamilies
      val filters: HBaseSQLFilters = new HBaseSQLFilters(colFamilies,
        rowKeyPredicates, colPredicates)
      val colFilters = filters.createColumnFilters

      // TODO: Perform Partition pruning based on the rowKeyPredicates

    }
  }

  val applyFilters = false

  def getScanner(split: Partition): Scan = {
    val hbPartition = split.asInstanceOf[HBasePartition]
    val scan = if (applyFilters) {
      new Scan(hbPartition.bounds.start.get,
        hbPartition.bounds.end.get)
    } else {
      new Scan
    }
    if (applyFilters) {
      colFamilies.foreach { cf =>
        scan.addFamily(s2b(cf))
      }
    }
    scan
  }

  @transient val logger = Logger.getLogger(getClass.getName)

  lazy val partitionKeys: Seq[Attribute] = catalogTable.rowKey.asAttributes

  lazy val attributes = catalogTable.columns.asAttributes

  lazy val colFamilies = catalogTable.colFamilies

  @transient lazy val rowKeyParser = HBaseRelation.RowKeyParser

  def buildPut(schema: StructType, row: Row): Put = {
    val ctab = catalogTable
    val rkey = rowKeyParser.createKeyFromCatalystRow(schema, ctab.rowKey, row)
    val p = new Put(rkey)
    DataTypeUtils.catalystRowToHBaseRawVals(schema, row, ctab.columns).zip(ctab.columns.columns)
      .map { case (raw, col) => p.add(s2b(col.family), s2b(col.qualifier), raw)
    }
    p
  }

  @transient lazy val connection = getHBaseConnection(configuration)

  lazy val hbPartitions = HBaseRelation
    .getPartitions(catalogTable.hbaseTableName.tableName, configuration).toArray

  def getPartitions(): Array[Partition] = hbPartitions.asInstanceOf[Array[Partition]]

  override def output: Seq[Attribute] = attributes ++ partitionKeys


  def buildFilters(rowKeyPredicates: Seq[Expression], colPredicates: Seq[Expression])
  : HBaseSQLFilters = {
    new HBaseSQLFilters(colFamilies, rowKeyPredicates, colPredicates)
  }

  def getRowPrefixPredicates(predicates: Seq[Expression]) = {

    // Filter out all predicates that only deal with partition keys, these are given to the
    // hive table scan operator to be used for partition pruning.
    val partitionKeys = catalogTable.rowKey.asAttributes()

    val partitionKeyIds = AttributeSet(partitionKeys)
    var (rowKeyPredicates, _ /*otherPredicates*/ ) = predicates.partition {
      _.references.subsetOf(partitionKeyIds)
    }

    // Find and sort all of the rowKey dimension elements and stop as soon as one of the
    // composite elements is not found in any predicate
    val loopx = new AtomicLong
    val foundx = new AtomicLong
    val rowPrefixPredicates = for {pki <- partitionKeyIds
                                   if ((loopx.incrementAndGet >= 0)
                                     && rowKeyPredicates.flatMap {
                                     _.references
                                   }.contains(pki)
                                     && (foundx.incrementAndGet == loopx.get))
                                   attrib <- rowKeyPredicates.filter {
                                     _.references.contains(pki)
                                   }
    } yield attrib
    rowPrefixPredicates
  }


  def isOnlyBinaryComparisonPredicates(predicates: Seq[Expression]) = {
    predicates.forall(_.isInstanceOf[BinaryPredicate])
  }

  class HBaseSQLFilters(colFamilies: Seq[String],
                        rowKeyPreds: Seq[Expression],
                        opreds: Seq[Expression])
    extends FilterBase {
    @transient val logger = Logger.getLogger(getClass.getName)

    def createColumnFilters(): Option[FilterList] = {
      val colFilters: FilterList =
        new FilterList(FilterList.Operator.MUST_PASS_ALL)
      //      colFilters.addFilter(new HBaseRowFilter(colFamilies,
      //          catalogTable.rowKeyColumns.columns,
      //        rowKeyPreds.orNull))
      opreds.foreach {
        case preds: Seq[Expression] =>
          // TODO; re-do the predicates logic using expressions
          //          new SingleColumnValueFilter(s2b(col.colName.family.get),
          //          colFilters.addFilter(f)
          //        }
          colFilters
      }
      Some(colFilters)
    }
  }

  /**
   * Presently only a sequence of AND predicates supported. TODO(sboesch): support simple tree
   * of AND/OR predicates
   */
  class HBaseRowFilter(colFamilies: Seq[String],
                       rkCols: Seq[Column],
                       rowKeyPreds: Seq[Expression]
                        ) extends FilterBase {
    @transient val logger = Logger.getLogger(getClass.getName)

    override def filterRowKey(rowKey: Array[Byte], offset: Int, length: Int): Boolean = {

      if (!isOnlyBinaryComparisonPredicates(rowKeyPreds)) {
        false // Presently only simple binary comparisons supported
      } else {
        val catColumns: Columns = catalogTable.columns
        val keyColumns: Columns = catalogTable.rowKey
        def catalystToHBaseColumnName(catColName: String) = {
          catColumns.findBySqlName(catColName)
        }

        def getName(expression: NamedExpression) = expression.asInstanceOf[NamedExpression].name

        val rowPrefixPreds = getRowPrefixPredicates(rowKeyPreds
          .asInstanceOf[Seq[BinaryExpression]])
        // TODO: fix sorting of rowprefix preds
        val rowKeyColsMap = rowKeyParser.parseRowKeyWithMetaData(rkCols, rowKey)
        val result = rowKeyPreds.forall { p =>
          p.eval(Row(rowKeyColsMap.values.map {
            _._2
          })).asInstanceOf[Boolean]
        }
        result
      }
    }

    override def isFamilyEssential(name: Array[Byte]): Boolean = {
      colFamilies.contains(new String(name, HBaseByteEncoding).toLowerCase())
    }

    def rowKeyOrdinal(name: ColumnName) = catalogTable.rowKey(name).ordinal

  }

}

object HBaseRelation {
  @transient private lazy val lazyConfig = HBaseConfiguration.create()

  def configuration() = lazyConfig

  def getHBaseConnection(configuration: Configuration) = {
    val connection = HConnectionManager.createConnection(configuration)
    connection
  }

  def getPartitions(tableName: TableName,
                    config: Configuration) = {
    import scala.collection.JavaConverters._
    val hConnection = getHBaseConnection(config)
    val regionLocations = hConnection.locateRegions(tableName)
    case class BoundsAndServers(startKey: HBaseRawType, endKey: HBaseRawType,
                                servers: Seq[String])
    val regionBoundsAndServers = regionLocations.asScala.map { hregionLocation =>
      val regionInfo = hregionLocation.getRegionInfo
      BoundsAndServers(regionInfo.getStartKey, regionInfo.getEndKey,
        Seq(hregionLocation.getServerName.getHostname))
    }
    val partSeq = regionBoundsAndServers.zipWithIndex.map { case (rb, ix) =>
      new HBasePartition(ix, HBasePartitionBounds(Some(rb.startKey), Some(rb.endKey)),
        Some(rb.servers(0)))
    }
    partSeq.toIndexedSeq
  }

  def rowKeysFromRows(schemaRdd: SchemaRDD, relation: HBaseRelation) = {
    assert(schemaRdd != null)
    assert(relation != null)
    assert(relation.rowKeyParser != null)
    schemaRdd.map { r: Row =>
      relation.rowKeyParser.createKeyFromCatalystRow(
        schemaRdd.schema,
        relation.catalogTable.rowKeyColumns,
        r)
    }
  }


  /**
   * Trait for RowKeyParser's that convert a raw array of bytes into their constituent
   * logical column values
   *
   */
  trait AbstractRowKeyParser {

    def createKey(rawBytes: HBaseRawRowSeq, version: Byte): HBaseRawType

    def parseRowKey(rowKey: HBaseRawType): HBaseRawRowSeq // .NavigableMap[String, HBaseRawType]

    def parseRowKeyWithMetaData(rkCols: Seq[Column], rowKey: HBaseRawType)
    : SortedMap[ColumnName, (Column, Any)]
  }

  case class RowKeySpec(offsets: Seq[Int], version: Byte = RowKeyParser.Version1)

  // TODO(Bo): replace the implementation with the null-byte terminated string logic
  object RowKeyParser extends AbstractRowKeyParser with Serializable {


    val Version1 = 1.toByte

    val VersionFieldLen = 1
    // Length in bytes of the RowKey version field
    val DimensionCountLen = 1
    // One byte for the number of key dimensions
    val MaxDimensions = 255
    val OffsetFieldLen = 2

    // Two bytes for the value of each dimension offset.
    // Therefore max size of rowkey is 65535.  Note: if longer rowkeys desired in future
    // then simply define a new RowKey version to support it. Otherwise would be wasteful
    // to define as 4 bytes now.
    def computeLength(keys: HBaseRawRowSeq) = {
      VersionFieldLen + keys.map {
        _.length
      }.sum + OffsetFieldLen * keys.size + DimensionCountLen
    }

    override def createKey(keys: HBaseRawRowSeq, version: Byte = Version1): HBaseRawType = {
      var barr = new Array[Byte](computeLength(keys))
      val arrayx = new AtomicInteger(0)
      barr(arrayx.getAndAdd(VersionFieldLen)) = version // VersionByte

      // Remember the starting offset of first data value
      val valuesStartIndex = new AtomicInteger(arrayx.get)

      // copy each of the dimension values in turn
      keys.foreach { k => copyToArr(barr, k, arrayx.getAndAdd(k.length))}

      // Copy the offsets of each dim value
      // The valuesStartIndex is the location of the first data value and thus the first
      // value included in the Offsets sequence
      keys.foreach { k =>
        copyToArr(barr,
          short2b(valuesStartIndex.getAndAdd(k.length).toShort),
          arrayx.getAndAdd(OffsetFieldLen))
      }
      barr(arrayx.get) = keys.length.toByte // DimensionCountByte
      barr
    }

    def copyToArr[T](a: Array[T], b: Array[T], aoffset: Int) = {
      b.copyToArray(a, aoffset)
    }

    def short2b(sh: Short): Array[Byte] = {
      val barr = Array.ofDim[Byte](2)
      barr(0) = ((sh >> 8) & 0xff).toByte
      barr(1) = (sh & 0xff).toByte
      barr
    }

    def b2Short(barr: Array[Byte]) = {
      val out = (barr(0).toShort << 8) | barr(1).toShort
      out
    }

    def createKeyFromCatalystRow(schema: StructType, keyCols: Columns, row: Row) = {
      val rawKeyCols = DataTypeUtils.catalystRowToHBaseRawVals(schema, row, keyCols)
      createKey(rawKeyCols)
    }

    def getMinimumRowKeyLength = VersionFieldLen + DimensionCountLen

    override def parseRowKey(rowKey: HBaseRawType): HBaseRawRowSeq = {

      assert(rowKey.length >= getMinimumRowKeyLength,
        s"RowKey is invalid format - less than minlen . Actual length=${rowKey.length}")
      assert(rowKey(0) == Version1, s"Only Version1 supported. Actual=${rowKey(0)}")
      val ndims: Int = rowKey(rowKey.length - 1).toInt
      val offsetsStart = rowKey.length - DimensionCountLen - ndims * OffsetFieldLen
      val rowKeySpec = RowKeySpec(
        for (dx <- 0 to ndims - 1)
        yield b2Short(rowKey.slice(offsetsStart + dx * OffsetFieldLen,
          offsetsStart + (dx + 1) * OffsetFieldLen))
      )

      val endOffsets = rowKeySpec.offsets.tail :+ (rowKey.length - DimensionCountLen - 1)
      val colsList = rowKeySpec.offsets.zipWithIndex.map { case (off, ix) =>
        rowKey.slice(off, endOffsets(ix))
      }
      colsList
    }

    override def parseRowKeyWithMetaData(rkCols: Seq[Column], rowKey: HBaseRawType):
    SortedMap[ColumnName, (Column, Any)] = {
      import scala.collection.mutable.HashMap

      val rowKeyVals = parseRowKey(rowKey)
      val rmap = rowKeyVals.zipWithIndex.foldLeft(new HashMap[ColumnName, (Column, Any)]()) {
        case (m, (cval, ix)) =>
          m.update(rkCols(ix).toColumnName, (rkCols(ix),
            hbaseFieldToRowField(cval, rkCols(ix).dataType)))
          m
      }
      TreeMap(rmap.toArray: _*)(Ordering.by { cn: ColumnName => rmap(cn)._1.ordinal})
        .asInstanceOf[SortedMap[ColumnName, (Column, Any)]]
    }

    def show(bytes: Array[Byte]) = {
      val len = bytes.length
      val out = s"Version=${bytes(0).toInt} NumDims=${bytes(len - 1)} "
    }

  }

}
