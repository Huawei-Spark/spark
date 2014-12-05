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

import org.apache.hadoop.hbase.util.Bytes

class BytesUtils {
  lazy val booleanArray: HBaseRawType = new HBaseRawType(Bytes.SIZEOF_BOOLEAN)
  lazy val byteArray: HBaseRawType = new HBaseRawType(Bytes.SIZEOF_BYTE)
  lazy val charArray: HBaseRawType = new HBaseRawType(Bytes.SIZEOF_CHAR)
  lazy val doubleArray: HBaseRawType = new HBaseRawType(Bytes.SIZEOF_DOUBLE)
  lazy val floatArray: HBaseRawType = new HBaseRawType(Bytes.SIZEOF_FLOAT)
  lazy val intArray: HBaseRawType = new HBaseRawType(Bytes.SIZEOF_INT)
  lazy val longArray: HBaseRawType = new HBaseRawType(Bytes.SIZEOF_LONG)
  lazy val shortArray: HBaseRawType = new HBaseRawType(Bytes.SIZEOF_SHORT)

  def toBytes(input: String): HBaseRawType = {
    Bytes.toBytes(input)
  }

  def toString(input: HBaseRawType): String = {
    Bytes.toString(input)
  }

  def toBytes(input: Byte): HBaseRawType = {
    // Flip sign bit so that Byte is binary comparable
    byteArray(0) = (input ^ 0x80).asInstanceOf[Byte]
    byteArray
  }

  def toByte(input: HBaseRawType): Byte = {
    // Flip sign bit back
    val v: Int = input(0) ^ 0x80
    v.asInstanceOf[Byte]
  }

  def toBytes(input: Boolean): HBaseRawType = {
    booleanArray(0) = 0.asInstanceOf[Byte]
    if (input) {
      booleanArray(0) = (-1).asInstanceOf[Byte]
    }
    booleanArray
  }

  def toBoolean(input: HBaseRawType): Boolean = {
    input(0) != 0
  }

  def toBytes(input: Double): HBaseRawType = {
    var l: Long = java.lang.Double.doubleToLongBits(input)
    l = (l ^ ((l >> java.lang.Long.SIZE - 1) | java.lang.Long.MIN_VALUE)) + 1
    Bytes.putLong(longArray, 0, l)
    longArray
  }

  def toDouble(input: HBaseRawType): Double = {
    var l: Long = Bytes.toLong(input)
    l = l - 1
    l ^= (~l >> java.lang.Long.SIZE - 1) | java.lang.Long.MIN_VALUE
    java.lang.Double.longBitsToDouble(l)
  }

  def toBytes(input: Short): HBaseRawType = {
    shortArray(0) = ((input >> 8) ^ 0x80).asInstanceOf[Byte]
    shortArray(1) = input.asInstanceOf[Byte]
    shortArray
  }

  def toShort(input: HBaseRawType): Short = {
    // flip sign bit back
    var v: Int = input(0) ^ 0x80
    v = (v << 8) + (input(1) & 0xff)
    v.asInstanceOf[Short]
  }

  def toBytes(input: Float): HBaseRawType = {
    var i: Int = java.lang.Float.floatToIntBits(input)
    i = (i ^ ((i >> Integer.SIZE - 1) | Integer.MIN_VALUE)) + 1
    toBytes(i)
  }

  def toFloat(input: HBaseRawType): Float = {
    var i = toInt(input)
    i = i - 1
    i ^= (~i >> Integer.SIZE - 1) | Integer.MIN_VALUE
    java.lang.Float.intBitsToFloat(i)
  }

  def toBytes(input: Int): HBaseRawType = {
    // Flip sign bit so that INTEGER is binary comparable
    intArray(0) = ((input >> 24) ^ 0x80).asInstanceOf[Byte]
    intArray(1) = (input >> 16).asInstanceOf[Byte]
    intArray(2) = (input >> 8).asInstanceOf[Byte]
    intArray(3) = input.asInstanceOf[Byte]
    intArray
  }

  def toInt(input: HBaseRawType): Int = {
    // Flip sign bit back
    var v: Int = input(0) ^ 0x80
    for (i <- 1 to Bytes.SIZEOF_INT - 1) {
      v = (v << 8) + (input(i) & 0xff)
    }
    v
  }

  def toBytes(input: Long): HBaseRawType = {
    longArray(0) = ((input >> 56) ^ 0x80).asInstanceOf[Byte]
    longArray(1) = (input >> 48).asInstanceOf[Byte]
    longArray(2) = (input >> 40).asInstanceOf[Byte]
    longArray(3) = (input >> 32).asInstanceOf[Byte]
    longArray(4) = (input >> 24).asInstanceOf[Byte]
    longArray(5) = (input >> 16).asInstanceOf[Byte]
    longArray(6) = (input >> 8).asInstanceOf[Byte]
    longArray(7) = input.asInstanceOf[Byte]
    longArray
  }

  def toLong(input: HBaseRawType): Long = {
    // Flip sign bit back
    var v: Long = input(0) ^ 0x80
    for (i <- 1 to Bytes.SIZEOF_LONG - 1) {
      v = (v << 8) + (input(i) & 0xff)
    }
    v
  }
}
