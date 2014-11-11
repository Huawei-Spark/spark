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

package org.apache.spark.sql.hbase.catalyst.expressions

import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.hbase.catalyst.types.PartiallyOrderingDataType


object PartialPredicateOperations {
  // Partial evaluation is nullness-based, i.e., uninterested columns are assigned nulls,
  // which necessitates changes of the null handling from the normal evaluations
  // of predicate expressions
  implicit class partialPredicateEvaluator(e: Expression) {
    def partialEval(input: Row) : Any = {
      e match {
        case In(value, list) => {
          val evaluatedValue = value.partialEval(input)
          if (evaluatedValue == null) {
            null
          } else {
            if (list.exists(e=>e.partialEval(input) == evaluatedValue)) {
              true
            } else if (list.exists(e=>e.partialEval(input) == null)) {
              null
            } else {
              false
            }
          }
        }
        case InSet(value, hset, child) => {
          val evaluatedValue = value.partialEval(input)
          if (evaluatedValue == null) {
            null
          } else if (hset.contains(evaluatedValue)) {
            true
          } else if (hset.contains(null)) {
            null
          } else {
            false
          }
        }
        case b: BoundReference => b.eval(input)
        case l: Literal => l.eval(input)
        case IsNull(child) => {
          if (child.partialEval(input) == null) {
            // In partial evaluation, null indicates MAYBE
            null
          } else {
            // Now we only support non-nullable primary key components
            false
          }
        }
        // TODO: CAST/Arithithmetic can be treated more nicely
        case Cast(_, _) => null
        // case BinaryArithmetic => null
        case UnaryMinus(_) => null
        case EqualTo(left, right) => {
          val cmp = pc2(input, left, right)
          if (cmp.isDefined) {
            cmp.get == 0
          } else {
            null
          }
        }
        case LessThan(left, right) => {
          val cmp = pc2(input, left, right)
          if (cmp.isDefined) {
            cmp.get < 0
          } else {
            null
          }
        }
        case LessThanOrEqual(left, right) => {
          val cmp = pc2(input, left, right)
          if (cmp.isDefined) {
            cmp.get <= 0
          } else {
            null
          }
        }
        case GreaterThan(left, right) => {
          val cmp = pc2(input, left, right)
          if (cmp.isDefined) {
            cmp.get > 0
          } else {
            null
          }
        }
        case GreaterThanOrEqual(left, right) => {
          val cmp = pc2(input, left, right)
          if (cmp.isDefined) {
            cmp.get >= 0
          } else {
            null
          }
        }
        case If(predicate, trueE, falseE) => {
          val v = predicate.partialEval(input)
          if (v == null) {
            null
          } else if (v.asInstanceOf[Boolean]) {
            trueE.partialEval(input)
          } else {
            falseE.partialEval(input)
          }
        }
        case _ => null
      }
    }

    @inline
    protected def pc2(
      i: Row,
      e1: Expression,
      e2: Expression): Option[Int] = {
      if (e1.dataType != e2.dataType) {
        throw new TreeNodeException(e,  s"Types do not match ${e1.dataType} != ${e2.dataType}")
      }

      val evalE1 = e1.partialEval(i)
      if(evalE1 == null) {
        null
      } else {
        val evalE2 = e2.partialEval(i)
        if (evalE2 == null) {
          null
        } else {
          e1.dataType match {
            case i: PartiallyOrderingDataType =>
                i.partialOrdering.tryCompare(evalE1.asInstanceOf[i.JvmType],
                  evalE2.asInstanceOf[i.JvmType])
            case other => sys.error(s"Type $other does not support partially ordered operations")
          }
        }
      }
    }
  }
}
