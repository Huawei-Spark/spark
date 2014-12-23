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
import org.apache.spark.sql.catalyst.types.{DataType, NativeType}
import org.apache.spark.sql.hbase.catalyst.types._


object PartialPredicateOperations {

  // Partial evaluation is nullness-based, i.e., uninterested columns are assigned nulls,
  // which necessitates changes of the null handling from the normal evaluations
  // of predicate expressions
  implicit class partialPredicateEvaluator(e: Expression) {
    def partialEval(input: Row): Any = {
      e match {
        case And(left, right) =>
          val l = left.partialEval(input)
          if (l == false) {
            false
          } else {
            val r = right.partialEval(input)
            if (r == false) {
              false
            } else {
              if (l != null && r != null) {
                true
              } else {
                null
              }
            }
          }
        case Or(left, right) =>
          val l = left.partialEval(input)
          if (l == true) {
            true
          } else {
            val r = right.partialEval(input)
            if (r == true) {
              true
            } else {
              if (l != null && r != null) {
                false
              } else {
                null
              }
            }
          }
        case Not(child) =>
          child.partialEval(input) match {
            case null => null
            case b: Boolean => !b
          }
        case In(value, list) =>
          val evaluatedValue = value.partialEval(input)
          if (evaluatedValue == null) {
            null
          } else {
            val evaluatedList = list.map(_.partialEval(input))
            if (evaluatedList.contains(evaluatedValue)) {
              true
            } else if (evaluatedList.contains(null)) {
              null
            } else {
              false
            }
          }
        case InSet(value, hset) =>
          val evaluatedValue = value.partialEval(input)
          if (evaluatedValue == null) {
            null
          } else {
            hset.contains(evaluatedValue)
          }
        case l: LeafExpression => l.eval(input)
        case b: BoundReference => b.eval(input) // Really a LeafExpression but not declared as such
        case n: NamedExpression => n.eval(input) // Really a LeafExpression but not declared as such
        case IsNull(child) =>
          if (child.partialEval(input) == null) {
            // In partial evaluation, null indicates MAYBE
            null
          } else {
            // Now we only support non-nullable primary key components
            false
          }
        // TODO: CAST/Arithmetic can be treated more nicely
        case Cast(_, _) => null
        // case BinaryArithmetic => null
        case UnaryMinus(_) => null
        case EqualTo(left, right) =>
          val cmp = pc2(input, left, right)
          if (cmp.isDefined) {
            cmp.get == 0
          } else {
            null
          }
        case LessThan(left, right) =>
          val cmp = pc2(input, left, right)
          if (cmp.isDefined) {
            cmp.get == -2
          } else {
            null
          }
        case LessThanOrEqual(left, right) =>
          val cmp = pc2(input, left, right)
          if (cmp.isDefined) {
            cmp.get == -1
          } else {
            null
          }
        case GreaterThan(left, right) =>
          val cmp = pc2(input, left, right)
          if (cmp.isDefined) {
            cmp.get == 2
          } else {
            null
          }
        case GreaterThanOrEqual(left, right) =>
          val cmp = pc2(input, left, right)
          if (cmp.isDefined) {
            cmp.get == 1
          } else {
            null
          }
        case If(predicate, trueE, falseE) =>
          val v = predicate.partialEval(input)
          if (v == null) {
            null
          } else if (v.asInstanceOf[Boolean]) {
            trueE.partialEval(input)
          } else {
            falseE.partialEval(input)
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
        throw new TreeNodeException(e, s"Types do not match ${e1.dataType} != ${e2.dataType}")
      }

      val evalE1 = e1.partialEval(i)
      if (evalE1 == null) {
        None
      } else {
        val evalE2 = e2.partialEval(i)
        if (evalE2 == null) {
          None
        } else {
          e1.dataType match {
            case nativeType: NativeType =>
              val pdt = RangeType.primitiveToPODataTypeMap.getOrElse(nativeType, null)
              if (pdt == null) {
                sys.error(s"Type $i does not have corresponding partial ordered type")
              } else {
                pdt.partialOrdering.tryCompare(
                  pdt.toPartiallyOrderingDataType(evalE1, nativeType).asInstanceOf[pdt.JvmType],
                  pdt.toPartiallyOrderingDataType(evalE2, nativeType).asInstanceOf[pdt.JvmType])
              }
            case other => sys.error(s"Type $other does not support partially ordered operations")
          }
        }
      }
    }
  }

  // Partial reduction is nullness-based, i.e., uninterested columns are assigned nulls,
  // which necessitates changes of the null handling from the normal evaluations
  // of predicate expressions
  // There are 3 possible results: TRUE, FALSE, and MAYBE represented by a predicate
  // which will be used to further filter the results
  implicit class partialPredicateReducer(e: Expression) {
    def unboundAttributeReference(e: Expression, schema: Seq[Attribute]): Expression = {
      e transform {
        case b: BoundReference => schema(b.ordinal)
      }
    }

    def partialReduce(input: Row, schema: Seq[Attribute]): (Any, Expression) = {
      e match {
        case And(left, right) =>
          val l = left.partialReduce(input, schema)
          if (l._1 == false) {
            (false, null)
          } else {
            val r = right.partialReduce(input, schema)
            if (r._1 == false) {
              (false, null)
            } else {
              (l._1, r._1) match {
                case (true, true) => (true, null)
                case (true, _) => (null, r._2)
                case (_, true) => (null, l._2)
                case (_, _) =>
                  if ((l._2 fastEquals left) && (r._2 fastEquals right)) {
                    (null, unboundAttributeReference(e, schema))
                  } else {
                    (null, And(l._2, r._2))
                  }
                case _ => sys.error("unexpected child type(s) in partial reduction")
              }
            }
          }
        case Or(left, right) =>
          val l = left.partialReduce(input, schema)
          if (l._1 == true) {
            (true, null)
          } else {
            val r = right.partialReduce(input, schema)
            if (r._1 == true) {
              (true, null)
            } else {
              (l._1, r._1) match {
                case (false, false) => (false, null)
                case (false, _) => (null, r._2)
                case (_, false) => (null, l._2)
                case (_, _) =>
                  if ((l._2 fastEquals left) && (r._2 fastEquals right)) {
                    (null, unboundAttributeReference(e, schema))
                  } else {
                    (null, Or(l._2, r._2))
                  }
                case _ => sys.error("unexpected child type(s) in partial reduction")
              }
            }
          }
        case Not(child) =>
          child.partialReduce(input, schema) match {
            case (b: Boolean, null) => (!b, null)
            case (null, ec: Expression) => if (ec fastEquals child) {
              (null, unboundAttributeReference(e, schema))
            } else {
              (null, ec)
            }
          }
        case In(value, list) =>
          val (evaluatedValue, expr) = value.partialReduce(input, schema)
          if (evaluatedValue == null) {
            val evaluatedList = list.map(e=>e.partialReduce(input, schema) match {
              case (null, e: Expression) => e
              case (d, _)  => Literal(d, e.dataType)
            })
            (null, In(expr, evaluatedList))
          } else {
            val evaluatedList = list.map(_.partialReduce(input, schema))
            if (evaluatedList.exists(e => e._1 == evaluatedValue)) {
              (true, null)
            } else {
              val newList = evaluatedList.filter(p=>p._1 != null && p._1 != evaluatedValue)
              if (newList.isEmpty) (false, null)
              else (null, In(Literal(evaluatedValue, value.dataType), newList.map(_._2)))
            }
          }
        case InSet(value, hset) =>
          val evaluatedValue = value.partialReduce(input, schema)
          if (evaluatedValue._1 == null) {
            (null, InSet(evaluatedValue._2, hset))
          } else {
            (hset.contains(evaluatedValue._1), null)
          }
        case l: LeafExpression =>
          val res = l.eval(input)
          (res, l)
        case b: BoundReference =>
          val res = b.eval(input)
          (res, schema(b.ordinal))
        case n: NamedExpression =>
          val res = n.eval(input)
          (res, n)
        case IsNull(child) => (null, unboundAttributeReference(e, schema))
        // TODO: CAST/Arithmetic could be treated more nicely
        case Cast(_, _) => (null, unboundAttributeReference(e, schema))
        // case BinaryArithmetic => null
        case UnaryMinus(_) => (null, unboundAttributeReference(e, schema))
        case EqualTo(left, right) =>
          val evalL = left.partialReduce(input, schema)
          val evalR = right.partialReduce(input, schema)
          if (evalL._1 == null && evalR._1 == null) {
            (null, EqualTo(evalL._2, evalR._2))
          } else if (evalL._1 == null) {
            (null, EqualTo(evalL._2, right))
          } else if (evalR._1 == null) {
            (null, EqualTo(left, evalR._2))
          } else {
            val cmp = prc2(input, left.dataType, right.dataType, evalL._1, evalR._1)
            if (cmp.isDefined) {
              (cmp.get == 0, null)
            } else {
              (null, EqualTo(evalL._2, evalR._2))
            }
          }
        case LessThan(left, right) =>
          val evalL = left.partialReduce(input, schema)
          val evalR = right.partialReduce(input, schema)
          if (evalL._1 == null && evalR._1 == null) {
            (null, LessThan(evalL._2, evalR._2))
          } else if (evalL._1 == null) {
            (null, LessThan(evalL._2, right))
          } else if (evalR._1 == null) {
            (null, LessThan(left, evalR._2))
          } else {
            val cmp = prc2(input, left.dataType, right.dataType, evalL._1, evalR._1)
            if (cmp.isDefined) {
              (cmp.get == -2, null)
            } else {
              (null, LessThan(evalL._2, evalR._2))
            }
          }
        case LessThanOrEqual(left, right) =>
          val evalL = left.partialReduce(input, schema)
          val evalR = right.partialReduce(input, schema)
          if (evalL._1 == null && evalR._1 == null) {
            (null, LessThanOrEqual(evalL._2, evalR._2))
          } else if (evalL._1 == null) {
            (null, LessThanOrEqual(evalL._2, right))
          } else if (evalR._1 == null) {
            (null, LessThanOrEqual(left, evalR._2))
          } else {
            val cmp = prc2(input, left.dataType, right.dataType, evalL._1, evalR._1)
            if (cmp.isDefined) {
              (cmp.get <= 0, null)
            } else {
              (null, LessThanOrEqual(evalL._2, evalR._2))
            }
          }
        case GreaterThan(left, right) =>
          val evalL = left.partialReduce(input, schema)
          val evalR = right.partialReduce(input, schema)
          if (evalL._1 == null && evalR._1 == null) {
            (null, GreaterThan(evalL._2, evalR._2))
          } else if (evalL._1 == null) {
            (null, GreaterThan(evalL._2, right))
          } else if (evalR._1 == null) {
            (null, GreaterThan(left, evalR._2))
          } else {
            val cmp = prc2(input, left.dataType, right.dataType, evalL._1, evalR._1)
            if (cmp.isDefined) {
              (cmp.get == 2, null)
            } else {
              (null, GreaterThan(evalL._2, evalR._2))
            }
          }
        case GreaterThanOrEqual(left, right) =>
          val evalL = left.partialReduce(input, schema)
          val evalR = right.partialReduce(input, schema)
          if (evalL._1 == null && evalR._1 == null) {
            (null, GreaterThanOrEqual(evalL._2, evalR._2))
          } else if (evalL._1 == null) {
            (null, GreaterThanOrEqual(evalL._2, right))
          } else if (evalR._1 == null) {
            (null, GreaterThanOrEqual(left, evalR._2))
          } else {
            val cmp = prc2(input, left.dataType, right.dataType, evalL._1, evalR._1)
            if (cmp.isDefined) {
              (cmp.get >= 0, null)
            } else {
              (null, GreaterThanOrEqual(evalL._2, evalR._2))
            }
          }
        case If(predicate, trueE, falseE) =>
          val (v, expression) = predicate.partialReduce(input, schema)
          if (v == null) {
            (null, unboundAttributeReference(e, schema))
          } else if (v.asInstanceOf[Boolean]) {
            trueE.partialReduce(input, schema)
          } else {
            falseE.partialReduce(input, schema)
          }
        case _ => (null, unboundAttributeReference(e, schema))
      }
    }

    @inline
    protected def pc2(
                       i: Row,
                       e1: Expression,
                       e2: Expression): Option[Int] = {
      if (e1.dataType != e2.dataType) {
        throw new TreeNodeException(e, s"Types do not match ${e1.dataType} != ${e2.dataType}")
      }

      val evalE1 = e1.partialEval(i)
      if (evalE1 == null) {
        None
      } else {
        val evalE2 = e2.partialEval(i)
        if (evalE2 == null) {
          None
        } else {
          e1.dataType match {
            case nativeType: NativeType =>
              val pdt = RangeType.primitiveToPODataTypeMap.getOrElse(nativeType, null)
              if (pdt == null) {
                sys.error(s"Type $i does not have corresponding partial ordered type")
              } else {
                pdt.partialOrdering.tryCompare(
                  pdt.toPartiallyOrderingDataType(evalE1, nativeType).asInstanceOf[pdt.JvmType],
                  pdt.toPartiallyOrderingDataType(evalE2, nativeType).asInstanceOf[pdt.JvmType])
              }
            case other => sys.error(s"Type $other does not support partially ordered operations")
          }
        }
      }
    }

    @inline
    protected def prc2(
                        i: Row,
                        dataType1: DataType,
                        dataType2: DataType,
                        eval1: Any,
                        eval2: Any): Option[Int] = {
      if (dataType1 != dataType2) {
        throw new TreeNodeException(e, s"Types do not match $dataType1 != $dataType2")
      }

      dataType1 match {
        case nativeType: NativeType =>
          val pdt = RangeType.primitiveToPODataTypeMap.getOrElse(nativeType, null)
          if (pdt == null) {
            sys.error(s"Type $i does not have corresponding partial ordered type")
          } else {
            pdt.partialOrdering.tryCompare(
              pdt.toPartiallyOrderingDataType(eval1, nativeType).asInstanceOf[pdt.JvmType],
              pdt.toPartiallyOrderingDataType(eval2, nativeType).asInstanceOf[pdt.JvmType])
          }
        case other => sys.error(s"Type $other does not support partially ordered operations")
      }
    }
  }

}
