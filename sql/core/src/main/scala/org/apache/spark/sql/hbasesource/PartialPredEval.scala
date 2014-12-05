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

import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.types.{DataType, NativeType}


object PartialPredicateOperations {

  // Partial evaluation is nullness-based, i.e., uninterested columns are assigned nulls,
  // which necessitates changes of the null handling from the normal evaluations
  // of predicate expressions
  implicit class partialPredicateEvaluator(e: Expression) {
    def partialEval(input: Row): Any = {
      e match {
        case And(left, right)  => {
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
        }
        case Or(left, right)  => {
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
        }
        case Not(child)  => {
          child.partialEval(input) match {
            case null => null
            case b: Boolean => !b
          }
        }
        case In(value, list) => {
          val evaluatedValue = value.partialEval(input)
          if (evaluatedValue == null) {
            null
          } else {
            val evaluatedList = list.map(_.partialEval(input))
            if (evaluatedList.exists(e=> e == evaluatedValue)) {
              true
            } else  if (evaluatedList.exists(e=> e == null)) {
              null
            } else {
              false
            }
          }
        }
        case InSet(value, hset) => {
          val evaluatedValue = value.partialEval(input)
          if (evaluatedValue == null) {
            null
          } else {
            hset.contains(evaluatedValue)
          }
        }
        case l: LeafExpression => l.eval(input)
        case b: BoundReference => b.eval(input) //Really a LeafExpression but not declared as such
        case n: NamedExpression => n.eval(input) //Really a LeafExpression but not declared as such
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
            case nativeType: NativeType => {
              val pdt = RangeType.primitiveToPODataTypeMap.get(nativeType).getOrElse(null)
              if (pdt == null) {
                sys.error(s"Type $i does not have corresponding partial ordered type")
              } else {
                pdt.partialOrdering.tryCompare(
                  pdt.toPartiallyOrderingDataType(evalE1, nativeType).asInstanceOf[pdt.JvmType],
                  pdt.toPartiallyOrderingDataType(evalE2, nativeType).asInstanceOf[pdt.JvmType])
              }
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
    def partialReduce(input: Row): Any = {
      e match {
        case And(left, right)  => {
          val l = left.partialReduce(input)
          if (l == false) {
            false
          } else {
            val r = right.partialReduce(input)
            if (r == false) {
              false
            } else {
              (l, r) match {
                case (true, true) => true
                case (true, _) => r
                case (_, true) => l
                case (nl: Expression, nr: Expression) => {
                  if ((nl fastEquals left) && (nr fastEquals right)) {
                    e
                  } else {
                    And(nl, nr)
                  }
                }
                case _ => sys.error("unexpected child type(s) in partial reduction")
              }
            }
          }
        }
        case Or(left, right)  => {
          val l = left.partialReduce(input)
          if (l == true) {
            true
          } else {
            val r = right.partialReduce(input)
            if (r == true) {
              true
            } else {
              (l, r) match {
                case (false, false) => false
                case (false, _) => r
                case (_, false) => l
                case (nl: Expression, nr: Expression) => {
                  if ((nl fastEquals left) && (nr fastEquals right)) {
                    e
                  } else {
                    Or(nl, nr)
                  }
                }
                case _ => sys.error("unexpected child type(s) in partial reduction")
              }
            }
          }
        }
        case Not(child)  => {
          child.partialReduce(input) match {
            case b: Boolean => !b
            case ec: Expression => if (ec fastEquals child) { e } else { Not(ec) }
          }
        }
        case In(value, list) => {
          val evaluatedValue = value.partialReduce(input)
          if (evaluatedValue.isInstanceOf[Expression]) {
            val evaluatedList = list.map(e=>e.partialReduce(input) match {
              case e: Expression => e
              case d => Literal(d, e.dataType)
            })
            In(evaluatedValue.asInstanceOf[Expression], evaluatedList)
          } else {
            val evaluatedList = list.map(_.partialReduce(input))
            if (evaluatedList.exists(e=> e == evaluatedValue)) {
              true
            } else {
              val newList = evaluatedList.filter(_.isInstanceOf[Expression])
                .map(_.asInstanceOf[Expression])
              if (newList.isEmpty) {
                false
              } else {
                In(Literal(evaluatedValue, value.dataType), newList)
              }
            }
          }
        }
        case InSet(value, hset) => {
          val evaluatedValue = value.partialReduce(input)
          if (evaluatedValue.isInstanceOf[Expression]) {
            InSet(evaluatedValue.asInstanceOf[Expression], hset)
          } else {
            hset.contains(evaluatedValue)
          }
        }
        case l: LeafExpression => {
          val res = l.eval(input)
          if (res == null) { l } else {res}
        }
        case b: BoundReference => {
          val res = b.eval(input)
          // If the result is a MAYBE, returns the original expression
          if (res == null) { b } else {res}
        }
        case n: NamedExpression => {
          val res = n.eval(input)
          if(res == null) { n } else { res }
        }
        case IsNull(child) => e
        // TODO: CAST/Arithithmetic could be treated more nicely
        case Cast(_, _) => e
        // case BinaryArithmetic => null
        case UnaryMinus(_) => e
        case EqualTo(left, right) => {
          val evalL = left.partialReduce(input)
          val evalR = right.partialReduce(input)
          if (evalL.isInstanceOf[Expression] && evalR.isInstanceOf[Expression]) {
            EqualTo(evalL.asInstanceOf[Expression], evalR.asInstanceOf[Expression])
          } else  if (evalL.isInstanceOf[Expression]) {
            EqualTo(evalL.asInstanceOf[Expression], right)
          } else  if (evalR.isInstanceOf[Expression]) {
            EqualTo(left.asInstanceOf[Expression], evalR.asInstanceOf[Expression])
          } else {
            val cmp = prc2(input, left.dataType, right.dataType, evalL, evalR)
            if (cmp.isDefined) {
              cmp.get == 0
            } else {
              e
            }
          }
        }
        case LessThan(left, right) => {
          val evalL = left.partialReduce(input)
          val evalR = right.partialReduce(input)
          if (evalL.isInstanceOf[Expression] && evalR.isInstanceOf[Expression]) {
            EqualTo(evalL.asInstanceOf[Expression], evalR.asInstanceOf[Expression])
          } else  if (evalL.isInstanceOf[Expression]) {
            EqualTo(evalL.asInstanceOf[Expression], right)
          } else  if (evalR.isInstanceOf[Expression]) {
            EqualTo(left, evalR.asInstanceOf[Expression])
          } else {
            val cmp = prc2(input, left.dataType, right.dataType, evalL, evalR)
            if (cmp.isDefined) {
              cmp.get < 0
            } else {
              e
            }
          }
        }
        case LessThanOrEqual(left, right) => {
          val evalL = left.partialReduce(input)
          val evalR = right.partialReduce(input)
          if (evalL.isInstanceOf[Expression] && evalR.isInstanceOf[Expression]) {
            EqualTo(evalL.asInstanceOf[Expression], evalR.asInstanceOf[Expression])
          } else  if (evalL.isInstanceOf[Expression]) {
            EqualTo(evalL.asInstanceOf[Expression], right)
          } else  if (evalR.isInstanceOf[Expression]) {
            EqualTo(left, evalR.asInstanceOf[Expression])
          } else {
            val cmp = prc2(input, left.dataType, right.dataType, evalL, evalR)
            if (cmp.isDefined) {
              cmp.get <= 0
            } else {
              e
            }
          }
        }
        case GreaterThan(left, right) => {
          val evalL = left.partialReduce(input)
          val evalR = right.partialReduce(input)
          if (evalL.isInstanceOf[Expression] && evalR.isInstanceOf[Expression]) {
            EqualTo(evalL.asInstanceOf[Expression], evalR.asInstanceOf[Expression])
          } else  if (evalL.isInstanceOf[Expression]) {
            EqualTo(evalL.asInstanceOf[Expression], right)
          } else  if (evalR.isInstanceOf[Expression]) {
            EqualTo(left, evalR.asInstanceOf[Expression])
          } else {
            val cmp = prc2(input, left.dataType, right.dataType, evalL, evalR)
            if (cmp.isDefined) {
              cmp.get > 0
            } else {
              e
            }
          }
        }
        case GreaterThanOrEqual(left, right) => {
          val evalL = left.partialReduce(input)
          val evalR = right.partialReduce(input)
          if (evalL.isInstanceOf[Expression] && evalR.isInstanceOf[Expression]) {
            EqualTo(evalL.asInstanceOf[Expression], evalR.asInstanceOf[Expression])
          } else  if (evalL.isInstanceOf[Expression]) {
            EqualTo(evalL.asInstanceOf[Expression], right)
          } else  if (evalR.isInstanceOf[Expression]) {
            EqualTo(left, evalR.asInstanceOf[Expression])
          } else {
            val cmp = prc2(input, left.dataType, right.dataType, evalL, evalR)
            if (cmp.isDefined) {
              cmp.get >= 0
            } else {
              e
            }
          }
        }
        case If(predicate, trueE, falseE) => {
          val v = predicate.partialReduce(input)
          if (v.isInstanceOf[Expression]) {
           If(v.asInstanceOf[Expression],
              trueE.partialReduce(input).asInstanceOf[Expression],
              falseE.partialReduce(input).asInstanceOf[Expression])
          } else if (v.asInstanceOf[Boolean]) {
            trueE.partialReduce(input)
          } else {
            falseE.partialReduce(input)
          }
        }
        case _ => e
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
            case nativeType: NativeType => {
              val pdt = RangeType.primitiveToPODataTypeMap.get(nativeType).getOrElse(null)
              if (pdt == null) {
                sys.error(s"Type $i does not have corresponding partial ordered type")
              } else {
                pdt.partialOrdering.tryCompare(
                  pdt.toPartiallyOrderingDataType(evalE1, nativeType).asInstanceOf[pdt.JvmType],
                  pdt.toPartiallyOrderingDataType(evalE2, nativeType).asInstanceOf[pdt.JvmType])
              }
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
        throw new TreeNodeException(e, s"Types do not match ${dataType1} != ${dataType2}")
      }

      dataType1 match {
        case nativeType: NativeType => {
          val pdt = RangeType.primitiveToPODataTypeMap.get(nativeType).getOrElse(null)
          if (pdt == null) {
            sys.error(s"Type $i does not have corresponding partial ordered type")
          } else {
            pdt.partialOrdering.tryCompare(
              pdt.toPartiallyOrderingDataType(eval1, nativeType).asInstanceOf[pdt.JvmType],
              pdt.toPartiallyOrderingDataType(eval2, nativeType).asInstanceOf[pdt.JvmType])
          }
        }
        case other => sys.error(s"Type $other does not support partially ordered operations")
      }
    }
  }
}
