/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.table.planner.utils

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.api.java.typeutils.TupleTypeInfo
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.catalog.DataTypeFactory
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.table.legacy.api.Types
import org.apache.flink.table.runtime.typeutils.BigDecimalTypeInfo
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.inference.{TypeInference, TypeStrategies}
import org.apache.flink.table.types.logical.DecimalType
import org.apache.flink.table.types.logical.utils.LogicalTypeMerging

import java.lang.{Iterable => JIterable}
import java.math.{BigDecimal, BigInteger, MathContext}

/** The initial accumulator for Integral Avg aggregate function */
class IntegralAvgAccumulator extends JTuple2[Long, Long] {
  f0 = 0L // sum
  f1 = 0L // count
}

/**
 * Base class for built-in Integral Avg aggregate function
 *
 * @tparam T
 *   the type for the aggregation result
 */
// NOTE: T is always scala.Double in subclasses; however it's problematics
//       if we remove [T] and replace T with Double;
//       specifically, null.asInstanceOf[Double] == 0.0 !
abstract class IntegralAvgAggFunction[T] extends AggregateFunction[T, IntegralAvgAccumulator] {

  override def createAccumulator(): IntegralAvgAccumulator = {
    new IntegralAvgAccumulator
  }

  def accumulate(acc: IntegralAvgAccumulator, value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[Number].longValue()
      acc.f0 += v
      acc.f1 += 1L
    }
  }

  def retract(acc: IntegralAvgAccumulator, value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[Number].longValue()
      acc.f0 -= v
      acc.f1 -= 1L
    }
  }

  override def getValue(acc: IntegralAvgAccumulator): T = {
    if (acc.f1 == 0) {
      null.asInstanceOf[T]
    } else {
      resultTypeConvert(acc.f0 / acc.f1)
    }
  }

  def merge(acc: IntegralAvgAccumulator, its: JIterable[IntegralAvgAccumulator]): Unit = {
    val iter = its.iterator()
    while (iter.hasNext) {
      val a = iter.next()
      acc.f1 += a.f1
      acc.f0 += a.f0
    }
  }

  override def getTypeInference(typeFactory: DataTypeFactory): TypeInference = {
    TypeInference.newBuilder
      .typedArguments(getValueDataType)
      .accumulatorTypeStrategy(
        TypeStrategies.explicit(
          DataTypes.STRUCTURED(
            classOf[IntegralAvgAccumulator],
            DataTypes.FIELD("f0", DataTypes.BIGINT()),
            DataTypes.FIELD("f1", DataTypes.BIGINT()))))
      .outputTypeStrategy(TypeStrategies.explicit(getValueDataType))
      .build
  }

  def getValueDataType: DataType

  /**
   * Convert the intermediate result to the expected aggregation result type
   *
   * @param value
   *   the intermediate result. We use a Long container to save the intermediate result to avoid the
   *   overflow by sum operation.
   * @return
   *   the result value with the expected aggregation result type
   */
  def resultTypeConvert(value: Long): T
}

/** Built-in Byte Avg aggregate function */
class ByteAvgAggFunction extends IntegralAvgAggFunction[Byte] {
  override def resultTypeConvert(value: Long): Byte = value.toByte

  override def getValueDataType: DataType = DataTypes.TINYINT()
}

/** Built-in Short Avg aggregate function */
class ShortAvgAggFunction extends IntegralAvgAggFunction[Short] {
  override def resultTypeConvert(value: Long): Short = value.toShort

  override def getValueDataType: DataType = DataTypes.SMALLINT()
}

/** Built-in Int Avg aggregate function */
class IntAvgAggFunction extends IntegralAvgAggFunction[Int] {
  override def resultTypeConvert(value: Long): Int = value.toInt

  override def getValueDataType: DataType = DataTypes.INT()
}

/** The initial accumulator for Big Integral Avg aggregate function */
class BigIntegralAvgAccumulator extends JTuple2[BigInteger, Long] {
  f0 = BigInteger.ZERO // sum
  f1 = 0L // count
}

/**
 * Base Class for Built-in Big Integral Avg aggregate function
 *
 * @tparam T
 *   the type for the aggregation result
 */
abstract class BigIntegralAvgAggFunction[T]
  extends AggregateFunction[T, BigIntegralAvgAccumulator] {

  override def createAccumulator(): BigIntegralAvgAccumulator = {
    new BigIntegralAvgAccumulator
  }

  def accumulate(acc: BigIntegralAvgAccumulator, value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[Long]
      acc.f0 = acc.f0.add(BigInteger.valueOf(v))
      acc.f1 += 1L
    }
  }

  def retract(acc: BigIntegralAvgAccumulator, value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[Long]
      acc.f0 = acc.f0.subtract(BigInteger.valueOf(v))
      acc.f1 -= 1L
    }
  }

  override def getValue(acc: BigIntegralAvgAccumulator): T = {
    if (acc.f1 == 0) {
      null.asInstanceOf[T]
    } else {
      resultTypeConvert(acc.f0.divide(BigInteger.valueOf(acc.f1)))
    }
  }

  def merge(acc: BigIntegralAvgAccumulator, its: JIterable[BigIntegralAvgAccumulator]): Unit = {
    val iter = its.iterator()
    while (iter.hasNext) {
      val a = iter.next()
      acc.f1 += a.f1
      acc.f0 = acc.f0.add(a.f0)
    }
  }

  override def getAccumulatorType: TypeInformation[BigIntegralAvgAccumulator] = {
    new TupleTypeInfo(classOf[BigIntegralAvgAccumulator], Types.INT, Types.LONG)
  }

  /**
   * Convert the intermediate result to the expected aggregation result type
   *
   * @param value
   *   the intermediate result. We use a BigInteger container to save the intermediate result to
   *   avoid the overflow by sum operation.
   * @return
   *   the result value with the expected aggregation result type
   */
  def resultTypeConvert(value: BigInteger): T
}

/** Built-in Long Avg aggregate function */
class LongAvgAggFunction extends BigIntegralAvgAggFunction[Long] {
  override def resultTypeConvert(value: BigInteger): Long = value.longValue()
}

/** The initial accumulator for Floating Avg aggregate function */
class FloatingAvgAccumulator extends JTuple2[Double, Long] {
  f0 = 0 // sum
  f1 = 0L // count
}

/**
 * Base class for built-in Floating Avg aggregate function
 *
 * @tparam T
 *   the type for the aggregation result
 */
abstract class FloatingAvgAggFunction[T] extends AggregateFunction[T, FloatingAvgAccumulator] {

  override def createAccumulator(): FloatingAvgAccumulator = {
    new FloatingAvgAccumulator
  }

  def accumulate(acc: FloatingAvgAccumulator, value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[Number].doubleValue()
      acc.f0 += v
      acc.f1 += 1L
    }
  }

  def retract(acc: FloatingAvgAccumulator, value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[Number].doubleValue()
      acc.f0 -= v
      acc.f1 -= 1L
    }
  }

  override def getValue(acc: FloatingAvgAccumulator): T = {
    if (acc.f1 == 0) {
      null.asInstanceOf[T]
    } else {
      resultTypeConvert(acc.f0 / acc.f1)
    }
  }

  def merge(acc: FloatingAvgAccumulator, its: JIterable[FloatingAvgAccumulator]): Unit = {
    val iter = its.iterator()
    while (iter.hasNext) {
      val a = iter.next()
      acc.f1 += a.f1
      acc.f0 += a.f0
    }
  }

  override def getAccumulatorType: TypeInformation[FloatingAvgAccumulator] = {
    new TupleTypeInfo(classOf[FloatingAvgAccumulator], Types.DOUBLE, Types.LONG)
  }

  /**
   * Convert the intermediate result to the expected aggregation result type
   *
   * @param value
   *   the intermediate result. We use a Double container to save the intermediate result to avoid
   *   the overflow by sum operation.
   * @return
   *   the result value with the expected aggregation result type
   */
  def resultTypeConvert(value: Double): T
}

/** Built-in Float Avg aggregate function */
class FloatAvgAggFunction extends FloatingAvgAggFunction[Float] {
  override def resultTypeConvert(value: Double): Float = value.toFloat
}

/** Built-in Int Double aggregate function */
class DoubleAvgAggFunction extends FloatingAvgAggFunction[Double] {
  override def resultTypeConvert(value: Double): Double = value
}

/** The initial accumulator for Big Decimal Avg aggregate function */
class DecimalAvgAccumulator extends JTuple2[BigDecimal, Long] {
  f0 = BigDecimal.ZERO // sum
  f1 = 0L // count
}

/** Base class for built-in Big Decimal Avg aggregate function */
class DecimalAvgAggFunction(argType: DecimalType)
  extends AggregateFunction[BigDecimal, DecimalAvgAccumulator] {

  override def createAccumulator(): DecimalAvgAccumulator = {
    new DecimalAvgAccumulator
  }

  def accumulate(acc: DecimalAvgAccumulator, value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[BigDecimal]
      acc.f0 = acc.f0.add(v)
      acc.f1 += 1L
    }
  }

  def retract(acc: DecimalAvgAccumulator, value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[BigDecimal]
      acc.f0 = acc.f0.subtract(v)
      acc.f1 -= 1L
    }
  }

  override def getValue(acc: DecimalAvgAccumulator): BigDecimal = {
    if (acc.f1 == 0) {
      null.asInstanceOf[BigDecimal]
    } else {
      acc.f0.divide(BigDecimal.valueOf(acc.f1), MathContext.DECIMAL128)
    }
  }

  def merge(acc: DecimalAvgAccumulator, its: JIterable[DecimalAvgAccumulator]): Unit = {
    val iter = its.iterator()
    while (iter.hasNext) {
      val a = iter.next()
      acc.f0 = acc.f0.add(a.f0)
      acc.f1 += a.f1
    }
  }

  override def getAccumulatorType: TypeInformation[DecimalAvgAccumulator] = {
    val decimalType = getSumType
    new TupleTypeInfo(
      classOf[DecimalAvgAccumulator],
      new BigDecimalTypeInfo(decimalType.getPrecision, decimalType.getScale),
      Types.LONG)
  }

  def getSumType: DecimalType =
    LogicalTypeMerging.findSumAggType(argType).asInstanceOf[DecimalType]

  override def getResultType: BigDecimalTypeInfo = {
    val t = LogicalTypeMerging.findAvgAggType(argType).asInstanceOf[DecimalType]
    new BigDecimalTypeInfo(t.getPrecision, t.getScale)
  }

}
