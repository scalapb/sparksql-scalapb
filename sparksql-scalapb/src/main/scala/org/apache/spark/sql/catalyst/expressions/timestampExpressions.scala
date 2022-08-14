package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.{AbstractDataType, DataType, IntegralType, LongType, TimestampType}

case class MicrosLongToTimestamp(child: Expression) extends UnaryExpression
  with ExpectsInputTypes with NullIntolerant {

  override def prettyName: String = "micros_long_to_timestamp"

  override def inputTypes: Seq[AbstractDataType] = Seq(IntegralType)

  override def dataType: DataType = TimestampType

  override def nullSafeEval(input: Any): Any = {
    input.asInstanceOf[Number].longValue()
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, c => c)
  }

  protected def withNewChildInternal(newChild: Expression): MicrosLongToTimestamp =
    copy(child = newChild)
}



case class TimestampToMicrosLong(child: Expression) extends UnaryExpression
  with ExpectsInputTypes with NullIntolerant {

  override def prettyName: String = "timestamp_to_micros_long"

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType)

  override def dataType: DataType = LongType

  override def nullSafeEval(input: Any): Any = {
    input.asInstanceOf[Number].longValue()
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, c => c)
  }

  protected def withNewChildInternal(newChild: Expression): TimestampToMicrosLong =
    copy(child = newChild)
}
