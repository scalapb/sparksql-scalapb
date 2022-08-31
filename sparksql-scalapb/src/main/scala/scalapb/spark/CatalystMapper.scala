package scalapb.spark

import com.google.protobuf.timestamp.Timestamp
import frameless.{SQLDate, SQLTimestamp, TypedEncoder}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.types.{DataType, DateType, ObjectType, TimestampType}
import scalapb.descriptors._

import scala.collection.immutable

trait CatalystMapper[T] extends TypedEncoder[T] {
  def convertMessage(descriptor: Descriptor, msg: PMessage): Any
}

class GoogleTimestampCatalystMapper(
    fromCatalystHelpers: FromCatalystHelpers,
    toCatalystHelpers: ToCatalystHelpers
) extends CatalystMapper[Timestamp] {
  override def nullable: Boolean = false

  override def jvmRepr: DataType = ScalaReflection.dataTypeFor[SQLTimestamp]

  override def catalystRepr: DataType = TimestampType

  override def fromCatalyst(input: Expression): Expression = {
    val cmp = Timestamp.messageCompanion
    val args = immutable.Seq(
      StaticInvoke(
        JavaTimestampHelpers.getClass,
        ObjectType(classOf[PValue]),
        "extractSecondsFromMicrosTimestamp",
        input :: Nil
      ),
      StaticInvoke(
        JavaTimestampHelpers.getClass,
        ObjectType(classOf[PValue]),
        "extractNanosFromMicrosTimestamp",
        input :: Nil
      )
    )
    fromCatalystHelpers.pmessageFromCatalyst(input, cmp, args)
  }

  override def toCatalyst(input: Expression): Expression = {
    val cmp = Timestamp.messageCompanion
    val secondsFd: FieldDescriptor = cmp.scalaDescriptor.fields(0)
    val nanosFd: FieldDescriptor = cmp.scalaDescriptor.fields(1)
    val secondsExpr = toCatalystHelpers.fieldToCatalyst(cmp, secondsFd, input)
    val nanosExpr = toCatalystHelpers.fieldToCatalyst(cmp, nanosFd, input)
    StaticInvoke(
      JavaTimestampHelpers.getClass,
      TimestampType,
      "combineSecondsAndNanosIntoMicrosTimestamp",
      secondsExpr :: nanosExpr :: Nil
    )
  }

  override def convertMessage(descriptor: Descriptor, msg: PMessage): Any = {
    val fSeconds = descriptor.findFieldByName("seconds").get
    val fNanos = descriptor.findFieldByName("nanos").get
    val seconds = msg.value(fSeconds).asInstanceOf[PLong].value
    val nanos = msg.value(fNanos).asInstanceOf[PInt].value
    seconds * 1000000 + nanos / 1000
  }
}
