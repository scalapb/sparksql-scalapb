package scalapb.spark

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.types.{DataType, ObjectType, TimestampType}
import scalapb.GeneratedMessageCompanion
import scalapb.descriptors.{Descriptor, FieldDescriptor, PInt, PLong, PMessage, PValue}

import scala.collection.immutable

trait CatalystMapper {
  def convertedType(descriptor: Descriptor): Option[DataType]
  def fromCatalyst(
      cmp: GeneratedMessageCompanion[_],
      input: Expression,
      helpers: FromCatalystHelpers
  ): immutable.Seq[Expression]
  def toCatalyst(
      cmp: GeneratedMessageCompanion[_],
      input: Expression,
      helpers: ToCatalystHelpers
  ): Expression
  def convertMessage(descriptor: Descriptor, msg: PMessage): Any
}

object GoogleTimestampCatalystMapper extends CatalystMapper {
  override def convertedType(descriptor: Descriptor): Option[DataType] = {
    if (descriptor.fullName == "google.protobuf.Timestamp") {
      Some(TimestampType)
    } else {
      None
    }
  }

  override def fromCatalyst(
      cmp: GeneratedMessageCompanion[_],
      input: Expression,
      helpers: FromCatalystHelpers
  ): immutable.Seq[Expression] = {
    immutable.Seq(
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
  }

  override def toCatalyst(
      cmp: GeneratedMessageCompanion[_],
      input: Expression,
      helpers: ToCatalystHelpers
  ): Expression = {
    val secondsFd: FieldDescriptor = cmp.scalaDescriptor.fields(0)
    val nanosFd: FieldDescriptor = cmp.scalaDescriptor.fields(1)
    val secondsExpr = helpers.fieldToCatalyst(cmp, secondsFd, input)
    val nanosExpr = helpers.fieldToCatalyst(cmp, nanosFd, input)
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
