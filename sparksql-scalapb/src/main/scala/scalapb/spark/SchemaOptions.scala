package scalapb.spark

import org.apache.spark.sql.types._
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}
import scalapb.descriptors.{Descriptor, FieldDescriptor}
import frameless.TypedEncoder
import org.apache.spark.sql.catalyst.plans.logical.Generate
import scala.util.control

case class SchemaOptions(
    columnNaming: ColumnNaming,
    retainPrimitiveWrappers: Boolean,
    messageEncoders: Map[Descriptor, TypedEncoder[_]]
) {
  def withScalaNames = copy(columnNaming = ColumnNaming.ScalaNames)

  def withProtoNames = copy(columnNaming = ColumnNaming.ProtoNames)

  def withRetainedPrimitiveWrappers = copy(retainPrimitiveWrappers = true)

  def withMessageEncoders(messageEncoders: Map[Descriptor, TypedEncoder[_]]) =
    copy(messageEncoders = messageEncoders)

  def addMessageEncoder[T <: GeneratedMessage](
      typedEncoder: TypedEncoder[T]
  )(implicit cmp: GeneratedMessageCompanion[T]): SchemaOptions = {
    copy(messageEncoders = messageEncoders + (cmp.scalaDescriptor -> typedEncoder))
  }

  def withSparkTimestamps = addMessageEncoder(CustomTypedEncoders.timestampToSqlTimestamp)

  private[scalapb] def customDataTypeFor(descriptor: Descriptor): Option[DataType] =
    messageEncoders.get(descriptor) match {
      case Some(encoder) => Some(encoder.catalystRepr)
      case None =>
        if (retainPrimitiveWrappers) {
          None
        } else {
          SchemaOptions.PrimitiveWrapperTypes.get(descriptor)
        }
    }

  private[scalapb] def isUnpackedPrimitiveWrapper(message: Descriptor) =
    !retainPrimitiveWrappers && SchemaOptions.PrimitiveWrapperTypes.contains(message)
}

object SchemaOptions {
  val Default =
    SchemaOptions(ColumnNaming.ProtoNames, retainPrimitiveWrappers = false, messageEncoders = Map())

  def apply(): SchemaOptions = Default

  private def buildWrapper[T <: GeneratedMessage](implicit
      cmp: GeneratedMessageCompanion[T]
  ) = {
    cmp.scalaDescriptor -> ProtoSQL.dataTypeFor(
      cmp.scalaDescriptor.fields.find(_.name == "value").get
    )
  }

  private[scalapb] val PrimitiveWrapperTypes = Seq(
    com.google.protobuf.wrappers.DoubleValue.scalaDescriptor -> DoubleType,
    com.google.protobuf.wrappers.BoolValue.scalaDescriptor -> BooleanType,
    com.google.protobuf.wrappers.BytesValue.scalaDescriptor -> BinaryType,
    com.google.protobuf.wrappers.Int32Value.scalaDescriptor -> IntegerType,
    com.google.protobuf.wrappers.Int64Value.scalaDescriptor -> LongType,
    com.google.protobuf.wrappers.StringValue.scalaDescriptor -> StringType,
    com.google.protobuf.wrappers.FloatValue.scalaDescriptor -> FloatType,
    com.google.protobuf.wrappers.UInt32Value.scalaDescriptor -> IntegerType,
    com.google.protobuf.wrappers.UInt64Value.scalaDescriptor -> LongType
  ).toMap
}

abstract class ColumnNaming {
  def fieldName(fd: FieldDescriptor): String
}

object ColumnNaming {
  case object ProtoNames extends ColumnNaming {
    def fieldName(fd: FieldDescriptor): String = fd.name
  }

  case object ScalaNames extends ColumnNaming {
    override def fieldName(fd: FieldDescriptor): String = fd.scalaName
  }
}
