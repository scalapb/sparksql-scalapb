package scalapb.spark

import scalapb.descriptors.FieldDescriptor
import scalapb.GeneratedMessage
import scalapb.GeneratedMessageCompanion
import com.google.protobuf.Field
import org.apache.parquet.example.data.simple.Primitive
import org.apache.arrow.flatbuf.Schema
import scalapb.descriptors.Descriptor
import org.apache.spark.sql.types.{BinaryType, BooleanType, DataType, DoubleType, FloatType, IntegerType, LongType, StringType, TimestampType}

case class SchemaOptions(
    columnNaming: ColumnNaming,
    retainPrimitiveWrappers: Boolean,
    sparkTimestamps: Boolean,
) {
  def withScalaNames = copy(columnNaming = ColumnNaming.ScalaNames)

  def withProtoNames = copy(columnNaming = ColumnNaming.ProtoNames)

  def withRetainedPrimitiveWrappers = copy(retainPrimitiveWrappers = true)

  def withSparkTimestamps = copy(sparkTimestamps = true)

  private[scalapb] def customDataTypeFor(message: Descriptor): Option[DataType] = {
    if (sparkTimestamps && message.fullName == "google.protobuf.Timestamp") Some(TimestampType)
    else if (retainPrimitiveWrappers) None
    else SchemaOptions.PrimitiveWrapperTypes.get(message)
  }

  private[scalapb] def isUnpackedPrimitiveWrapper(message: Descriptor) =
    !retainPrimitiveWrappers && SchemaOptions.PrimitiveWrapperTypes.contains(message)
}

object SchemaOptions {
  val Default = SchemaOptions(ColumnNaming.ProtoNames, retainPrimitiveWrappers = false, sparkTimestamps = false)

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
