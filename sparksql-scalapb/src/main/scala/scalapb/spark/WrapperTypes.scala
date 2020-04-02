package scalapb.spark

import org.apache.spark.sql.types.{
  BinaryType,
  BooleanType,
  DataType,
  DoubleType,
  FloatType,
  IntegerType,
  LongType,
  StringType,
  StructField,
  StructType
}
import scalapb.GeneratedMessageCompanion
import scalapb.Message
import scalapb.GeneratedMessage
import scalapb.descriptors.Descriptor

trait WrapperTypes {
  private[scalapb] val types: Map[Descriptor, DataType]
}

trait AllWrapperTypes extends WrapperTypes {
  self: ProtoSQL =>

  private def buildWrapper[T <: GeneratedMessage](
      implicit cmp: GeneratedMessageCompanion[T]
  ) = {
    cmp.scalaDescriptor -> dataTypeFor(
      cmp.scalaDescriptor.fields.find(_.name == "value").get
    )
  }

  private[scalapb] val types = Seq(
    buildWrapper[com.google.protobuf.wrappers.DoubleValue],
    buildWrapper[com.google.protobuf.wrappers.BoolValue],
    buildWrapper[com.google.protobuf.wrappers.BytesValue],
    buildWrapper[com.google.protobuf.wrappers.Int32Value],
    buildWrapper[com.google.protobuf.wrappers.Int64Value],
    buildWrapper[com.google.protobuf.wrappers.StringValue],
    buildWrapper[com.google.protobuf.wrappers.FloatValue],
    buildWrapper[com.google.protobuf.wrappers.UInt32Value],
    buildWrapper[com.google.protobuf.wrappers.UInt64Value]
  ).toMap
}

trait NoWrapperTypes extends WrapperTypes {
  private[scalapb] val types: Map[Descriptor, DataType] = Map.empty
}
