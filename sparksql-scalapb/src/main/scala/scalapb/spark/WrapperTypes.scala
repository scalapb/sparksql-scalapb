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

trait WrapperTypes {}

trait AllWrapperTypes extends WrapperTypes {
  self: ProtoSQL =>
}

trait NoWrapperTypes extends WrapperTypes {}
