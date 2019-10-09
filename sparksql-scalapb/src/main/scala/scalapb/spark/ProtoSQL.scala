package scalapb.spark

import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.types.{
  ArrayType,
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
import scalapb.{GeneratedMessage, GeneratedMessageCompanion, Message}
import scalapb.descriptors.{
  FieldDescriptor,
  PBoolean,
  PByteString,
  PDouble,
  PEmpty,
  PEnum,
  PFloat,
  PInt,
  PLong,
  PMessage,
  PRepeated,
  PString,
  PValue,
  ScalaType
}

object ProtoSQL {
  import scala.language.existentials

  def protoToDataFrame[T <: GeneratedMessage with Message[T]: GeneratedMessageCompanion](
      sparkSession: SparkSession,
      protoRdd: org.apache.spark.rdd.RDD[T]
  ): DataFrame = {
    sparkSession.createDataFrame(protoRdd.map(messageToRow[T]), schemaFor[T])
  }

  def protoToDataFrame[T <: GeneratedMessage with Message[T]: GeneratedMessageCompanion](
      sqlContext: SQLContext,
      protoRdd: org.apache.spark.rdd.RDD[T]
  ): DataFrame = {
    protoToDataFrame(sqlContext.sparkSession, protoRdd)
  }

  def schemaFor[T <: GeneratedMessage with Message[T]](
      implicit cmp: GeneratedMessageCompanion[T]
  ): StructType = {
    StructType(cmp.scalaDescriptor.fields.map(structFieldFor))
  }

  private def toRowData(pvalue: PValue): Any = pvalue match {
    case PString(value)     => value
    case PInt(value)        => value
    case PLong(value)       => value
    case PDouble(value)     => value
    case PFloat(value)      => value
    case PBoolean(value)    => value
    case PByteString(value) => value.toByteArray
    case value: PMessage    => pMessageToRow(value)
    case PRepeated(value)   => value.map(toRowData)
    case PEnum(descriptor)  => descriptor.name
    case PEmpty             => null
  }

  def messageToRow[T <: GeneratedMessage with Message[T]](msg: T): Row = {
    pMessageToRow(msg.toPMessage)
  }

  def pMessageToRow(msg: PMessage): Row = {
    Row(
      msg.value.toVector
        .sortBy(_._1.index)
        .map(entry => toRowData(entry._2)): _*
    )
  }

  def dataTypeFor(fd: FieldDescriptor): DataType = fd.scalaType match {
    case ScalaType.Int         => IntegerType
    case ScalaType.Long        => LongType
    case ScalaType.Float       => FloatType
    case ScalaType.Double      => DoubleType
    case ScalaType.Boolean     => BooleanType
    case ScalaType.String      => StringType
    case ScalaType.ByteString  => BinaryType
    case ScalaType.Message(md) => StructType(md.fields.map(structFieldFor))
    case _: ScalaType.Enum     => StringType
  }

  def structFieldFor(fd: FieldDescriptor): StructField = {
    val dataType = dataTypeFor(fd)
    StructField(
      fd.name,
      if (fd.isRepeated) ArrayType(dataType, containsNull = false)
      else dataType,
      nullable = !fd.isRequired && !fd.isRepeated
    )
  }
}
