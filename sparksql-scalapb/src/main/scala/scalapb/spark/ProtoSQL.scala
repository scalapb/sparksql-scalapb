package scalapb.spark

import org.apache.spark.sql.catalyst.{InternalRow, ScalaReflection}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{
  AttributeReference,
  GenericInternalRow
}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.execution.ExternalRDD
import org.apache.spark.sql.{
  DataFrame,
  Dataset,
  Encoder,
  FramelessInternals,
  Row,
  SQLContext,
  SparkSession
}
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
import org.apache.spark.unsafe.types.UTF8String
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
import scalapb.descriptors.Descriptor
import com.google.protobuf.wrappers.Int32Value

trait ProtoSQL {
  self: WrapperTypes =>
  import scala.language.existentials

  def protoToDataFrame[T <: GeneratedMessage: Encoder](
      sparkSession: SparkSession,
      protoRdd: org.apache.spark.rdd.RDD[T]
  ): DataFrame = {
    val logicalPlan: LogicalPlan = ExternalRDD(protoRdd, sparkSession)
    FramelessInternals.ofRows(sparkSession, logicalPlan)
  }

  def protoToDataFrame[T <: GeneratedMessage: Encoder](
      sqlContext: SQLContext,
      protoRdd: org.apache.spark.rdd.RDD[T]
  ): DataFrame = {
    protoToDataFrame(sqlContext.sparkSession, protoRdd)
  }

  def schemaFor[T <: GeneratedMessage](implicit
      cmp: GeneratedMessageCompanion[T]
  ): DataType = schemaFor(cmp.scalaDescriptor)

  def schemaFor(descriptor: Descriptor): DataType =
    types.get(descriptor) match {
      case Some(dt) => dt
      case _        => StructType(descriptor.fields.map(structFieldFor))
    }

  def toRowData(fd: FieldDescriptor, pvalue: PValue): Any =
    pvalue match {
      case PString(value)     => UTF8String.fromString(value)
      case PInt(value)        => value
      case PLong(value)       => value
      case PDouble(value)     => value
      case PFloat(value)      => value
      case PBoolean(value)    => value
      case PByteString(value) => value.toByteArray
      case value: PMessage =>
        pMessageToRowOrAny(
          fd.scalaType.asInstanceOf[ScalaType.Message].descriptor,
          value
        )
      case PRepeated(value) =>
        new GenericArrayData(value.map(toRowData(fd, _)))
      case penum: PEnum => JavaHelpers.penumToString(penum)
      case PEmpty       => null
    }

  def messageToRow[T <: GeneratedMessage](
      msg: T
  )(implicit cmp: GeneratedMessageCompanion[T]): InternalRow = {
    pMessageToRow(cmp.scalaDescriptor, msg.toPMessage)
  }

  def pMessageToRowOrAny(descriptor: Descriptor, msg: PMessage): Any =
    if (types.contains(descriptor))
      (for {
        fd <- descriptor.findFieldByName("value")
        value <- msg.value.get(fd)
      } yield toRowData(fd, value)).getOrElse(
        throw new RuntimeException(
          "Could not get value out of primitive wrapper"
        )
      )
    else pMessageToRow(descriptor, msg)

  def pMessageToRow(descriptor: Descriptor, msg: PMessage): InternalRow =
    descriptor.fullName match {
      case _ =>
        InternalRow(
          msg.value.toVector
            .sortBy(_._1.index)
            .map(entry => toRowData(entry._1, entry._2)): _*
        )
    }

  def singularDataType(fd: FieldDescriptor): DataType =
    fd.scalaType match {
      case ScalaType.Int         => IntegerType
      case ScalaType.Long        => LongType
      case ScalaType.Float       => FloatType
      case ScalaType.Double      => DoubleType
      case ScalaType.Boolean     => BooleanType
      case ScalaType.String      => StringType
      case ScalaType.ByteString  => BinaryType
      case ScalaType.Message(md) => schemaFor(md)
      case _: ScalaType.Enum     => StringType
    }

  def dataTypeFor(fd: FieldDescriptor): DataType =
    if (fd.isRepeated) ArrayType(singularDataType(fd), containsNull = false)
    else singularDataType(fd)

  def structFieldFor(fd: FieldDescriptor): StructField = {
    StructField(
      fd.name,
      dataTypeFor(fd),
      nullable = !fd.isRequired && !fd.isRepeated
    )
  }

  def createDataFrame[T <: GeneratedMessage: GeneratedMessageCompanion](
      spark: SparkSession,
      data: Seq[T]
  ): DataFrame = {
    val schema = schemaFor[T]
    schema match {
      case schema: StructType =>
        val attributeSeq = schema.map(f =>
          AttributeReference(f.name, f.dataType, f.nullable, f.metadata)()
        )
        val logicalPlan = LocalRelation(attributeSeq, data.map(messageToRow[T]))
        new Dataset[Row](spark, logicalPlan, RowEncoder(schema))
      case _ => ???
    }
  }

  val implicits: Implicits = new Implicits {
    val typedEncoders = new TypedEncoders {
      val protoSql = self
    }
  }
}

object ProtoSQL extends ProtoSQL with Udfs with NoWrapperTypes {
  val withPrimitiveWrappers = new ProtoSQL with Udfs with AllWrapperTypes
}
