package scalapb.spark

import com.google.protobuf.timestamp.Timestamp
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.execution.ExternalRDD
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.unsafe.types.UTF8String
import scalapb.descriptors._
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}
import scala.reflect.ClassTag
import frameless.TypedEncoder

class ProtoSQL(val schemaOptions: SchemaOptions) extends Udfs {
  self =>
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
    schemaOptions
      .customDataTypeFor(descriptor)
      .getOrElse(StructType(descriptor.fields.map(structFieldFor)))

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
    if (fd.isMapField) fd.scalaType match {
      case ScalaType.Message(mapEntry) =>
        MapType(
          singularDataType(mapEntry.findFieldByNumber(1).get),
          singularDataType(mapEntry.findFieldByNumber(2).get)
        )
      case _ =>
        throw new RuntimeException(
          "Unexpected: field marked as map, but does not have an entry message associated"
        )
    }
    else if (fd.isRepeated) ArrayType(singularDataType(fd), containsNull = false)
    else singularDataType(fd)

  def structFieldFor(fd: FieldDescriptor): StructField = {
    StructField(
      schemaOptions.columnNaming.fieldName(fd),
      dataTypeFor(fd),
      nullable = !fd.isRequired && !fd.isRepeated
    )
  }

  def createDataFrame[T <: GeneratedMessage: GeneratedMessageCompanion: ClassTag](
      spark: SparkSession,
      data: Seq[T]
  ): DataFrame = {
    import implicits._
    spark.createDataset(data).toDF()
  }

  val implicits: Implicits = new Implicits {
    val typedEncoders = new TypedEncoders {
      val protoSql = self
    }
  }
}

object ProtoSQL extends ProtoSQL(SchemaOptions.Default) {
  @deprecated("Primitive wrappers are unpacked by default. Use ProtoSQL directly", "0.11.0")
  lazy val withPrimitiveWrappers: ProtoSQL = new ProtoSQL(SchemaOptions.Default)

  val withSparkTimestamps: ProtoSQL = new ProtoSQL(SchemaOptions.Default.withSparkTimestamps)

  val withRetainedPrimitiveWrappers: ProtoSQL = new ProtoSQL(
    SchemaOptions.Default.withRetainedPrimitiveWrappers
  )
}
