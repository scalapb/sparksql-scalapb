package com.trueaccord.scalapb.spark

import com.google.protobuf.ByteString
import com.google.protobuf.Descriptors.{EnumValueDescriptor, FieldDescriptor}
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType
import com.trueaccord.scalapb.{GeneratedMessage, GeneratedMessageCompanion, Message}
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}

object ProtoSQL {
  import scala.language.existentials

  def protoToDataFrame[T <: GeneratedMessage with Message[T] : GeneratedMessageCompanion](
    sparkSession: SparkSession, protoRdd: org.apache.spark.rdd.RDD[T]): DataFrame = {
    sparkSession.createDataFrame(protoRdd.map(messageToRow[T]), schemaFor[T])
  }

  def protoToDataFrame[T <: GeneratedMessage with Message[T] : GeneratedMessageCompanion](
    sqlContext: SQLContext, protoRdd: org.apache.spark.rdd.RDD[T]): DataFrame = {
    protoToDataFrame(sqlContext.sparkSession, protoRdd)
  }

  def schemaFor[T <: GeneratedMessage with Message[T]](implicit cmp: GeneratedMessageCompanion[T]) = {
    import org.apache.spark.sql.types._
    import collection.JavaConverters._
    StructType(cmp.descriptor.getFields.asScala.map(structFieldFor))
  }

  private def toRowData(fd: FieldDescriptor, obj: Any) = fd.getJavaType match {
    case JavaType.BYTE_STRING => obj.asInstanceOf[ByteString].toByteArray
    case JavaType.ENUM => obj.asInstanceOf[EnumValueDescriptor].getName
    case JavaType.MESSAGE => messageToRow(obj.asInstanceOf[T forSome { type T <: GeneratedMessage with Message[T] }])
    case _ => obj
  }

  def messageToRow[T <: GeneratedMessage with Message[T]](msg: T): Row = {
    import collection.JavaConversions._
    Row(
      msg.companion.descriptor.getFields.map {
        fd =>
          val obj = msg.getField(fd)
          if (obj != null) {
            if (fd.isRepeated) {
              obj.asInstanceOf[Vector[Any]].map(toRowData(fd, _))
            } else {
              toRowData(fd, obj)
            }
          } else null
      }: _*)
  }

  private def structFieldFor(fd: FieldDescriptor): StructField = {
    import com.google.protobuf.Descriptors.FieldDescriptor.JavaType._
    import org.apache.spark.sql.types._
    val dataType = fd.getJavaType match {
      case INT => IntegerType
      case LONG => LongType
      case FLOAT => FloatType
      case DOUBLE => DoubleType
      case BOOLEAN => BooleanType
      case STRING => StringType
      case BYTE_STRING => BinaryType
      case ENUM => StringType
      case MESSAGE =>
        import collection.JavaConverters._
        StructType(fd.getMessageType.getFields.asScala.map(structFieldFor))
    }
    StructField(
      fd.getName,
      if (fd.isRepeated) ArrayType(dataType, containsNull = false) else dataType,
      nullable = !fd.isRequired && !fd.isRepeated
    )
  }
}
