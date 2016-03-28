package com.trueaccord.scalapb.spark

import com.google.protobuf.ByteString
import com.google.protobuf.Descriptors.{EnumValueDescriptor, FieldDescriptor}
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType
import com.trueaccord.scalapb.{GeneratedMessage, GeneratedMessageCompanion, Message}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}

object ProtoSQL {
  import scala.language.existentials

  def rddToDataFrame[T <: GeneratedMessage with Message[T]](sqlContext: SQLContext, protoRdd: org.apache.spark.rdd.RDD[T])(
    implicit cmp: GeneratedMessageCompanion[T]) = {
    sqlContext.createDataFrame(protoRdd.map(messageToRow[T]), schemaFor[T])
  }

  def schemaFor[T <: GeneratedMessage with Message[T]](implicit cmp: GeneratedMessageCompanion[T]) = {
    import org.apache.spark.sql.types._
    import collection.JavaConversions._
    StructType(cmp.descriptor.getFields.map(structFieldFor))
  }

  private def toRowData(fd: FieldDescriptor, obj: Any) = fd.getJavaType match {
    case JavaType.BYTE_STRING => obj.asInstanceOf[ByteString].toByteArray
    case JavaType.ENUM => obj.asInstanceOf[EnumValueDescriptor].getName
    case JavaType.MESSAGE => messageToRow(obj.asInstanceOf[T forSome { type T <: GeneratedMessage with Message[T] }])
    case _ => obj
  }

  def messageToRow[T <: GeneratedMessage with Message[T]](msg: T): Row = {
    val allFields: Map[FieldDescriptor, Any] = msg.getAllFields
    import collection.JavaConversions._
    Row(
      msg.companion.descriptor.getFields.map {
        fd =>
          if (allFields.containsKey(fd)) {
            val obj = allFields(fd)
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
        import collection.JavaConversions._
        StructType(fd.getMessageType.getFields.map(structFieldFor))
    }
    StructField(
      fd.getName,
      if (fd.isRepeated) ArrayType(dataType, containsNull = false) else dataType,
      nullable = !fd.isRequired && !fd.isRepeated
    )
  }
}
