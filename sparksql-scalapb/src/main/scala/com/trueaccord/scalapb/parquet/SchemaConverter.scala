package com.trueaccord.scalapb.parquet

import com.google.protobuf.Descriptors.{ Descriptor, FieldDescriptor }
import org.apache.parquet.schema.Types.{ Builder, GroupBuilder, MessageTypeBuilder, PrimitiveBuilder }
import org.apache.parquet.schema.{ MessageType, Type, Types }

import scala.collection.JavaConverters._

object SchemaConverter {
  def convert(descriptor: Descriptor): MessageType = {
    val builder: MessageTypeBuilder = Types.buildMessage()
    addAllFields(descriptor, builder).named(descriptor.getFullName)
  }

  private def addAllFields[T](descriptor: Descriptor, builder: GroupBuilder[T]): GroupBuilder[T] = {
    descriptor.getFields.asScala.foreach {
      fd =>
        addSingleField(fd, builder).id(fd.getNumber).named(fd.getName)
    }
    builder
  }

  private def addSingleField[T](fd: FieldDescriptor, builder: GroupBuilder[T]) = {
    import FieldDescriptor.JavaType
    import org.apache.parquet.schema.OriginalType
    import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
    val repetition = if (fd.isRequired) Type.Repetition.REQUIRED
    else if (fd.isRepeated) Type.Repetition.REPEATED
    else Type.Repetition.OPTIONAL

    fd.getJavaType match {
      case JavaType.BOOLEAN => builder.primitive(PrimitiveTypeName.BOOLEAN, repetition)
      case JavaType.INT => builder.primitive(PrimitiveTypeName.INT32, repetition)
      case JavaType.LONG => builder.primitive(PrimitiveTypeName.INT64, repetition)
      case JavaType.FLOAT => builder.primitive(PrimitiveTypeName.FLOAT, repetition)
      case JavaType.DOUBLE => builder.primitive(PrimitiveTypeName.DOUBLE, repetition)
      case JavaType.BYTE_STRING => builder.primitive(PrimitiveTypeName.BINARY, repetition)
      case JavaType.STRING => builder.primitive(PrimitiveTypeName.BINARY, repetition).as(OriginalType.UTF8)
      case JavaType.MESSAGE =>
        val subgroup = builder.group(repetition)
        addAllFields(fd.getMessageType, subgroup)
      case JavaType.ENUM => builder.primitive(PrimitiveTypeName.BINARY, repetition).as(OriginalType.UTF8)
      case javaType =>
        throw new UnsupportedOperationException("Cannot convert Protocol Buffer: unknown type " + javaType)
    }
  }
}
