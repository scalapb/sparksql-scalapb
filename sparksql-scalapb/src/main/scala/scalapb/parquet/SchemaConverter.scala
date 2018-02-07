package scalapb.parquet

import com.google.protobuf.Descriptors.{Descriptor, FieldDescriptor}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema.Types.{GroupBuilder, MessageTypeBuilder}
import org.apache.parquet.schema.{MessageType, OriginalType, Type, Types}

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
      case JavaType.BOOLEAN => addField(builder, repetition, PrimitiveTypeName.BOOLEAN)
      case JavaType.INT => addField(builder, repetition, PrimitiveTypeName.INT32)
      case JavaType.LONG => addField(builder, repetition, PrimitiveTypeName.INT64)
      case JavaType.FLOAT => addField(builder, repetition, PrimitiveTypeName.FLOAT)
      case JavaType.DOUBLE => addField(builder, repetition, PrimitiveTypeName.DOUBLE)
      case JavaType.BYTE_STRING => addField(builder, repetition, PrimitiveTypeName.BINARY)
      case JavaType.STRING => addField(builder, repetition,PrimitiveTypeName.BINARY , OriginalType.UTF8)
      case JavaType.MESSAGE =>
        val subgroup = builder.group(repetition)
        addAllFields(fd.getMessageType, subgroup)
      case JavaType.ENUM => builder.primitive(PrimitiveTypeName.BINARY, repetition).as(OriginalType.UTF8)
      case javaType =>
        throw new UnsupportedOperationException("Cannot convert Protocol Buffer: unknown type " + javaType)
    }
  }

  private def addField[T](builder: GroupBuilder[T],
                              repetition: Repetition,
                              primitiveType: PrimitiveTypeName,
                              originalType: OriginalType = null) = {
    import org.apache.parquet.schema.OriginalType
    (Option(originalType), repetition) match {
      case (None, Type.Repetition.REPEATED) =>
        builder
          .group(Type.Repetition.REQUIRED).as(OriginalType.LIST)
          .group(Type.Repetition.REPEATED)
          .primitive(primitiveType,Type.Repetition.REQUIRED)
          .named("element")
          .named("list")
      case (Some(oType), Type.Repetition.REPEATED) =>
        builder
          .group(Type.Repetition.REQUIRED).as(OriginalType.LIST)
          .group(Type.Repetition.REPEATED)
          .primitive(primitiveType,Type.Repetition.REQUIRED).as(oType)
          .named("element")
          .named("list")
      case (None, _) =>
        builder.primitive(primitiveType, repetition)
      case (Some(oType), _) =>
        builder.primitive(primitiveType, repetition).as(oType)
    }
  }

}
