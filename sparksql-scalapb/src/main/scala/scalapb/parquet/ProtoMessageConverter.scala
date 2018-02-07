package scalapb.parquet

import com.google.protobuf.ByteString
import com.google.protobuf.Descriptors.FieldDescriptor
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType
import org.apache.parquet.io.api.{Binary, Converter, GroupConverter, PrimitiveConverter}
import org.apache.parquet.schema.{OriginalType, Type}

import scala.collection.JavaConverters._
import scala.language.existentials
import scalapb.{GeneratedMessage, GeneratedMessageCompanion, Message}

class ProtoMessageConverter[T <: GeneratedMessage with Message[T]](cmp: GeneratedMessageCompanion[T], schemat: Type, onEnd: T => Unit) extends GroupConverter {
  val fields: scala.collection.mutable.Map[FieldDescriptor, Any] = scala.collection.mutable.Map[FieldDescriptor, Any]()

  private val converters =
    schemat.asGroupType().getFields.asScala
      .map {
        t =>
          val fd = cmp.javaDescriptor.findFieldByName(t.getName)
          val e: (Any) => Unit = if (fd.isRepeated) addValue(fd) else setValue(fd)
          t.getOriginalType match {
            case OriginalType.LIST =>
              new ListConverter[T](fd, e)
            case _ => fd.getJavaType match {
              case JavaType.MESSAGE =>
                new ProtoMessageConverter(cmp.messageCompanionForField(fd).asInstanceOf[GeneratedMessageCompanion[X] forSome {type X <: GeneratedMessage with Message[X]}], t.asGroupType(), e)
              case _ =>
                new ProtoPrimitiveConverter(fd, e)
            }
          }
      }

  def getCurrentRecord: T = cmp.fromFieldsMap(fields.toMap)

  private def setValue[P](fd: FieldDescriptor)(v: P): Unit = { fields(fd) = v }

  private def addValue[P](fd: FieldDescriptor)(v: P): Unit = { fields(fd) = fields.getOrElse(fd, Seq.empty).asInstanceOf[Seq[P]] :+ v }

  override def getConverter(fieldIndex: Int): Converter = {
    converters(fieldIndex)
  }

  override def end(): Unit = onEnd(getCurrentRecord)

  override def start(): Unit = {
    fields.clear()
  }
}

class ListConverter[T <: GeneratedMessage with Message[T]](fd: FieldDescriptor, add: Any => Unit) extends GroupConverter {

  private val converter = {
    new ElementConverter(fd, add)
  }

  override def start(): Unit = {}

  override def end(): Unit = {}

  override def getConverter(fieldIndex: Int): Converter = converter
}

class ElementConverter(fd: FieldDescriptor, add: Any => Unit) extends GroupConverter {
  private val converter = {
    new ProtoPrimitiveConverter(fd, add)
  }

  override def start(): Unit = {}

  override def end(): Unit = {}

  override def getConverter(fieldIndex: Int): Converter = converter
}

class ProtoPrimitiveConverter(fd: FieldDescriptor, add: Any => Unit) extends PrimitiveConverter {
  override def addFloat(value: Float): Unit = add(value)

  override def addBinary(value: Binary): Unit =
    fd.getJavaType match {
      case JavaType.STRING => add(value.toStringUsingUTF8)
      case JavaType.ENUM => add(fd.getEnumType.findValueByName(value.toStringUsingUTF8))
      case JavaType.BYTE_STRING => add(ByteString.copyFrom(value.getBytes))
      case _ => throw new RuntimeException("Unexpected type")
    }

  override def addDouble(value: Double): Unit = add(value)

  override def addInt(value: Int): Unit = add(value)

  override def addBoolean(value: Boolean): Unit = add(value)

  override def addLong(value: Long): Unit = add(value)
}
