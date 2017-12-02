package scalapb.parquet

import com.google.protobuf.ByteString
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType
import com.google.protobuf.Descriptors.{EnumValueDescriptor, FieldDescriptor}
import scalapb.{GeneratedMessage, Message}
import org.apache.parquet.io.api.Binary
import org.apache.parquet.Log
import org.apache.parquet.io.api.RecordConsumer

object MessageWriter {
  val log = Log.getLog(this.getClass)

  def writeTopLevelMessage[T <: GeneratedMessage with Message[T]](consumer: RecordConsumer, m: T) = {
    consumer.startMessage()
    writeAllFields(consumer, m)
    consumer.endMessage()
  }

  private def writeAllFields[T <: GeneratedMessage](consumer: RecordConsumer, m: T): Unit = {
    m.getAllFields.foreach {
      case (fd, value) =>
        consumer.startField(fd.getName, fd.getIndex)
        if (fd.isRepeated) {
          value.asInstanceOf[Seq[Any]].foreach {
            v =>
              writeSingleField(consumer, fd, v)
          }
        } else {
          writeSingleField(consumer, fd, value)
        }
        consumer.endField(fd.getName, fd.getIndex)
    }
  }

  private def writeSingleField(consumer: RecordConsumer, fd: FieldDescriptor, v: Any) = fd.getJavaType match {
    case JavaType.BOOLEAN => consumer.addBoolean(v.asInstanceOf[Boolean])
    case JavaType.INT => consumer.addInteger(v.asInstanceOf[Int])
    case JavaType.LONG => consumer.addLong(v.asInstanceOf[Long])
    case JavaType.FLOAT => consumer.addFloat(v.asInstanceOf[Float])
    case JavaType.DOUBLE => consumer.addDouble(v.asInstanceOf[Double])
    case JavaType.BYTE_STRING => consumer.addBinary(Binary.fromByteArray(v.asInstanceOf[ByteString].toByteArray))
    case JavaType.STRING => consumer.addBinary(Binary.fromString(v.asInstanceOf[String]))
    case JavaType.MESSAGE =>
      consumer.startGroup()
      writeAllFields(consumer, v.asInstanceOf[GeneratedMessage])
      consumer.endGroup()
    case JavaType.ENUM => consumer.addBinary(Binary.fromString(v.asInstanceOf[EnumValueDescriptor].getName))
    case javaType =>
      throw new UnsupportedOperationException("Cannot convert Protocol Buffer: unknown type " + javaType)
  }
}
