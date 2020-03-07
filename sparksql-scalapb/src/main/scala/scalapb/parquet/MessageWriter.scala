package scalapb.parquet

import com.google.protobuf.ByteString
import scalapb.{GeneratedMessage, Message}
import org.apache.parquet.io.api.Binary
import org.apache.parquet.Log
import org.apache.parquet.io.api.RecordConsumer
import scalapb.descriptors.PRepeated
import scalapb.descriptors.FieldDescriptor
import scalapb.descriptors.PValue
import scalapb.descriptors.PBoolean
import scalapb.descriptors.PByteString
import scalapb.descriptors.PDouble
import scalapb.descriptors.PEmpty
import scalapb.descriptors.PEnum
import scalapb.descriptors.PFloat
import scalapb.descriptors.PInt
import scalapb.descriptors.PLong
import scalapb.descriptors.PMessage
import scalapb.descriptors.PString

object MessageWriter {
  val log = Log.getLog(this.getClass)

  def writeTopLevelMessage[T <: GeneratedMessage with Message[T]](
      consumer: RecordConsumer,
      m: T
  ) = {
    consumer.startMessage()
    writeAllFields(consumer, m)
    consumer.endMessage()
  }

  private def writeAllFields[T <: GeneratedMessage](
      consumer: RecordConsumer,
      m: T
  ): Unit = {
    writeSingleField(consumer, m.toPMessage)
  }

  private def writeSingleField(
      consumer: RecordConsumer,
      v: PValue
  ): Unit = v match {
    case PBoolean(value) => consumer.addBoolean(value)
    case PByteString(value) => consumer.addBinary(Binary.fromByteArray(value.toByteArray()))
    case PDouble(value) => consumer.addDouble(value)
    case PEmpty => throw new RuntimeException("Should not happen")
    case PEnum(value) => consumer.addBinary(Binary.fromString(value.name))
    case PFloat(value) => consumer.addFloat(value)
    case PDouble(value) => consumer.addDouble(value)
    case PInt(value) => consumer.addInteger(value)
    case PLong(value) => consumer.addLong(value)
    case PMessage(value) =>
      consumer.startGroup()
      value.foreach {
        case (fd, PEmpty) => // skip
        case (fd, v) =>
          consumer.startField(fd.name, fd.index)
          writeSingleField(consumer, v)
          consumer.endField(fd.name, fd.index)
      }
    case PRepeated(value) => value.foreach(writeSingleField(consumer, _))
    case PString(value) => consumer.addBinary(Binary.fromString(value))
  }
}
