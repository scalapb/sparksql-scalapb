package com.trueaccord.scalapb.parquet

import java.util

import com.google.protobuf.Descriptors.Descriptor
import com.trueaccord.scalapb.{GeneratedMessage, Message}
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.BadConfigurationException
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.hadoop.api.WriteSupport.WriteContext
import org.apache.parquet.io.api.RecordConsumer
import org.apache.parquet.schema.MessageType

class ScalaPBWriteSupport[T <: GeneratedMessage with Message[T]] extends WriteSupport[T] {
  var pbClass: Class[T] = null
  var recordConsumer: RecordConsumer = null

  override def init(configuration: Configuration): WriteContext = {
    if (pbClass == null) {
      pbClass = configuration.getClass(ScalaPBWriteSupport.SCALAPB_CLASS_WRITE, null, classOf[GeneratedMessage]).asInstanceOf[Class[T]]
      if (pbClass == null) {
        throw new BadConfigurationException("ScalaPB class not specified. Please use ScalaPBOutputFormat.setMessageClass.")
      }
    }
    val descriptor: Descriptor = pbClass.getMethod("descriptor").invoke(null).asInstanceOf[Descriptor]
    val rootSchema: MessageType = SchemaConverter.convert(descriptor)
    val extraMetaDtata = new util.HashMap[String, String]
    extraMetaDtata.put(ScalaPBReadSupport.PB_CLASS, pbClass.getName)
    new WriteContext(rootSchema, extraMetaDtata)
  }

  override def write(record: T): Unit = {
    MessageWriter.writeTopLevelMessage(recordConsumer, record)
  }

  override def prepareForWrite(recordConsumer: RecordConsumer): Unit = {
    this.recordConsumer = recordConsumer
  }
}

object ScalaPBWriteSupport {
  val SCALAPB_CLASS_WRITE = "parquet.scalapb.writeClass"

  def setSchema[T <: GeneratedMessage](config: Configuration, protoClass: Class[T]) = {
    config.setClass(SCALAPB_CLASS_WRITE, protoClass, classOf[GeneratedMessage])
  }
}
