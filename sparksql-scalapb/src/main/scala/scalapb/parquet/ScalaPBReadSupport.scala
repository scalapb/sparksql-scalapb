package scalapb.parquet

import java.util

import scalapb.{GeneratedMessage, GeneratedMessageCompanion, Message}
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.Log
import org.apache.parquet.hadoop.api.{InitContext, ReadSupport}
import org.apache.parquet.hadoop.api.ReadSupport.ReadContext
import org.apache.parquet.io.api.{GroupConverter, RecordMaterializer}
import org.apache.parquet.schema.MessageType

class ScalaPBReadSupport[T <: GeneratedMessage with Message[T]] extends ReadSupport[T] {
  val log = Log.getLog(this.getClass)

  override def prepareForRead(
    configuration: Configuration,
    keyValueMetaData: util.Map[String, String],
    fileSchema: MessageType,
    readContext: ReadContext): RecordMaterializer[T] = {

    val headerScalaPBClass = Option(keyValueMetaData.get(ScalaPBReadSupport.PB_CLASS))
    val configuredScalaPBClass = Option(configuration.get(ScalaPBReadSupport.PB_CLASS))

    val protoClass = headerScalaPBClass.orElse(configuredScalaPBClass)
      .getOrElse(throw new RuntimeException(s"Value for ${ScalaPBReadSupport.PB_CLASS} not found."))
    log.debug("Reading data with ScalaPB class " + protoClass)

    val cmp = {
      import scala.reflect.runtime.universe

      val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)

      val module = runtimeMirror.staticModule(protoClass)

      val obj = runtimeMirror.reflectModule(module)

      obj.instance.asInstanceOf[GeneratedMessageCompanion[T]]
    }

    new RecordMaterializer[T] {
      val root = new ProtoMessageConverter[T](cmp, readContext.getRequestedSchema, onEnd = _ => ())

      override def getRootConverter: GroupConverter = root

      override def getCurrentRecord: T = root.getCurrentRecord
    }
  }

  override def init(context: InitContext): ReadContext = {
    val schema = Option(context.getConfiguration.get(ScalaPBReadSupport.PB_REQUESTED_PROJECTION))
      .filterNot(_.trim.isEmpty) match {
      case Some(requestedProjectionString) =>
        log.info("Reading data with projection " + requestedProjectionString)
        ReadSupport.getSchemaForRead(context.getFileSchema, requestedProjectionString)
      case None =>
        log.info("Reading data with scheme " + context.getFileSchema)
        context.getFileSchema
    }

    new ReadContext(schema)
  }
}

object ScalaPBReadSupport {
  val PB_CLASS = "parquet.scalapb.class"
  val PB_REQUESTED_PROJECTION = "parquet.scalapb.projection"

  def setRequestedProjection(configuration: Configuration, requestedProjection: String): Unit = {
    configuration.set(PB_REQUESTED_PROJECTION, requestedProjection)
  }

  /**
    * Set name of ScalaPB class to be used for reading data.
    * If no class is set, value from file header is used.
    * Note that the value in header is present only if the file was written
    * using [[scalapb.parquet.ScalaPBWriteSupport ScalaPBWriteSupport]] project, it will fail otherwise.
    **/
  def setScalaPBClass(configuration: Configuration, protobufClass: String): Unit = {
    configuration.set(PB_CLASS, protobufClass)
  }
}
