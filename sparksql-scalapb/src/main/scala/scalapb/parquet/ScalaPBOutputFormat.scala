package scalapb.parquet

import scalapb.{GeneratedMessage, Message}
import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.hadoop.ParquetOutputFormat
import org.apache.parquet.hadoop.util.ContextUtil

class ScalaPBOutputFormat[T <: GeneratedMessage with Message[T]]
    extends ParquetOutputFormat[T](new ScalaPBWriteSupport[T]) {}

object ScalaPBOutputFormat {
  def setMessageClass[T <: GeneratedMessage with Message[T]](
      job: Job,
      protoClass: Class[T]
  ) = {
    ScalaPBWriteSupport.setSchema(ContextUtil.getConfiguration(job), protoClass)
  }
}
