package scalapb.parquet

import scalapb.{GeneratedMessage, Message}
import org.apache.parquet.hadoop.ParquetInputFormat

class ScalaPBInputFormat[T <: GeneratedMessage with Message[T]] extends ParquetInputFormat[T](classOf[ScalaPBReadSupport[T]]) {
}
