package scalapb.spark

import scalapb.descriptors.{PInt, PLong, PValue}
import com.google.protobuf.timestamp.Timestamp

// TODO(nadav): Consider adding functions like this to ScalaPB core.  Note that protobuf-java-util
// has Timestamps class with versions of these that does normalization and more.
private[scalapb] object TimestampHelpers {
  def fromMicros(micros: Long): Timestamp = Timestamp(
    micros / 1000000,
    ((micros % 1000000) * 1000).toInt
  )

  def toMicros(ts: Timestamp) = ts.seconds * 1000000 + ts.nanos / 1000
}
