package scalapb.spark

import com.google.protobuf.timestamp.Timestamp
import frameless.{SQLDate, SQLTimestamp}
import scalapb.descriptors._

import frameless.Injection
import frameless.TypedEncoder
import scala.reflect.ClassTag

object CustomTypedEncoders {

  val timestampToSqlTimestamp: TypedEncoder[Timestamp] =
    fromInjection(
      Injection[Timestamp, SQLTimestamp](
        { ts: Timestamp => SQLTimestamp(TimestampHelpers.toMicros(ts)) },
        { timestamp: SQLTimestamp => TimestampHelpers.fromMicros(timestamp.us) }
      )
    )

  def fromInjection[A: ClassTag, B: TypedEncoder](injection: Injection[A, B]): TypedEncoder[A] = {
    implicit val inj = injection
    TypedEncoder.usingInjection[A, B]
  }
}
