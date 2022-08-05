package scalapb.spark

import java.sql.{Timestamp => SQLTimestamp}
import com.google.protobuf.timestamp.{Timestamp => GoogleTimestamp}
import scalapb.TypeMapper

import java.time.Instant

object TypeMappers {

  implicit val googleTsToSqlTsMapper: TypeMapper[GoogleTimestamp, SQLTimestamp] =
    TypeMapper({ googleTs: GoogleTimestamp =>
      SQLTimestamp.from(Instant.ofEpochSecond(googleTs.seconds, googleTs.nanos))
    })({ sqlTs: SQLTimestamp =>
      val instant = sqlTs.toInstant
      new GoogleTimestamp(instant.getEpochSecond, instant.getNano)
    })

}
