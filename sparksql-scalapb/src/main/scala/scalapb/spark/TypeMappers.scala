package scalapb.spark

import java.sql.{Timestamp => SQLTimestamp}
import com.google.protobuf.timestamp.{Timestamp => GoogleTimestamp}
import scalapb.TypeMapper

object TypeMappers {

//  implicit val msToTimestampMapper: TypeMapper[Long, SQLTimestamp] = TypeMapper({
//    ms: Long => new SQLTimestamp(ms)
//  })(_.getTime)

  implicit val googleTsToSqlTsMapper: TypeMapper[GoogleTimestamp, SQLTimestamp] = TypeMapper({
    googleTs: GoogleTimestamp =>
      val sqlTs = new SQLTimestamp(googleTs.seconds * 1000000)
      sqlTs.setNanos(googleTs.nanos)
      sqlTs
  })({
    sqlTs: SQLTimestamp =>
      new GoogleTimestamp(sqlTs.getTime / 1000000, sqlTs.getNanos)
  })

}
