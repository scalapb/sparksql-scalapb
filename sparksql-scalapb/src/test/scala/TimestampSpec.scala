package scalapb.spark

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import scalapb.spark.test3.customizations.{
  BothTimestampTypes,
  SQLTimestampFromGoogleTimestamp,
  StructFromGoogleTimestamp
}

import java.sql.{Timestamp => SQLTimestamp}
import com.google.protobuf.timestamp.{Timestamp => GoogleTimestamp}

import java.time.Instant

case class TestTimestampsHolder(
    justLong: Long,
    ts: SQLTimestamp,
    bothTimestampTypes: BothTimestampTypes
)

class TimestampSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("ScalaPB Demo")
    .master("local[2]")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  val data = Seq(
    TestTimestampsHolder(
      0,
      new SQLTimestamp(1),
      BothTimestampTypes(
        // 2 seconds + 3 milliseconds + 4 microseconds + 5 nanoseconds
        googleTs = Some(GoogleTimestamp(2, 3 * 1000000 + 4 * 1000 + 5)),
        googleTsAsSqlTs = Some({
          // 5 seconds
          val ts = new SQLTimestamp(5 * 1000)
          // + 6 milliseconds + 7 microseconds + 8 nanoseconds
          ts.setNanos(6 * 1000000 + 7 * 1000 + 8)
          ts
        })
      )
    )
  )

  // 4 seconds + 5 microseconds + 6 nanoseconds
  val googleTimestampNanosPrecision = GoogleTimestamp(4, 5 * 1000 + 6)
  val googleTimestampMicrosPrecision = GoogleTimestamp(
    googleTimestampNanosPrecision.seconds,
    googleTimestampNanosPrecision.nanos / 1000 * 1000
  )
  val sqlTimestampMicrosPrecision = SQLTimestamp.from(
    Instant.ofEpochSecond(
      googleTimestampMicrosPrecision.seconds,
      googleTimestampMicrosPrecision.nanos
    )
  )

  val protoMessagesWithGoogleTimestamp = Seq(
    StructFromGoogleTimestamp(
      googleTs = Some(googleTimestampNanosPrecision)
    ),
    StructFromGoogleTimestamp(
      googleTs = Some(googleTimestampNanosPrecision)
    )
  )

  val protoMessagesWithGoogleTimestampMappedToSQLTimestamp = Seq(
    SQLTimestampFromGoogleTimestamp(googleTsAsSqlTs = Some(sqlTimestampMicrosPrecision)),
    SQLTimestampFromGoogleTimestamp(googleTsAsSqlTs = Some(sqlTimestampMicrosPrecision))
  )

  "ProtoSQL.createDataFrame from proto messages with google timestamp" should "have a spark schema field type of TimestampType" in {
    val df: DataFrame =
      ProtoSQL.withSparkTimestamps.createDataFrame(spark, protoMessagesWithGoogleTimestamp)
    df.schema.fields.map(_.dataType).toSeq must be(
      Seq(
        TimestampType
      )
    )
  }

  "ProtoSQL.createDataFrame from proto messages with google timestamp" should "be able to collect items with microsecond timestamp precision" in {
    val df: DataFrame =
      ProtoSQL.withSparkTimestamps.createDataFrame(spark, protoMessagesWithGoogleTimestamp)

    df.collect().map(_.toSeq) must contain theSameElementsAs Seq(
      Seq(sqlTimestampMicrosPrecision),
      Seq(sqlTimestampMicrosPrecision)
    )
  }

  "spark.createDataset from proto messages with google timestamp" should "have a spark schema field type of TimestampType" in {
    import ProtoSQL.withSparkTimestamps.implicits._

    val ds: Dataset[StructFromGoogleTimestamp] =
      spark.createDataset(protoMessagesWithGoogleTimestamp)
    ds.schema.fields.map(_.dataType).toSeq must be(
      Seq(
        TimestampType
      )
    )
  }

  "spark.createDataset from proto messages with google timestamp" should "be able to collect items with correct timestamp values" in {
    import ProtoSQL.withSparkTimestamps.implicits._

    val ds: Dataset[StructFromGoogleTimestamp] =
      spark.createDataset(protoMessagesWithGoogleTimestamp)
    ds.collect() must contain theSameElementsAs Seq(
      StructFromGoogleTimestamp(googleTs = Some(googleTimestampMicrosPrecision)),
      StructFromGoogleTimestamp(googleTs = Some(googleTimestampMicrosPrecision))
    )
  }

  "spark.createDataset from proto messages with google timestamp" should "be able to convert items with correct timestamp values" in {
    import ProtoSQL.withSparkTimestamps.implicits._

    val ds: Dataset[StructFromGoogleTimestamp] =
      spark.createDataset(protoMessagesWithGoogleTimestamp)

    val dsMapped: Dataset[StructFromGoogleTimestamp] = ds.map(record => record)

    dsMapped.collect() must contain theSameElementsAs Seq(
      StructFromGoogleTimestamp(googleTs = Some(googleTimestampMicrosPrecision)),
      StructFromGoogleTimestamp(googleTs = Some(googleTimestampMicrosPrecision))
    )
  }

  "spark.createDataset from proto messages with spark timestamp" should "have a spark schema field type of TimestampType" in {
    import ProtoSQL.withSparkTimestamps.implicits._

    val ds: Dataset[SQLTimestampFromGoogleTimestamp] =
      spark.createDataset(protoMessagesWithGoogleTimestampMappedToSQLTimestamp)
    ds.schema.fields.map(_.dataType).toSeq must be(
      Seq(
        TimestampType
      )
    )
  }

  "spark.createDataset from proto messages with spark timestamp" should "be able to convert items with correct timestamp values" in {
    import ProtoSQL.withSparkTimestamps.implicits._

    val ds: Dataset[SQLTimestampFromGoogleTimestamp] =
      spark.createDataset(protoMessagesWithGoogleTimestampMappedToSQLTimestamp)

    val dsMapped = ds.map(record => record)

    dsMapped.collect() must contain theSameElementsAs Seq(
      SQLTimestampFromGoogleTimestamp(googleTsAsSqlTs = Some(sqlTimestampMicrosPrecision)),
      SQLTimestampFromGoogleTimestamp(googleTsAsSqlTs = Some(sqlTimestampMicrosPrecision))
    )
  }

  "df with case class timestamp as well as both types of google timestamp" should "not have StructType for timestamps" in {
    import ProtoSQL.withSparkTimestamps.implicits._

    implicit val timestampInjection =
      new frameless.Injection[SQLTimestamp, frameless.SQLTimestamp] {
        def apply(ts: SQLTimestamp): frameless.SQLTimestamp = {
          val i = ts.toInstant()
          frameless.SQLTimestamp(i.getEpochSecond() * 1000000 + i.getNano() / 1000)
        }

        def invert(l: frameless.SQLTimestamp): SQLTimestamp = SQLTimestamp.from(
          java.time.Instant.EPOCH.plus(l.us, java.time.temporal.ChronoUnit.MICROS)
        )
      }

    val ds: Dataset[TestTimestampsHolder] = spark.createDataset(data)

    ds.schema must be(
      StructType(
        Seq(
          StructField("justLong", LongType),
          StructField("ts", TimestampType),
          StructField(
            "bothTimestampTypes",
            StructType(
              Seq(
                StructField("google_ts", TimestampType),
                StructField("google_ts_as_sql_ts", TimestampType)
              )
            )
          )
        )
      )
    )

  }

}
