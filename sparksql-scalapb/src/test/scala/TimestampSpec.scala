package scalapb.spark

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers

import scalapb.spark.test3.customizations.{SQLTimestampFromGoogleTimestamp, StructFromGoogleTimestamp, BothTimestampTypes}

import java.sql.{Timestamp => SQLTimestamp}
import com.google.protobuf.timestamp.{Timestamp => GoogleTimestamp}

case class TestTimestampsHolder(
                               justLong: Long,
                               ts: SQLTimestamp,
                               bothTimestampTypes: BothTimestampTypes)

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
        }),
      )
    )
  )

  val dataStructFromGoogleTimestamp = Seq(
    StructFromGoogleTimestamp(
      // 4 seconds + 5 microseconds + 6 nanoseconds
      googleTs = Some(GoogleTimestamp(4, 5 * 1000 + 6))
    )
  )

  val dataSQLTimestampFromGoogleTimestamp = Seq(
    SQLTimestampFromGoogleTimestamp(
      googleTsAsSqlTs = Some(new SQLTimestamp(2)),
    )
  )


  "df with google timestamp and SchemaOptions of withSparkTimestamps" should "have a spark schema field type of TimestampType" in {
    val df: DataFrame = ProtoSQL.withSparkTimestamps.createDataFrame(spark, dataStructFromGoogleTimestamp)
    df.schema.fields.map(_.dataType).toSeq must be(
      Seq(
        TimestampType,
      )
    )
  }

  "df with google timestamp and SchemaOptions of withSparkTimestamps" should "be able to convert values to and from spark TimestampType" in {
    val df: DataFrame = ProtoSQL.withSparkTimestamps.createDataFrame(spark, dataStructFromGoogleTimestamp)

    df.collect().map(_.toSeq) must contain theSameElementsAs Seq(
      Seq(
        {
          val ts = new SQLTimestamp(4 * 1000)
          ts.setNanos(5 * 1000 + 6 * 0)
          ts
        }
      )
    )
  }

  "ds with google timestamp and SchemaOptions of withSparkTimestamps" should "have a spark schema field type of TimestampType" in {
    import ProtoSQL.withSparkTimestamps.implicits._

    val ds: Dataset[StructFromGoogleTimestamp] = spark.createDataset(dataStructFromGoogleTimestamp)
    ds.schema.fields.map(_.dataType).toSeq must be(
      Seq(
        TimestampType,
      )
    )
  }

  "ds with google timestamp and SchemaOptions of withSparkTimestamps" should "be able to convert values to and from spark TimestampType" in {
    import ProtoSQL.withSparkTimestamps.implicits._

    val ds: Dataset[StructFromGoogleTimestamp] = spark.createDataset(dataStructFromGoogleTimestamp)
    ds.collect() must contain theSameElementsAs Seq(
      StructFromGoogleTimestamp(
        // 4 seconds + 5 microseconds + 0 nanoseconds (loss of the nanoseconds)
        googleTs = Some(GoogleTimestamp(4, 5 * 1000 + 6 * 0))
      )
    )
  }

  "ds with sql timestamp and SchemaOptions of withSparkTimestamps" should "have a spark schema field type of TimestampType" in {
    import ProtoSQL.withSparkTimestamps.implicits._

    val ds: Dataset[SQLTimestampFromGoogleTimestamp] = spark.createDataset(dataSQLTimestampFromGoogleTimestamp)
    ds.schema.fields.map(_.dataType).toSeq must be(
      Seq(
        TimestampType,
      )
    )
  }

  "ds with sql timestamp and SchemaOptions of withSparkTimestamps" should "be able to convert values to and from spark TimestampType" in {
    import ProtoSQL.withSparkTimestamps.implicits._

    val ds: Dataset[SQLTimestampFromGoogleTimestamp] = spark.createDataset(dataSQLTimestampFromGoogleTimestamp)
    ds.collect() must contain theSameElementsAs Seq(
      SQLTimestampFromGoogleTimestamp(
        googleTsAsSqlTs = Some(new SQLTimestamp(2)),
      )
    )
  }

  "df with case class timestamp as well as both types of google timestamp" should "not have StructType for timestamps" in {
    import ProtoSQL.withSparkTimestamps.implicits._

    implicit val timestampInjection = new frameless.Injection[SQLTimestamp, frameless.SQLTimestamp] {
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
      StructType(Seq(
        StructField("justLong", LongType),
        StructField("ts", TimestampType),
        StructField("bothTimestampTypes", StructType(Seq(
          StructField("google_ts", TimestampType),
          StructField("google_ts_as_sql_ts", TimestampType),
        )))
      ))
    )

  }

}
