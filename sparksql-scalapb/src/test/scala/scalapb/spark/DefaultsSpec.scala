package scalapb.spark

import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers

class DefaultsSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("ScalaPB Demo")
    .master("local[2]")
    .getOrCreate()

  "Proto2 RDD[DefaultsRequired]" should "have non-null default values after converting to Dataframe" in {
    import com.example.protos.defaults.DefaultsRequired
    val defaults = DefaultsRequired.defaultInstance
    val row = ProtoSQL.createDataFrame(spark, Seq(defaults)).collect().head
    val expected = Row(
      defaults.i32Value,
      defaults.i64Value,
      defaults.u32Value,
      defaults.u64Value,
      defaults.dValue,
      defaults.fValue,
      defaults.bValue,
      defaults.sValue,
      defaults.binaryValue.toByteArray
    )
    row must be(expected)
  }

  "Proto2 RDD[DefaultsOptional]" should "have null values after converting to Dataframe" in {
    import com.example.protos.defaults.DefaultsOptional
    val defaults = DefaultsOptional.defaultInstance
    val row = ProtoSQL.createDataFrame(spark, Seq(defaults)).collect().head
    val expected = Row(null, null, null, null, null, null, null, null, null)
    row must be(expected)
  }

  "Proto3 RDD[DefaultsV3]" should "have non-null default values after converting to Dataframe" in {
    import com.example.protos.defaultsv3.DefaultsV3
    val defaults = DefaultsV3.defaultInstance
    val row = ProtoSQL.createDataFrame(spark, Seq(defaults)).collect().head
    val expected = Row(
      defaults.i32Value,
      defaults.i64Value,
      defaults.u32Value,
      defaults.u64Value,
      defaults.dValue,
      defaults.fValue,
      defaults.bValue,
      defaults.sValue,
      defaults.binaryValue.toByteArray
    )
    row must be(expected)
  }
}
