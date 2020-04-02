package scalapb.spark

import com.example.protos.wrappers._
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.io.ArrayPrimitiveWritable
import scalapb.GeneratedMessageCompanion
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Row

import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers

class WrappersSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("ScalaPB Demo")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits.StringToColumn

  val data = Seq(
    PrimitiveWrappers(
      intValue = Option(45),
      stringValue = Option("boo"),
      ints = Seq(17, 19, 25),
      strings = Seq("foo", "bar")
    ),
    PrimitiveWrappers(
      intValue = None,
      stringValue = None,
      ints = Seq(17, 19, 25),
      strings = Seq("foo", "bar")
    )
  )

  "converting df with primitive wrappers" should "work with primitive implicits" in {
    import ProtoSQL.withPrimitiveWrappers.implicits._
    val df = ProtoSQL.withPrimitiveWrappers.createDataFrame(spark, data)
    df.schema.fields.map(_.dataType).toSeq must be(
      Seq(
        IntegerType,
        StringType,
        ArrayType(IntegerType, false),
        ArrayType(StringType, false)
      )
    )
    df.collect must contain theSameElementsAs (
      Seq(
        Row(45, "boo", Seq(17, 19, 25), Seq("foo", "bar")),
        Row(null, null, Seq(17, 19, 25), Seq("foo", "bar"))
      )
    )
  }

  "converting df with primitive wrappers" should "work with default implicits" in {
    import ProtoSQL.implicits._
    val df = ProtoSQL.createDataFrame(spark, data)
    df.schema.fields.map(_.dataType).toSeq must be(
      Seq(
        StructType(Seq(StructField("value", IntegerType, true))),
        StructType(Seq(StructField("value", StringType, true))),
        ArrayType(
          StructType(Seq(StructField("value", IntegerType, true))),
          false
        ),
        ArrayType(
          StructType(Seq(StructField("value", StringType, true))),
          false
        )
      )
    )
    df.collect must contain theSameElementsAs (
      Seq(
        Row(
          Row(45),
          Row("boo"),
          Seq(Row(17), Row(19), Row(25)),
          Seq(Row("foo"), Row("bar"))
        ),
        Row(
          null,
          null,
          Seq(Row(17), Row(19), Row(25)),
          Seq(Row("foo"), Row("bar"))
        )
      )
    )
  }
}
