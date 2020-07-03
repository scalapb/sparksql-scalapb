package scalapb.spark

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.scalacheck.Arbitrary
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.BeforeAndAfterAll
import scalapb.spark.test.{all_types2 => AT2}
import scalapb.spark.test3.{all_types3 => AT3}
import scalapb.{GeneratedMessage, GeneratedMessageCompanion, Message}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import scala.reflect.ClassTag

class AllTypesSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with ScalaCheckDrivenPropertyChecks {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("ScalaPB Demo")
    .master("local[2]")
    .getOrCreate()

  import ArbitraryProtoUtils._
  import org.scalacheck.ScalacheckShapeless._
  import spark.implicits.{newProductEncoder => _}

  def verifyTypes[
      T <: GeneratedMessage: Arbitrary: GeneratedMessageCompanion: ClassTag
  ](
      protoSQL: ProtoSQL
  ): Unit =
    forAll { (n: Seq[T]) =>
      import protoSQL.implicits._

      // ProtoSQL conversion to dataframe
      val df1 = protoSQL.createDataFrame(spark, n)

      // Creates dataset using encoder deserialization:
      val ds1: Dataset[T] = df1.as[T]
      ds1.collect() must contain theSameElementsAs (n)

      // Creates dataframe using encoder serialization:
      val ds2 = spark.createDataset(n)
      ds2.collect() must contain theSameElementsAs (n)
      ds2.toDF.coalesce(1).except(df1.coalesce(1)).count() must be(0)
    }

  def verifyTypes[
      T <: GeneratedMessage: Arbitrary: GeneratedMessageCompanion: ClassTag
  ]: Unit =
    verifyTypes[T](ProtoSQL)

  "AllTypes" should "work for int32" in {
    verifyTypes[AT2.Int32Test]
    verifyTypes[AT3.Int32Test]
  }

  it should "work for int64" in {
    verifyTypes[AT2.Int64Test]
    verifyTypes[AT3.Int64Test]
  }

  it should "work for bools" in {
    verifyTypes[AT2.BoolTest]
    verifyTypes[AT3.BoolTest]
  }

  it should "work for strings" in {
    verifyTypes[AT2.StringTest]
    verifyTypes[AT3.StringTest]
  }

  it should "work for floats" in {
    verifyTypes[AT2.FloatTest]
    verifyTypes[AT3.FloatTest]
  }

  it should "work for doubles" in {
    verifyTypes[AT2.DoubleTest]
    verifyTypes[AT3.DoubleTest]
  }

  it should "work for bytes" in {
    verifyTypes[AT2.BytesTest]
    verifyTypes[AT3.BytesTest]
  }

  it should "work for enums" in {
    verifyTypes[AT2.EnumTest]
    verifyTypes[AT3.EnumTest]
  }

  it should "work for messages" in {
    verifyTypes[AT2.MessageTest]
    verifyTypes[AT3.MessageTest]
  }

  it should "work for oneofs" in {
    verifyTypes[AT2.OneofTest]
    verifyTypes[AT3.OneofTest]
  }

  it should "work for levels" in {
    verifyTypes[AT2.Level1]
    verifyTypes[AT3.Level1]
  }

  it should "work for any" in {
    verifyTypes[AT2.AnyTest]
    verifyTypes[AT3.AnyTest]
  }

  it should "work for time types" in {
    verifyTypes[AT2.WellKnownTypes]
    verifyTypes[AT3.WellKnownTypes]
  }

  it should "work for wrapper types" in {
    verifyTypes[AT3.WrappersTest]
    verifyTypes[AT3.WrappersTest](ProtoSQL.withRetainedPrimitiveWrappers)
    verifyTypes[AT3.WrappersTest](new ProtoSQL(SchemaOptions().withScalaNames))
  }
}
