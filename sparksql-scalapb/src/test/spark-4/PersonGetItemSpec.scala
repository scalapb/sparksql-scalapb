package scalapb.spark

import com.example.protos.demo.{Address, Gender, Person}
import com.google.protobuf.ByteString
import org.apache.spark.sql.{SparkSession, functions => F}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers

class PersonGetItemSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("ScalaPB Demo")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits.StringToColumn
  import Implicits._

  val TestPerson = Person().update(
    _.name := "Owen M",
    _.age := 35,
    _.gender := Gender.MALE,
    _.addresses := Seq(
      Address().update(
        _.city := "San Francisco"
      )
    ),
    _.data := ByteString.copyFrom(Array[Byte](1, 2, 3))
  )

  "selecting message fields into dataset should work" should "work" in {
    val df = ProtoSQL.createDataFrame(
      spark,
      Seq(
        TestPerson,
        TestPerson.withName("Other").clearAddresses,
        TestPerson
          .withName("Other2")
          .clearData
          .clearGender
          .clearAddresses
          .addAddresses(Address(street = Some("FooBar")))
      )
    )

    val ds = df.select($"name", F.get($"addresses", F.lit(0)))

    ds.as[(String, Option[Address])].collect() must contain theSameElementsAs (
      Seq(
        (TestPerson.getName, Some(TestPerson.addresses.head)),
        ("Other", None),
        ("Other2", Some(Address(street = Some("FooBar"))))
      )
    )

    ds.as[(String, Address)].collect() must contain theSameElementsAs (
      Seq(
        (TestPerson.getName, TestPerson.addresses.head),
        null,
        ("Other2", Address(street = Some("FooBar")))
      )
    )

    val ds2 = df.select($"name", $"gender")
    ds2.as[(String, Option[Gender])].collect() must contain theSameElementsAs (
      Seq(
        (TestPerson.getName, Some(Gender.MALE)),
        ("Other", Some(Gender.MALE)),
        ("Other2", None)
      )
    )
    ds2.as[(String, Gender)].collect() must contain theSameElementsAs (
      Seq(
        (TestPerson.getName, Gender.MALE),
        ("Other", Gender.MALE),
        null
      )
    )

    val ds3 = df.select($"name", $"data")
    ds3
      .as[(String, Option[ByteString])]
      .collect() must contain theSameElementsAs (
      Seq(
        (TestPerson.getName, Some(TestPerson.getData)),
        ("Other", Some(TestPerson.getData)),
        ("Other2", None)
      )
    )
  }
}
