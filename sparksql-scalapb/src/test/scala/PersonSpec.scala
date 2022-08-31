package scalapb.spark

import com.example.protos.base.Base
import com.example.protos.demo.Person.Inner.InnerEnum
import com.example.protos.demo.{Address, Event, Gender, Hit, Person, SimplePerson}
import com.google.protobuf.ByteString
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, types, functions => F}
import org.scalatest.events.TestPending
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers

import java.sql.Timestamp

case class InnerLike(inner_value: String)

case class AddressLike(street: Option[String], city: Option[String])

case class BaseLike()

case class PersonLike(
    name: String,
    age: Int,
    addresses: Seq[AddressLike],
    gender: String,
    tags: Seq[String] = Seq.empty,
    base: Option[BaseLike] = None,
    inner: Option[InnerLike] = None,
    data: Option[Array[Byte]] = None,
    address: Option[AddressLike] = None,
    nums: Vector[Int] = Vector.empty
)

case class OuterCaseClass(x: Person, y: String)
case class OuterCaseClassTimestamp(x: Person, y: Timestamp)

class PersonSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
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

  "mapping datasets" should "work" in {
    val s = (
      14,
      SimplePerson(
        name = Some("foo"),
        address = Some(Address(street = Some("St"), city = Some("Ct")))
      ),
      17
    )
    val ds1 = spark.createDataset(Seq(s)).map(_._2)
    ds1.collect() must contain theSameElementsAs (Seq(s._2))
    ds1.map(_.getAddress).collect() must contain theSameElementsAs (Seq(
      s._2.getAddress
    ))
    spark
      .createDataset(Seq(s))
      .toDF()
      .select($"_2.address.*")
      .as[Address]
      .collect() must contain theSameElementsAs (Seq(s._2.getAddress))
  }

  "Creating person dataset" should "work" in {
    val s = Seq(Person().withName("Foo"), Person().withName("Bar"))

    val ds = spark.sqlContext.createDataset(s)
    ds.count() must be(2)
  }

  "Creating enum dataset" should "work" in {
    val gendersStr = Seq((1, "MALE"), (2, "MALE"), (3, "FEMALE"), (5, "15"))
    val gendersObj = Seq(
      (1, Gender.MALE),
      (2, Gender.MALE),
      (3, Gender.FEMALE),
      (5, Gender.Unrecognized(15))
    )

    spark
      .createDataset(gendersStr)
      .as[(Int, Gender)]
      .collect()
      .toVector must contain theSameElementsAs (gendersObj)
    spark
      .createDataset(gendersObj)
      .as[(Int, String)]
      .collect() must contain theSameElementsAs (gendersStr)
  }

  "Creating bytestring dataset" should "work" in {
    val byteStrings: Seq[ByteString] =
      Seq(ByteString.copyFrom(Array[Byte](1, 2, 3)), ByteString.EMPTY)
    val bytesArrays = byteStrings.map(_.toByteArray)

    spark
      .createDataset(byteStrings)
      .as[Array[Byte]]
      .collect()
      .toVector must contain theSameElementsAs (bytesArrays)
    spark
      .createDataset(bytesArrays)
      .as[ByteString]
      .collect() must contain theSameElementsAs (byteStrings)

    spark
      .createDataset(byteStrings)
      .map(bs => (bs.toString, bs))
      .show()
  }

  "Dataset[Person]" should "work" in {
    val ds: Dataset[Person] = spark.createDataset(Seq(TestPerson))
    ds.where($"age" > 30).count() must be(1)
    ds.where($"age" > 40).count() must be(0)
    ds.where($"gender" === "MALE").count() must be(1)
    ds.collect() must be(Array(TestPerson))
    ds.toDF().as[Person].collect() must be(Array(TestPerson))
    ds.select("data").printSchema()
    ds.select(F.sha1(F.col("data"))).printSchema()
    ds.show()
    ds.toDF().printSchema()
  }

  "as[SimplePerson]" should "work for manual building" in {
    val pl = PersonLike(
      name = "Owen M",
      age = 35,
      addresses = Seq.empty,
      gender = "MALE",
      inner = Some(InnerLike("V1")),
      tags = Seq("foo", "bar"),
      address = Some(AddressLike(Some("Main"), Some("Bar"))),
      nums = Vector(3, 4, 5)
    )
    val p =
      SimplePerson().update(
        _.name := "Owen M",
        _.age := 35,
        _.tags := Seq("foo", "bar"),
        _.address.street := "Main",
        _.address.city := "Bar",
        _.nums := Seq(3, 4, 5)
      )
    val manualDF: DataFrame = spark.createDataFrame(Seq(pl))
    val manualDS: Dataset[SimplePerson] = spark.createDataset(Seq(p))
    manualDF.as[SimplePerson].collect()(0) must be(p)
    manualDS.collect()(0) must be(p)
  }

  "as[Person]" should "work for manual building" in {
    val pl = PersonLike(
      name = "Owen M",
      age = 35,
      addresses = Seq(
        AddressLike(Some("foo"), Some("bar")),
        AddressLike(Some("baz"), Some("taz"))
      ),
      gender = "MALE",
      inner = Some(InnerLike("V1")),
      data = Some(TestPerson.getData.toByteArray)
    )
    val manualDF: DataFrame = spark.createDataFrame(Seq(pl))
    manualDF.show()
    manualDF.as[Person].collect()(0) must be(
      Person().update(
        _.name := "Owen M",
        _.age := 35,
        _.gender := Gender.MALE,
        _.inner.innerValue := InnerEnum.V1,
        _.data := ByteString.copyFrom(Array[Byte](1, 2, 3)),
        _.addresses := pl.addresses.map(a => Address(city = a.city, street = a.street))
      )
    )
    spark.createDataset(Seq(Person(gender = Some(Gender.FEMALE)))).toDF().show()
  }

  "converting from rdd to dataframe" should "work" in {
    val rdd = spark.sparkContext.parallelize(Seq(Person(name = Some("foo"))))
    rdd
      .toDataFrame(spark)
      .select($"name")
      .collect()
      .map(_.getAs[String]("name")) must contain theSameElementsAs (Vector(
      "foo"
    ))
  }

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

    val ds = df.select($"name", $"addresses".getItem(0))

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

  "serialize and deserialize" should "work on dataset of bytes" in {
    val s = Seq(
      TestPerson.update(_.name := "p1"),
      TestPerson.update(_.name := "p2"),
      TestPerson.update(_.name := "p3")
    )
    val bs: Dataset[Array[Byte]] = spark.createDataset(s).map(_.toByteArray)
    bs.map(Person.parseFrom).collect() must contain theSameElementsAs (s)
  }

  "UDFs that involve protos" should "work when using ProtoSQL.udfs" in {
    val h1 = Hit(
      id = Some(ByteString.copyFrom(Array[Byte](112, 75, 6))),
      target = Some("foo")
    )
    val events: Seq[Event] =
      Seq(
        Event(
          eventId = Some("xyz"),
          action = Some(com.google.protobuf.any.Any.pack(h1))
        )
      )
    val df = ProtoSQL.createDataFrame(spark, events)
    val parseHit = ProtoSQL.udf { s: Array[Byte] => Hit.parseFrom(s) }
    df.withColumn("foo", parseHit($"action.value")).show()
  }

  "UDFs that returns protos" should "work when using ProtoSQL.createDataFrame" in {
    val h1 = Hit(
      id = Some(ByteString.copyFrom(Array[Byte](112, 75, 6))),
      target = Some("foo")
    )

    val events: Seq[Address] =
      Seq(
        Address()
      )
    val df = ProtoSQL.createDataFrame(spark, events)

    val returnAddress = ProtoSQL.udf { s: String => Address() }

    df.withColumn("address", returnAddress($"street"))
      .write
      .mode("overwrite")
      .save("/tmp/address1")
  }

  "UDFs that returns protos" should "work when reading local files" in {
    val df = spark.read.json("./sparksql-scalapb/src/test/assets/address.json")

    val returnAddress = ProtoSQL.udf { s: String => Address() }

    df.withColumn("address", returnAddress($"foo"))
      .write
      .mode("overwrite")
      .save("/tmp/address2")
  }

  "OuterCaseClass" should "use our type encoders" in {
    val outer = OuterCaseClass(TestPerson, "foo")
    val df = spark.createDataset(Seq(outer)).toDF()
    df.select($"x.*").as[Person].collect() must contain theSameElementsAs (Seq(TestPerson))
  }

  "OuterCaseClassTimestamp" should "serialize a java.sql.Timestamp" in {
    implicit val timestampInjection = new frameless.Injection[Timestamp, frameless.SQLTimestamp] {
      def apply(ts: Timestamp): frameless.SQLTimestamp = {
        val i = ts.toInstant()
        frameless.SQLTimestamp(i.getEpochSecond() * 1000000 + i.getNano() / 1000)
      }

      def invert(l: frameless.SQLTimestamp): Timestamp = Timestamp.from(
        java.time.Instant.EPOCH.plus(l.us, java.time.temporal.ChronoUnit.MICROS)
      )
    }

    val ts = Timestamp.valueOf("2020-11-17 21:34:56.157")
    val outer = OuterCaseClassTimestamp(TestPerson, ts)
    val df = spark.createDataset(Seq(outer)).toDF()

    df.select($"x.*").as[Person].collect() must contain theSameElementsAs (Seq(TestPerson))
    df.select($"y").as[Timestamp].collect() must contain theSameElementsAs Seq(ts)
  }

  "parsing null repeated from json" should "work" in {
    spark.read
      .schema(ProtoSQL.schemaFor[Person].asInstanceOf[types.StructType])
      .json("./sparksql-scalapb/src/test/assets/person_null_repeated.json")
      .as[Person]
      .collect() must contain theSameElementsAs Seq(
      Person().withTags(Seq("foo", "bar")),
      Person(),
      Person(),
      Person()
    )
  }
}
