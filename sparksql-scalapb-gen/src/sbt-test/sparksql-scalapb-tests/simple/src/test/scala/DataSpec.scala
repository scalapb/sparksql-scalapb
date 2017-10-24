
import java.io.{File, FileOutputStream}

import com.example.protos.base.Base
import com.example.protos.demo.{Address, DemoProtoUdt, Gender, Person}
import org.apache.hadoop.fs.FileUtil
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, MustMatchers}
import org.apache.spark.sql.functions.udf

case class PersonLike(
  name: String, age: Int, addresses: Seq[Address], gender: Gender,
  tags: Seq[String] = Seq.empty, base: Option[Base] = None)

class DataSpec extends FlatSpec with MustMatchers with BeforeAndAfterAll {
  val spark: SparkSession = SparkSession.builder().appName("ScalaPB Demo").master("local[2]").getOrCreate()

  import spark.implicits._

  DemoProtoUdt.register()

  val GenderFromString = udf[Option[Gender], String](Gender.fromName)
  val GenderToString = udf[Option[String], Gender](g => Option(g).map(_.name))


  val TestPerson = Person().update(
    _.name := "Owen M",
    _.age := 35,
    _.gender := Gender.MALE,
    _.addresses := Seq(
      Address().update(
        _.city := "San Francisco"
      )
    )
  )

  override def afterAll(): Unit = {
    spark.stop()
  }

  "Creating person dataset" should "work" in {
    val s = Seq(Person().withName("Foo"), Person().withName("Bar"))

    val ds = spark.sqlContext.createDataset(s)
    ds.count() must be(2)
  }

  "Creating enum dataset" should "work" in {
    val genders = Seq("MALE", "MALE", "FEMALE", null)
    val gendersDS = spark.createDataset(genders).withColumnRenamed("value", "genderString")
    val typedDF = gendersDS.withColumn("gender", GenderFromString(gendersDS("genderString")))
    val localTypedDF: Array[Row] = typedDF.collect()
    typedDF.filter(GenderToString($"gender") === "MALE").count() must be(2)
    typedDF.filter(GenderToString($"gender") === "FEMALE").count() must be(1)
    localTypedDF must be(
      Array(
        Row("MALE", Gender.MALE),
        Row("MALE", Gender.MALE),
        Row("FEMALE", Gender.FEMALE),
        Row(null, null)))
  }

  "Dataset[Person]" should "work" in {
    val ds: Dataset[Person] = spark.createDataset(Seq(TestPerson))
    ds.where($"age" > 30).count() must be(1)
    ds.where($"age" > 40).count() must be(0)
    ds.where(GenderToString($"gender") === "MALE").count() must be(1)
    ds.collect() must be(Array(TestPerson))
    ds.toDF().as[Person].collect() must be (Array(TestPerson))
  }

  "as[Person]" should "work for manual building" in {
    val pl = PersonLike(
      name = "Owen M", age = 35, addresses=Seq.empty, gender = Gender.MALE)
    val manualDF: DataFrame = spark.createDataFrame(Seq(pl))
    manualDF.as[Person].collect()(0) must be(Person().update(
      _.name := "Owen M",
      _.age := 35,
      _.gender := Gender.MALE
    ))
  }

  "Dataset[Person" should "be loadable into a Dataframe" in {
    import com.trueaccord.scalapb.spark._
    
  }
  "Dataset[Person]" should "roundtrip" in {
    val f = FileUtil.createLocalTempFile(new File(""),"testPersonProtoFile", true)
    val os = new FileOutputStream(f)
    Seq(TestPerson).foreach(_.writeDelimitedTo(os))
    os.close()
    println(f.getAbsolutePath)
    val ds = spark.read.format("com.example.protos.demo.PersonProtoSource").load(f.getAbsolutePath)
    ds.where($"age" > 30).count() must be(1)
    ds.where($"age" > 40).count() must be(0)
    ds.where(GenderToString($"gender") === "MALE").count() must be(1)
    ds.as[Person].collect() must be (Array(TestPerson))
  }
}
