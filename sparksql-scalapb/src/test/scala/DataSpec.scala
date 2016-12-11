
import com.example.protos.demo.{ DemoProtoUdt, Person }
import org.apache.spark.sql.SparkSession
import org.scalatest.{ FlatSpec, MustMatchers }

class DataSpec extends FlatSpec with MustMatchers {

  "DemoProtoUdt" should "work" in {
    val spark = SparkSession.builder().appName("ScalaPB Demo").master("local[2]").getOrCreate()
    import spark.implicits._
    DemoProtoUdt.register()
    val s = Seq(Person().withName("Foo"), Person().withName("Bar"))

    val ds = spark.sqlContext.createDataset(s)
    ds.count() must be(2)

    spark.stop()
  }


}