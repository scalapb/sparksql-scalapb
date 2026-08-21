package scalapb.spark

import com.example.protos.maps._
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll

class MapsSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("ScalaPB Demo")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits.StringToColumn
  import Implicits._

  val data = Seq(
    MapTest(attributes = Map("foo" -> "bar"))
  )

  "converting maps to df" should "work" in {
    val df = ProtoSQL.createDataFrame(spark, data)
    val res = df.as[MapTest].map(r => r)

    res.show()
  }
}
