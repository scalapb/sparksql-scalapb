import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FlatSpec, MustMatchers}
import org.scalatest.prop.GeneratorDrivenPropertyChecks

case class B(f1: String, f2: Int)

case class CC(a: Int, b: B)

class SparkSpec
    extends FlatSpec
    with MustMatchers
    with BeforeAndAfterAll
    with GeneratorDrivenPropertyChecks {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("ScalaPB Demo")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  "fff" should "do" in {
    val x = Seq(("x", CC(3, B("ffo", 4)), 17))
    spark.createDataset(x).map(_._2).as[CC].show()

  }
}
