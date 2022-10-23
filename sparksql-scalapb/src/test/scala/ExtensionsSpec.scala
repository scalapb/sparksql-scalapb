package scalapb.spark

import com.example.protos.extensions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scalapb.spark.Implicits._

class ExtensionsSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("ScalaPB Demo")
    .master("local[2]")
    .getOrCreate()

  "Creating Dataset from message with nested extension" should "work" in {
    val data = Seq(
      Baz(id = Some(1)),
      Baz(id = Some(2)),
      Baz(id = Some(3)),
    )

    val binaryDS = spark.createDataset(data.map(_.toByteArray))
    binaryDS.show()

    val protosDS = binaryDS.map(Baz.parseFrom(_))
    protosDS.show()
  }
}
