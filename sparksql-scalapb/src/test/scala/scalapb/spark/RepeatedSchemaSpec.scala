package scalapb.spark

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import scalapb.spark.schema.bug.schema_bug.{
  Nested,
  Read,
  RepeatedNestedRead,
  RepeatedNestedWrite,
  RepeatedOmitWrite,
  Write
}

// See https://github.com/scalapb/sparksql-scalapb/issues/313
class RepeatedSchemaSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("ScalaPB Demo")
    .master("local[2]")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  "Read from data with extra columns" should "work" in {
    val data = Seq(
      Write(
        nestedField = Seq(
          RepeatedNestedWrite(
            omitForRead = Some(
              RepeatedOmitWrite(
                fieldOne = "field_one_repeated_11",
                fieldTwo = "field_two_repeated_11"
              )
            ),
            fieldOne = "field_one_11",
            fieldTwo = "field_two_11",
            persisted = Some(Nested(fieldOne = "field_one_nested_11"))
          ),
          RepeatedNestedWrite(
            omitForRead = Some(
              RepeatedOmitWrite(
                fieldOne = "field_one_repeated_21",
                fieldTwo = "field_two_repeated_21"
              )
            ),
            fieldOne = "field_one_21",
            fieldTwo = "field_two_21",
            persisted = Some(Nested(fieldOne = "field_one_nested_21"))
          ),
          RepeatedNestedWrite(
            omitForRead = Some(
              RepeatedOmitWrite(
                fieldOne = "field_one_repeated_31",
                fieldTwo = "field_two_repeated_31"
              )
            ),
            fieldOne = "field_one_31",
            fieldTwo = "field_two_31",
            persisted = Some(Nested(fieldOne = "field_one_nested_31"))
          )
        ),
        additionalField = "additional_1"
      ),
      Write(
        nestedField = Seq(
          RepeatedNestedWrite(
            omitForRead = Some(
              RepeatedOmitWrite(
                fieldOne = "field_one_repeated_12",
                fieldTwo = "field_two_repeated_12"
              )
            ),
            fieldOne = "field_one_12",
            fieldTwo = "field_two_12",
            persisted = Some(Nested(fieldOne = "field_one_nested_12"))
          ),
          RepeatedNestedWrite(
            omitForRead = Some(
              RepeatedOmitWrite(
                fieldOne = "field_one_repeated_22",
                fieldTwo = "field_two_repeated_22"
              )
            ),
            fieldOne = "field_one_22",
            fieldTwo = "field_two_22",
            persisted = Some(Nested(fieldOne = "field_one_nested_22"))
          ),
          RepeatedNestedWrite(
            omitForRead = Some(
              RepeatedOmitWrite(
                fieldOne = "field_one_repeated_32",
                fieldTwo = "field_two_repeated_32"
              )
            ),
            fieldOne = "field_one_32",
            fieldTwo = "field_two_32",
            persisted = Some(Nested(fieldOne = "field_one_nested_32"))
          )
        ),
        additionalField = "additional_2"
      )
    )

    import ProtoSQL.implicits._
    val path = "/tmp/repeated-nested-bug"
    spark.createDataset(data).write.mode("overwrite").parquet(path)
    val readDf = spark.read.parquet(path)
    val readDs = readDf.as[Read]
    val readExpected = Seq(
      Read(
        nestedField = Seq(
          RepeatedNestedRead(
            fieldOne = "field_one_11",
            fieldTwo = "field_two_11",
            persisted = Some(Nested(fieldOne = "field_one_nested_11"))
          ),
          RepeatedNestedRead(
            fieldOne = "field_one_21",
            fieldTwo = "field_two_21",
            persisted = Some(Nested(fieldOne = "field_one_nested_21"))
          ),
          RepeatedNestedRead(
            fieldOne = "field_one_31",
            fieldTwo = "field_two_31",
            persisted = Some(Nested(fieldOne = "field_one_nested_31"))
          )
        ),
        additionalField = "additional_1"
      ),
      Read(
        nestedField = Seq(
          RepeatedNestedRead(
            fieldOne = "field_one_12",
            fieldTwo = "field_two_12",
            persisted = Some(Nested(fieldOne = "field_one_nested_12"))
          ),
          RepeatedNestedRead(
            fieldOne = "field_one_22",
            fieldTwo = "field_two_22",
            persisted = Some(Nested(fieldOne = "field_one_nested_22"))
          ),
          RepeatedNestedRead(
            fieldOne = "field_one_32",
            fieldTwo = "field_two_32",
            persisted = Some(Nested(fieldOne = "field_one_nested_32"))
          )
        ),
        additionalField = "additional_2"
      )
    )
    readDs.collect() must contain theSameElementsAs readExpected
  }
}
