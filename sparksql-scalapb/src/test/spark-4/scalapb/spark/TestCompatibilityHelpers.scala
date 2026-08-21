package scalapb.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object TestCompatibilityHelpers {
  def personGetItem(df: DataFrame): DataFrame = {
    df.select(col("name"), get(col("addresses"), lit(0)))
  }
}
