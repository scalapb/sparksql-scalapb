package scalapb.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

private[spark] object TestCompatibilityHelpers {
  def personGetItem(df: DataFrame): DataFrame = {
    df.select(col("name"), col("addresses").getItem(0))
  }
}
