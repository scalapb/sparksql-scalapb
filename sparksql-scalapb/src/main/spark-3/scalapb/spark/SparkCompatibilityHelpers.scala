package scalapb.spark

import frameless.functions.FramelessUdf
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.ExternalRDD
import org.apache.spark.sql.{Column, DataFrame, Encoder, FramelessInternals, SparkSession}
import scalapb.GeneratedMessage

object SparkCompatibilityHelpers {
  def protoToDataFrame[T <: GeneratedMessage: Encoder](
      sparkSession: SparkSession,
      protoRdd: org.apache.spark.rdd.RDD[T]
  ): DataFrame = {
    val logicalPlan: LogicalPlan = ExternalRDD(protoRdd, sparkSession)
    FramelessInternals.ofRows(sparkSession, logicalPlan)
  }

  def getUdfColumn[T, R](framelessUdf: FramelessUdf[T, R]): Column = {
    new Column(framelessUdf)
  }
}
