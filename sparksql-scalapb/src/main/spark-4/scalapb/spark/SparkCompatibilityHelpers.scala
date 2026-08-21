package scalapb.spark

import frameless.functions.FramelessUdf
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.ExternalRDD
import org.apache.spark.sql.{DataFrame, Encoder, FramelessInternals, SparkSession}
import scalapb.GeneratedMessage

object SparkCompatibilityHelpers {
  def protoToDataFrame[T <: GeneratedMessage: Encoder](
      sparkSession: SparkSession,
      protoRdd: org.apache.spark.rdd.RDD[T]
  ): DataFrame = {
    val classSession = sparkSession.asInstanceOf[classic.SparkSession]
    val logicalPlan: LogicalPlan = ExternalRDD(protoRdd, classSession)
    FramelessInternals.ofRows(sparkSession, logicalPlan)
  }

  def getUdfColumn[T, R](framelessUdf: FramelessUdf[T, R]): Column = {
    FramelessInternals.column(framelessUdf)
  }
}
