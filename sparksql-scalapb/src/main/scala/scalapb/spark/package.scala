package scalapb

import org.apache.spark.sql.{DataFrame, Encoder, SQLContext, SparkSession}

import scala.reflect.ClassTag

package object spark {
  implicit class ProtoSQLContext(val sqlContext: SQLContext) extends AnyVal {
    def protoToDataFrame[T <: GeneratedMessage: Encoder](
        protoRdd: org.apache.spark.rdd.RDD[T]
    ) = {
      ProtoSQL.protoToDataFrame(sqlContext, protoRdd)
    }
  }

  implicit class ProtoRDD[T <: GeneratedMessage](
      val protoRdd: org.apache.spark.rdd.RDD[T]
  ) extends AnyVal {
    def toDataFrame(
        sqlContext: SQLContext
    )(implicit encoder: Encoder[T]): DataFrame = {
      ProtoSQL.protoToDataFrame(sqlContext, protoRdd)
    }

    def toDataFrame(
        sparkSession: SparkSession
    )(implicit encoder: Encoder[T]): DataFrame = {
      ProtoSQL.protoToDataFrame(sparkSession, protoRdd)
    }
  }
}
