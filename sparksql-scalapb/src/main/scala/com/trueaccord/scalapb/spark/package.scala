package com.trueaccord.scalapb

import org.apache.spark.sql._

import scala.reflect.ClassTag

package object spark {
  implicit class ProtoSQLContext(val sqlContext: SQLContext) extends AnyVal {
    @deprecated("Please use sparkContext.createDataset()", "sparksql-scalapb 1.9")
    def protoToDataFrame[T <: GeneratedMessage with Message[T] : Encoder](protoRdd: org.apache.spark.rdd.RDD[T])(
      implicit cmp: GeneratedMessageCompanion[T]) = {
      ProtoSQL.protoToDataFrame(sqlContext, protoRdd)
    }
  }

  implicit class ProtoRDD[T <: GeneratedMessage with Message[T]](val protoRdd: org.apache.spark.rdd.RDD[T]) extends AnyVal {
    def saveAsParquet(path: String)(implicit ct: ClassTag[T]): Unit = {
      ProtoParquet.saveParquet(protoRdd, path)
    }

    @deprecated("Please use sparkContext.createDataset().toDF", "sparksql-scalapb 1.9")
    def toDataFrame(sqlContext: SQLContext)(implicit cmp: GeneratedMessageCompanion[T], encoder: Encoder[T]): DataFrame = {
      ProtoSQL.protoToDataFrame(sqlContext, protoRdd)
    }
    @deprecated("Please use sparkSession.createDataset().toDF", "sparksql-scalapb 1.9")
    def toDataFrame(sparkSession: SparkSession)(implicit cmp: GeneratedMessageCompanion[T], encoder: Encoder[T]): DataFrame = {
      ProtoSQL.protoToDataFrame(sparkSession, protoRdd)
    }
  }
}
