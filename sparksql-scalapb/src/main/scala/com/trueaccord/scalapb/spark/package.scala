package com.trueaccord.scalapb

import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

import scala.reflect.ClassTag

package object spark {
  implicit class ProtoSQLContext(val sqlContext: SQLContext) extends AnyVal {
    def protoToDataFrame[T <: GeneratedMessage with Message[T]](protoRdd: org.apache.spark.rdd.RDD[T])(
      implicit cmp: GeneratedMessageCompanion[T]) = {
      ProtoSQL.protoToDataFrame(sqlContext, protoRdd)
    }
  }

  implicit class ProtoRDD[T <: GeneratedMessage with Message[T]](val protoRdd: org.apache.spark.rdd.RDD[T]) extends AnyVal {
    def saveAsParquet(path: String)(implicit ct: ClassTag[T]): Unit = {
      ProtoParquet.saveParquet(protoRdd, path)
    }

    def toDataFrame(sqlContext: SQLContext)(implicit cmp: GeneratedMessageCompanion[T]): DataFrame = {
      ProtoSQL.protoToDataFrame(sqlContext, protoRdd)
    }

    def toDataFrame(sparkSession: SparkSession)(implicit cmp: GeneratedMessageCompanion[T]): DataFrame = {
      ProtoSQL.protoToDataFrame(sparkSession, protoRdd)
    }
  }
}
