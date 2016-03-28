package com.trueaccord.scalapb

import org.apache.spark.sql.SQLContext

package object spark {
  implicit class ProtoSQLContext(val sqlContext: SQLContext) extends AnyVal {
    def createDataFrame[T <: GeneratedMessage with Message[T]](protoRdd: org.apache.spark.rdd.RDD[T])(
      implicit cmp: GeneratedMessageCompanion[T]) = {
      ProtoSQL.rddToDataFrame(sqlContext, protoRdd)
    }
  }
}
