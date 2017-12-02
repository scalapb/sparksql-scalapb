package scalapb.spark

import scalapb.parquet.{ScalaPBInputFormat, ScalaPBOutputFormat, ScalaPBWriteSupport}
import scalapb.{GeneratedMessage, Message}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

object ProtoParquet {
  def loadParquet[T <: GeneratedMessage with Message[T]](sc: SparkContext, input: String)(implicit vt: ClassTag[T]): RDD[T] = {
    sc.newAPIHadoopRDD(
      conf = sc.hadoopConfiguration,
      fClass = classOf[ScalaPBInputFormat[T]],
      kClass = classOf[Void],
      vClass = vt.runtimeClass.asInstanceOf[Class[T]])
      .map(_._2)
  }

  def loadParquet[T <: GeneratedMessage with Message[T]](sc: SparkSession, input: String)(implicit vt: ClassTag[T]): RDD[T] = {
    sc.sparkContext.newAPIHadoopFile(
      input,
      fClass = classOf[ScalaPBInputFormat[T]],
      kClass = classOf[Void],
      vClass = vt.runtimeClass.asInstanceOf[Class[T]])
      .map(_._2)
  }

  def saveParquet[T <: GeneratedMessage with Message[T]](rdd: RDD[T], path: String)(implicit vt: ClassTag[T]) = {
    val config = rdd.context.hadoopConfiguration
    ScalaPBWriteSupport.setSchema(config, vt.runtimeClass.asInstanceOf[Class[T]])
    rdd.map(t => (null, t))
      .saveAsNewAPIHadoopFile(
        path = path,
        keyClass = classOf[Void],
        valueClass = vt.runtimeClass,
        outputFormatClass = classOf[ScalaPBOutputFormat[T]],
        conf = config)
  }
}
