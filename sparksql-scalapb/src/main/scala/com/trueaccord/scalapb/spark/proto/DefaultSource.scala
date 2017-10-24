package com.trueaccord.scalapb.spark.proto

import java.io._

import com.trueaccord.scalapb.spark.ProtoSQL
import com.trueaccord.scalapb.{GeneratedMessage, Message}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.TaskContext
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriterFactory}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources._

import scala.reflect.runtime.universe._
import com.trueaccord.scalapb.GeneratedMessageCompanion
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection

import scala.reflect.ClassTag
import org.apache.spark.sql.catalyst.encoders._

/**
  * Created 10/20/17 3:40 AM
  * Author: ljacobsen
  *
  * This code is copyright (C) 2017 ljacobsen
  *
  */

abstract class DefaultSource[T <: GeneratedMessage with Message[T] : GeneratedMessageCompanion : ClassTag : TypeTag](companion: GeneratedMessageCompanion[T]) extends FileFormat
  with DataSourceRegister
  with Serializable {

  val encoder = ExpressionEncoder[T]

  override def equals(other: Any): Boolean = other match {
    case _: DefaultSource[T] => true
    case _ => false
  }

  override def inferSchema(
                            sparkSession: SparkSession,
                            options: Map[String, String],
                            files: Seq[FileStatus]): Option[StructType] =
  {
    Option(encoder.schema)
  }

  override def isSplitable(
                            sparkSession: SparkSession,
                            options: Map[String, String],
                            path: Path): Boolean = false


  override def buildReader(
                            sparkSession: SparkSession,
                            dataSchema: StructType,
                            partitionSchema: StructType,
                            requiredSchema: StructType,
                            filters: Seq[Filter],
                            options: Map[String, String],
                            hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] =
  {
    val broadcastedHadoopConf = sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    (file: PartitionedFile) => {
      val reader = new HadoopFileProtoReader[T](file, broadcastedHadoopConf.value.value)
      Option(TaskContext.get()).foreach(_.addTaskCompletionListener(_ => reader.close()))

      val fullOutput = dataSchema.map { f =>
        AttributeReference(f.name, f.dataType, f.nullable, f.metadata)()
      }
      val requiredOutput = fullOutput.filter { a =>
        requiredSchema.fieldNames.contains(a.name)
      }

      val requiredColumns = GenerateUnsafeProjection.generate(requiredOutput, fullOutput)
      reader.map(encoder.toRow(_).copy()).map(requiredColumns)
    }
  }

  override def prepareWrite(sparkSession: SparkSession, job: Job, options: Map[String, String], dataSchema: StructType): OutputWriterFactory = throw new UnsupportedOperationException("Write is not supported in this version of package")


}

private[proto]
class SerializableConfiguration(@transient var value: Configuration) extends Serializable {

  private def writeObject(out: ObjectOutputStream): Unit =  {
    out.defaultWriteObject()
    value.write(out)
  }

  private def readObject(in: ObjectInputStream): Unit =  {
    value = new Configuration(false)
    value.readFields(in)
  }
}