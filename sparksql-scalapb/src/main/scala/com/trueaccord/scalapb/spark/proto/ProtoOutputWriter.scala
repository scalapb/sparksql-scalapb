package com.trueaccord.scalapb.spark.proto

import java.io.{OutputStream, OutputStreamWriter}

import com.trueaccord.scalapb.{GeneratedMessage, GeneratedMessageCompanion, Message}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.types.StructType

import scala.reflect.runtime.universe._
import scala.reflect.ClassTag

/**
  * Created 10/24/17 3:46 PM
  * Author: ljacobsen
  *
  * This code is copyright (C) 2017 ljacobsen
  *
  */
private[proto] class ProtoOutputWriter[T <: GeneratedMessage with Message[T]](
                                          path: String,
                                          dataSchema: StructType,
                                          context: TaskAttemptContext, encoder: ExpressionEncoder[T])
  extends OutputWriter with Serializable {

  private val writer = CodecStreams.createOutputStream(context, new Path(path))

  private var internalConverter: Row => InternalRow = CatalystTypeConverters.createToScalaConverter(dataSchema).asInstanceOf[Row => InternalRow]

  override def write(row: Row): Unit = {
    throw new NotImplementedError("This function only gets called by writeInternal, until spark 2.2.0")
  }

  override def close(): Unit = {
    writer.close()
  }

  override def writeInternal(row: InternalRow): Unit = {
    encoder.fromRow(row).writeDelimitedTo(writer)
  }
}