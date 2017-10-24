package com.trueaccord.scalapb.spark.proto

import com.trueaccord.scalapb.{GeneratedMessage, GeneratedMessageCompanion, Message}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.{InputSplit, JobContext, RecordReader, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.spark.internal.Logging

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
  * Created 10/20/17 4:19 AM
  * Author: ljacobsen
  *
  * This code is copyright (C) 2017 ljacobsen
  *
  */
class ProtoFileInputFormat[T <: GeneratedMessage with Message[T] : GeneratedMessageCompanion] extends FileInputFormat[LongWritable,T] with Serializable {

  override def isSplitable(context: JobContext, filename: Path): Boolean = false

  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[LongWritable, T] = new ProtoRecordReader[T]

}
