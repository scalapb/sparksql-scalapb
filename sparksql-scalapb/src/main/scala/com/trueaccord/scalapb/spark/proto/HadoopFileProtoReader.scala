package com.trueaccord.scalapb.spark.proto

import java.io.Closeable
import java.net.URI

import com.trueaccord.scalapb.{GeneratedMessage, GeneratedMessageCompanion, Message}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.{PartitionedFile, RecordReaderIterator}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
  * Created 10/20/17 4:14 AM
  * Author: ljacobsen
  *
  * This code is copyright (C) 2017 ljacobsen
  *
  */
class HadoopFileProtoReader[T <: GeneratedMessage with Message[T] : GeneratedMessageCompanion](file: PartitionedFile, conf: Configuration) extends Iterator[T] with Closeable with Serializable {
  private var reader: RecordReader[LongWritable,T]= _
  private val iterator = {
    val fileSplit = new FileSplit(new Path(new URI(file.filePath)),file.start,file.length,Array.empty)
    val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
    val hadoopAttemptContext = new TaskAttemptContextImpl(conf, attemptId)
    val inputFormat = new ProtoFileInputFormat[T]
    reader = inputFormat.createRecordReader(fileSplit,hadoopAttemptContext)
    reader.initialize(fileSplit, hadoopAttemptContext)
    new RecordReaderIterator(reader)
  }

  override def hasNext: Boolean = iterator.hasNext

  override def next(): T = iterator.next()

  override def close(): Unit = {
    if (reader!=null) {
      reader.close()
    }
  }
}
