package com.trueaccord.scalapb.spark.proto

import java.io.{BufferedInputStream, FilterInputStream, InputStream}

import com.trueaccord.scalapb.{GeneratedMessage, GeneratedMessageCompanion, Message}
import org.apache.commons.io.input.CountingInputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.compress.{CompressionCodec, CompressionCodecFactory}
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.spark.internal.Logging

import scala.reflect.runtime.{universe => ru}
import com.trueaccord.scalapb.GeneratedMessageCompanion

import scala.util.control.NonFatal
import scala.util.{Failure, Try}
import scala.collection.JavaConverters._
import scala.reflect.ClassTag

/**
  * Created 10/20/17 4:23 AM
  * Author: ljacobsen
  *
  * This code is copyright (C) 2017 ljacobsen
  *
  */
private[proto] class ProtoRecordReader[T <: GeneratedMessage with Message[T] : GeneratedMessageCompanion](implicit companion: GeneratedMessageCompanion[T]) extends RecordReader[LongWritable, T] with Serializable {

  private var splitStart: Long = 0L
  private var splitEnd: Long = 0L
  private var currentPosition: Long = 0L
  private var fileInputStream: CountingInputStream = _
  private var recordKey: LongWritable = _
  private var recordValue: T = _

  override def close() {
    if (fileInputStream != null) {
      fileInputStream.close()
    }
  }

  override def getCurrentKey: LongWritable = {
    recordKey
  }

  override def getCurrentValue: T = {
    recordValue
  }

  override def getProgress: Float = {
    splitStart match {
      case x if x == splitEnd => 0.0.toFloat
      case _ => Math.min(
        ((currentPosition - splitStart) / (splitEnd - splitStart)).toFloat, 1.0
      ).toFloat
    }
  }

  override def initialize(inputSplit: InputSplit, context: TaskAttemptContext) {
    // the file input
    val fileSplit = inputSplit.asInstanceOf[FileSplit]

    // the actual file we will be reading from
    val path = fileSplit.getPath
    // job configuration
    val conf = context.getConfiguration
    // check compression
    val compressionCodec = Option(new CompressionCodecFactory(conf).getCodec(path))

    // the byte position this fileSplit starts at
    splitStart = fileSplit.getStart

    // splitEnd byte marker that the fileSplit ends at
    splitEnd = compressionCodec.map( _ => Long.MaxValue).getOrElse(splitStart + fileSplit.getLength)

    // get the filesystem
    val fs = path.getFileSystem(conf)
    // open the File
    fileInputStream = new CountingInputStream({
      compressionCodec match {
        case Some(codec) => codec.createInputStream(fs.open(path))
        case None => fs.open(path)
      }
    })

    // seek to the splitStart position
    currentPosition = splitStart
  }

  override def nextKeyValue(): Boolean = {
    if (recordKey == null) {
      recordKey = new LongWritable()
    }
    // the key is a linear index of the record, given by the
    // position the record starts divided by the record length
    recordKey.set(currentPosition)
    // read a record if the currentPosition is less than the split end
    companion.parseDelimitedFrom(fileInputStream) match {
      case Some(r) => {
        recordValue = r
        currentPosition = fileInputStream.getByteCount
        true
      }
      case None => false
    }
  }

//  private def firstStreamOf(input: BufferedInputStream, streamGens: List[Any => InputStream]): Try[InputStream] =
//    streamGens match {
//      case x :: xs =>
//        Try({
//          val s = x(())
//          s.read()
//          s.reset()
//          s
//        }).recoverWith {
//          case NonFatal(_) =>
//            // reset in case of failure
//            input.reset()
//            firstStreamOf(input,xs)
//        }
//      case Nil =>
//        // shouldn't get to this line because of last streamGens element
//        Failure(new Exception)
//    }
//
//  private def tryDecompressStream(input: FilterInputStream, conf: Configuration): Try[CountingInputStream] = {
//    val codecs = CompressionCodecFactory.getCodecClasses(conf)
//    val ccf = new CompressionCodecFactory(conf)
//    val bis = new BufferedInputStream(input)
//    val streamGens: List[Any => InputStream] = codecs.asScala.map(_.getCanonicalName).map(ccf.getCodecByClassName).map(c => {_: Any => c.createInputStream(bis)}).toList ++ Seq({_: Any => bis})
//    bis.mark(16)
//    firstStreamOf(bis,streamGens).map(new CountingInputStream(_))
//  }
}
