package com.trueaccord.scalapb.spark.proto

import java.io.{InputStream, OutputStream, OutputStreamWriter}
import java.nio.charset.{Charset, StandardCharsets}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress._
import org.apache.hadoop.mapreduce.JobContext
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.ReflectionUtils

import org.apache.spark.TaskContext

object CodecStreams {
  private def getDecompressionCodec(config: Configuration, file: Path): Option[CompressionCodec] = {
    val compressionCodecs = new CompressionCodecFactory(config)
    Option(compressionCodecs.getCodec(file))
  }

  def createInputStream(config: Configuration, file: Path): InputStream = {
    val fs = file.getFileSystem(config)
    val inputStream: InputStream = fs.open(file)

    getDecompressionCodec(config, file)
      .map(codec => codec.createInputStream(inputStream))
      .getOrElse(inputStream)
  }

  /**
    * Creates an input stream from the string path and add a closure for the input stream to be
    * closed on task completion.
    */
  def createInputStreamWithCloseResource(config: Configuration, path: String): InputStream = {
    val inputStream = createInputStream(config, new Path(path))
    Option(TaskContext.get()).foreach(_.addTaskCompletionListener(_ => inputStream.close()))
    inputStream
  }

  private def getCompressionCodec(
                                   context: JobContext,
                                   file: Option[Path] = None): Option[CompressionCodec] = {
    if (FileOutputFormat.getCompressOutput(context)) {
      val compressorClass = FileOutputFormat.getOutputCompressorClass(
        context,
        classOf[GzipCodec])

      Some(ReflectionUtils.newInstance(compressorClass, context.getConfiguration))
    } else {
      file.flatMap { path =>
        val compressionCodecs = new CompressionCodecFactory(context.getConfiguration)
        Option(compressionCodecs.getCodec(path))
      }
    }
  }

  /**
    * Create a new file and open it for writing.
    * If compression is enabled in the [[JobContext]] the stream will write compressed data to disk.
    * An exception will be thrown if the file already exists.
    */
  def createOutputStream(context: JobContext, file: Path): OutputStream = {
    val fs = file.getFileSystem(context.getConfiguration)
    val outputStream: OutputStream = fs.create(file, false)

    getCompressionCodec(context, Some(file))
      .map(codec => codec.createOutputStream(outputStream))
      .getOrElse(outputStream)
  }

  def createOutputStreamWriter(
                                context: JobContext,
                                file: Path,
                                charset: Charset = StandardCharsets.UTF_8): OutputStreamWriter = {
    new OutputStreamWriter(createOutputStream(context, file), charset)
  }

  /** Returns the compression codec extension to be used in a file name, e.g. ".gzip"). */
  def getCompressionExtension(context: JobContext): String = {
    getCompressionCodec(context)
      .map(_.getDefaultExtension)
      .getOrElse("")
  }
}