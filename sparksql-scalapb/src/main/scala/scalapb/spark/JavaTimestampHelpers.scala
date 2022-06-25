package scalapb.spark

import scalapb.descriptors.{PInt, PLong, PValue}

object JavaTimestampHelpers {

  def extractSecondsFromMicrosTimestamp(tsMicros: Long): PValue = {
    PLong(tsMicros / 1000000)
  }

  def extractNanosFromMicrosTimestamp(tsMicros: Long): PValue = {
    val micros = tsMicros % 1000000
    PInt((micros * 1000).toInt)
  }

  def combineSecondsAndNanosIntoMicrosTimestamp(seconds: Long, nanos: Int): Long = {
    seconds * 1000000 + nanos / 1000
  }

}
