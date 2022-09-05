package scalapb.spark

import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.unsafe.types.UTF8String
import scalapb.descriptors._
import scalapb.{
  GeneratedEnum,
  GeneratedEnumCompanion,
  GeneratedMessage,
  GeneratedMessageCompanion,
  Message
}

object JavaHelpers {
  def enumToString(
      cmp: GeneratedEnumCompanion[_],
      value: GeneratedEnum
  ): UTF8String = {
    UTF8String.fromString(
      if (value.isUnrecognized) value.value.toString
      else value.name
    )
  }

  def penumToString(value: PValue): UTF8String = {
    value match {
      case PEmpty => null
      case PEnum(_) =>
        val ev = value.asInstanceOf[PEnum].value
        if (ev.isUnrecognized)
          UTF8String.fromString(ev.number.toString)
        else
          UTF8String.fromString(ev.name)
      case _ =>
        throw new RuntimeException(
          s"Unexpected type for enum: ${value.getClass}"
        )
    }
  }

  def enumValueFromString[T <: GeneratedEnum](
      cmp: GeneratedEnumCompanion[T],
      inputUtf8: UTF8String
  ): Int = {
    val input = inputUtf8.toString
    cmp.fromName(input) match {
      case Some(r) => r.value
      case None =>
        try {
          input.toInt
        } catch {
          case _: NumberFormatException =>
            throw new RuntimeException(
              s"Unexpected value: ${input} for enum ${cmp.scalaDescriptor.fullName}"
            )
        }
    }
  }

  def penumFromString(
      cmp: GeneratedEnumCompanion[_],
      inputUtf8: UTF8String
  ): PValue = {
    val input = inputUtf8.toString
    cmp.fromName(input) match {
      case Some(r) => PEnum(r.asInstanceOf[GeneratedEnum].scalaValueDescriptor)
      case None =>
        try {
          val enumValue = input.toInt
          PEnum(
            cmp.scalaDescriptor.findValueByNumberCreatingIfUnknown(enumValue)
          )
        } catch {
          case _: NumberFormatException =>
            throw new RuntimeException(
              s"Unexpected value: ${input} for enum ${cmp.scalaDescriptor.fullName}"
            )
        }
    }
  }

  def mkMap(cmp: GeneratedMessageCompanion[_], args: ArrayData): Map[FieldDescriptor, PValue] = {
    cmp.scalaDescriptor.fields
      .zip(args.array)
      .filter {
        case (_, null) | (_, PEmpty) => false
        case _                       => true
      }
      .toMap
      .asInstanceOf[Map[FieldDescriptor, PValue]]
  }

  def mkPRepeated(args: ArrayData): PValue = {
    PRepeated(args.array.toVector.asInstanceOf[Vector[PValue]])
  }

  def mkPRepeatedMap(
      mapEntryCmp: GeneratedMessageCompanion[_],
      args: Vector[(PValue, PValue)]
  ): PValue = {
    val keyDesc = mapEntryCmp.scalaDescriptor.findFieldByNumber(1).get
    val valDesc = mapEntryCmp.scalaDescriptor.findFieldByNumber(2).get
    PRepeated(args.map { case (k, v) =>
      PMessage(Map(keyDesc -> k, valDesc -> v))
    })
  }

  // ExternalMapToCatalyst only needs iterator. We create this view into s to
  // avoid making a copy.
  def mkMap(s: Seq[GeneratedMessage]): Map[Any, Any] =
    scalapb.spark.internal.MapHelpers.fromIterator {
      if (s.isEmpty) Iterator.empty
      else {
        val cmp = s.head.companion
        val keyDesc = cmp.scalaDescriptor.findFieldByNumber(1).get
        val valueDesc = cmp.scalaDescriptor.findFieldByNumber(2).get

        if (valueDesc.scalaType.isInstanceOf[ScalaType.Message])
          s.iterator.map { m =>
            (m.getField(keyDesc), m.getFieldByNumber(valueDesc.number))
          }
        else
          s.iterator.map { m =>
            (m.getField(keyDesc), m.getField(valueDesc))
          }
      }
    }

  def isEmpty(pv: PValue): Boolean = pv == PEmpty

  def intFromPValue(pv: PValue): Int = pv.asInstanceOf[PInt].value

  def longFromPValue(pv: PValue): Long = pv.asInstanceOf[PLong].value

  def stringFromPValue(pv: PValue): UTF8String =
    UTF8String.fromString(pv.asInstanceOf[PString].value)

  def doubleFromPValue(pv: PValue): Double = pv.asInstanceOf[PDouble].value

  def floatFromPValue(pv: PValue): Float = pv.asInstanceOf[PFloat].value

  def byteStringFromPValue(pv: PValue): Array[Byte] =
    pv.asInstanceOf[PByteString].value.toByteArray

  def booleanFromPValue(pv: PValue): Boolean = pv.asInstanceOf[PBoolean].value

  def enumFromPValue(pv: PValue): UTF8String =
    JavaHelpers.penumToString(pv.asInstanceOf[PEnum])

  def vectorFromPValue(p: PValue): Vector[PValue] = {
    p.asInstanceOf[PRepeated].value
  }

  def toPMessageAsPValue(g: GeneratedMessage): PValue = g.toPMessage
}
