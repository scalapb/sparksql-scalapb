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

  def enumFromString[T <: GeneratedEnum](
      cmp: GeneratedEnumCompanion[T],
      inputUtf8: UTF8String
  ): T = {
    val input = inputUtf8.toString
    cmp.fromName(input) match {
      case Some(r) => r.asInstanceOf[T]
      case None =>
        try {
          val enumValue = input.toInt
          cmp.fromValue(enumValue)
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
  ): Any = {
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

  def fromPMessage[T <: GeneratedMessage with Message[T]](
      cmp: GeneratedMessageCompanion[T],
      input: Object
  ): GeneratedMessage = {
    cmp.messageReads
      .read(input.asInstanceOf[PMessage])
  }

  def mkPMessage(cmp: GeneratedMessageCompanion[_], args: ArrayData): Any = {
    // returning Any to ensure the any-value doesn't get unwrapped in runtime.
    PMessage(
      cmp.scalaDescriptor.fields
        .zip(args.array)
        .filterNot(_._2 == PEmpty)
        .toMap
        .asInstanceOf[Map[FieldDescriptor, PValue]]
    )
  }

  def mkPRepeated(args: ArrayData): Any = {
    PRepeated(args.array.toVector.asInstanceOf[Vector[PValue]])
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
}
