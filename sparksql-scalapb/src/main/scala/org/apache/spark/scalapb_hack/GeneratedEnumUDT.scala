package org.apache.spark.scalapb_hack

import com.trueaccord.scalapb.{ GeneratedEnum, GeneratedEnumCompanion }
import org.apache.spark.sql.types.{ DataType, StringType, UDTRegistration, UserDefinedType }

import scala.reflect.ClassTag

class GeneratedEnumUDT[T >: Null <: GeneratedEnum](implicit cmp: GeneratedEnumCompanion[T], ct: ClassTag[T]) extends UserDefinedType[T] {
  override def sqlType: DataType = StringType

  override def serialize(obj: T): Any = obj.name

  override def deserialize(datum: Any): T = cmp.fromName(datum.asInstanceOf[String]).get

  override def userClass: Class[T] = ct.runtimeClass.asInstanceOf[Class[T]]
}

object GeneratedEnumUDT {
  def register(a: String, b: String) = {
    UDTRegistration.register(a, b)
  }
}
