package org.apache.spark.scalapb_hack

import org.apache.spark.sql.catalyst.ScalaReflection
import scalapb.{GeneratedEnum, GeneratedEnumCompanion}
import org.apache.spark.sql.types.{
  DataType,
  StringType,
  UDTRegistration,
  UserDefinedType
}
import org.apache.spark.unsafe.types.UTF8String

import scala.reflect.ClassTag

class GeneratedEnumUDT[T >: Null <: GeneratedEnum](
    implicit cmp: GeneratedEnumCompanion[T],
    ct: ClassTag[T]
) extends UserDefinedType[T] {
  override def sqlType: DataType = StringType

  override def serialize(obj: T): Any = UTF8String.fromString(obj.name)

  override def deserialize(datum: Any): T =
    cmp.fromName(datum.asInstanceOf[UTF8String].toString).get

  override def userClass: Class[T] = ct.runtimeClass.asInstanceOf[Class[T]]
}

object GeneratedEnumUDT {
  def register(a: String, b: String) = {
    UDTRegistration.register(a, b)
  }

  def register[A: ScalaReflection.universe.TypeTag: ClassTag, B: ClassTag]() = {
    // We have to register both under the runtime class name and through `getClassNameFromType`
    // for spark to find it. This roughly changes $ in the name with. The need for this is
    // demonstrated in the test (removing one of these lines would fail a test)
    UDTRegistration.register(
      scala.reflect.classTag[A].runtimeClass.getName,
      scala.reflect.classTag[B].runtimeClass.getName
    )

    UDTRegistration.register(
      ScalaReflection.getClassNameFromType(ScalaReflection.localTypeOf[A]),
      scala.reflect.classTag[B].runtimeClass.getName
    )
  }
}
