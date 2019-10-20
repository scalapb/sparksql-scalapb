package scalapb.spark

import com.google.protobuf.ByteString
import frameless.{TypedEncoder, TypedExpressionEncoder}
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, StaticInvoke}
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Encoder, SQLContext}
import scalapb._

import scala.reflect.ClassTag

class MessageTypedEncoder[T <: GeneratedMessage with Message[T]](
    implicit cmp: GeneratedMessageCompanion[T],
    ct: ClassTag[T]
) extends TypedEncoder[T] {
  override def nullable: Boolean = false

  override def jvmRepr: DataType = ObjectType(ct.runtimeClass)

  override def catalystRepr: DataType = ProtoSQL.schemaFor(cmp)

  def fromCatalyst(path: Expression): Expression = {
    val expr = FromCatalystHelpers.pmessageFromCatalyst(cmp, path)

    val finalExpr = StaticInvoke(
      JavaHelpers.getClass,
      ObjectType(classOf[GeneratedMessage]),
      "fromPMessage",
      Literal.fromObject(cmp) :: expr :: Nil
    )

    finalExpr
  }

  override def toCatalyst(path: Expression): Expression = {
    ToCatalystHelpers.messageToCatalyst(cmp, path)
  }
}

class EnumTypedEncoder[T <: GeneratedEnum](
    implicit cmp: GeneratedEnumCompanion[T],
    ct: ClassTag[T]
) extends TypedEncoder[T] {
  override def nullable: Boolean = false

  override def jvmRepr: DataType = ObjectType(ct.runtimeClass)

  override def catalystRepr: DataType = StringType

  override def fromCatalyst(path: Expression): Expression =
    StaticInvoke(
      JavaHelpers.getClass,
      ObjectType(classOf[GeneratedEnum]),
      "enumFromString",
      Literal.fromObject(cmp) :: path :: Nil
    )

  override def toCatalyst(path: Expression): Expression =
    StaticInvoke(
      JavaHelpers.getClass,
      StringType,
      "enumToString",
      Literal.fromObject(cmp) :: path :: Nil
    )
}

class ByteStringTypedEncoder extends TypedEncoder[ByteString] {
  override def nullable: Boolean = true

  override def jvmRepr: DataType = ObjectType(classOf[ByteString])

  override def catalystRepr: DataType = BinaryType

  override def fromCatalyst(path: Expression): Expression = StaticInvoke(
    classOf[ByteString],
    ObjectType(classOf[ByteString]),
    "copyFrom",
    path :: Nil
  )

  override def toCatalyst(path: Expression): Expression =
    Invoke(path, "toByteArray", BinaryType, Seq.empty)
}

object Implicits {
  implicit def messageTypedEncoder[T <: GeneratedMessage with Message[T]: GeneratedMessageCompanion: ClassTag]
      : MessageTypedEncoder[T] = new MessageTypedEncoder[T]

  implicit def enumTypedEncoder[T <: GeneratedEnum](
      implicit cmp: GeneratedEnumCompanion[T],
      ct: ClassTag[T]
  ) = new EnumTypedEncoder[T]

  implicit val byteStringTypedEncoder = new ByteStringTypedEncoder

  implicit def te[T: ClassTag](implicit ev: TypedEncoder[T]): Encoder[T] =
    TypedExpressionEncoder(ev)
}
