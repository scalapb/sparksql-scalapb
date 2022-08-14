package scalapb.spark

import com.google.protobuf.ByteString
import frameless.{TypedEncoder, TypedExpressionEncoder}
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, StaticInvoke}
import org.apache.spark.sql.catalyst.expressions.{Expression, If, IsNull, Literal}
import org.apache.spark.sql.types._
import scalapb._
import scalapb.descriptors.{PValue, Reads}

import scala.reflect.ClassTag
import org.apache.spark.sql.catalyst.expressions.CreateNamedStruct
import org.apache.spark.sql.catalyst.expressions.BoundReference
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.catalyst.WalkedTypePath
import org.apache.spark.sql.catalyst.expressions.objects.NewInstance
import scalapb.descriptors.PMessage
import org.apache.spark.sql.catalyst.expressions.CreateArray

trait TypedEncoders extends FromCatalystHelpers with ToCatalystHelpers with Serializable {
  class MessageTypedEncoder[T <: GeneratedMessage](implicit
      cmp: GeneratedMessageCompanion[T],
      ct: ClassTag[T]
  ) extends TypedEncoder[T] {
    override def nullable: Boolean = false

    override def jvmRepr: DataType = ObjectType(ct.runtimeClass)

    override def catalystRepr: DataType = protoSql.schemaFor(cmp)

    def fromCatalyst(path: Expression): Expression = {
      val expr = pmessageFromCatalyst(cmp, path)

      val reads = Invoke(
        Literal.fromObject(cmp),
        "messageReads",
        ObjectType(classOf[Reads[_]]),
        Nil
      )

      val read = Invoke(reads, "read", ObjectType(classOf[Function[_, _]]))

      val ret = Invoke(read, "apply", ObjectType(ct.runtimeClass), expr :: Nil)
      ret
    }

    override def toCatalyst(path: Expression): Expression = {
      val ret = messageToCatalyst(cmp, path)
      ret
    }
  }

  class EnumTypedEncoder[T <: GeneratedEnum](implicit
      cmp: GeneratedEnumCompanion[T],
      ct: ClassTag[T]
  ) extends TypedEncoder[T] {
    override def nullable: Boolean = false

    override def jvmRepr: DataType = ObjectType(ct.runtimeClass)

    override def catalystRepr: DataType = StringType

    override def fromCatalyst(path: Expression): Expression = {
      val expr = Invoke(
        Literal.fromObject(cmp),
        "fromValue",
        ObjectType(ct.runtimeClass),
        StaticInvoke(
          JavaHelpers.getClass,
          IntegerType,
          "enumValueFromString",
          Literal.fromObject(cmp) :: path :: Nil
        ) :: Nil
      )
      If(IsNull(path), Literal.create(null, expr.dataType), expr)
    }

    override def toCatalyst(path: Expression): Expression =
      StaticInvoke(
        JavaHelpers.getClass,
        StringType,
        "enumToString",
        Literal.fromObject(cmp) :: path :: Nil
      )
  }

  object ByteStringTypedEncoder extends TypedEncoder[ByteString] {
    override def nullable: Boolean = false

    override def jvmRepr: DataType = ObjectType(classOf[ByteString])

    override def catalystRepr: DataType = BinaryType

    override def fromCatalyst(path: Expression): Expression =
      StaticInvoke(
        classOf[ByteString],
        ObjectType(classOf[ByteString]),
        "copyFrom",
        path :: Nil
      )

    override def toCatalyst(path: Expression): Expression =
      Invoke(path, "toByteArray", BinaryType, Seq.empty)
  }
}

trait Implicits {
  private[scalapb] val typedEncoders: TypedEncoders

  implicit def messageTypedEncoder[
      T <: GeneratedMessage: GeneratedMessageCompanion: ClassTag
  ]: TypedEncoder[T] = new typedEncoders.MessageTypedEncoder[T]

  implicit def enumTypedEncoder[T <: GeneratedEnum](implicit
      cmp: GeneratedEnumCompanion[T],
      ct: ClassTag[T]
  ): TypedEncoder[T] = new typedEncoders.EnumTypedEncoder[T]

  implicit def byteStringTypedEncoder = typedEncoders.ByteStringTypedEncoder

  implicit def typedEncoderToEncoder[T: ClassTag](implicit
      ev: TypedEncoder[T]
  ): Encoder[T] =
    TypedExpressionEncoder(ev)
}

object Implicits extends Implicits {
  private[scalapb] val typedEncoders: TypedEncoders = new TypedEncoders {
    @transient
    lazy val protoSql = ProtoSQL
  }
}
