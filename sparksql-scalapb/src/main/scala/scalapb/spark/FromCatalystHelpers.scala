package scalapb.spark

import com.google.protobuf.ByteString
import org.apache.spark.sql.catalyst.analysis.{
  UnresolvedAttribute,
  UnresolvedExtractValue
}
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.objects.{
  Invoke,
  MapObjects,
  NewInstance,
  StaticInvoke
}
import org.apache.spark.sql.catalyst.expressions.{
  BoundReference,
  CreateArray,
  Expression,
  If,
  IsNull,
  Literal
}
import org.apache.spark.sql.types.ObjectType
import scalapb.GeneratedMessageCompanion
import scalapb.descriptors._

object FromCatalystHelpers {
  def pmessageFromCatalyst(
      cmp: GeneratedMessageCompanion[_],
      input: Expression
  ): Expression = {
    val args = cmp.scalaDescriptor.fields.map { fd =>
      val newPath = addToPath(input, fd.name)
      fieldFromCatalyst(cmp, fd, newPath)
    }
    StaticInvoke(
      JavaHelpers.getClass,
      ObjectType(classOf[Object]),
      "mkPMessage",
      Literal.fromObject(cmp) :: CreateArray(args) :: Nil
    )
  }

  def fieldFromCatalyst(
      cmp: GeneratedMessageCompanion[_],
      fd: FieldDescriptor,
      input: Expression
  ): Expression = {
    if (fd.isRepeated) {
      val objs = MapObjects(
        (input: Expression) => singleFieldValueFromCatalyst(cmp, fd, input),
        input,
        ProtoSQL.singularDataType(fd)
      )
      StaticInvoke(
        JavaHelpers.getClass,
        ObjectType(classOf[Object]),
        "mkPRepeated",
        objs :: Nil
      )
    } else if (fd.isOptional)
      If(
        IsNull(input),
        Literal.fromObject(PEmpty, ObjectType(classOf[Any])),
        singleFieldValueFromCatalyst(cmp, fd, input)
      )
    else singleFieldValueFromCatalyst(cmp, fd, input)
  }

  def singleFieldValueFromCatalyst(
      cmp: GeneratedMessageCompanion[_],
      fd: FieldDescriptor,
      input: Expression
  ): Expression = {
    fd.scalaType match {
      case ScalaType.ByteString =>
        val bs = StaticInvoke(
          classOf[ByteString],
          ObjectType(classOf[ByteString]),
          "copyFrom",
          input :: Nil
        )
        NewInstance(
          classOf[PByteString],
          bs :: Nil,
          ObjectType(classOf[Object])
        )
      case ScalaType.Enum(_) =>
        val evd = cmp.enumCompanionForFieldNumber(fd.number)
        StaticInvoke(
          JavaHelpers.getClass,
          ObjectType(classOf[Object]),
          "penumFromString",
          Literal.fromObject(evd) :: input :: Nil
        )
      case ScalaType.Message(_) =>
        pmessageFromCatalyst(
          cmp.messageCompanionForFieldNumber(fd.number),
          input
        )
      case ScalaType.String =>
        val asString = Invoke(input, "toString", ObjectType(classOf[String]))
        NewInstance(
          classOf[PString],
          asString :: Nil,
          ObjectType(classOf[Object])
        )
      case ScalaType.Int =>
        NewInstance(classOf[PInt], input :: Nil, ObjectType(classOf[Object]))
      case ScalaType.Long =>
        NewInstance(classOf[PLong], input :: Nil, ObjectType(classOf[Object]))
      case ScalaType.Double =>
        NewInstance(classOf[PDouble], input :: Nil, ObjectType(classOf[Object]))
      case ScalaType.Float =>
        NewInstance(classOf[PFloat], input :: Nil, ObjectType(classOf[Object]))
      case ScalaType.Boolean =>
        NewInstance(
          classOf[PBoolean],
          input :: Nil,
          ObjectType(classOf[Object])
        )
    }
  }

  def addToPath(path: Expression, name: String): Expression = {
    val res = path match {
      case _: BoundReference =>
        UnresolvedAttribute.quoted(name)
      case _ =>
        UnresolvedExtractValue(path, expressions.Literal(name))
    }
    res
  }
}
