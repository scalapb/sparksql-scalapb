package scalapb.spark

import com.google.protobuf.ByteString
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedExtractValue}
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

trait FromCatalystHelpers {
  def protoSql: ProtoSQL with WrapperTypes with ColumnNaming

  def pmessageFromCatalyst(
      cmp: GeneratedMessageCompanion[_],
      input: Expression
  ): Expression = {
    val args =
      if (protoSql.types.contains(cmp.scalaDescriptor))
        cmp.scalaDescriptor.fields.map { fd =>
          fieldFromCatalyst(cmp, fd, input)
        }
      else
        cmp.scalaDescriptor.fields.map { fd =>
          val newPath = addToPath(input, protoSql.fieldName(fd))
          fieldFromCatalyst(cmp, fd, newPath)
        }
    StaticInvoke(
      JavaHelpers.getClass,
      ObjectType(classOf[PValue]),
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
        protoSql.singularDataType(fd)
      )
      StaticInvoke(
        JavaHelpers.getClass,
        ObjectType(classOf[PValue]),
        "mkPRepeated",
        objs :: Nil
      )
    } else if (fd.isOptional)
      If(
        IsNull(input),
        Literal.fromObject(PEmpty, ObjectType(classOf[PValue])),
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
          ObjectType(classOf[PValue])
        )
      case ScalaType.Enum(_) =>
        val evd = cmp.enumCompanionForFieldNumber(fd.number)
        StaticInvoke(
          JavaHelpers.getClass,
          ObjectType(classOf[PValue]),
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
          ObjectType(classOf[PValue])
        )
      case ScalaType.Int =>
        NewInstance(classOf[PInt], input :: Nil, ObjectType(classOf[PValue]))
      case ScalaType.Long =>
        NewInstance(classOf[PLong], input :: Nil, ObjectType(classOf[PValue]))
      case ScalaType.Double =>
        NewInstance(classOf[PDouble], input :: Nil, ObjectType(classOf[PValue]))
      case ScalaType.Float =>
        NewInstance(classOf[PFloat], input :: Nil, ObjectType(classOf[PValue]))
      case ScalaType.Boolean =>
        NewInstance(
          classOf[PBoolean],
          input :: Nil,
          ObjectType(classOf[PValue])
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
