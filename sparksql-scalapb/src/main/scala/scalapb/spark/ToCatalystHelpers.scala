package scalapb.spark

import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, MapObjects, StaticInvoke}
import org.apache.spark.sql.catalyst.expressions.{
  CreateNamedStruct,
  Expression,
  If,
  IsNull,
  Literal
}
import org.apache.spark.sql.types.{BooleanType, IntegerType, ObjectType}
import scalapb.descriptors.{Descriptor, FieldDescriptor, PValue, ScalaType}
import scalapb.GeneratedMessageCompanion

trait ToCatalystHelpers {
  def protoSql: ProtoSQL with WrapperTypes with ColumnNaming

  def messageToCatalyst(
      cmp: GeneratedMessageCompanion[_],
      input: Expression
  ): Expression =
    if (protoSql.types.contains(cmp.scalaDescriptor)) {
      val fd = cmp.scalaDescriptor.fields(0)
      fieldToCatalyst(cmp, fd, input)
    } else {
      val nameExprs = cmp.scalaDescriptor.fields.map { field =>
        Literal(protoSql.fieldName(field))
      }

      val valueExprs = cmp.scalaDescriptor.fields.map { field =>
        fieldToCatalyst(cmp, field, input)
      }

      // the way exprs are encoded in CreateNamedStruct
      val exprs = nameExprs.zip(valueExprs).flatMap {
        case (nameExpr, valueExpr) => nameExpr :: valueExpr :: Nil
      }

      val createExpr = CreateNamedStruct(exprs)
      val nullExpr = Literal.create(null, createExpr.dataType)
      If(IsNull(input), nullExpr, createExpr)
    }

  def fieldToCatalyst(
      cmp: GeneratedMessageCompanion[_],
      fd: FieldDescriptor,
      inputObject: Expression
  ): Expression = {
    def getField =
      Invoke(
        inputObject,
        "getField",
        ObjectType(classOf[PValue]),
        Invoke(
          Invoke(
            Invoke(
              Literal.fromObject(cmp),
              "scalaDescriptor",
              ObjectType(classOf[Descriptor]),
              Nil
            ),
            "findFieldByNumber",
            ObjectType(classOf[Option[_]]),
            Literal(fd.number) :: Nil
          ),
          "get",
          ObjectType(classOf[FieldDescriptor])
        ) :: Nil
      )

    def messageFieldCompanion = cmp.messageCompanionForFieldNumber(fd.number)

    def getFieldByNumber =
      Invoke(
        inputObject,
        "getFieldByNumber",
        if (fd.isRepeated)
          ObjectType(classOf[Seq[_]])
        else
          ObjectType(messageFieldCompanion.defaultInstance.getClass),
        Literal(fd.number, IntegerType) :: Nil
      )

    val isMessage = fd.scalaType.isInstanceOf[ScalaType.Message]

    val (fieldGetter, transform): (Expression, Expression => Expression) =
      if (!isMessage) {
        (getField, { e: Expression => singularFieldToCatalyst(fd, e) })
      } else {
        (
          getFieldByNumber,
          { e: Expression =>
            messageToCatalyst(messageFieldCompanion, e)
          }
        )
      }

    if (fd.isRepeated) {
      if (isMessage)
        MapObjects(
          transform,
          fieldGetter,
          ObjectType(messageFieldCompanion.defaultInstance.getClass)
        )
      else {
        val getter = StaticInvoke(
          JavaHelpers.getClass,
          ObjectType(classOf[Vector[_]]),
          "vectorFromPValue",
          fieldGetter :: Nil
        )
        MapObjects(transform, getter, ObjectType(classOf[PValue]))
      }
    } else {
      if (isMessage) transform(fieldGetter)
      else
        If(
          StaticInvoke(
            JavaHelpers.getClass,
            BooleanType,
            "isEmpty",
            getField :: Nil
          ),
          Literal.create(null, protoSql.dataTypeFor(fd)),
          transform(fieldGetter)
        )
    }
  }

  def singularFieldToCatalyst(
      fd: FieldDescriptor,
      input: Expression
  ): Expression = {
    val obj = fd.scalaType match {
      case ScalaType.Int        => "intFromPValue"
      case ScalaType.Long       => "longFromPValue"
      case ScalaType.Float      => "floatFromPValue"
      case ScalaType.Double     => "doubleFromPValue"
      case ScalaType.Boolean    => "booleanFromPValue"
      case ScalaType.String     => "stringFromPValue"
      case ScalaType.ByteString => "byteStringFromPValue"
      case ScalaType.Enum(_)    => "enumFromPValue"
      case ScalaType.Message(_) =>
        throw new RuntimeException("Should not happen")
    }
    StaticInvoke(
      JavaHelpers.getClass,
      protoSql.singularDataType(fd),
      obj,
      input :: Nil
    )
  }
}
