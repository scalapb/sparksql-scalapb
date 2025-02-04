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
import org.apache.spark.sql.catalyst.expressions.objects.ExternalMapToCatalyst
import scalapb.GeneratedMessage

trait ToCatalystHelpers {
  def protoSql: ProtoSQL

  def schemaOptions: SchemaOptions

  def messageToCatalyst(
      cmp: GeneratedMessageCompanion[?],
      input: Expression
  ): Expression = {
    schemaOptions.messageEncoders.get(cmp.scalaDescriptor) match {
      case Some(encoder) =>
        encoder.toCatalyst(input)
      case None =>
        if (protoSql.schemaOptions.isUnpackedPrimitiveWrapper(cmp.scalaDescriptor)) {
          val fd = cmp.scalaDescriptor.fields(0)
          fieldToCatalyst(cmp, fd, input)
        } else {
          val nameExprs = cmp.scalaDescriptor.fields.map { field =>
            Literal(schemaOptions.columnNaming.fieldName(field))
          }

          val valueExprs = cmp.scalaDescriptor.fields.map { field =>
            fieldToCatalyst(cmp, field, input)
          }

          // the way exprs are encoded in CreateNamedStruct
          val exprs = nameExprs.zip(valueExprs).flatMap { case (nameExpr, valueExpr) =>
            nameExpr :: valueExpr :: Nil
          }

          val createExpr = CreateNamedStruct(exprs)
          val nullExpr = Literal.create(null, createExpr.dataType)
          If(IsNull(input), nullExpr, createExpr)
        }
    }
  }

  def fieldGetterAndTransformer(
      cmp: GeneratedMessageCompanion[?],
      fd: FieldDescriptor
  ): (Expression => Expression, Expression => Expression) = {
    def messageFieldCompanion = cmp.messageCompanionForFieldNumber(fd.number)

    val isMessage = fd.scalaType.isInstanceOf[ScalaType.Message]

    def getField(inputObject: Expression): Expression =
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
            ObjectType(classOf[Option[?]]),
            Literal(fd.number) :: Nil
          ),
          "get",
          ObjectType(classOf[FieldDescriptor])
        ) :: Nil
      )

    def getFieldByNumber(inputObject: Expression): Expression =
      Invoke(
        inputObject,
        "getFieldByNumber",
        if (fd.isRepeated)
          ObjectType(classOf[Seq[?]])
        else
          ObjectType(messageFieldCompanion.defaultInstance.getClass),
        Literal(fd.number, IntegerType) :: Nil
      )

    if (!isMessage) {
      (getField, { (e: Expression) => singularFieldToCatalyst(fd, e) })
    } else {
      (
        getFieldByNumber,
        { (e: Expression) =>
          messageToCatalyst(messageFieldCompanion, e)
        }
      )
    }
  }

  def fieldToCatalyst(
      cmp: GeneratedMessageCompanion[?],
      fd: FieldDescriptor,
      inputObject: Expression
  ): Expression = {

    val isMessage = fd.scalaType.isInstanceOf[ScalaType.Message]

    val (fieldGetter, transform) = fieldGetterAndTransformer(cmp, fd)

    def messageFieldCompanion = cmp.messageCompanionForFieldNumber(fd.number)

    if (fd.isRepeated) {
      if (fd.isMapField) {
        val keyDesc =
          fd.scalaType.asInstanceOf[ScalaType.Message].descriptor.findFieldByNumber(1).get
        val valueDesc =
          fd.scalaType.asInstanceOf[ScalaType.Message].descriptor.findFieldByNumber(2).get
        val (_, valueTransform) =
          fieldGetterAndTransformer(cmp.messageCompanionForFieldNumber(fd.number), valueDesc)
        val valueType = valueDesc.scalaType match {
          case ScalaType.Message(_) => ObjectType(classOf[GeneratedMessage])
          case _                    => ObjectType(classOf[PValue])
        }

        ExternalMapToCatalyst(
          StaticInvoke(
            JavaHelpers.getClass,
            ObjectType(classOf[Map[?, ?]]),
            "mkMap",
            fieldGetter(inputObject) :: Nil
          ),
          ObjectType(classOf[PValue]),
          singularFieldToCatalyst(keyDesc, _),
          false,
          valueType,
          valueTransform,
          true
        )
      } else if (isMessage)
        MapObjects(
          transform,
          fieldGetter(inputObject),
          ObjectType(messageFieldCompanion.defaultInstance.getClass)
        )
      else {
        val getter = StaticInvoke(
          JavaHelpers.getClass,
          ObjectType(classOf[Vector[?]]),
          "vectorFromPValue",
          fieldGetter(inputObject) :: Nil
        )
        MapObjects(transform, getter, ObjectType(classOf[PValue]))
      }
    } else {
      if (isMessage) transform(fieldGetter(inputObject))
      else
        If(
          StaticInvoke(
            JavaHelpers.getClass,
            BooleanType,
            "isEmpty",
            fieldGetter(inputObject) :: Nil
          ),
          Literal.create(null, protoSql.dataTypeFor(fd)),
          transform(fieldGetter(inputObject))
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
