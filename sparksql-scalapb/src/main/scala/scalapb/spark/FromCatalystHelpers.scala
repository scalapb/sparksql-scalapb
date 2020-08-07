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
import org.apache.spark.sql.catalyst.expressions.objects.CatalystToExternalMap
import org.apache.spark.sql.types.MapType
import org.apache.spark.sql.catalyst.expressions.objects.LambdaVariable

trait FromCatalystHelpers {
  def protoSql: ProtoSQL

  def schemaOptions: SchemaOptions = protoSql.schemaOptions

  def pmessageFromCatalyst(
      cmp: GeneratedMessageCompanion[_],
      input: Expression
  ): Expression = {
    val args =
      if (schemaOptions.isUnpackedPrimitiveWrapper(cmp.scalaDescriptor))
        cmp.scalaDescriptor.fields.map { fd =>
          fieldFromCatalyst(cmp, fd, input)
        }
      else
        cmp.scalaDescriptor.fields.map { fd =>
          val newPath = addToPath(input, schemaOptions.columnNaming.fieldName(fd))
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
    if (fd.isRepeated && !fd.isMapField) {
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
    } else if (fd.isRepeated && fd.isMapField) {
      val mapEntryCmp = cmp.messageCompanionForFieldNumber(fd.number)
      val keyDesc = mapEntryCmp.scalaDescriptor.findFieldByNumber(1).get
      val valDesc = mapEntryCmp.scalaDescriptor.findFieldByNumber(2).get
      val objs = MyCatalystToExternalMap(
        (in: Expression) => singleFieldValueFromCatalyst(mapEntryCmp, keyDesc, in),
        (in: Expression) => singleFieldValueFromCatalyst(mapEntryCmp, valDesc, in),
        input,
        ProtoSQL.dataTypeFor(fd).asInstanceOf[MapType],
        classOf[Vector[(Any, Any)]]
      )
      StaticInvoke(
        JavaHelpers.getClass,
        ObjectType(classOf[PValue]),
        "mkPRepeatedMap",
        Literal.fromObject(mapEntryCmp) ::
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

object MyCatalystToExternalMap {
  private val curId = new java.util.concurrent.atomic.AtomicInteger()

  /**
    * Construct an instance of CatalystToExternalMap case class.
    *
    * @param keyFunction The function applied on the key collection elements.
    * @param valueFunction The function applied on the value collection elements.
    * @param inputData An expression that when evaluated returns a map object.
    * @param mapType,
    * @param collClass The type of the resulting collection.
    */
  def apply(
      keyFunction: Expression => Expression,
      valueFunction: Expression => Expression,
      inputData: Expression,
      mapType: MapType,
      collClass: Class[_]
  ): CatalystToExternalMap = {
    val id = curId.getAndIncrement()
    val keyLoopValue = s"CatalystToExternalMap_keyLoopValue$id"
    val keyLoopVar = LambdaVariable(keyLoopValue, "", mapType.keyType, nullable = false)
    val valueLoopValue = s"CatalystToExternalMap_valueLoopValue$id"
    val valueLoopIsNull = if (mapType.valueContainsNull) {
      s"CatalystToExternalMap_valueLoopIsNull$id"
    } else {
      "false"
    }
    val valueLoopVar = LambdaVariable(valueLoopValue, valueLoopIsNull, mapType.valueType)
    CatalystToExternalMap(
      keyLoopValue,
      keyFunction(keyLoopVar),
      valueLoopValue,
      valueLoopIsNull,
      valueFunction(valueLoopVar),
      inputData,
      collClass
    )
  }
}
