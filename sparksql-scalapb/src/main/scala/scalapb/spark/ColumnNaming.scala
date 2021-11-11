package scalapb.spark

import scalapb.descriptors.FieldDescriptor

trait ColumnNaming {
  def fieldName(field: FieldDescriptor): String
}

trait ProtoColumnNaming extends ColumnNaming {
  self: ProtoSQL =>
  override def fieldName(field: FieldDescriptor): String = field.name
}

trait ScalaColumnNaming extends ColumnNaming {
  self: ProtoSQL =>
  override def fieldName(field: FieldDescriptor): String = field.scalaName
}
