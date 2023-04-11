package scalapb.spark

trait WrapperTypes {}

trait AllWrapperTypes extends WrapperTypes {
  self: ProtoSQL =>
}

trait NoWrapperTypes extends WrapperTypes {}
