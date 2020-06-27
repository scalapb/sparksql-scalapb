package scalapb.spark

import com.google.protobuf.ByteString
import org.scalacheck.Arbitrary
import org.scalacheck.derive.MkArbitrary
import scalapb.spark.test.{all_types2 => AT2}
import scalapb.spark.test3.{all_types3 => AT3}
import scalapb.{GeneratedEnum, GeneratedEnumCompanion, GeneratedMessage, Message}
import shapeless.Strict
import org.scalacheck.Gen
import scalapb.UnknownFieldSet

object ArbitraryProtoUtils {
  import org.scalacheck.ScalacheckShapeless._

  implicit val arbitraryBS = Arbitrary(
    implicitly[Arbitrary[Array[Byte]]].arbitrary
      .map(t => ByteString.copyFrom(t))
  )

  // Default scalacheck-shapeless would chose Unrecognized instances with recognized values.
  private def fixEnum[A <: GeneratedEnum](
      e: A
  )(implicit cmp: GeneratedEnumCompanion[A]): A = {
    if (e.isUnrecognized) cmp.values.find(_.value == e.value).getOrElse(e)
    else e
  }

  def arbitraryEnum[A <: GeneratedEnum: Arbitrary: GeneratedEnumCompanion] = {
    Arbitrary(implicitly[Arbitrary[A]].arbitrary.map(fixEnum(_)))
  }

  implicit val arbitraryUnknownFields = Arbitrary(
    Gen.const(UnknownFieldSet.empty)
  )

  implicit val nestedEnum2 = arbitraryEnum[AT2.EnumTest.NestedEnum]

  implicit val nestedEnum3 = arbitraryEnum[AT3.EnumTest.NestedEnum]

  implicit val topLevelEnum2 = arbitraryEnum[AT2.TopLevelEnum]

  implicit val topLevelEnum3 = arbitraryEnum[AT3.TopLevelEnum]

  implicit def arbitraryMessage[A <: GeneratedMessage](implicit
      ev: Strict[MkArbitrary[A]]
  ) = {
    implicitly[Arbitrary[A]]
  }
}
