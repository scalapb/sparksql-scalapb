package scalapb.spark

import org.apache.spark.sql.Column
import frameless.TypedEncoder
import frameless.TypedColumn
import frameless.functions.FramelessUdf

trait Udfs {
  def udf[RT: TypedEncoder, A1: TypedEncoder](f: A1 => RT): Column => Column = { u1 =>
    val inputs = new TypedColumn[Any, A1](u1) :: Nil
    new Column(
      FramelessUdf(f, inputs, TypedEncoder[RT])
    )
  }

  def udf[RT: TypedEncoder, A1: TypedEncoder, A2: TypedEncoder](
      f: (A1, A2) => RT
  ): (Column, Column) => Column = { (u1, u2) =>
    val inputs =
      new TypedColumn[Any, A1](u1) ::
        new TypedColumn[Any, A2](u2) ::
        Nil
    new Column(
      FramelessUdf(f, inputs, TypedEncoder[RT])
    )
  }

  def udf[
      RT: TypedEncoder,
      A1: TypedEncoder,
      A2: TypedEncoder,
      A3: TypedEncoder
  ](
      f: (A1, A2, A3) => RT
  ): (Column, Column, Column) => Column = { (u1, u2, u3) =>
    val inputs =
      new TypedColumn[Any, A1](u1) ::
        new TypedColumn[Any, A2](u2) ::
        new TypedColumn[Any, A3](u3) ::
        Nil
    new Column(
      FramelessUdf(f, inputs, TypedEncoder[RT])
    )
  }

  def udf[
      RT: TypedEncoder,
      A1: TypedEncoder,
      A2: TypedEncoder,
      A3: TypedEncoder,
      A4: TypedEncoder
  ](
      f: (A1, A2, A3, A4) => RT
  ): (Column, Column, Column, Column) => Column = { (u1, u2, u3, u4) =>
    val inputs =
      new TypedColumn[Any, A1](u1) ::
        new TypedColumn[Any, A2](u2) ::
        new TypedColumn[Any, A3](u3) ::
        new TypedColumn[Any, A4](u4) ::
        Nil
    new Column(
      FramelessUdf(f, inputs, TypedEncoder[RT])
    )
  }

  def udf[
      RT: TypedEncoder,
      A1: TypedEncoder,
      A2: TypedEncoder,
      A3: TypedEncoder,
      A4: TypedEncoder,
      A5: TypedEncoder
  ](
      f: (A1, A2, A3, A4, A5) => RT
  ): (Column, Column, Column, Column, Column) => Column = { (u1, u2, u3, u4, u5) =>
    val inputs =
      new TypedColumn[Any, A1](u1) ::
        new TypedColumn[Any, A2](u2) ::
        new TypedColumn[Any, A3](u3) ::
        new TypedColumn[Any, A4](u4) ::
        new TypedColumn[Any, A5](u5) ::
        Nil
    new Column(
      FramelessUdf(f, inputs, TypedEncoder[RT])
    )
  }

  def udf[
      RT: TypedEncoder,
      A1: TypedEncoder,
      A2: TypedEncoder,
      A3: TypedEncoder,
      A4: TypedEncoder,
      A5: TypedEncoder,
      A6: TypedEncoder
  ](
      f: (A1, A2, A3, A4, A5, A6) => RT
  ): (Column, Column, Column, Column, Column, Column) => Column = { (u1, u2, u3, u4, u5, u6) =>
    val inputs =
      new TypedColumn[Any, A1](u1) ::
        new TypedColumn[Any, A2](u2) ::
        new TypedColumn[Any, A3](u3) ::
        new TypedColumn[Any, A4](u4) ::
        new TypedColumn[Any, A5](u5) ::
        new TypedColumn[Any, A6](u6) ::
        Nil
    new Column(
      FramelessUdf(f, inputs, TypedEncoder[RT])
    )
  }

  def udf[
      RT: TypedEncoder,
      A1: TypedEncoder,
      A2: TypedEncoder,
      A3: TypedEncoder,
      A4: TypedEncoder,
      A5: TypedEncoder,
      A6: TypedEncoder,
      A7: TypedEncoder
  ](
      f: (A1, A2, A3, A4, A5, A6, A7) => RT
  ): (Column, Column, Column, Column, Column, Column, Column) => Column = {
    (u1, u2, u3, u4, u5, u6, u7) =>
      val inputs =
        new TypedColumn[Any, A1](u1) ::
          new TypedColumn[Any, A2](u2) ::
          new TypedColumn[Any, A3](u3) ::
          new TypedColumn[Any, A4](u4) ::
          new TypedColumn[Any, A5](u5) ::
          new TypedColumn[Any, A6](u6) ::
          new TypedColumn[Any, A7](u7) ::
          Nil
      new Column(
        FramelessUdf(f, inputs, TypedEncoder[RT])
      )
  }

  def udf[
      RT: TypedEncoder,
      A1: TypedEncoder,
      A2: TypedEncoder,
      A3: TypedEncoder,
      A4: TypedEncoder,
      A5: TypedEncoder,
      A6: TypedEncoder,
      A7: TypedEncoder,
      A8: TypedEncoder
  ](
      f: (A1, A2, A3, A4, A5, A6, A7, A8) => RT
  ): (
      Column,
      Column,
      Column,
      Column,
      Column,
      Column,
      Column,
      Column
  ) => Column = { (u1, u2, u3, u4, u5, u6, u7, u8) =>
    val inputs =
      new TypedColumn[Any, A1](u1) ::
        new TypedColumn[Any, A2](u2) ::
        new TypedColumn[Any, A3](u3) ::
        new TypedColumn[Any, A4](u4) ::
        new TypedColumn[Any, A5](u5) ::
        new TypedColumn[Any, A6](u6) ::
        new TypedColumn[Any, A7](u7) ::
        new TypedColumn[Any, A8](u8) ::
        Nil
    new Column(
      FramelessUdf(f, inputs, TypedEncoder[RT])
    )
  }

  def udf[
      RT: TypedEncoder,
      A1: TypedEncoder,
      A2: TypedEncoder,
      A3: TypedEncoder,
      A4: TypedEncoder,
      A5: TypedEncoder,
      A6: TypedEncoder,
      A7: TypedEncoder,
      A8: TypedEncoder,
      A9: TypedEncoder
  ](
      f: (A1, A2, A3, A4, A5, A6, A7, A8, A9) => RT
  ): (
      Column,
      Column,
      Column,
      Column,
      Column,
      Column,
      Column,
      Column,
      Column
  ) => Column = { (u1, u2, u3, u4, u5, u6, u7, u8, u9) =>
    val inputs =
      new TypedColumn[Any, A1](u1) ::
        new TypedColumn[Any, A2](u2) ::
        new TypedColumn[Any, A3](u3) ::
        new TypedColumn[Any, A4](u4) ::
        new TypedColumn[Any, A5](u5) ::
        new TypedColumn[Any, A6](u6) ::
        new TypedColumn[Any, A7](u7) ::
        new TypedColumn[Any, A8](u8) ::
        new TypedColumn[Any, A9](u9) ::
        Nil
    new Column(
      FramelessUdf(f, inputs, TypedEncoder[RT])
    )
  }

  def udf[
      RT: TypedEncoder,
      A1: TypedEncoder,
      A2: TypedEncoder,
      A3: TypedEncoder,
      A4: TypedEncoder,
      A5: TypedEncoder,
      A6: TypedEncoder,
      A7: TypedEncoder,
      A8: TypedEncoder,
      A9: TypedEncoder,
      A10: TypedEncoder
  ](
      f: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10) => RT
  ): (
      Column,
      Column,
      Column,
      Column,
      Column,
      Column,
      Column,
      Column,
      Column,
      Column
  ) => Column = { (u1, u2, u3, u4, u5, u6, u7, u8, u9, u10) =>
    val inputs =
      new TypedColumn[Any, A1](u1) ::
        new TypedColumn[Any, A2](u2) ::
        new TypedColumn[Any, A3](u3) ::
        new TypedColumn[Any, A4](u4) ::
        new TypedColumn[Any, A5](u5) ::
        new TypedColumn[Any, A6](u6) ::
        new TypedColumn[Any, A7](u7) ::
        new TypedColumn[Any, A8](u8) ::
        new TypedColumn[Any, A9](u9) ::
        new TypedColumn[Any, A10](u10) ::
        Nil
    new Column(
      FramelessUdf(f, inputs, TypedEncoder[RT])
    )
  }
}
