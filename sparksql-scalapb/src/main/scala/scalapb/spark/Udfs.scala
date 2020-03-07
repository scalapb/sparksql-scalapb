package scalapb.spark

import org.apache.spark.sql.Column
import frameless.TypedEncoder
import frameless.TypedColumn
import frameless.functions.FramelessUdf

trait Udfs {
  def udf[RT: TypedEncoder, A1: TypedEncoder](f: A1 => RT): Column => Column = {
    u1 =>
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
}
