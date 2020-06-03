package scalapb.spark

import com.example.protos.demo.{Address}

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.expressions.Aggregator
import scalapb.spark.Implicits._

object AddressAggregator extends Aggregator[Address, Address, Address] {

  override def zero: Address = Address()

  override def reduce(b: Address, a: Address): Address = {
    merge(b, a)
  }

  override def merge(b1: Address, b2: Address): Address = { b1 }

  override def finish(a: Address): Address = { a }

  override def bufferEncoder: Encoder[Address] = implicitly[Encoder[Address]]
  override def outputEncoder: Encoder[Address] = implicitly[Encoder[Address]]
}
