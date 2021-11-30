package scalapb.spark.internal

private[spark] object MapHelpers {
  def fromIterator[K, V](it: => Iterator[(K, V)]): Map[K, V] = new Map[K, V] {
    def iterator: Iterator[(K, V)] = it

    def +[V1 >: V](kv: (K, V1)): scala.collection.immutable.Map[K, V1] = ???
    def -(key: K): scala.collection.immutable.Map[K, V] = ???
    def get(key: K): Option[V] = ???
  }
}
