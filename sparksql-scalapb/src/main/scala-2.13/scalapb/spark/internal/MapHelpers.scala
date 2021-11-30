package scalapb.spark.internal

private[spark] object MapHelpers {
  def fromIterator[K, V](it: => Iterator[(K, V)]): Map[K, V] = new Map[K, V] {
    def iterator: Iterator[(K, V)] = it

    def get(key: K): Option[V] = ???

    // Members declared in scala.collection.immutable.MapOps
    def removed(key: K): scala.collection.immutable.Map[K, V] = ???
    def updated[V1 >: V](key: K, value: V1): scala.collection.immutable.Map[K, V1] = ???
  }
}
