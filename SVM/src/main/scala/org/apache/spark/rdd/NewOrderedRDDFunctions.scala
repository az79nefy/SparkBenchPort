package org.apache.spark.rdd

import scala.reflect.ClassTag

/**
 * Extra functions available on ordered ProvRDDs through an implicit conversion.
 *
 * These are mostly wrapper functions of the original OrderedRDDFunctions returning ProvRDDs instead.
 */
class NewOrderedRDDFunctions[K : Ordering : ClassTag,
                          V: ClassTag,
                          P <: Product2[K, V] : ClassTag](
    self: ProvRDD[P]) extends OrderedRDDFunctions[K,V,P](self) {
  
  /**
   * This returns a provenance RDD whose provenance function will trace the data
   * of the previous RDD.
   */
  override def sortByKey(ascending: Boolean = true, numPartitions: Int = self.partitions.length)
      : ProvRDD[(K, V)] = self.withScope {
    val superRDD = super.sortByKey(ascending, numPartitions)
    // We keep on tracking the same data, while performing the sort
    new ProvRDD(superRDD, self.trackedRDD.asInstanceOf[RDD[(K,V)]])
  }
}