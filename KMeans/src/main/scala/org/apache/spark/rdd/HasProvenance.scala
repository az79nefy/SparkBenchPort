package org.apache.spark.rdd

/**
 * RDDs can implement this to provide provenance information.
 */
private[spark] trait HasProvenance {
  /**
   * Computes the provenace.
   */
  private[spark] def provenance(trackedData: RDD[_], dependencyNum: Int): RDD[_]

  /**
   * The ordered parents of this RDD, for the provenance computation
   */
  private[spark] def provenanceParents: Seq[ProvRDD[_]]

  /**
   * Provide the explanation for the tracked elements. This is similar to provenance,
   * but includes only those elements necessary to reproduce the trackedData.
   */
  private[spark] def explain(trackedData: RDD[_], dependencyNum: Int): RDD[_]
  
  /**
   * Create a copy of this (same transformation), but exchange the parent(s).
   */
  private[spark] def copyWithParents(parents: Seq[ProvRDD[_]]) : RDD[_]

}

/**
 * This trait signals that a transformation t is non-monotonic. This means if t(x)=y
 * and t(x')=y' and x' subset x; y' subset y does NOT always hold.
 * 
 * So far, all nonmonotonic transformations in Spark produce KV-pairs.
 */
private[spark] trait NonMonotonic[K, V] extends RDD[(K,V)] {
  
  /**
   * Finds the equivalent records in this, that also exist in records. These are the records in
   * this, that have the same keys as those in records.
   */
  private[spark] def equivalentRecords(records: RDD[(K,V)]) : RDD[(K,V)]
}