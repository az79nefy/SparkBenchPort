package org.apache.spark.rdd

import scala.reflect.ClassTag
import org.apache.spark.{ Partition, TaskContext }

/**
 * Saves additional provenance information for map, if an inverse function is provided.
 *
 * In particular, this is the inverse function.
 */
private[spark] class InvertableMapInfoRDD[U: ClassTag, T: ClassTag](
  val parent: ProvRDD[T],
  f: (TaskContext, Int, Iterator[T]) => Iterator[U],
  val inverseF: U => T,
  preservesPartitioning: Boolean = false)
    extends MapPartitionsRDD[U, T](parent, f, preservesPartitioning)
    with HasProvenance {
  
  private[spark] override def provenanceParents = Seq(parent)
  
  private[spark] override def copyWithParents(parents: Seq[ProvRDD[_]]): RDD[_] =
    new InvertableMapInfoRDD(parents(0).asInstanceOf[ProvRDD[T]], f, inverseF, preservesPartitioning)

  /**
   * Computes provenance for an invertable map transformation. If multiple records in the tracked data are equal,
   * the same number of equal entries will be in the result.
   */
  private[spark] override def provenance(trackedData: RDD[_], dependencyNum: Int): RDD[T] = {
    require(dependencyNum == 0, "invertableMap has only a single parent")

    // just apply the reverse function
    trackedData.asInstanceOf[RDD[U]].map(inverseF)
  }
  
  /**
   * Same as provenance.
   */
  private[spark] override def explain(trackedData: RDD[_], dependencyNum: Int): RDD[T] = 
    provenance(trackedData, dependencyNum)

}