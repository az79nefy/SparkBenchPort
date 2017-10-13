package org.apache.spark.rdd

import scala.reflect.ClassTag
import org.apache.spark.{ Partition, TaskContext }

/**
 * Saves additional provenance information for filter.
 *
 * No information is needed aprt from the fact that this is a filter operation.
 */
private[spark] class FilterInfoRDD[T: ClassTag](
  val parent: ProvRDD[T],
  f: (TaskContext, Int, Iterator[T]) => Iterator[T],
  preservesPartitioning: Boolean = false)
    extends MapPartitionsRDD[T, T](parent, f, preservesPartitioning)
    with HasProvenance {
  
  private[spark] override def provenanceParents = Seq(parent)
  
  private[spark] override def copyWithParents(parents: Seq[ProvRDD[_]]): RDD[_] =
    new FilterInfoRDD(parents(0).asInstanceOf[ProvRDD[T]], f, preservesPartitioning)

  private[spark] override def provenance(trackedData: RDD[_], dependencyNum: Int): RDD[T] = {
    require(dependencyNum == 0, "filter has only a single parent")

    // The content stays identical for filter provenance
    trackedData.asInstanceOf[RDD[T]]
  }
  
  /**
   * Same as provenance.
   */
  private[spark] override def explain(trackedData: RDD[_], dependencyNum: Int): RDD[T] = 
    provenance(trackedData, dependencyNum)
}