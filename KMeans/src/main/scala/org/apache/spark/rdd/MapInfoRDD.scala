package org.apache.spark.rdd

import scala.reflect.ClassTag
import org.apache.spark.{ Partition, TaskContext }

/**
 * Saves additional provenance information for map.
 *
 * In particular, this is the original mapping function origF, passed to map.
 * We must be able to access it (before it gets changes to work on Iterators),
 * to compute the backwards trace.
 */
private[spark] class MapInfoRDD[U: ClassTag, T: ClassTag](
  val parent: ProvRDD[T],
  f: (TaskContext, Int, Iterator[T]) => Iterator[U],
  val origF: T => U,
  preservesPartitioning: Boolean = false)
    extends MapPartitionsRDD[U, T](parent, f, preservesPartitioning)
    with HasProvenance {

  private[spark] override def provenanceParents = Seq(parent)
  
  private[spark] override def copyWithParents(parents: Seq[ProvRDD[_]]): RDD[_] =
    new MapInfoRDD(parents(0).asInstanceOf[ProvRDD[T]], f, origF, preservesPartitioning)

  /**
   * Computes provenance for a map transformation. If multiple records in the tracked data are equal,
   * the same number of equal entries will be in the result.
   */
  private[spark] override def provenance(trackedData: RDD[_], dependencyNum: Int): RDD[T] = {
    require(dependencyNum == 0, "map has only a single parent")
    // Removing duplicates here
    val rddToJoin = trackedData.asInstanceOf[RDD[U]].distinct().map(v => (v, null))
    parent.dataRDD.keyBy(origF).join(rddToJoin).values.keys
  }

  /**
   * Computes explanations for a map transformation.
   */
  private[spark] override def explain(trackedData: RDD[_], dependencyNum: Int): RDD[T] = {
    require(dependencyNum == 0, "map has only a single parent")
    // Count all output values
    val rddToJoin = trackedData.asInstanceOf[RDD[U]].map(v => (v, 1)).reduceByKey(_ + _)

    // From all input values that map to the same output value, we build a list
    val inputValLists = parent.dataRDD.keyBy(origF).aggregateByKey(Seq[T]())(_ :+ _, _ ++ _)

    // From the lists of values take exactly how many were tracked from the output value count
    inputValLists.join(rddToJoin).values.flatMap({
      case (seq, howmany) =>
        for (i <- (0 until howmany)) yield seq(i)
    })
  }
}