package org.apache.spark.rdd

import scala.reflect.ClassTag
import org.apache.spark.{ Partition, TaskContext }

/**
 * Saves additional provenance information for distinct.
 *
 * The parent is the RDD that distinct was invoked on. Distinct is implemented as map/reduceByKey/map.
 * The realParent is the RDD before the second map.
 */
private[spark] class DistinctInfoRDD[T: ClassTag](
  val parent: ProvRDD[T],
  val numPartitions: Int)
    extends MapPartitionsRDD[T, (T, Null)](parent.dataRDD.map(x => (x, null)).reduceByKey((x, y) => x, numPartitions),
      (context, pid, iter) => iter.map(_._1))
    with HasProvenance {

  private[spark] override def provenanceParents = Seq(parent)

  private[spark] override def copyWithParents(parents: Seq[ProvRDD[_]]): RDD[_] =
    new DistinctInfoRDD(parents(0), numPartitions)

  private[spark] override def provenance(trackedData: RDD[_], dependencyNum: Int): RDD[T] = {
    require(dependencyNum == 0, "distinct has only a single parent")

    // TrackedData is guaranteed to be duplicate free (it is the subset of a result from a distinct)
    val rddToJoin = trackedData.asInstanceOf[RDD[T]].map(v => (v, null))

    // We join the source's RDD (with duplicates) with the duplicate free tracked data
    parent.dataRDD.map(v => (v, null)).join(rddToJoin).keys
  }

  private[spark] override def explain(trackedData: RDD[_], dependencyNum: Int): RDD[T] = {
    require(dependencyNum == 0, "distinct has only a single parent")

    // We do not modify the tracked data. That means, we do not track duplicate elements 
    // that existed before the call to distinct.
    trackedData.asInstanceOf[RDD[T]]
  }
}