package org.apache.spark.rdd

import scala.reflect.ClassTag
import org.apache.spark.{ Partition, TaskContext, Partitioner }

/**
 * Saves additional provenance information for intersection.
 *
 * The parents are the RDDs that were intersected. Intersection is implemented as map/cogroup/filter/keys.
 * The realParent is the RDD before the keys transformation.
 */
private[spark] class IntersectionInfoRDD[T: ClassTag](
  val parent0: ProvRDD[T],
  val parent1: ProvRDD[T],
  val p: Option[Partitioner])
    extends MapPartitionsRDD[T, (T, (Iterable[Null], Iterable[Null]))](
      p.map { p => parent0.dataRDD.map(v => (v, null)).cogroup(parent1.dataRDD.map(v => (v, null)), p) }
        .getOrElse(parent0.dataRDD.map(v => (v, null)).cogroup(parent1.dataRDD.map(v => (v, null))))
        .filter { case (_, (leftGroup, rightGroup)) => leftGroup.nonEmpty && rightGroup.nonEmpty },
      (context, pid, iter) => iter.map(_._1))
    with HasProvenance {

  private[spark] override def provenanceParents = Seq(parent0, parent1)
  
  private[spark] override def copyWithParents(parents: Seq[ProvRDD[_]]): RDD[_] =
    new IntersectionInfoRDD(parents(0).asInstanceOf[ProvRDD[T]], parents(1).asInstanceOf[ProvRDD[T]],
        p)

  private[spark] override def provenance(trackedData: RDD[_], dependencyNum: Int): RDD[T] = {
    require(dependencyNum < 2, "intersection has only two parents")

    val source = if (dependencyNum == 0) parent0 else parent1
    // TrackedData is guaranteed to be duplicate free (it is the subset of a result from an intersection)
    val rddToJoin = trackedData.asInstanceOf[RDD[T]].map(v => (v, null))

    // We join the source's RDD (with duplicates) with the duplicate free tracked data
    source.dataRDD.map(v => (v, null)).join(rddToJoin).keys
  }

  private[spark] override def explain(trackedData: RDD[_], dependencyNum: Int): RDD[T] = {
    require(dependencyNum < 2, "intersection has only two parents")
    // We know that the parent RDD contains each of the tracked elements (since they are part of the intersection)
    // We do not want to include any duplicates from the parent, so that the tracked content can simply stay identical
    trackedData.asInstanceOf[RDD[T]]
  }
}