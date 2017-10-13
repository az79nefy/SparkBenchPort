package org.apache.spark.rdd

import scala.reflect.ClassTag
import org.apache.spark._

/**
 * Saves additional provenance information for substract.
 *
 * The parents are the RDDs that were subtracted. Subtract is implemented as map/subtractByKey(other.map)/keys.
 * The realParent is the RDD before the keys transformation.
 */
private[spark] class SubtractInfoRDD[T: ClassTag](
  val parent0: ProvRDD[T],
  val parent1: ProvRDD[T],
  val p: Partitioner)
    extends MapPartitionsRDD[T, (T, Null)](
      parent0.dataRDD.map(x => (x, null)).subtractByKey(parent1.dataRDD.map((_, null)), p),
      (context, pid, iter) => iter.map(_._1))
    with HasProvenance {

  private[spark] override def provenanceParents = Seq(parent0, parent1)
  
  private[spark] override def copyWithParents(parents: Seq[ProvRDD[_]]): RDD[_] =
    new SubtractInfoRDD(parents(0).asInstanceOf[ProvRDD[T]], parents(1).asInstanceOf[ProvRDD[T]], p)

  private[spark] override def provenance(trackedData: RDD[_], dependencyNum: Int): RDD[_] = {
    dependencyNum match {
      case 0 => subtractMinuendProvenance(trackedData.asInstanceOf[RDD[T]])
      case 1 => subtractSubtrahendProvenance(trackedData.asInstanceOf[RDD[T]])
      case _ => require(dependencyNum < 2, "subtract has only two parents"); ???
    }
  }

  /**
   * Same as provenance.
   */
  private[spark] override def explain(trackedData: RDD[_], dependencyNum: Int): RDD[_] =
    provenance(trackedData, dependencyNum)

  /**
   * Computes provenance for a subtract transformation for the minuend.
   */
  private def subtractMinuendProvenance(trackedData: RDD[T]): RDD[T] = {
    val source = parent0

    // The content stays identical for the minuend
    trackedData
  }

  /**
   * Computes provenance for a subtract transformation for the subtrahend.
   * It will be always empty.
   */
  private def subtractSubtrahendProvenance(trackedData: RDD[T]): RDD[T] = {
    val source = parent1

    // The content is always empty
    sparkContext.parallelize[T](Seq())
  }
}