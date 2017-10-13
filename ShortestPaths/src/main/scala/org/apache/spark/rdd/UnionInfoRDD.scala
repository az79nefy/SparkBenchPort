package org.apache.spark.rdd

import scala.reflect.ClassTag
import org.apache.spark.{ Partition, TaskContext, SparkContext }

/**
 * Saves additional provenance information for union.
 */
private[spark] class UnionInfoRDD[T: ClassTag](
  sc: SparkContext,
  val parents: Seq[ProvRDD[T]])
    extends UnionRDD[T](sc, parents)
    with HasProvenance {

  private[spark] override def provenanceParents = parents

  private[spark] override def copyWithParents(parents: Seq[ProvRDD[_]]): RDD[_] =
    new UnionInfoRDD(sparkContext, parents.asInstanceOf[Seq[ProvRDD[T]]])

  private[spark] override def provenance(trackedData: RDD[_], dependencyNum: Int): RDD[T] = {
    require(dependencyNum < parents.length, s"this union has only ${parents.length} parents")

    val source = parents(dependencyNum)

    ProvRDD.intersectWithDups(sparkContext, source.dataRDD, trackedData.asInstanceOf[RDD[T]])
  }

  private[spark] override def explain(trackedData: RDD[_], dependencyNum: Int): RDD[T] = {
    require(dependencyNum < parents.length, s"this union has only ${parents.length} parents")

    var content: RDD[T] = null
    var remainingTracked = trackedData.asInstanceOf[RDD[T]]

    // XXX if we compute explain for more than one parent,  explain for the previous parents will be recomputed
    for (i <- 0 to dependencyNum) {
      val parent = parents(i)
      if (i < parents.length - 1) {
        content = ProvRDD.intersectWithDups(sparkContext, parent.dataRDD, remainingTracked)
        // This is what remains from the trackedData after matching some tracked elements with the current parent
        remainingTracked = ProvRDD.subtractWithDups(sparkContext, remainingTracked, content)
      } else {
        // No intersection necessary for the last parent
        content = remainingTracked
      }
    }
    content
  }

}