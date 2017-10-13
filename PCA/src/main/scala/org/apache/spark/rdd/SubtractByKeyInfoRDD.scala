package org.apache.spark.rdd

import scala.reflect.ClassTag
import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.serializer.Serializer

/**
 * Saves additional provenance information for substractByKey.
 */
private[spark] class SubtractByKeyInfoRDD[K: ClassTag, V: ClassTag, W: ClassTag](
    val parent0: ProvRDD[(K,V)],
    val parent1: ProvRDD[(K,W)],
    part: Partitioner)
  extends SubtractedRDD[K,V,W](parent0, parent1, part) with HasProvenance {
  
  private[spark] override def provenanceParents = Seq(parent0, parent1)
  
  private[spark] override def copyWithParents(parents: Seq[ProvRDD[_]]): RDD[_] =
    new SubtractByKeyInfoRDD(parents(0).asInstanceOf[ProvRDD[(K,V)]], parents(1).asInstanceOf[ProvRDD[(K,W)]], part)
  
  private[spark] override def provenance(trackedData: RDD[_], dependencyNum: Int): RDD[_] = {
    dependencyNum match {
      case 0 => subtractByKeyMinuendProvenance(trackedData.asInstanceOf[RDD[(K, V)]])
      case 1 => subtractByKeySubtrahendProvenance(trackedData.asInstanceOf[RDD[(K, V)]])
      case _ => require(dependencyNum < 2, "subtractByKey has only two parents"); ???
    }
  }
  
  /**
   * Same as provenance.
   */
  private[spark] override def explain(trackedData: RDD[_], dependencyNum: Int): RDD[_] = 
    provenance(trackedData, dependencyNum)

  /**
   * Computes provenance for a subtractByKey transformation for the minuend.
   */
  private def subtractByKeyMinuendProvenance(trackedData: RDD[(K, V)]): RDD[(K,V)] = {
    val source = parent0

    // The content stays identical for the minuend
    trackedData
  }

  /**
   * Computes provenance for a subtractByKey transformation for the subtrahend.
   * It will be always empty.
   */
  private def subtractByKeySubtrahendProvenance(trackedData: RDD[(K, V)]): RDD[(K,W)] = {
    val source = parent1

    // The content is always empty
    sparkContext.parallelize[(K,W)](Seq())
  }
}