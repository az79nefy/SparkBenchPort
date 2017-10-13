package org.apache.spark.rdd

import scala.reflect.ClassTag
import org.apache.spark.{ Partition, TaskContext, SparkContext }

/**
 * Saves additional provenance information for cartesian.
 */
private[spark] class CartesianInfoRDD[T: ClassTag, U: ClassTag](
  sc: SparkContext,
  val parent0: ProvRDD[T],
  val parent1: ProvRDD[U])
    extends CartesianRDD[T, U](sc, parent0, parent1)
    with HasProvenance {

  private[spark] override def provenance(trackedData: RDD[_], dependencyNum: Int): RDD[_] = {
    dependencyNum match {
      case 0 => cartesianProvenance0(trackedData.asInstanceOf[RDD[(T, U)]])
      case 1 => cartesianProvenance1(trackedData.asInstanceOf[RDD[(T, U)]])
      case _ => require(dependencyNum < 2, "cartesian has only two parents"); ???
    }
  }

  private[spark] override def provenanceParents = Seq(parent0, parent1)

  private[spark] override def copyWithParents(parents: Seq[ProvRDD[_]]): RDD[_] =
    new CartesianInfoRDD(sparkContext, parents(0).asInstanceOf[ProvRDD[T]], parents(1).asInstanceOf[ProvRDD[U]])

  /**
   * General cartesian provenance
   */
  private def cartesianProvenance[S: ClassTag](source: ProvRDD[S], rddToIntersect: RDD[S]): RDD[S] = {
    ProvRDD.intersectWithDups(sparkContext, source.dataRDD, rddToIntersect)
  }

  /**
   * Computes provenance for a cartesian transformation for the first parent.
   */
  private def cartesianProvenance0(trackedData: RDD[(T, U)]): RDD[T] = {
    cartesianProvenance(parent0, trackedData.map({ case (s, _) => s }))
  }

  /**
   * Computes provenance for a cartesian transformation for the second parent.
   */
  private def cartesianProvenance1(trackedData: RDD[(T, U)]): RDD[U] = {
    cartesianProvenance(parent1, trackedData.map({ case (_, s) => s }))
  }

  private[spark] override def explain(trackedData: RDD[_], dependencyNum: Int): RDD[_] = {
    dependencyNum match {
      case 0 => cartesianExplain0(trackedData.asInstanceOf[RDD[(T, U)]])
      case 1 => cartesianExplain1(trackedData.asInstanceOf[RDD[(T, U)]])
      case _ => require(dependencyNum < 2, "cartesian has only two parents"); ???
    }
  }

  /**
   * Computes explain for a cartesian transformation for the first parent.
   */
  private def cartesianExplain0(trackedData: RDD[(T, U)]): RDD[T] = {
    // First we count how often each KV pair is tracked.
    val countedTrackedData = trackedData.map((_, 1)).reduceByKey(_ + _)
    // Then we compute the maximum times an input value appears in equal output pairs
    val maxCountTrackedData = countedTrackedData.map({ case ((k, v), c) => (k, c) }).reduceByKey(Integer.max)
    // Now we generate this amount of input values with flatMap
    val maxSourceData = maxCountTrackedData.flatMap({ case (k, c) => for (i <- (0 until c)) yield k })
    // Finally we intersect with dups, as in cartesian provenance
    ProvRDD.intersectWithDups(sparkContext, parent0.dataRDD, maxSourceData)
  }

  /**
   * Computes explain for a cartesian transformation for the second parent.
   *
   * Here we use the explanation from the first parent to shrink this explanation in size.
   */
  private def cartesianExplain1(trackedData: RDD[(T, U)]): RDD[U] = {
    // We compute the explanation for the first parent and count the occurences of each tuple
    val explanation0count = cartesianExplain0(trackedData).map((_, 1)).reduceByKey(_ + _)
    
    // We also count how often each pair is tracked.
    val countedTrackedData = trackedData.map((_, 1)).reduceByKey(_ + _).map { case ((t, u), c) => (t, (u, c)) }
    
    // The join gives a tuples (t, ((u,c), n)) where (t,u) is the tracked tuple,
    // c is how often it was tracked and n is how often t was in explanation0
    val joined = countedTrackedData.join(explanation0count)
    
    // By dividing c/n and rounding up, we get tuples (u, count)
    // where count is how often we need u in the explanation of parent1 to reproduce c copies of (t,u)
    val countT = joined.map { case (t, ((u,c), n)) => (u, Math.ceil(c.toDouble / n.toDouble).toInt) }
    
    // For each u we compute the maximum of the counts ..
    val maxCountKW = countT.reduceByKey(Integer.max)
    // .. and yield a u record that many times
    maxCountKW.flatMap { case (u, count) => for (i <- 1 to count) yield u }
  }
}