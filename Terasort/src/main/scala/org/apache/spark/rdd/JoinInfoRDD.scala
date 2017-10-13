package org.apache.spark.rdd

import scala.reflect.ClassTag
import org.apache.spark.{ Partition, TaskContext, Partitioner }

/**
 * Saves additional provenance information for join.
 *
 * The parents are the RDDs that were joined. Join is implemented as cogroup/flatMapValues.
 * The realParent is the RDD before the keys flatMapValues.
 */
private[spark] class JoinInfoRDD[K: ClassTag, V: ClassTag, W: ClassTag](
  val parent0: ProvRDD[(K, V)],
  val parent1: ProvRDD[(K, W)],
  val p: Partitioner)
    extends MapPartitionsRDD[(K, (V, W)), (K, (Iterable[V], Iterable[W]))](
        parent0.dataRDD.cogroup(parent1.dataRDD, p),
      (context, pid, iter) => iter.flatMap({
        case (k, (vs, ws)) =>
          for (v <- vs.iterator; w <- ws.iterator) yield (k, (v, w))
      }),
      preservesPartitioning = true) 
      with HasProvenance {

  private[spark] override def provenanceParents = Seq(parent0, parent1)
  
  private[spark] override def copyWithParents(parents: Seq[ProvRDD[_]]): RDD[_] =
    new JoinInfoRDD(parents(0).asInstanceOf[ProvRDD[(K, V)]], parents(1).asInstanceOf[ProvRDD[(K, W)]], p)

  private[spark] override def provenance(trackedData: RDD[_], dependencyNum: Int): RDD[_] = {
    dependencyNum match {
      case 0 => joinProvenance0(trackedData.asInstanceOf[RDD[(K, (V, W))]])
      case 1 => joinProvenance1(trackedData.asInstanceOf[RDD[(K, (V, W))]])
      case _ => require(dependencyNum < 2, "join has only two parents"); ???
    }
  }

  /**
   * Computes provenance for a join transformation for the first parent.
   * If multiple distinct records in the parent RDD could have produced one
   * entry in the trackedData, all of them will be in the result.
   */
  private def joinProvenance0(trackedData: RDD[(K, (V, W))]): RDD[(K, V)] = {
    val source = parent0

    // Remove the tuple value we don't need
    val mappedTD = trackedData.map({ case (k, (v, w)) => (k, v) })
    // Intersect preserving duplicates
    // This is needed because mappedTD might contain more duplicates than present in source.dataRDD
    ProvRDD.intersectWithDups(sparkContext, source.dataRDD, mappedTD)
  }

  /**
   * Computes provenance for a join transformation for the second parent.
   * If multiple distinct records in the parent RDD could have produced one
   * entry in the trackedData, all of them will be in the result.
   */
  private def joinProvenance1(trackedData: RDD[(K, (V, W))]): RDD[(K, W)] = {
    val source = parent1

    val mappedTD = trackedData.map({ case (k, (v, w)) => (k, w) })
    ProvRDD.intersectWithDups(sparkContext, source.dataRDD, mappedTD)
  }

  // TODO We need to think how to avoid computing the same explanation twice (not only here in Join)
  // A solution might be saving AND caching it.
  private[spark] override def explain(trackedData: RDD[_], dependencyNum: Int): RDD[_] = {
    dependencyNum match {
      case 0 => joinExplain0(trackedData.asInstanceOf[RDD[(K, (V, W))]])
      case 1 => joinExplain1(trackedData.asInstanceOf[RDD[(K, (V, W))]])
      case _ => require(dependencyNum < 2, "join has only two parents"); ???
    }
  }

  /**
   * Computes explain for a join transformation for the first parent.
   */
  private def joinExplain0(trackedData: RDD[(K, (V, W))]): RDD[(K, V)] = {
    // First we count how often each KV pair is tracked.
    val countedTrackedData = trackedData.map((_, 1)).reduceByKey(_ + _)
    // Then we compute the maximum times an input KV appears in equal output pairs KVW
    val maxCountTrackedData = countedTrackedData.map({ case ((k, (v, w)), c) => ((k, v), c) }).reduceByKey(Integer.max)
    // Now we generate this amount of KV input values with flatMap
    val maxSourceData = maxCountTrackedData.flatMap({ case ((k, v), c) => for (i <- (0 until c)) yield (k, v) })
    // Finally we intersect with dups, as in cartesian provenance
    ProvRDD.intersectWithDups(sparkContext, parent0.dataRDD, maxSourceData)
  }

  /**
   * Computes explain for a join transformation for the second parent.
   *
   * Here we use the explanation from the first parent to shrink this explanation in size.
   */
  private def joinExplain1(trackedData: RDD[(K, (V, W))]): RDD[(K, W)] = {
    val source = parent1

    // We compute the explanation for the first parent and count the occurences of each tuple
    val explanation0count = joinExplain0(trackedData).map((_, 1)).reduceByKey(_ + _)

    // We also count each triple in the tracked data
    val countKVW = trackedData.map((_, 1)).reduceByKey(_ + _).map { case ((k, (v, w)), c) => ((k, v), (w, c)) }
    
    // The join gives a tuples ((k,v), ((w,c), t)) where k,v,w is the tracked triple,
    // c is how often it was tracked and t is how often (k,v) was in explanation0
    val joined = countKVW.join(explanation0count)

    // By dividing c/t and rounding up, we get tuples ((k,w), count)
    // where count is how often we need (k,w) in the explanation of parent1 to reproduce c copies of (k,(v,w))
    val countKW = joined.map { case ((k, v), ((w, c), t)) => ((k, w), Math.ceil(c.toDouble / t.toDouble).toInt) }
    
    // For each (k,w) we compute the maximum of the counts ..
    val maxCountKW = countKW.reduceByKey(Integer.max)
    // .. and yield a (k,w) tuple that many times
    maxCountKW.flatMap { case (tup, c) => for (i <- 1 to c) yield tup }
  }
}