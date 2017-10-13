package org.apache.spark.rdd

import scala.reflect.ClassTag
import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.serializer.Serializer
import org.apache.spark.sa.StaticAnalyzer

/**
 * Can be used to give hints what combine operations do,
 * so that explanations can be computed efficiently.
 */
sealed trait CombineAnnotation[V]

/**
 * Marker trait for annotations that can be added manually.
 */
sealed trait ManualAnnotation

/**
 * Indicates that a combine operation performs a selection of only one value per key.
 */
case class Selection[V]() extends CombineAnnotation[V] with ManualAnnotation

/**
 * Indicates that an aggregation is performed. The zeroValue and optionally a guard can be specified.
 * Only those values not equal to the zero value and passing the guard will be tracked.
 */
case class Aggregation[V](zeroValue: V, guard: (V) => Boolean = ((v: V) => true))
  extends CombineAnnotation[V] with ManualAnnotation

/**
 * Indicates that an duplicates are removed during the aggregation (that means, duplicate values for the same
 * key don't influence the result more than once).
 *
 * Only one of the duplicate values will be tracked.
 */
case class RemovingDups[V]() extends CombineAnnotation[V] with ManualAnnotation

/**
 * Indicates that reduceByKey was used:
 * reduceByKey(func: (V, V) ⇒ V)
 * = combineByKey(x => x, func, func)
 */
private[spark] case class ReduceByKey[V]() extends CombineAnnotation[V]

/**
 * Indicates that aggregateByKey was used:
 * aggregateByKey(zeroValue: C)(seqOp: (C, V) => C, combOp: (C, C) => C)
 * = combineByKey(x=> seqOp(zeroValue, x), seqOp, combOp)
 */
private[spark] case class AggregateByKey[V, C](zeroValue: C) extends CombineAnnotation[V]

/**
 * Indicates that groupByKey was used.
 */
private[spark] case class GroupByKey[V]() extends CombineAnnotation[V]

/**
 * Indicates that foldByKey was used:
 * foldByKey(zeroValue: V)(func: (V, V) => V)
    = combineByKey(x => func(zeroValue, x), func, func)
 */
private[spark] case class FoldByKey[V]() extends CombineAnnotation[V]

/**
 * Saves additional provenance information for combineByKey.
 *
 * In particular, this is the original combine functions and CombineAnnotations.
 */
private[spark] class CombineInfoRDD[K: ClassTag, V: ClassTag, C: ClassTag](
  val parent: ProvRDD[(K, V)],
  part: Partitioner,
  serializer: Serializer,
  aggregator: Aggregator[K, V, C],
  mapSideCombine: Boolean,
  val origCreateCombiner: V => C,
  val origMergeValue: (C, V) => C,
  val origMergeCombiners: (C, C) => C,
  var annotations: Seq[CombineAnnotation[V]])
    extends ShuffledRDD[K, V, C](parent, part)
    with HasProvenance with NonMonotonic[K, C] {

  // modify super ShuffledRDD:
  setSerializer(serializer)
  setAggregator(aggregator)
  setMapSideCombine(mapSideCombine)

  private[spark] override def provenanceParents = Seq(parent)

  private[spark] override def copyWithParents(parents: Seq[ProvRDD[_]]): RDD[_] =
    new CombineInfoRDD(parents(0).asInstanceOf[ProvRDD[(K, V)]], part, serializer, aggregator, mapSideCombine,
      origCreateCombiner, origMergeValue, origMergeCombiners, annotations)

  private[spark] override def equivalentRecords(records: RDD[(K, C)]) = {
    // In this and in records, each key should exist at most once. Therefore,
    // the sequences after cogroup have at most 1 element.
    this.cogroup(records).flatMapValues { case (cs, xs) => for (c <- cs; x <- xs) yield c }
  }

  private[spark] override def provenance(trackedData: RDD[_], dependencyNum: Int): RDD[(K, V)] = {
    require(dependencyNum == 0, "combine has only a single parent")

    val rddToJoin = trackedData.asInstanceOf[RDD[(K, V)]].map({ case (k, _) => (k, null) })
    parent.dataRDD.join(rddToJoin).mapValues(_._1)
  }

  private[spark] override def explain(trackedData: RDD[_], dependencyNum: Int): RDD[(K, V)] = {
    require(dependencyNum == 0, "combine has only a single parent")

    if (!annotations.exists { a => a.isInstanceOf[ManualAnnotation] }
    && ProvRDD.useStaticAnalyis) {
      // Do some static analysis
      annotations = StaticAnalyzer.inferAnnotations(annotations, origCreateCombiner, origMergeValue, origMergeCombiners)
    }

    annotations.foreach { anno =>
      anno match {
        case sel: Selection[V] => {
          // We must assume that this is a reduction operation (V=C)
          // We can track the same elements without a join
          return trackedData.asInstanceOf[RDD[(K, V)]]
        }
        case agg: Aggregation[V] => {
          val rddToJoin = trackedData.asInstanceOf[RDD[(K, V)]].map({ case (k, _) => (k, null) })
          // We filter out zeroValues and data not passing the guard
          // FIXME if the filtered is empty for a key that produced a tracked value, we got a problem
          val filtered = parent.dataRDD.filter({ case (_, v) => v != agg.zeroValue && agg.guard(v) })
          return filtered.join(rddToJoin).mapValues(_._1)
        }
        case rd: RemovingDups[V] => {
          val rddToJoin = trackedData.asInstanceOf[RDD[(K, V)]].map({ case (k, _) => (k, null) })
          // We call distinct on the content to account for removed dups
          return parent.dataRDD.join(rddToJoin).mapValues(_._1).distinct()
        }
        case _ =>
      }
    }

    // Normal provenance
    provenance(trackedData, dependencyNum)
  }
}