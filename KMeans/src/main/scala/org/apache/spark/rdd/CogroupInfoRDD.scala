package org.apache.spark.rdd

import scala.reflect.ClassTag
import org.apache.spark.{ Partition, TaskContext, Partitioner }

/**
 * Saves additional provenance information for cogroup.
 *
 * The parents are the RDDs that were cogrouped.
 * Cogroup is implemented as CoGroupedRDD.map.
 * The realParent is the CoGroupedRDD before the map transformation.
 */
private[spark] class Cogroup2InfoRDD[K: ClassTag, V: ClassTag, W: ClassTag](
  val parent0: ProvRDD[(K, V)],
  val parent1: ProvRDD[(K, W)],
  val p: Partitioner)
    extends MapPartitionsRDD[(K, (Iterable[V], Iterable[W])), (K, Array[Iterable[_]])](
      new CoGroupedRDD[K](Seq(parent0, parent1), p),
      (context, pid, iter) => iter.map(
        { case (k, Array(vs, w1s)) => (k, (vs.asInstanceOf[Iterable[V]], w1s.asInstanceOf[Iterable[W]])) }))
    with HasProvenance with NonMonotonic[K, (Iterable[V], Iterable[W])] {

  private[spark] override def provenanceParents = Seq(parent0, parent1)

  private[spark] override def copyWithParents(parents: Seq[ProvRDD[_]]): RDD[_] =
    new Cogroup2InfoRDD(parents(0).asInstanceOf[ProvRDD[(K, V)]], parents(1).asInstanceOf[ProvRDD[(K, W)]], p)

  private[spark] override def equivalentRecords(records: RDD[(K, (Iterable[V], Iterable[W]))]) = {
    this.cogroup(records).flatMapValues { case (cs, xs) => for (c <- cs; x <- xs) yield c }
  }

  private[spark] override def provenance(trackedData: RDD[_], dependencyNum: Int): RDD[_] = {
    dependencyNum match {
      case 0 => cogroupProvenance0(trackedData.asInstanceOf[RDD[(K, (Iterable[V], Iterable[W]))]])
      case 1 => cogroupProvenance1(trackedData.asInstanceOf[RDD[(K, (Iterable[V], Iterable[W]))]])
      case _ => require(dependencyNum < 2, "this cogroup has only two parents"); ???
    }
  }

  /**
   * Same as provenance.
   */
  private[spark] override def explain(trackedData: RDD[_], dependencyNum: Int): RDD[_] =
    provenance(trackedData, dependencyNum)

  /**
   * Computes provenance for a cogroup2 transformation for the first parent.
   */
  private def cogroupProvenance0(trackedData: RDD[(K, (Iterable[V], Iterable[W]))]): RDD[(K, V)] = {
    trackedData.flatMap({ case (k, (vs, _)) => for (v <- vs) yield (k, v) })
  }

  /**
   * Computes provenance for a cogroup2 transformation for the second parent.
   */
  private def cogroupProvenance1(trackedData: RDD[(K, (Iterable[V], Iterable[W]))]): RDD[(K, W)] = {
    trackedData.flatMap({ case (k, (_, vs)) => for (v <- vs) yield (k, v) })
  }
}

private[spark] class Cogroup3InfoRDD[K: ClassTag, V: ClassTag, W1: ClassTag, W2: ClassTag](
  val parent0: ProvRDD[(K, V)],
  val parent1: ProvRDD[(K, W1)],
  val parent2: ProvRDD[(K, W2)],
  val p: Partitioner)
    extends MapPartitionsRDD[(K, (Iterable[V], Iterable[W1], Iterable[W2])), (K, Array[Iterable[_]])](
      new CoGroupedRDD[K](Seq(parent0, parent1, parent2), p),
      (context, pid, iter) => iter.map(
        {
          case (k, Array(vs, w1s, w2s)) => (k, (vs.asInstanceOf[Iterable[V]],
            w1s.asInstanceOf[Iterable[W1]], w2s.asInstanceOf[Iterable[W2]]))
        }))
    with HasProvenance with NonMonotonic[K, (Iterable[V], Iterable[W1], Iterable[W2])] {

  private[spark] override def provenanceParents = Seq(parent0, parent1, parent2)

  private[spark] override def copyWithParents(parents: Seq[ProvRDD[_]]): RDD[_] =
    new Cogroup3InfoRDD(parents(0).asInstanceOf[ProvRDD[(K, V)]], parents(1).asInstanceOf[ProvRDD[(K, W1)]],
      parents(2).asInstanceOf[ProvRDD[(K, W2)]], p)

  private[spark] override def equivalentRecords(records: RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2]))]) = {
    this.cogroup(records).flatMapValues { case (cs, xs) => for (c <- cs; x <- xs) yield c }
  }

  private[spark] override def provenance(trackedData: RDD[_], dependencyNum: Int): RDD[_] = {
    dependencyNum match {
      case 0 => cogroupProvenance0(trackedData.asInstanceOf[RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2]))]])
      case 1 => cogroupProvenance1(trackedData.asInstanceOf[RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2]))]])
      case 2 => cogroupProvenance2(trackedData.asInstanceOf[RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2]))]])
      case _ => require(dependencyNum < 3, "this cogroup has only three parents"); ???
    }
  }

  /**
   * Same as provenance.
   */
  private[spark] override def explain(trackedData: RDD[_], dependencyNum: Int): RDD[_] =
    provenance(trackedData, dependencyNum)

  /**
   * Computes provenance for a cogroup3 transformation for the first parent.
   */
  private def cogroupProvenance0(trackedData: RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2]))]): RDD[(K, V)] = {
    trackedData.flatMap({ case (k, (vs, _, _)) => for (v <- vs) yield (k, v) })
  }

  /**
   * Computes provenance for a cogroup3 transformation for the second parent.
   */
  private def cogroupProvenance1(trackedData: RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2]))]): RDD[(K, W1)] = {
    trackedData.flatMap({ case (k, (_, vs, _)) => for (v <- vs) yield (k, v) })
  }

  /**
   * Computes provenance for a cogroup3 transformation for the third parent.
   */
  private def cogroupProvenance2(trackedData: RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2]))]): RDD[(K, W2)] = {
    trackedData.flatMap({ case (k, (_, _, vs)) => for (v <- vs) yield (k, v) })
  }
}

private[spark] class Cogroup4InfoRDD[K: ClassTag, V: ClassTag, W1: ClassTag, W2: ClassTag, W3: ClassTag](
  val parent0: ProvRDD[(K, V)],
  val parent1: ProvRDD[(K, W1)],
  val parent2: ProvRDD[(K, W2)],
  val parent3: ProvRDD[(K, W3)],
  val p: Partitioner)
    extends MapPartitionsRDD[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3])), (K, Array[Iterable[_]])](
      new CoGroupedRDD[K](Seq(parent0, parent1, parent2, parent3), p),
      (context, pid, iter) => iter.map(
        {
          case (k, Array(vs, w1s, w2s, w3s)) => (k, (vs.asInstanceOf[Iterable[V]],
            w1s.asInstanceOf[Iterable[W1]], w2s.asInstanceOf[Iterable[W2]], w3s.asInstanceOf[Iterable[W3]]))
        }))
    with HasProvenance with NonMonotonic[K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3])] {

  private[spark] override def provenanceParents = Seq(parent0, parent1, parent2, parent3)

  private[spark] override def copyWithParents(parents: Seq[ProvRDD[_]]): RDD[_] =
    new Cogroup4InfoRDD(parents(0).asInstanceOf[ProvRDD[(K, V)]], parents(1).asInstanceOf[ProvRDD[(K, W1)]],
      parents(2).asInstanceOf[ProvRDD[(K, W2)]], parents(3).asInstanceOf[ProvRDD[(K, W3)]], p)

  private[spark] override def equivalentRecords(records: RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))]) = {
    this.cogroup(records).flatMapValues { case (cs, xs) => for (c <- cs; x <- xs) yield c }
  }

  private[spark] override def provenance(trackedData: RDD[_], dependencyNum: Int): RDD[_] = {
    dependencyNum match {
      case 0 => cogroupProvenance0(trackedData.asInstanceOf[RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))]])
      case 1 => cogroupProvenance1(trackedData.asInstanceOf[RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))]])
      case 2 => cogroupProvenance2(trackedData.asInstanceOf[RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))]])
      case 3 => cogroupProvenance3(trackedData.asInstanceOf[RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))]])
      case _ => require(dependencyNum < 4, "this cogroup has only four parents"); ???
    }
  }

  /**
   * Same as provenance.
   */
  private[spark] override def explain(trackedData: RDD[_], dependencyNum: Int): RDD[_] =
    provenance(trackedData, dependencyNum)

  /**
   * Computes provenance for a cogroup4 transformation for the first parent.
   */
  private def cogroupProvenance0(trackedData: RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))]): RDD[(K, V)] = {
    trackedData.flatMap({ case (k, (vs, _, _, _)) => for (v <- vs) yield (k, v) })
  }

  /**
   * Computes provenance for a cogroup4 transformation for the second parent.
   */
  private def cogroupProvenance1(trackedData: RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))]): RDD[(K, W1)] = {
    trackedData.flatMap({ case (k, (_, vs, _, _)) => for (v <- vs) yield (k, v) })
  }

  /**
   * Computes provenance for a cogroup4 transformation for the third parent.
   */
  private def cogroupProvenance2(trackedData: RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))]): RDD[(K, W2)] = {
    trackedData.flatMap({ case (k, (_, _, vs, _)) => for (v <- vs) yield (k, v) })
  }

  /**
   * Computes provenance for a cogroup4 transformation for the fourth parent.
   */
  private def cogroupProvenance3(trackedData: RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))]): RDD[(K, W3)] = {
    trackedData.flatMap({ case (k, (_, _, _, vs)) => for (v <- vs) yield (k, v) })
  }
}
