package org.apache.spark.rdd

import scala.reflect.ClassTag
import org.apache.spark._
import org.apache.spark.Partitioner.defaultPartitioner
import org.apache.spark.serializer.Serializer
import org.apache.spark.annotation.Experimental
import scala.collection.mutable.{ Map => MMap, Buffer }

/**
 * Provenance functions available on ProvRDDs through an implicit conversion.
 */
class RDDProvFunctions[T](self: ProvRDD[T])(implicit tt: ClassTag[T]) {

  /**
   * Query provenance for a subset of this RDD. This returns an RDD with the subset of records
   * of the trackedRDD, that produces the records in dataRDD that match the filter.
   *
   * If this is invoked on an RDD that has no provenance information
   * (e.g. one build with sc.parallelize or from a native Spark transformation with ProvRDD wrapper)
   * an UnsupportedOperationException will be thrown.
   *
   * filter is a function that filters the contents of the tracked data before calculating the provenance.
   * dependendyNum can be used to query provenance for different source RDDs if this RDD has multiple parents
   * (e.g. after a join or a union). A value of 0 obtains provenance for the rdd the transformation was called on
   * and a higher number n will get the provenance for the nth argument of the transformation
   */
  def provenance[S: ClassTag](filter: T => Boolean = (_ => true), dependencyNum: Int = 0): ProvRDD[S] = {
    require(dependencyNum >= 0, "dependencyNum < 0 is not allowed")
    val trackedData = self.dataRDD.filter(filter)
    self.trackedRDD match {
      case rdd: HasProvenance => {
        val content = rdd.provenance(trackedData, dependencyNum).asInstanceOf[RDD[S]]
        val prevTrackedRDD = rdd.provenanceParents(dependencyNum).trackedRDD.asInstanceOf[RDD[S]]
        new ProvRDD(content, prevTrackedRDD)
      }
      case default => {
        throw new UnsupportedOperationException("A " + default.getClass.getSimpleName +
          " does not provide provenance information")
      }
    }
  }

  /**
   * Provide the explanation for the tracked elements. This is similar to provenance,
   * but includes only those elements necessary to reproduce the trackedData.
   *
   * To that end, it can lead to more or less results as provenance.
   */
  def explain[S: ClassTag](filter: T => Boolean = (_ => true), dependencyNum: Int = 0): ProvRDD[S] = {
    require(dependencyNum >= 0, "dependencyNum < 0 is not allowed")
    val trackedData = self.dataRDD.filter(filter)
    self.trackedRDD match {
      case rdd: HasProvenance => {
        val content = rdd.explain(trackedData, dependencyNum).asInstanceOf[RDD[S]]
        val prevTrackedRDD = rdd.provenanceParents(dependencyNum).trackedRDD.asInstanceOf[RDD[S]]
        new ProvRDD(content, prevTrackedRDD)
      }
      case default => {
        throw new UnsupportedOperationException("A " + default.getClass.getSimpleName +
          " does not provide provenance information")
      }
    }
  }

  /**
   * Go back to to a named RDD. In contrast to the provenance function, this will consider all paths in the DAG
   * that led from the seeked RDD to this one. The results of the tracked elements along all paths are merged (union)
   * and returned.
   */
  def goBackTo[S: ClassTag](
    name: String,
    filter: T => Boolean = (_ => true),
    iterate: Boolean = false): ProvRDD[S] = {
    goBackToMultiple(Seq(name), filter, iterate).head._2.asInstanceOf[ProvRDD[S]]
  }

  /**
   * Go back to multiple named RDDs at the same time, without recomputing intermediate results.
   * The returned sequence contains the provenances of each given name in the same order.
   */
  def goBackToMultiple(
    names: Seq[String],
    filter: T => Boolean = (_ => true),
    iterate: Boolean = false): Map[String, ProvRDD[_]] = {
    goBackToMultipleWithFunction(names, (rdd, d) => rdd.provenance(dependencyNum = d), filter, iterate)
  }

  /**
   * Same as goBackTo, but uses explain.
   */
  def goBackAndExplain[S: ClassTag](
    name: String,
    filter: T => Boolean = (_ => true),
    iterate: Boolean = true): ProvRDD[S] = {
    goBackToMultipleAndExplain(Seq(name), filter, iterate).head._2.asInstanceOf[ProvRDD[S]]
  }

  /**
   * Same as goBackToMultiple, but uses explain.
   */
  def goBackToMultipleAndExplain(
    names: Seq[String],
    filter: T => Boolean = (_ => true),
    iterate: Boolean = true): Map[String, ProvRDD[_]] = {
    goBackToMultipleWithFunction(names, (rdd, d) => rdd.explain(dependencyNum = d), filter, iterate)
  }
  
  /**
   * Go back to multiple named RDDs at the same time, without recomputing intermediate results.
   * The returned sequence contains the provenances/explanations of each given name in the same order.
   *
   * func denotes the function used to actually go back. Typically it will be provenance or explain.
   *
   * Iterate denotes whether the iteration algorithm should be used to guarantee reproducibility.
   */
  private def goBackToMultipleWithFunction(
    names: Seq[String],
    func: (ProvRDD[_], Int) => ProvRDD[Any],
    filter: T => Boolean = (_ => true),
    iterate: Boolean = false): Map[String, ProvRDD[_]] = {
    val graph = new RDDGraph(self)
    graph.backwardsTrace(names, func, filter.asInstanceOf[Any => Boolean], iterate)
  }

}