package org.apache.spark.rdd

import scala.reflect.ClassTag
import org.apache.spark.{ Partition, TaskContext }

/**
 * Saves additional provenance information for flatMap.
 *
 * In particular, this is the original mapping function origF, passed to flatMap.
 */
private[spark] class FlatMapInfoRDD[U: ClassTag, T: ClassTag](
  val parent: ProvRDD[T],
  f: (TaskContext, Int, Iterator[T]) => Iterator[U],
  val origF: T => TraversableOnce[U],
  preservesPartitioning: Boolean = false)
    extends MapPartitionsRDD[U, T](parent, f, preservesPartitioning)
    with HasProvenance {

  private[spark] override def provenanceParents = Seq(parent)

  private[spark] override def copyWithParents(parents: Seq[ProvRDD[_]]): RDD[_] =
    new FlatMapInfoRDD(parents(0).asInstanceOf[ProvRDD[T]], f, origF, preservesPartitioning)

  /**
   * Computes provenance for a flatmap transformation. If multiple records in the tracked data are equal,
   * the same number of equal entries will be in the result.
   */
  private[spark] override def provenance(trackedData: RDD[_], dependencyNum: Int): RDD[T] = {
    require(dependencyNum == 0, "flatMap has only a single parent")
    
    // always use the flatMapBased implementation. The cartesian is simply slower.
    flatMapBasedProvenance(trackedData, dependencyNum)
  }

  /**
   * Not used anymore!
   * 
   * Using cartesian to compute the provenance.
   * For t source tuples, a spreading factor sf, and n tracked elements,
   * this has a space complexity of t*sf+t*n, up to t^2*sf+t*sf
   */
  private[spark] def cartesianBasedProvenance(trackedData: RDD[_], dependencyNum: Int): RDD[T] = {
    require(dependencyNum == 0, "flatMap has only a single parent")

    // All tracked elements grouped into a single list
    val trackedGroup = trackedData.asInstanceOf[RDD[U]].map { (1, _) }.groupByKey().values
    // Cartesian of the source data with the list of tracked elements
    val cart = parent.dataRDD.cartesian(trackedGroup)
    // Those source items that produce one or more tracked elements
    cart.filter { case (x, us) => origF(x).exists { x => us.exists { y => x == y } } }.keys
  }
  
  /**
   * Using flatMap to compute the provenance.
   */
  private[spark] def flatMapBasedProvenance(trackedData: RDD[_], dependencyNum: Int): RDD[T] = {
    require(dependencyNum == 0, "flatMap has only a single parent")

    val rddToJoin = trackedData.asInstanceOf[RDD[U]].map(v => (v, null))
    
    // We create an index with source to save memory in the next step
    val zippedSource = parent.dataRDD.zipWithIndex()

    // Then we perform flatMap to create KV pairs with K being the value after flatMap and V being source index
    val mappedOrigTuple = zippedSource.flatMap { case (x, index) => for (t <- origF(x)) yield (t, index) }

    // Next, we cogroup with the tracked data, filter untracked entries , remove the tuple fields we do not need
    val indicesLists = mappedOrigTuple.cogroup(rddToJoin).values.filter(!_._2.isEmpty).keys
    
    // FlatMap to get a list of indices and remove duplicate indices
    val indices = indicesLists.flatMap { x => x }.distinct()
    
    // Now we obtain the original data back from the indices
    indices.map { x => (x, null) }.join(zippedSource.map(_.swap)).values.values
  }

  /**
   * Computes explain for a flatmap transformation. It tries to find as little input values
   * as possible to explain the output values.
   * 
   * XXX still very slow for a high spreading factor
   */
  private[spark] override def explain(trackedData: RDD[_], dependencyNum: Int): RDD[T] = {
    require(dependencyNum == 0, "flatMap has only a single parent")

    // Counting how often each tracked element appears
    val countedTracked = trackedData.asInstanceOf[RDD[U]].map(v => (v, 1)).reduceByKey(_ + _)
    
    // We create an index with source to save memory in the next step
    val zippedSource = parent.dataRDD.zipWithIndex()

    // Here we apply the original flat map function f and built tuples
    // (u, (index, count)) where u is the output value, index is the source index
    // and count is how often u appears in f(t)
    val mappedOrigTuple = zippedSource.flatMap { case (x,index) =>
      // origF gives a TraversableONCE. Copy to a Seq so we can traverse more than once.
      val mapped = origF(x).toSeq
      val mappedSet = mapped.toSet
      for (t <- mappedSet) yield (t, (index, mapped.count(_ == t)))
    }

    // Now we aggregate all these (u, (index, count)) tuples into lists
    val listed = mappedOrigTuple.aggregateByKey(Seq[(Long, Int)]())(_ :+ _, _ ++ _)

    // We join this with the counted tracked. This gives us tuples
    // (u, (list[Index, Count], n)) where u is the output values,
    // list is the above built list of input value indices that create this ouput value and how often,
    // n tells how often u appears in the tracked data
    val joined = listed.join(countedTracked)

    // By iterating through each list, we can take as many t elements that are needed to
    // obtain n copies of u
    val inputsNeededPerOutput = joined.mapValues {
      case (list, needed) =>
        // We sort so that t's producing many u's come first
        val sortedList = list.sortBy(_._2)(Ordering[Int].reverse)
        var acc = 0
        var index = 0
        while (acc < needed) {
          acc += sortedList(index)._2
          index += 1
        }
        sortedList.take(index).map(_._1)
    }

    // Now we forget about the output u and count how often the remaining input indices appear
    // in each list
    val countedInputsNeeded = inputsNeededPerOutput.flatMap {
      case (_, list) =>
        val set = list.toSet
        for (input <- set) yield (input, list.count(_ == input))
    }

    // We compute the maximum appearances m for each index
    val maxInputsNeededIndices = countedInputsNeeded.reduceByKey(Integer.max)
    
    // Now we get obtain the original data back from the indices
    val maxInputsNeeded = zippedSource.map(_.swap).join(maxInputsNeededIndices).values

    // Now we produce m copies of t and we're done
    maxInputsNeeded.flatMap { case (input, count) => for (i <- 1 to count) yield input }
  }

  /**
   * This union implementation keeps duplicates in each set, but not across lists.
   *
   * For example: (a, a, b, c) u (a, b, b, b, d) = (a, a, b, b, b, c, d)
   *
   * Not used anymore!
   */
  private def multiUnion[X](s1: Seq[X], s2: Seq[X]): Seq[X] = {
    val counted1 = s1.foldLeft(Map[X, Int]())((map, elem) => map + ((elem, map.getOrElse(elem, 0) + 1)))
    val counted2 = s2.foldLeft(Map[X, Int]())((map, elem) => map + ((elem, map.getOrElse(elem, 0) + 1)))

    var merged = counted1
    counted2.foreach {
      case (elem, count) =>
        merged = merged + ((elem, Integer.max(count, counted1.getOrElse(elem, 0))))
    }

    merged.flatMap { case (elem, count) => for (i <- 1 to count) yield elem }.toSeq
  }
}